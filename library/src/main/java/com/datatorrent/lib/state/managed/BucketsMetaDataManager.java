/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.lib.state.managed;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeSet;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeBasedTable;

import com.datatorrent.netlet.util.Slice;

/**
 * Each bucket has a meta-data file persisted on disk.<br/>
 * The format of a bucket meta-data file is:
 * <ol>
 * <li>total number of time-buckets (int)</li>
 * <li>For each time bucket
 * <ol>
 * <li>time bucket key (long)</li>
 * <li>size of data (sum of bytes) (long)</li>
 * <li>last transferred window id (long)</li>
 * <li>length of the first key in the time-bucket file (int)</li>
 * <li>first key in the time-bucket file (byte[])</li>
 * </ol>
 * </li>
 * </ol>
 * <p/>
 * This class manages the meta-data for each bucket. Meta data information is updated by {@link BucketsDataManager}.
 * Any updates are restricted to the package.
 */
public class BucketsMetaDataManager
{
  public static final String META_FILE_NAME = "_META";

  private final transient TreeBasedTable<Long, Long, MutableTimeBucketMeta> timeBucketsMeta = TreeBasedTable.create();

  private final transient ManagedStateContext managedStateContext;

  private BucketsMetaDataManager()
  {
    //for kryo
    managedStateContext = null;
  }

  public BucketsMetaDataManager(@NotNull ManagedStateContext managedStateContext)
  {
    this.managedStateContext = Preconditions.checkNotNull(managedStateContext, "context provider");
  }

  /**
   * Retrieves the time bucket meta of a particular time-bucket. If the time bucket doesn't exist then a new one
   * is created.
   *
   * @param bucketId     bucket id
   * @param timeBucketId time bucket id
   * @return time bucket meta of the time bucket
   * @throws IOException
   */
  @NotNull
  MutableTimeBucketMeta getOrCreateTimeBucketMeta(long bucketId, long timeBucketId) throws IOException
  {
    synchronized (timeBucketsMeta) {
      MutableTimeBucketMeta tbm = timeBucketMetaHelper(bucketId, timeBucketId);
      if (tbm == null) {
        tbm = new MutableTimeBucketMeta(bucketId, timeBucketId);
        timeBucketsMeta.put(bucketId, timeBucketId, tbm);
      }
      return tbm;
    }
  }

  /**
   * Returns the time bucket meta of a particular time-bucket which is immutable.
   *
   * @param bucketId     bucket id
   * @param timeBucketId time bucket id
   * @return immutable time bucket meta
   * @throws IOException
   */
  @Nullable
  public ImmutableTimeBucketMeta getTimeBucketMeta(long bucketId, long timeBucketId) throws IOException
  {
    synchronized (timeBucketsMeta) {
      MutableTimeBucketMeta tbm = timeBucketMetaHelper(bucketId, timeBucketId);
      if (tbm != null) {
        return tbm.getImmutableTimeBucketMeta();
      }
      return null;
    }
  }

  private MutableTimeBucketMeta timeBucketMetaHelper(long bucketId, long timeBucketId) throws IOException
  {
    MutableTimeBucketMeta tbm = timeBucketsMeta.get(bucketId, timeBucketId);
    if (tbm != null) {
      return tbm;
    }
    if (managedStateContext.getFileAccess().exists(bucketId, META_FILE_NAME)) {
      try (DataInputStream dis = managedStateContext.getFileAccess().getInputStream(bucketId, META_FILE_NAME)) {
        //Load meta info of all the time buckets of the bucket identified by bucketId.
        loadBucketMetaFile(bucketId, dis);
      }
    } else {
      return null;
    }
    return timeBucketsMeta.get(bucketId, timeBucketId);
  }

  /**
   * Returns the meta information of all the time buckets in the bucket in descending order - latest to oldest.
   *
   * @param bucketId bucket id
   * @return all the time buckets in order - latest to oldest
   */
  public TreeSet<ImmutableTimeBucketMeta> getAllTimeBuckets(long bucketId) throws IOException
  {
    synchronized (timeBucketsMeta) {
      TreeSet<ImmutableTimeBucketMeta> immutableTimeBucketMetas = Sets.newTreeSet(
          Collections.<ImmutableTimeBucketMeta>reverseOrder());

      if (timeBucketsMeta.containsRow(bucketId)) {
        for (Map.Entry<Long, MutableTimeBucketMeta> entry : timeBucketsMeta.row(bucketId).entrySet()) {
          immutableTimeBucketMetas.add(entry.getValue().getImmutableTimeBucketMeta());
        }
        return immutableTimeBucketMetas;
      }
      if (managedStateContext.getFileAccess().exists(bucketId, META_FILE_NAME)) {
        try (DataInputStream dis = managedStateContext.getFileAccess().getInputStream(bucketId, META_FILE_NAME)) {
          //Load meta info of all the time buckets of the bucket identified by bucket id
          loadBucketMetaFile(bucketId, dis);
          for (Map.Entry<Long, MutableTimeBucketMeta> entry : timeBucketsMeta.row(bucketId).entrySet()) {
            immutableTimeBucketMetas.add(entry.getValue().getImmutableTimeBucketMeta());
          }
          return immutableTimeBucketMetas;
        }
      } else {
        return null;
      }
    }
  }

  /**
   * Loads the bucket meta-file.
   *
   * @param bucketId bucket id
   * @param dis      data input stream
   * @throws IOException
   */
  private void loadBucketMetaFile(long bucketId, DataInputStream dis) throws IOException
  {
    int numberOfEntries = dis.readInt();

    for (int i = 0; i < numberOfEntries; i++) {
      long timeBucketId = dis.readLong();
      long dataSize = dis.readLong();
      long lastTransferredWindow = dis.readLong();

      MutableTimeBucketMeta tbm = new MutableTimeBucketMeta(bucketId, timeBucketId);
      tbm.setSizeInBytes(dataSize);
      tbm.setLastTransferredWindowId(lastTransferredWindow);

      int sizeOfFirstKey = dis.readInt();
      byte[] firstKeyBytes = new byte[sizeOfFirstKey];
      dis.readFully(firstKeyBytes, 0, firstKeyBytes.length);
      tbm.setFirstKey(new Slice(firstKeyBytes));

      timeBucketsMeta.put(bucketId, timeBucketId, tbm);
    }
  }

  /**
   * Saves the updated bucket meta on disk.
   *
   * @param bucketId bucket id
   * @throws IOException
   */
  void updateBucketMetaFile(long bucketId) throws IOException
  {
    Map<Long, MutableTimeBucketMeta> timeBuckets;
    synchronized (timeBucketsMeta) {
      timeBuckets = timeBucketsMeta.row(bucketId);
    }
    Preconditions.checkNotNull(timeBuckets, "timeBuckets");
    String tmpFileName = BucketsDataManager.getTmpFileName();

    try (DataOutputStream dos = managedStateContext.getFileAccess().getOutputStream(bucketId, tmpFileName)) {
      dos.writeInt(timeBuckets.size());
      for (Map.Entry<Long, MutableTimeBucketMeta> entry : timeBuckets.entrySet()) {
        MutableTimeBucketMeta tbm = entry.getValue();
        dos.writeLong(tbm.getTimeBucketId());
        dos.writeLong(tbm.getSizeInBytes());
        dos.writeLong(tbm.getLastTransferredWindowId());
        dos.writeInt(tbm.getFirstKey().length);
        dos.write(tbm.getFirstKey().toByteArray());
      }

    }
    managedStateContext.getFileAccess().rename(bucketId, tmpFileName, META_FILE_NAME);
  }

  void invalidateTimeBucket(long bucketId, long timeBucketId) throws IOException
  {
    synchronized (timeBucketsMeta) {
      timeBucketsMeta.remove(bucketId, timeBucketId);
    }
    updateBucketMetaFile(bucketId);
  }

  private static class TimeBucketMeta implements Comparable<TimeBucketMeta>
  {
    protected final long bucketId;
    protected final long timeBucketId;
    protected long lastTransferredWindowId = -1;
    protected long sizeInBytes;
    protected Slice firstKey;

    private TimeBucketMeta()
    {
      //for kryo
      bucketId = -1;
      timeBucketId = -1;
    }

    private TimeBucketMeta(long bucketId, long timeBucketId)
    {
      this.bucketId = bucketId;
      this.timeBucketId = timeBucketId;
    }

    public long getLastTransferredWindowId()
    {
      return lastTransferredWindowId;
    }

    public long getSizeInBytes()
    {
      return this.sizeInBytes;
    }

    public long getBucketId()
    {
      return bucketId;
    }

    public long getTimeBucketId()
    {
      return timeBucketId;
    }

    public Slice getFirstKey()
    {
      return firstKey;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (!(o instanceof TimeBucketMeta)) {
        return false;
      }

      TimeBucketMeta that = (TimeBucketMeta)o;

      if (bucketId != that.bucketId) {
        return false;
      }
      if (timeBucketId != that.timeBucketId) {
        return false;
      }
      if (lastTransferredWindowId != that.lastTransferredWindowId) {
        return false;
      }
      return sizeInBytes == that.sizeInBytes && firstKey.equals(that.firstKey);

    }

    @Override
    public int hashCode()
    {
      int result = (int)(bucketId ^ (bucketId >>> 32));
      result = 31 * result + (int)(timeBucketId ^ (timeBucketId >>> 32));
      result = 31 * result + (int)(lastTransferredWindowId ^ (lastTransferredWindowId >>> 32));
      result = 31 * result + (int)(sizeInBytes ^ (sizeInBytes >>> 32));
      result = 31 * result + firstKey.hashCode();
      return result;
    }

    @Override
    public int compareTo(@NotNull TimeBucketMeta o)
    {
      if (bucketId < o.bucketId) {
        return -1;
      }
      if (bucketId > o.bucketId) {
        return 1;
      }
      if (timeBucketId < o.timeBucketId) {
        return -1;
      }
      if (timeBucketId > o.timeBucketId) {
        return 1;
      }
      return 0;
    }
  }

  /**
   * Represents time bucket meta information.
   */
  static class MutableTimeBucketMeta extends TimeBucketMeta
  {
    private transient ImmutableTimeBucketMeta immutableTimeBucketMeta;

    private volatile boolean changed;

    MutableTimeBucketMeta()
    {
    }

    public MutableTimeBucketMeta(long bucketId, long timeBucketId)
    {
      super(bucketId, timeBucketId);
    }

    synchronized void setLastTransferredWindowId(long lastTransferredWindowId)
    {
      changed = true;
      this.lastTransferredWindowId = lastTransferredWindowId;
    }

    synchronized void setSizeInBytes(long bytes)
    {
      changed = true;
      this.sizeInBytes = bytes;
    }

    synchronized void setFirstKey(@NotNull Slice firstKey)
    {
      changed = true;
      this.firstKey = Preconditions.checkNotNull(firstKey, "first key");
    }

    synchronized ImmutableTimeBucketMeta getImmutableTimeBucketMeta()
    {
      if (immutableTimeBucketMeta == null || changed) {
        immutableTimeBucketMeta = new ImmutableTimeBucketMeta(getBucketId(), getTimeBucketId(),
            getLastTransferredWindowId(), getSizeInBytes(), getFirstKey());
        changed = false;
      }
      return immutableTimeBucketMeta;
    }

  }

  public static final class ImmutableTimeBucketMeta extends TimeBucketMeta
  {
    private ImmutableTimeBucketMeta()
    {
      super();
    }

    private ImmutableTimeBucketMeta(long bucketId, long timeBucketId, long lastTransferredWindowId, long sizeInBytes,
        Slice firstKey)
    {
      super(bucketId, timeBucketId);
      this.lastTransferredWindowId = lastTransferredWindowId;
      this.sizeInBytes = sizeInBytes;
      this.firstKey = firstKey;
    }
  }
}
