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

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.api.Component;
import com.datatorrent.api.Context;
import com.datatorrent.lib.fileaccess.FileAccess;
import com.datatorrent.netlet.util.Slice;

/**
 * A bucket that groups events.
 */
public interface Bucket extends Component<Context.OperatorContext>
{
  /**
   * @return bucket id
   */
  long getBucketId();

  /**
   *
   * @return size of bucket in memory.
   */
  long getSizeInBytes();

  /**
   * Get value of a key.
   *
   * @param key        key.
   * @param timeBucket time bucket of the key if known; -1 otherwise.
   * @param source     source to read from
   * @return value of the key.
   */
  Slice get(Slice key, long timeBucket, ReadSource source);

  /**
   * Set value of a key.
   *
   * @param key        key.
   * @param timeBucket timeBucket of the key.
   * @param value      value of the key.
   */
  void put(Slice key, long timeBucket, Slice value);

  /**
   * Triggers the bucket to checkpoint. Returns the non checkpointed data so far.
   *
   * @return non checkpointed data.
   */
  Map<Slice, BucketedValue> checkpoint(long windowId);

  /**
   * Triggers the bucket to commit data till provided window id.
   *
   * @param windowId window id
   */
  void committed(long windowId);

  /**
   * Triggers bucket to free memory which is already persisted in bucket data files.
   *
   * @return amount of memory freed in bytes.
   * @throws IOException
   */
  long freeMemory() throws IOException;

  enum ReadSource
  {
    MEMORY,      //state in memory in key/value form
    READERS,     //these are streams in which the key will be searched and serialized.
    ALL          //both the above states.
  }

  class BucketedValue
  {
    private long timeBucket;
    private Slice value;

    protected BucketedValue()
    {
    }

    protected BucketedValue(long timeBucket, Slice value)
    {
      this.timeBucket = timeBucket;
      this.value = value;
    }

    protected long getTimeBucket()
    {
      return timeBucket;
    }

    protected void setTimeBucket(long timeBucket)
    {
      this.timeBucket = timeBucket;
    }

    public Slice getValue()
    {
      return value;
    }

    public void setValue(Slice value)
    {
      this.value = value;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (!(o instanceof BucketedValue)) {
        return false;
      }

      BucketedValue that = (BucketedValue)o;

      return timeBucket == that.timeBucket && value.equals(that.value);

    }

    @Override
    public int hashCode()
    {
      return Objects.hash(timeBucket, value);
    }
  }

  /**
   * Default bucket.<br/>
   * Not thread-safe.
   */
  class DefaultBucket implements Bucket
  {
    private final long bucketId;

    //Key -> Ordered values
    private Map<Slice, BucketedValue> flash = Maps.newHashMap();

    //Data persisted in write ahead logs. window -> bucket
    private final transient TreeMap<Long, Map<Slice, BucketedValue>> checkpointedData = Maps.newTreeMap();

    //Data persisted in bucket data files
    private final transient Map<Slice, BucketedValue> committedData = Maps.newHashMap();

    //Data serialized/deserialized from bucket data files
    private final transient Map<Slice, BucketedValue> readCache = Maps.newConcurrentMap();

    //TimeBucket -> FileReaders
    private final transient Map<Long, FileAccess.FileReader> readers = Maps.newTreeMap();

    private final transient ManagedStateContext managedStateContext;

    private AtomicLong sizeInBytes = new AtomicLong(0);

    private final transient Slice dummyGetKey = new Slice(null, 0, 0);

    private transient TreeSet<BucketsMetaDataManager.ImmutableTimeBucketMeta> cachedBucketMetas;

    private DefaultBucket()
    {
      //for kryo
      bucketId = -1;
      managedStateContext = null;
    }

    protected DefaultBucket(long bucketId, @NotNull ManagedStateContext managedStateContext)
    {
      this.bucketId = bucketId;
      this.managedStateContext = Preconditions.checkNotNull(managedStateContext);
    }

    @Override
    public void setup(Context.OperatorContext context)
    {
    }

    @Override
    public long getBucketId()
    {
      return bucketId;
    }

    @Override
    public long getSizeInBytes()
    {
      return sizeInBytes.longValue();
    }

    private Slice getFromMemory(Slice key)
    {
      //search the cache for key
      BucketedValue bucketedValue = flash.get(key);
      if (bucketedValue != null) {
        return bucketedValue.getValue();
      }

      for (Long window : checkpointedData.descendingKeySet()) {
        //traversing the checkpointed data in reverse order
        bucketedValue = checkpointedData.get(window).get(key);
        if (bucketedValue != null) {
          return bucketedValue.getValue();
        }
      }

      bucketedValue = committedData.get(key);
      if (bucketedValue != null) {
        return bucketedValue.getValue();
      }

      bucketedValue = readCache.get(key);
      if (bucketedValue != null) {
        return bucketedValue.getValue();
      }
      return null;
    }

    private Slice getFromReaders(Slice key, long timeBucket)
    {
      if (timeBucket != -1) {
        Slice valSlice = getKeyFromTimeBucketReader(key, timeBucket);
        if (valSlice != null) {
          BucketedValue bucketedValue = new BucketedValue(timeBucket, valSlice);
          readCache.put(key, bucketedValue);
        }
        return valSlice;
      } else {
        //search all the time buckets
        try {
          if (cachedBucketMetas == null) {
            cachedBucketMetas = managedStateContext.getBucketsMetaDataManager().getAllTimeBuckets(bucketId);
          }

          for (BucketsMetaDataManager.ImmutableTimeBucketMeta immutableTimeBucketMeta : cachedBucketMetas) {

            if (managedStateContext.getKeyComparator().compare(key, immutableTimeBucketMeta.getFirstKey()) >= 0) {
              //keys in the time bucket files are sorted so if the first key in the file is greater than the key being
              //searched, the key will not be present in that file.
              Slice valSlice = getKeyFromTimeBucketReader(key, immutableTimeBucketMeta.getTimeBucketId());
              if (valSlice != null) {
                BucketedValue bucketedValue = new BucketedValue(immutableTimeBucketMeta.getTimeBucketId(), valSlice);
                readCache.put(key, bucketedValue);
                return valSlice;
              }
            }
          }

        } catch (IOException e) {
          throw new RuntimeException("get time-buckets " + bucketId, e);
        }
      }
      return null;
    }

    @Override
    public Slice get(Slice key, long timeBucket, ReadSource readSource)
    {
      switch (readSource) {
        case MEMORY:
          return getFromMemory(key);
        case READERS:
          return getFromReaders(key, timeBucket);
        case ALL:
        default:
          Slice value = getFromMemory(key);
          if (value != null) {
            return value;
          }
          return getFromReaders(key, timeBucket);
      }
    }

    /**
     * Returns the value for the key from a time-bucket reader
     * @param key        key
     * @param timeBucket time bucket
     * @return value if key is found in the time bucket; false otherwise
     */
    private Slice getKeyFromTimeBucketReader(Slice key, long timeBucket)
    {
      FileAccess.FileReader fileReader = readers.get(timeBucket);
      if (fileReader != null) {
        return readKey(fileReader, key, timeBucket);
      }
      //file reader is not loaded and is null
      try {
        if (loadFileReader(timeBucket)) {
          return readKey(readers.get(timeBucket), key, timeBucket);
        }
        return null;
      } catch (IOException e) {
        throw new RuntimeException("while loading " + bucketId + ", " + timeBucket, e);
      }
    }

    private Slice readKey(FileAccess.FileReader fileReader, Slice key, long timeBucket)
    {
      Slice valSlice = new Slice(null, 0, 0);
      try {
        if (fileReader.seek(key)) {
          fileReader.next(dummyGetKey, valSlice);
          return valSlice;
        } else {
          return null;
        }
      } catch (IOException e) {
        throw new RuntimeException("reading " + bucketId + ", " + timeBucket, e);
      }
    }

    private boolean loadFileReader(long timeBucketId) throws IOException
    {
      BucketsMetaDataManager.ImmutableTimeBucketMeta tbm = managedStateContext.getBucketsMetaDataManager()
          .getTimeBucketMeta(bucketId, timeBucketId);

      if (tbm != null) {
        FileAccess.FileReader reader = managedStateContext.getFileAccess().getReader(bucketId,
            BucketsDataManager.getFileName(timeBucketId));
        readers.put(timeBucketId, reader);
        sizeInBytes.getAndAdd(tbm.getSizeInBytes());
        return true;
      }
      return false;
    }

    @Override
    public void put(Slice key, long timeBucket, Slice value)
    {
      BucketedValue bucketedValue = flash.get(key);
      if (bucketedValue == null) {
        bucketedValue = new BucketedValue();
        flash.put(key, bucketedValue);
        sizeInBytes.getAndAdd(key.length);
        sizeInBytes.getAndAdd(Long.SIZE);
      }
      if (timeBucket > bucketedValue.getTimeBucket()) {

        if (bucketedValue.getValue() != null) {
          //overriding the value so need to subtract the size of previous value
          sizeInBytes.getAndAdd(-bucketedValue.getValue().length);
        }

        sizeInBytes.getAndAdd(value.length);
        bucketedValue.setTimeBucket(timeBucket);
        bucketedValue.setValue(value);
      }
    }

    @Override
    public long freeMemory() throws IOException
    {
      long memoryFreed = 0;
      for (Map.Entry<Slice, BucketedValue> entry : committedData.entrySet()) {
        memoryFreed += entry.getKey().length + entry.getValue().getValue().length;
      }
      committedData.clear();
      readCache.clear();
      if (cachedBucketMetas != null) {

        for (BucketsMetaDataManager.ImmutableTimeBucketMeta tbm : cachedBucketMetas) {
          FileAccess.FileReader reader = readers.remove(tbm.getTimeBucketId());
          if (reader != null) {
            memoryFreed += tbm.getSizeInBytes();
            reader.close();
          }
        }

      }
      sizeInBytes.getAndAdd(-memoryFreed);
      return memoryFreed;
    }

    @Override
    public Map<Slice, BucketedValue> checkpoint(long windowId)
    {
      try {
        return flash;
      } finally {
        checkpointedData.put(windowId, flash);
        flash = Maps.newHashMap();
      }
    }

    @Override
    public void committed(long windowId)
    {
      Iterator<Map.Entry<Long, Map<Slice, BucketedValue>>> stateIterator = checkpointedData.entrySet().iterator();

      while (stateIterator.hasNext()) {
        Map.Entry<Long, Map<Slice, BucketedValue>> entry = stateIterator.next();

        long savedWindow = entry.getKey();
        if (savedWindow <= windowId) {
          Map<Slice, BucketedValue> bucketData = entry.getValue();

          for (BucketedValue bucketedValue : bucketData.values()) {
            FileAccess.FileReader reader = readers.get(bucketedValue.getTimeBucket());
            if (reader != null) {
              //closing the file reader for the time bucket if it is in memory because the time-bucket is modified
              //so it will be re-written by BucketsDataManager
              try {
                LOG.debug("closing reader {} {}", bucketId, bucketedValue.getTimeBucket());
                reader.close();
              } catch (IOException e) {
                throw new RuntimeException("closing reader " + bucketId + ", " + bucketedValue.getTimeBucket(), e);
              }
              readers.remove(bucketedValue.getTimeBucket());
            }
            if (readers.isEmpty()) {
              break;
            }
          }
          committedData.putAll(bucketData);
          stateIterator.remove();
        } else {
          break;
        }
      }
      cachedBucketMetas = null;
    }

    @Override
    public void teardown()
    {
      Set<Long> failureBuckets = Sets.newHashSet();
      for (Map.Entry<Long, FileAccess.FileReader> entry : readers.entrySet()) {
        try {
          LOG.debug("closing reader {} {}", bucketId, entry.getKey());
          entry.getValue().close();
        } catch (IOException e) {
          //will try to close all readers
          failureBuckets.add(entry.getKey());
        }
      }
      if (!failureBuckets.isEmpty()) {
        StringBuilder builder = new StringBuilder("teardown of ");
        builder.append(bucketId).append(" < ");
        for (Long timeBucket : failureBuckets) {
          builder.append(timeBucket);
        }
        builder.append(">");
        throw new RuntimeException(builder.toString());
      }
    }

    @VisibleForTesting
    Map<Long, FileAccess.FileReader> getReaders()
    {
      return readers;
    }

    private static final Logger LOG = LoggerFactory.getLogger(DefaultBucket.class);
  }
}
