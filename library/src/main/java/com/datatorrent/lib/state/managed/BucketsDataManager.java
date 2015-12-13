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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Queues;
import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;

import com.datatorrent.api.Context;
import com.datatorrent.common.util.NameableThreadFactory;
import com.datatorrent.lib.fileaccess.FileAccess;
import com.datatorrent.lib.util.WindowDataManager;
import com.datatorrent.netlet.util.DTThrowable;
import com.datatorrent.netlet.util.Slice;

/**
 * Manages state which is written to files by windows. The state from the window files are then transferred to bucket
 * data files. This class listens to time expiry events issued by {@link TimeBucketAssigner}.
 *
 * This component is also responsible for purging old time buckets.
 */
public class BucketsDataManager extends WindowDataManager.FSWindowDataManager implements TimeBucketAssigner.Listener
{
  private static final String WAL_RELATIVE_PATH = "managed_state";

  //windowId => (bucketId => data)
  private final transient Map<Long, Map<Long, Map<Slice, Bucket.BucketedValue>>> savedWindows = new
      ConcurrentSkipListMap<>();

  private final transient ExecutorService writerService = Executors.newSingleThreadExecutor(
      new NameableThreadFactory("managed-state-writer"));
  private transient boolean transfer;

  private final transient LinkedBlockingQueue<Long> windowsToTransfer = Queues.newLinkedBlockingQueue();
  private final transient AtomicReference<Throwable> throwable = new AtomicReference<>();

  private final transient ManagedStateContext managedStateContext;

  private final transient AtomicLong purgeTime = new AtomicLong();

  private BucketsDataManager()
  {
    //for kryo
    managedStateContext = null;
  }

  public BucketsDataManager(@NotNull ManagedStateContext managedStateContext)
  {
    this.managedStateContext = Preconditions.checkNotNull(managedStateContext, "context provider");
    setRecoveryPath(WAL_RELATIVE_PATH);
  }

  protected static String getFileName(long timeBucketId)
  {
    return Long.toString(timeBucketId);
  }

  protected static String getTmpFileName()
  {
    return System.currentTimeMillis() + ".tmp";
  }

  private transient Context.OperatorContext context;
  private transient int waitMillis;

  @Override
  public void setup(final Context.OperatorContext context)
  {
    this.context = context;
    waitMillis = context.getValue(Context.OperatorContext.SPIN_MILLIS);
    super.setup(context);

    writerService.submit(new Runnable()
    {
      @Override
      public void run()
      {
        while (transfer) {
          transferWindowFiles();
          if (purgeTime.get() > 0) {
            deleteFilesBefore(purgeTime.get());
          }
        }
      }
    });
    transfer = true;
  }

  /**
   * Loads all the data in window files.
   *
   * @param operatorId operator id
   * @return data per bucket in window files.
   */
  @Nullable
  public Map<Long, Map<Slice, Bucket.BucketedValue>> load(int operatorId) throws IOException
  {
    Map<Long, Map<Slice, Bucket.BucketedValue>> dataPerBucket = Maps.newHashMap();

    long[] allWindows = getWindowIds(operatorId);
    if (allWindows == null) {
      return null;
    }
    Arrays.sort(allWindows);

    for (long windowId : allWindows) {
      @SuppressWarnings("unchecked")
      Map<Long, Map<Slice, Bucket.BucketedValue>> recoveredData = (Map<Long, Map<Slice, Bucket.BucketedValue>>)load(
          operatorId, windowId);

      for (Map.Entry<Long, Map<Slice, Bucket.BucketedValue>> entry : recoveredData.entrySet()) {
        Map<Slice, Bucket.BucketedValue> target = dataPerBucket.get(entry.getKey());

        if (target == null) {
          target = Maps.newHashMap();
          dataPerBucket.put(entry.getKey(), target);
        }
        target.putAll(entry.getValue());
      }
    }

    return dataPerBucket;
  }

  protected void transferWindowFiles()
  {
    try {
      Long windowId = windowsToTransfer.poll();
      if (windowId != null) {
        try {
          LOG.debug("transfer window {}", windowId);
          Map<Long, Map<Slice, Bucket.BucketedValue>> buckets = savedWindows.remove(windowId);

          for (Map.Entry<Long, Map<Slice, Bucket.BucketedValue>> singleBucket : buckets.entrySet()) {

            transferBucket(windowId, singleBucket.getKey(), singleBucket.getValue());
            managedStateContext.getBucketsMetaDataManager().updateBucketMetaFile(singleBucket.getKey());
          }
          storageAgent.delete(context.getId(), windowId);
        } catch (IOException ex) {
          throwable.set(ex);
          LOG.debug("transfer window {}", windowId, ex);
          throw new RuntimeException("transfer window " + windowId, ex);
        }
      } else {
        Thread.sleep(waitMillis);
      }
    } catch (InterruptedException ex) {
      throwable.set(ex);
      LOG.debug("interrupted", ex);
      throw new RuntimeException(ex);
    }
  }

  protected void transferBucket(long windowId, long bucketId, Map<Slice, Bucket.BucketedValue> data) throws IOException
  {
    Table<Long, Slice, Bucket.BucketedValue> timeBucketedKeys = TreeBasedTable.create(Ordering.<Long>natural(),
        managedStateContext.getKeyComparator());

    for (Map.Entry<Slice, Bucket.BucketedValue> entry : data.entrySet()) {
      long timeBucketId = entry.getValue().getTimeBucket();
      timeBucketedKeys.put(timeBucketId, entry.getKey(), entry.getValue());
    }

    for (long timeBucket : timeBucketedKeys.rowKeySet()) {
      BucketsMetaDataManager.MutableTimeBucketMeta tbm =
          managedStateContext.getBucketsMetaDataManager().getOrCreateTimeBucketMeta(bucketId, timeBucket);

      long dataSize = 0;
      Slice firstKey = null;

      FileAccess.FileWriter fileWriter;
      String tmpFileName = getTmpFileName();
      if (tbm.getLastTransferredWindowId() == -1) {
        //A new time bucket so we append all the key/values to the new file
        fileWriter = managedStateContext.getFileAccess().getWriter(bucketId, tmpFileName);

        for (Map.Entry<Slice, Bucket.BucketedValue> entry : timeBucketedKeys.row(timeBucket).entrySet()) {
          Slice key = entry.getKey();
          Slice value = entry.getValue().getValue();

          dataSize += key.length;
          dataSize += value.length;

          fileWriter.append(key.toByteArray(), value.toByteArray());
          if (firstKey == null) {
            firstKey = key;
          }
        }
      } else {
        //the time bucket existed so we need to read the file and then re-write it
        TreeMap<Slice, Slice> fileData = new TreeMap<>(managedStateContext.getKeyComparator());
        FileAccess.FileReader fileReader = managedStateContext.getFileAccess().getReader(bucketId,
            getFileName(timeBucket));
        fileReader.readFully(fileData);
        fileReader.close();

        for (Map.Entry<Slice, Bucket.BucketedValue> entry : timeBucketedKeys.row(timeBucket).entrySet()) {
          fileData.put(entry.getKey(), entry.getValue().getValue());
        }

        fileWriter = managedStateContext.getFileAccess().getWriter(bucketId, tmpFileName);
        for (Map.Entry<Slice, Slice> entry : fileData.entrySet()) {
          Slice key = entry.getKey();
          Slice value = entry.getValue();

          dataSize += key.length;
          dataSize += value.length;

          fileWriter.append(key.toByteArray(), value.toByteArray());
          if (firstKey == null) {
            firstKey = key;
          }
        }
      }
      fileWriter.close();
      managedStateContext.getFileAccess().rename(bucketId, tmpFileName, getFileName(timeBucket));
      tbm.setLastTransferredWindowId(windowId);
      tbm.setSizeInBytes(dataSize);
      tbm.setFirstKey(firstKey);
    }
  }

  protected void deleteFilesBefore(long time)
  {
    LOG.debug("delete files before {}", time);
    try {
      List<Long> buckets = getBucketsOnFileSystem();
      if (buckets == null) {
        return;
      }
      for (Long bucketId : buckets) {
        RemoteIterator<LocatedFileStatus> timeBucketsIterator = managedStateContext.getFileAccess().listFiles(
            bucketId);
        boolean emptyBucket = true;
        while (timeBucketsIterator.hasNext()) {
          LocatedFileStatus timeBucketStatus = timeBucketsIterator.next();

          String timeBucketStr = timeBucketStatus.getPath().getName();
          if (timeBucketStr.equals(BucketsMetaDataManager.META_FILE_NAME) || timeBucketStr.endsWith(".tmp")) {
            //ignoring meta and tmp files
            continue;
          }
          long timeBucket = Long.parseLong(timeBucketStr);

          long startTime = managedStateContext.getTimeBucketAssigner().getStartTimeFor(timeBucket);
          if (startTime < time) {
            LOG.debug("deleting bucket {} time-bucket {}", timeBucket);
            managedStateContext.getFileAccess().delete(bucketId, timeBucketStatus.getPath().getName());
            managedStateContext.getBucketsMetaDataManager().invalidateTimeBucket(bucketId, timeBucket);
          } else {
            emptyBucket = false;
          }
        }
        if (emptyBucket) {
          LOG.debug("deleting bucket {}", bucketId);
          managedStateContext.getFileAccess().deleteBucket(bucketId);
        }
      }
    } catch (Throwable e) {
      throwable.set(e);
      LOG.debug("delete files", e);
      throw DTThrowable.wrapIfChecked(e);
    }
  }

  private List<Long> getBucketsOnFileSystem() throws IOException
  {
    RemoteIterator<LocatedFileStatus> iterator = managedStateContext.getFileAccess().listFiles();
    if (iterator == null) {
      return null;
    }
    List<Long> buckets = Lists.newArrayList();
    while (iterator.hasNext()) {
      LocatedFileStatus fileStatus = iterator.next();
      try {
        buckets.add(Long.parseLong(fileStatus.getPath().getName()));
      } catch (NumberFormatException nfe) {
        //ignoring any other files other than bucket keys.
      }
    }
    return buckets;
  }

  @Override
  public void save(Object object, int operatorId, long windowId) throws IOException
  {
    throw new UnsupportedOperationException("doesn't support saving any object");
  }

  /**
   * The unsaved state combines data received in multiple windows. This window data manager persists this data
   * on disk by the window id in which it was requested.
   * @param unsavedData   un-saved data of all buckets.
   * @param operatorId    operator id.
   * @param windowId      window id.
   * @throws IOException
   */
  public void save(Map<Long, Map<Slice, Bucket.BucketedValue>> unsavedData, int operatorId, long windowId)
      throws IOException
  {
    Throwable lthrowable;
    if ((lthrowable = throwable.get()) != null) {
      LOG.error("Error while transferring");
      throw DTThrowable.wrapIfChecked(lthrowable);
    }
    savedWindows.put(windowId, unsavedData);
    super.save(unsavedData, operatorId, windowId);
  }

  /**
   * Transfers the data which has been committed till windowId to data files.
   *
   * @param operatorId operator id
   * @param windowId   window id
   */
  @SuppressWarnings("UnusedParameters")
  protected void committed(int operatorId, long windowId) throws IOException, InterruptedException
  {
    LOG.debug("data manager committed {}", windowId);
    for (Long currentWindow : savedWindows.keySet()) {
      if (currentWindow <= windowId) {
        LOG.debug("to transfer {}", windowId);
        windowsToTransfer.add(currentWindow);
      } else {
        break;
      }
    }
  }

  @Override
  public void teardown()
  {
    super.teardown();
    transfer = false;
    writerService.shutdownNow();
  }

  @Override
  public void purgeTimeBucketsBefore(long time)
  {
    purgeTime.getAndSet(time);
  }

  private static final Logger LOG = LoggerFactory.getLogger(BucketsDataManager.class);

}
