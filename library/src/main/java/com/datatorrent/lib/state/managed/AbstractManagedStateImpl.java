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
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;

import com.datatorrent.api.Component;
import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.NameableThreadFactory;
import com.datatorrent.lib.fileaccess.FileAccess;
import com.datatorrent.lib.fileaccess.TFileImpl;
import com.datatorrent.lib.util.comparator.SliceComparator;
import com.datatorrent.netlet.util.DTThrowable;
import com.datatorrent.netlet.util.Slice;

/**
 * An abstract implementation of managed state.<br/>
 *
 * The important sub-components here are:
 * <ol>
 *   <li>
 *     {@link #dataManager}: writes incremental checkpoints in window files and transfers data from window
 *     files to bucket files.
 *   </li>
 *   <li>
 *     {@link #bucketsMetaDataManager}: a bucket on disk is sub-divided into time-buckets. This manages meta-bucket
 *     information (list of {@link com.datatorrent.lib.state.managed.BucketsMetaDataManager.TimeBucketMeta}) per bucket.
 *   </li>
 *   <li>
 *     {@link #timeBucketAssigner}: assigns time-buckets to keys and manages the time boundaries.
 *   </li>
 *   <li>
 *     {@link #stateTracker}: tracks the size of data in memory and requests buckets to free memory when enough memory
 *     is not available.
 *   </li>
 *   <li>
 *     {@link #fileAccess}: pluggable file system abstraction.
 *   </li>
 * </ol>
 *
 * The implementations of put, getSync and getAsync here use windowId as the time field to derive timeBucket of a key.
 */
public abstract class AbstractManagedStateImpl
    implements ManagedState, Component<OperatorContext>, Operator.CheckpointNotificationListener, ManagedStateContext
{
  private long maxMemorySize;

  protected int numBuckets;

  private int incrementalCheckpointWindowCount = DAGContext.CHECKPOINT_WINDOW_COUNT.defaultValue;

  @NotNull
  private FileAccess fileAccess = new TFileImpl.DTFileImpl();
  @NotNull
  protected TimeBucketAssigner timeBucketAssigner = new TimeBucketAssigner();

  protected Bucket[] buckets;

  @Min(1)
  private int numReaders = 1;
  @NotNull
  protected transient ExecutorService readerService;

  @NotNull
  private BucketsDataManager dataManager = new BucketsDataManager(this);

  private final BucketsMetaDataManager bucketsMetaDataManager = new BucketsMetaDataManager(this);

  private transient int operatorId;
  private transient long windowId;

  private transient int windowCount;

  private transient long largestRecoveryWindow;
  protected transient boolean replay;

  @NotNull
  protected Comparator<Slice> keyComparator = new SliceComparator();

  protected final transient AtomicReference<Throwable> throwable = new AtomicReference<>();

  @NotNull
  @FieldSerializer.Bind(JavaSerializer.class)
  private Duration checkStateSizeInterval = Duration.millis(
      DAGContext.STREAMING_WINDOW_SIZE_MILLIS.defaultValue * OperatorContext.APPLICATION_WINDOW_COUNT.defaultValue);

  @FieldSerializer.Bind(JavaSerializer.class)
  private Duration durationPreventingFreeingSpace;

  private transient StateTracker stateTracker = new StateTracker(this);

  //accessible to StateTracker
  final transient Object commitLock = new Object();

  @Override
  public void setup(OperatorContext context)
  {
    operatorId = context.getId();
    fileAccess.init();
    timeBucketAssigner.setListener(dataManager);
    timeBucketAssigner.setup(context);

    numBuckets = getNumBuckets();
    buckets = new Bucket[numBuckets];

    Preconditions.checkArgument(numReaders <= numBuckets, "no. of readers cannot exceed no. of buckets");
    //setup state data manager
    dataManager.setup(context);

    //recovering data for window files to bucket
    try {
      Map<Long, Map<Slice, Bucket.BucketedValue>> recovered = dataManager.load(operatorId);
      if (recovered != null && !recovered.isEmpty()) {

        for (Map.Entry<Long, Map<Slice, Bucket.BucketedValue>> entry : recovered.entrySet()) {
          int bucketIdx = prepareBucket(entry.getKey());

          for (Map.Entry<Slice, Bucket.BucketedValue> dataEntry : entry.getValue().entrySet()) {
            buckets[bucketIdx].put(dataEntry.getKey(), dataEntry.getValue().getTimeBucket(),
                dataEntry.getValue().getValue());
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("recovering", e);
    }

    largestRecoveryWindow = dataManager.getLargestRecoveryWindow();
    long activationWindow = context.getValue(OperatorContext.ACTIVATION_WINDOW_ID);

    if (activationWindow != Stateless.WINDOW_ID && largestRecoveryWindow <= activationWindow) {
      replay = true;
    }

    readerService = Executors.newFixedThreadPool(numReaders, new NameableThreadFactory("managedStateReaders"));
    stateTracker.setup(context);
  }

  /**
   * Gets the number of buckets which is required during setup to create the array of buckets.<br/>
   * {@link ManagedStateImpl} provides num of buckets which is injected using a property.<br/>
   * {@link TimeManagedStateImpl} provides num of buckets which are calculated based on time settings.
   *
   * @return number of buckets.
   */
  public abstract int getNumBuckets();

  public void beginWindow(long windowId)
  {
    if (throwable.get() != null) {
      throw DTThrowable.wrapIfChecked(throwable.get());
    }
    this.windowId = windowId;
    windowCount++;
    timeBucketAssigner.beginWindow(windowId);
    if (replay && windowId > largestRecoveryWindow) {
      replay = false;
    }
  }

  @Override
  public void put(long bucketId, Slice key, Slice value)
  {
    if (replay) {
      return;
    }
    long timeBucket = timeBucketAssigner.getTimeBucketFor(windowId);
    if (timeBucket != -1) {
      int bucketIdx = prepareBucket(bucketId);
      buckets[bucketIdx].put(key, timeBucket, value);
    }
  }

  @Override
  public Slice getSync(long bucketId, Slice key)
  {
    int bucketIdx = prepareBucket(bucketId);
    return buckets[bucketIdx].get(key, -1, Bucket.ReadSource.ALL);
  }

  @Override
  public Future<Slice> getAsync(long bucketId, Slice key)
  {
    int bucketIdx = prepareBucket(bucketId);
    Bucket bucket = buckets[bucketIdx];

    Slice cachedVal = buckets[bucketIdx].get(key, -1, Bucket.ReadSource.MEMORY);
    if (cachedVal != null) {
      return Futures.immediateFuture(cachedVal);
    }
    return readerService.submit(new KeyFetchTask(bucket, key, -1, throwable));
  }

  /**
   * Prepares the bucket and returns its index.
   * @param bucketId bucket key
   * @return bucket index
   */
  protected int prepareBucket(long bucketId)
  {
    stateTracker.bucketAccessed(bucketId);
    int bucketIdx = getBucketIdx(bucketId);

    Bucket bucket = buckets[bucketIdx];
    if (bucket == null) {
      //bucket is not in memory
      bucket = newBucket(bucketId);
      buckets[bucketIdx] = bucket;
    }
    return bucketIdx;
  }

  protected int getBucketIdx(long bucketId)
  {
    return (int)(bucketId % numBuckets);
  }

  Bucket getBucket(long bucketId)
  {
    return buckets[getBucketIdx(bucketId)];
  }

  protected Bucket newBucket(long bucketId)
  {
    return new Bucket.DefaultBucket(bucketId, this);
  }

  public void endWindow()
  {
    timeBucketAssigner.endWindow();
    if (!replay && windowCount == incrementalCheckpointWindowCount) {
      checkpointDifference();
      windowCount = 0;
    }
  }

  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  protected void checkpointDifference()
  {
    Map<Long, Map<Slice, Bucket.BucketedValue>> flashData = Maps.newHashMap();

    for (Bucket bucket : buckets) {
      if (bucket != null) {
        synchronized (bucket) {
          Map<Slice, Bucket.BucketedValue> flashDataForBucket = bucket.checkpoint(windowId);
          if (!flashDataForBucket.isEmpty()) {
            flashData.put(bucket.getBucketId(), flashDataForBucket);
          }
        }
      }
    }
    if (!flashData.isEmpty()) {
      try {
        dataManager.save(flashData, operatorId, windowId);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void beforeCheckpoint(long windowId)
  {
  }

  @Override
  public void checkpointed(long windowId)
  {
  }

  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  @Override
  public void committed(long windowId)
  {
    synchronized (commitLock) {
      try {
        for (Bucket bucket : buckets) {
          if (bucket != null) {
            synchronized (bucket) {
              bucket.committed(windowId);
            }
          }
        }
        dataManager.committed(operatorId, windowId);
      } catch (IOException | InterruptedException e) {
        throw new RuntimeException("committing " + windowId, e);
      }
    }
  }

  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  @Override
  public void teardown()
  {
    dataManager.teardown();
    timeBucketAssigner.teardown();
    readerService.shutdownNow();
    for (Bucket bucket : buckets) {
      if (bucket != null) {
        synchronized (bucket) {
          bucket.teardown();
        }
      }
    }
    stateTracker.teardown();
  }

  @Override
  public void setMaxMemorySize(long bytes)
  {
    maxMemorySize = bytes;
  }

  /**
   *
   * @return the optimal size of the cache that triggers eviction of committed data from memory.
   */
  public long getMaxMemorySize()
  {
    return maxMemorySize;
  }

  /**
   * Sets the {@link FileAccess} implementation.
   * @param fileAccess specific implementation of FileAccess.
   */
  public void setFileAccess(@NotNull FileAccess fileAccess)
  {
    this.fileAccess = Preconditions.checkNotNull(fileAccess);
  }

  @Override
  public FileAccess getFileAccess()
  {
    return fileAccess;
  }

  /**
   * Sets the time bucket assigner. This can be used for plugging any custom time bucket assigner.
   *
   * @param timeBucketAssigner a {@link TimeBucketAssigner}
   */
  public void setTimeBucketAssigner(@NotNull TimeBucketAssigner timeBucketAssigner)
  {
    this.timeBucketAssigner = Preconditions.checkNotNull(timeBucketAssigner);
  }

  public TimeBucketAssigner getTimeBucketAssigner()
  {
    return timeBucketAssigner;
  }

  @Override
  public Comparator<Slice> getKeyComparator()
  {
    return keyComparator;
  }

  /**
   * Sets the key comparator. The keys on the disk in time bucket files are sorted. This sets the comparator for the
   * key.
   * @param keyComparator key comparator
   */
  public void setKeyComparator(@NotNull Comparator<Slice> keyComparator)
  {
    this.keyComparator = Preconditions.checkNotNull(keyComparator);
  }

  @Override
  public BucketsMetaDataManager getBucketsMetaDataManager()
  {
    return bucketsMetaDataManager;
  }

  /**
   * @return number of worker threads in the reader service.
   */
  public int getNumReaders()
  {
    return numReaders;
  }

  /**
   * Sets the number of worker threads in the reader service which is responsible for asynchronously fetching
   * values of the keys. This should not exceed number of buckets.
   *
   * @param numReaders number of worker threads in the reader service.
   */
  public void setNumReaders(int numReaders)
  {
    this.numReaders = numReaders;
  }

  /**
   * @return regular interval at which the size of state is checked.
   */
  public Duration getCheckStateSizeInterval()
  {
    return checkStateSizeInterval;
  }

  /**
   * Sets the interval at which the size of state is regularly checked.

   * @param checkStateSizeInterval regular interval at which the size of state is checked.
   */
  public void setCheckStateSizeInterval(@NotNull Duration checkStateSizeInterval)
  {
    this.checkStateSizeInterval = Preconditions.checkNotNull(checkStateSizeInterval);
  }

  /**
   * @return duration which prevents a bucket being evicted.
   */
  public Duration getDurationPreventingFreeingSpace()
  {
    return durationPreventingFreeingSpace;
  }

  /**
   * Sets the duration which prevents buckets to free space. For example if this is set to an hour, then only
   * buckets which were not accessed in last one hour will be triggered to free spaces.
   *
   * @param durationPreventingFreeingSpace time duration
   */
  public void setDurationPreventingFreeingSpace(Duration durationPreventingFreeingSpace)
  {
    this.durationPreventingFreeingSpace = durationPreventingFreeingSpace;
  }

  /**
   * @return incremental checkpoint window count.
   */
  public int getIncrementalCheckpointWindowCount()
  {
    return incrementalCheckpointWindowCount;
  }

  /**
   * Sets the incremental checkpoint window count field which controls the no. of application windows at which
   * the new data is checkpointed.
   *
   * @param incrementalCheckpointWindowCount incremental checkpoint window count
   */
  public void setIncrementalCheckpointWindowCount(int incrementalCheckpointWindowCount)
  {
    this.incrementalCheckpointWindowCount = incrementalCheckpointWindowCount;
  }

  static class KeyFetchTask implements Callable<Slice>
  {
    private final Bucket bucket;
    private final long timeBucketId;
    private final Slice key;
    private final AtomicReference<Throwable> throwable;

    KeyFetchTask(@NotNull Bucket bucket, @NotNull Slice key, long timeBucketId, AtomicReference<Throwable> throwable)
    {
      this.bucket = Preconditions.checkNotNull(bucket);
      this.timeBucketId = timeBucketId;
      this.key = Preconditions.checkNotNull(key);
      this.throwable = Preconditions.checkNotNull(throwable);
    }

    @Override
    public Slice call() throws Exception
    {
      try {
        synchronized (bucket) {
          //a particular bucket should only be handled by one thread at any point of time. Handling of bucket here
          //involves creating readers for the time buckets and de-serializing key/value from a reader.
          return bucket.get(key, timeBucketId, Bucket.ReadSource.ALL);
        }
      } catch (Throwable t) {
        throwable.set(t);
        throw DTThrowable.wrapIfChecked(t);
      }
    }
  }

  @VisibleForTesting
  void setStateTracker(@NotNull StateTracker stateTracker)
  {
    this.stateTracker = Preconditions.checkNotNull(stateTracker, "state tracker");
  }

  @VisibleForTesting
  void setDataManager(@NotNull BucketsDataManager bucketsDataManager)
  {
    this.dataManager = Preconditions.checkNotNull(bucketsDataManager, "buckets data manager");
  }

  private static final transient Logger LOG = LoggerFactory.getLogger(AbstractManagedStateImpl.class);
}
