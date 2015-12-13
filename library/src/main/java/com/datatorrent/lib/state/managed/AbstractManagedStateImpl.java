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
import java.lang.reflect.Array;
import java.util.Comparator;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
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
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
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
 * Not thread safe.
 */
public abstract class AbstractManagedStateImpl
    implements ManagedState, Operator, Operator.CheckpointListener, ManagedStateContext
{
  private long maxCacheSize;

  protected int numBuckets;

  private int incrementalCheckpointWindowCount = DAGContext.CHECKPOINT_WINDOW_COUNT.defaultValue;

  @NotNull
  private FileAccess fileAccess = new TFileImpl.DTFileImpl();
  @NotNull
  protected TimeBucketAssigner timeBucketAssigner = new TimeBucketAssigner();

  protected Bucket[] buckets;

  private StateTracker tracker;

  @Min(1)
  private int numReaders = 10;
  @NotNull
  protected transient ExecutorService readerService;

  @NotNull
  private final BucketsDataManager dataManager = new BucketsDataManager(this);

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

  private transient StateTracker stateTracker;

  private final transient Object commitLock = new Object();

  @Override
  public void setup(OperatorContext context)
  {
    operatorId = context.getId();
    fileAccess.init();
    timeBucketAssigner.register(dataManager);
    timeBucketAssigner.setup(context);

    numBuckets = getNumBuckets();
    buckets = (Bucket[])Array.newInstance(Bucket.class, numBuckets);

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

    stateTracker = new StateTracker(this, throwable);
    stateTracker.setup(context);
  }

  public abstract int getNumBuckets();

  @Override
  public void beginWindow(long l)
  {
    if (throwable.get() != null) {
      throw DTThrowable.wrapIfChecked(throwable.get());
    }

    windowCount++;
    timeBucketAssigner.beginWindow(l);
    if (replay && l > largestRecoveryWindow) {
      replay = false;
    }
  }

  @Override
  public void put(long bucketId, Slice key, Slice value)
  {
    if (replay) {
      return;
    }
    int bucketIdx = prepareBucket(bucketId);
    long timeBucket = timeBucketAssigner.getTimeBucketFor(windowId);
    if (timeBucket != -1) {
      buckets[bucketIdx].put(key, timeBucket, value);
    }
  }

  @Override
  public Slice getSync(long bucketId, Slice key)
  {
    int bucketIdx = prepareBucket(bucketId);
    return buckets[bucketIdx].get(key, -1, Bucket.ReadSource.ALL);
  }

  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  @Override
  public Future<Slice> getAsync(long bucketId, Slice key)
  {
    int bucketIdx = prepareBucket(bucketId);
    Bucket bucket = buckets[bucketIdx];
    synchronized (bucket) {
      Slice cachedVal = buckets[bucketIdx].get(key, -1, Bucket.ReadSource.MEMORY);
      if (cachedVal != null) {
        return Futures.immediateFuture(cachedVal);
      }
      return readerService.submit(new KeyFetchTask(bucket, key, -1, throwable));
    }
  }

  /**
   * Prepares the bucket and returns its index.
   * @param groupId bucket key
   * @return bucket index
   */
  protected int prepareBucket(long groupId)
  {
    stateTracker.bucketAccessed(groupId);
    int bucketIdx = getBucketIdx(groupId);

    Bucket bucket = buckets[bucketIdx];
    if (bucket == null) {
      //bucket is not in memory
      bucket = newBucket(groupId);
      buckets[bucketIdx] = bucket;
    }
    return bucketIdx;
  }

  protected int getBucketIdx(long groupId)
  {
    return (int)(groupId % numBuckets);
  }

  Bucket getBucket(long groupId)
  {
    return buckets[getBucketIdx(groupId)];
  }

  protected Bucket newBucket(long groupId)
  {
    return new Bucket.DefaultBucket(groupId, this);
  }

  @Override
  public void endWindow()
  {
    timeBucketAssigner.endWindow();
    if (!replay && windowCount == incrementalCheckpointWindowCount) {
      checkpointDifference();
      windowCount = 0;
    }
  }

  protected void checkpointDifference()
  {
    Map<Long, Map<Slice, Bucket.BucketedValue>> flashData = Maps.newHashMap();

    for (Bucket bucket : buckets) {
      if (bucket != null) {
        Map<Slice, Bucket.BucketedValue> flashDataForBucket = bucket.checkpoint(windowId);
        if (!flashDataForBucket.isEmpty()) {
          flashData.put(bucket.getBucketId(), flashDataForBucket);
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
  public void checkpointed(long l)
  {
  }

  @Override
  public void committed(long l)
  {
    synchronized (commitLock) {
      try {
        for (Bucket bucket : buckets) {
          if (bucket != null) {
            bucket.committed(l);
          }
        }
        dataManager.committed(operatorId, l);
      } catch (IOException | InterruptedException e) {
        throw new RuntimeException("committing " + l, e);
      }
    }
  }

  @Override
  public void teardown()
  {
    dataManager.teardown();
    timeBucketAssigner.teardown();
    for (Bucket bucket : buckets) {
      if (bucket != null) {
        bucket.teardown();
      }
    }
    stateTracker.teardown();
  }

  @Override
  public void setCacheSize(long bytes)
  {
    maxCacheSize = bytes;
  }

  public long getCacheSize()
  {
    return maxCacheSize;
  }

  public void setFileAccess(@NotNull FileAccess fileAccess)
  {
    this.fileAccess = Preconditions.checkNotNull(fileAccess);
  }

  public void setKeyComparator(@NotNull Comparator<Slice> keyComparator)
  {
    this.keyComparator = Preconditions.checkNotNull(keyComparator);
  }

  @Override
  public FileAccess getFileAccess()
  {
    return fileAccess;
  }

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

  @Override
  public BucketsMetaDataManager getBucketsMetaDataManager()
  {
    return bucketsMetaDataManager;
  }

  public int getNumReaders()
  {
    return numReaders;
  }

  public void setNumReaders(int numReaders)
  {
    this.numReaders = numReaders;
  }

  public Duration getCheckStateSizeInterval()
  {
    return checkStateSizeInterval;
  }

  public void setCheckStateSizeInterval(@NotNull Duration checkStateSizeInterval)
  {
    this.checkStateSizeInterval = Preconditions.checkNotNull(checkStateSizeInterval);
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
        return bucket.get(key, timeBucketId, Bucket.ReadSource.READERS);
      } catch (Throwable t) {
        throwable.set(t);
        throw DTThrowable.wrapIfChecked(t);
      }
    }
  }

  static class StateTracker extends TimerTask implements Component<OperatorContext>
  {
    private final Map<Long, Long> bucketsLastAccessedTime = Maps.newConcurrentMap();

    //bucket keys in the order they are accessed
    private final MinMaxPriorityQueue<Long> bucketHeap = MinMaxPriorityQueue.orderedBy(
        new Comparator<Long>()
        {
          @Override
          public int compare(Long bucketId1, Long bucketId2)
          {
            return Long.compare(bucketsLastAccessedTime.get(bucketId1), bucketsLastAccessedTime.get(bucketId2));
          }
        })
        .create();

    private final Timer memoryFreeService = new Timer();

    private final AbstractManagedStateImpl managedState;

    private final AtomicReference<Throwable> throwable;

    StateTracker(@NotNull AbstractManagedStateImpl managedState, @NotNull AtomicReference<Throwable> throwable)
    {
      this.managedState = Preconditions.checkNotNull(managedState, "managed state");
      this.throwable = Preconditions.checkNotNull(throwable, "throwable");
    }

    @Override
    public void setup(OperatorContext context)
    {
      long intervalMillis = managedState.getCheckStateSizeInterval().getMillis();
      memoryFreeService.scheduleAtFixedRate(this, intervalMillis, intervalMillis);
    }

    void bucketAccessed(long bucketId)
    {
      bucketsLastAccessedTime.put(bucketId, System.currentTimeMillis());
      bucketHeap.add(bucketId);
    }

    @Override
    public void run()
    {
      synchronized (managedState.commitLock) {
        //free of bucket state needs to be stopped during commit
        long bytesSum = 0;
        for (Bucket bucket : managedState.buckets) {
          bytesSum += bucket.getSizeInBytes();
        }

        while (bytesSum > managedState.maxCacheSize) {

          //evict buckets from memory
          Long bucketId = bucketHeap.poll();
          if (bucketId != null) {
            Bucket bucket = managedState.getBucket(bucketId);
            if (bucket != null) {

              long sizeFreed;
              try {
                sizeFreed = bucket.freeMemory();
              } catch (IOException e) {
                throwable.set(e);
                throw new RuntimeException("freeing " + bucketId, e);
              }
              bytesSum -= sizeFreed;
            }
          }
        }
      }
    }

    public void teardown()
    {
      memoryFreeService.cancel();
    }
  }

  private static final transient Logger LOG = LoggerFactory.getLogger(AbstractManagedStateImpl.class);
}
