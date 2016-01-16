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
import java.util.PriorityQueue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import javax.validation.constraints.NotNull;

import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import com.datatorrent.api.Component;
import com.datatorrent.api.Context;

/**
 * Tracks the size of state in memory and evicts buckets.
 */
class StateTracker extends TimerTask implements Component<Context.OperatorContext>
{
  //bucket id -> last time the bucket was accessed
  private final transient ConcurrentHashMap<Long, Long> bucketAccessTimes = new ConcurrentHashMap<>();

  private final transient PriorityQueue<Map.Entry<Long, Long>> bucketHeap;

  private final transient Timer memoryFreeService = new Timer();

  protected final transient AbstractManagedStateImpl managedState;

  StateTracker(@NotNull AbstractManagedStateImpl managedState)
  {
    this.managedState = Preconditions.checkNotNull(managedState, "managed state");
    this.bucketHeap = new PriorityQueue<>(managedState.getNumBuckets(),
        new Comparator<Map.Entry<Long, Long>>()
        {

          @Override
          public int compare(Map.Entry<Long, Long> o1, Map.Entry<Long, Long> o2)
          {
            if (o1.getValue() < o2.getValue()) {
              return -1;
            }
            if (o1.getValue() > o2.getValue()) {
              return 1;
            }

            return Long.compare(o1.getKey(), o2.getKey());
          }
        });
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    long intervalMillis = managedState.getCheckStateSizeInterval().getMillis();
    memoryFreeService.scheduleAtFixedRate(this, intervalMillis, intervalMillis);
  }

  void bucketAccessed(long bucketId)
  {
    //avoiding any delays here because this is getting invoked anytime a bucket is accessed which is with every put/get.
    bucketAccessTimes.put(bucketId, System.currentTimeMillis());
  }

  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  @Override
  public void run()
  {
    synchronized (managedState.commitLock) {
      //freeing of state needs to be stopped during commit as commit results in transferring data to a state which
      // can be freed up as well.
      long bytesSum = 0;
      for (Bucket bucket : managedState.buckets) {
        if (bucket != null) {
          bytesSum += bucket.getSizeInBytes();
        }
      }

      if (bytesSum > managedState.getMaxMemorySize()) {
        //populate the bucket heap
        for (Map.Entry<Long, Long> entry : bucketAccessTimes.entrySet()) {
          bucketHeap.add(entry);
        }
      }
      Duration duration = managedState.getDurationPreventingFreeingSpace();
      long durationMillis = 0;
      if (duration != null) {
        durationMillis = duration.getMillis();
      }

      Map.Entry<Long, Long> pair;
      while (bytesSum > managedState.getMaxMemorySize() && null != (pair = bucketHeap.poll())) {
        //trigger buckets to free space

        if (System.currentTimeMillis() - pair.getValue() < durationMillis) {
          //if the least recently used bucket cannot free up space because it was accessed within the
          //specified duration then subsequent buckets cannot free space as well because this heap is ordered by time.
          break;
        }
        long bucketId = pair.getKey();
        Bucket bucket = managedState.getBucket(bucketId);
        if (bucket != null) {

          synchronized (bucket) {
            long sizeFreed;
            try {
              sizeFreed = bucket.freeMemory();
              LOG.debug("size freed {} {}", bucketId, sizeFreed);
            } catch (IOException e) {
              managedState.throwable.set(e);
              throw new RuntimeException("freeing " + bucketId, e);
            }
            bytesSum -= sizeFreed;
          }
        }
        bucketAccessTimes.remove(bucketId);
      }
      bucketHeap.clear();
    }
  }

  public void teardown()
  {
    memoryFreeService.cancel();
  }

  private static final Logger LOG = LoggerFactory.getLogger(StateTracker.class);

}
