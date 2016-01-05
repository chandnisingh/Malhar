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
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import javax.validation.constraints.NotNull;

import org.joda.time.Duration;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.UnmodifiableIterator;

import com.datatorrent.api.Component;
import com.datatorrent.api.Context;

/**
 * Tracks the size of state in memory and evicts buckets.
 */
class StateTracker extends TimerTask implements Component<Context.OperatorContext>
{
  //bucket keys in the order they are accessed
  private final Map<Long, Long> bucketHeap = new ConcurrentHashMap<>();

  private final Comparator<Map.Entry<Long, Long>> timeComparator = new Comparator<Map.Entry<Long, Long>>()
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
  };

  private final Timer memoryFreeService = new Timer();

  private final AbstractManagedStateImpl managedState;

  StateTracker(@NotNull AbstractManagedStateImpl managedState)
  {
    this.managedState = Preconditions.checkNotNull(managedState, "managed state");
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    long intervalMillis = managedState.getCheckStateSizeInterval().getMillis();
    memoryFreeService.scheduleAtFixedRate(this, intervalMillis, intervalMillis);
  }

  void bucketAccessed(long bucketId)
  {
    bucketHeap.put(bucketId, System.currentTimeMillis());
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
        bytesSum += bucket.getSizeInBytes();
      }
      UnmodifiableIterator<Map.Entry<Long, Long>> sortedEntryIterator = getTimeSortedEntries();

      while (bytesSum > managedState.getMaxMemorySize() && sortedEntryIterator.hasNext()) {

        //evict buckets from memory

        Map.Entry<Long, Long> pair = sortedEntryIterator.next();

        Duration duration = managedState.getDurationPreventingFreeingSpace();
        if (duration != null && System.currentTimeMillis() - pair.getValue() < duration.getMillis()) {
          //if the least recently used bucket cannot free up space because it was accessed within the
          //specified duration then subsequent buckets cannot free space as well because this set is ordered by time.
          break;
        }
        long bucketId = pair.getKey();
        Bucket bucket = managedState.getBucket(bucketId);
        if (bucket != null) {

          synchronized (bucket) {
            long sizeFreed;
            try {
              sizeFreed = bucket.freeMemory();
            } catch (IOException e) {
              managedState.throwable.set(e);
              throw new RuntimeException("freeing " + bucketId, e);
            }
            bytesSum -= sizeFreed;
          }
        }

        bucketHeap.remove(bucketId);
      }
    }
  }

  @VisibleForTesting
  UnmodifiableIterator<Map.Entry<Long, Long>> getTimeSortedEntries()
  {
    return ImmutableSortedSet.copyOf(timeComparator, bucketHeap.entrySet()).iterator();
  }

  public void teardown()
  {
    memoryFreeService.cancel();
  }

  @VisibleForTesting
  ImmutableMap<Long, Long> getImmutableCopyOfHeap()
  {
    return ImmutableMap.copyOf(bucketHeap);
  }
}
