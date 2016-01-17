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

import java.util.concurrent.Future;

import javax.validation.constraints.Min;

import com.google.common.util.concurrent.Futures;

import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.lib.state.BucketedState;
import com.datatorrent.netlet.util.Slice;

/**
 * This implementation of {@link ManagedState} lets the client to specify the time for each key. The value of time
 * is used to derive the time-bucket of a key.
 */
@OperatorAnnotation(checkpointableWithinAppWindow = false)
public class ManagedStateImpl extends AbstractManagedStateImpl implements BucketedState.TimeSlicedBucketedState
{
  public ManagedStateImpl()
  {
    this.numBuckets = 1;
  }

  @Override
  public void put(long bucketId, long time, Slice key, Slice value)
  {
    if (replay) {
      return;
    }

    int bucketIdx = prepareBucket(bucketId);
    long timeBucket = timeBucketAssigner.getTimeBucketFor(time);
    if (timeBucket != -1) {
      buckets[bucketIdx].put(key, timeBucket, value);
    }
  }

  @Override
  public Slice getSync(long bucketId, long time, Slice key)
  {
    int bucketIdx = prepareBucket(bucketId);
    long timeBucket = timeBucketAssigner.getTimeBucketFor(time);
    if (timeBucket == -1) {
      //time is expired so no point in looking further.
      return null;
    }
    return buckets[bucketIdx].get(key, timeBucket, Bucket.ReadSource.ALL);
  }

  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  @Override
  public Future<Slice> getAsync(long bucketId, long time, Slice key)
  {
    int bucketIdx = prepareBucket(bucketId);
    Bucket bucket = buckets[bucketIdx];
    long timeBucket = timeBucketAssigner.getTimeBucketFor(time);
    if (timeBucket == -1) {
      //time is expired so no point in looking further.
      return null;
    }
    synchronized (bucket) {
      Slice cachedVal = buckets[bucketIdx].get(key, timeBucket, Bucket.ReadSource.MEMORY);
      if (cachedVal != null) {
        return Futures.immediateFuture(cachedVal);
      }
      return readerService.submit(new KeyFetchTask(bucket, key, timeBucket, throwable));
    }
  }

  @Min(1)
  @Override
  public int getNumBuckets()
  {
    return numBuckets;
  }

  /**
   * Sets the number of buckets.
   *
   * @param numBuckets number of buckets
   */
  public void setNumBuckets(int numBuckets)
  {
    this.numBuckets = numBuckets;
  }

}
