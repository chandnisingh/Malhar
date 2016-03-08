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

import com.google.common.util.concurrent.Futures;

import com.datatorrent.lib.state.BucketedState;
import com.datatorrent.netlet.util.Slice;

/**
 * In this implementation of {@link ManagedState} the buckets in memory are time-buckets.
 * <p/>
 *
 * <b>Difference from {@link ManagedTimeStateImpl}</b>: <br/>
 * <ol>
 * <li>The main buckets in {@link ManagedTimeStateImpl} are unique adhoc long ids which the user provides with the
 * key. In this implementation the main buckets are time buckets. The user provides just the time and the time bucket is
 * derived from it.
 * </li>
 * <br/>
 *
 * <li>In regards to the bucket data on disk, in {@link ManagedTimeStateImpl} the buckets are persisted on disk
 * with each bucket data further grouped into time-buckets: {base_path}/{bucketId}/{time-bucket id}. <br/>
 * In this implementation operator id is used as bucketId and there is just one time-bucket under a particular
 * operator id :
 * {base_path}/{operator id}/{time bucket id}.
 * </li>
 * <br/>
 *
 * <li>In {@link ManagedTimeStateImpl} a bucket belongs to just one partition. Multiple partitions cannot write to
 * the same bucket. <br/>
 * In this implementation multiple partitions can be working with the same time-bucket (since time-bucket is derived
 * from time). This works because on the disk the time-bucket data is segregated under each operator id.
 * </li>
 * <br/>
 *
 * <li>While {@link ManagedTimeStateImpl} can support dynamic partitioning by pre-allocating buckets this will not
 * be able to support dynamic partitioning efficiently.
 * </li>

 * </ol>
 */
public class ManageTimeUnifiedStateImpl extends AbstractManagedStateImpl implements BucketedState
{

  @Override
  public int getNumBuckets()
  {
    return timeBucketAssigner.getNumBuckets();
  }

  @Override
  public void put(long time, Slice key, Slice value)
  {
    if (replay) {
      return;
    }
    time = timeBucketAssigner.getTimeBucketFor(time);
    if (time != -1) {
      int bucketIdx = prepareBucket(time);
      buckets[bucketIdx].put(key, time, value);
    }
  }

  @Override
  public Slice getSync(long time, Slice key)
  {
    time = timeBucketAssigner.getTimeBucketFor(time);
    if (time == -1) {
      //time is expired so return null.
      return null;
    }
    int bucketIdx = prepareBucket(time);
    return buckets[bucketIdx].get(key, time, Bucket.ReadSource.ALL);
  }

  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  @Override
  public Future<Slice> getAsync(long time, Slice key)
  {
    time = timeBucketAssigner.getTimeBucketFor(time);
    if (time == -1) {
      //time is expired so return null.
      return null;
    }
    int bucketIdx = prepareBucket(time);
    Bucket bucket = buckets[bucketIdx];
    synchronized (bucket) {
      Slice cachedVal = buckets[bucketIdx].get(key, time, Bucket.ReadSource.MEMORY);
      if (cachedVal != null) {
        return Futures.immediateFuture(cachedVal);
      }
      return readerService.submit(new KeyFetchTask(bucket, key, time, throwable));
    }
  }
}
