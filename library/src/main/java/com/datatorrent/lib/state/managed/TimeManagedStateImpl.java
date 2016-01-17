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

import com.datatorrent.netlet.util.Slice;

/**
 * In this implementation of {@link ManagedState}, the buckets in memory are time-buckets.
 */
public class TimeManagedStateImpl extends AbstractManagedStateImpl
{

  @Override
  public int getNumBuckets()
  {
    return timeBucketAssigner.getNumBuckets();
  }


  @Override
  public void put(long bucketId, Slice key, Slice value)
  {
    if (replay) {
      return;
    }
    bucketId = timeBucketAssigner.getTimeBucketFor(bucketId);
    if (bucketId != -1) {
      int bucketIdx = prepareBucket(bucketId);
      buckets[bucketIdx].put(key, bucketId, value);
    }
  }

  @Override
  public Slice getSync(long bucketId, Slice key)
  {
    bucketId = timeBucketAssigner.getTimeBucketFor(bucketId);
    if (bucketId == -1) {
      //time is expired so return null.
      return null;
    }
    int bucketIdx = prepareBucket(bucketId);
    return buckets[bucketIdx].get(key, bucketId, Bucket.ReadSource.ALL);
  }

  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  @Override
  public Future<Slice> getAsync(long bucketId, Slice key)
  {
    bucketId = timeBucketAssigner.getTimeBucketFor(bucketId);
    if (bucketId == -1) {
      //time is expired so return null.
      return null;
    }
    int bucketIdx = prepareBucket(bucketId);
    Bucket bucket = buckets[bucketIdx];
    synchronized (bucket) {
      Slice cachedVal = buckets[bucketIdx].get(key, bucketId, Bucket.ReadSource.MEMORY);
      if (cachedVal != null) {
        return Futures.immediateFuture(cachedVal);
      }
      return readerService.submit(new KeyFetchTask(bucket, key, bucketId, throwable));
    }
  }
}
