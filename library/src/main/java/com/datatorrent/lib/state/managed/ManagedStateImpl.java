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

import com.datatorrent.lib.state.BucketedState;
import com.datatorrent.netlet.util.Slice;

/**
 * Basic implementation of {@link ManagedState} where window is used to sub-group key of a particular bucket.<br/>
 *
 */
public class ManagedStateImpl extends AbstractManagedStateImpl implements BucketedState
{

  public ManagedStateImpl()
  {
    this.numBuckets = 1;
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
