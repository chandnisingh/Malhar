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

package com.datatorrent.lib.state;

import java.util.concurrent.Future;

import com.datatorrent.netlet.util.Slice;

/**
 * A state where keys are grouped in buckets.
 */
public interface BucketedState
{
  /**
   * Sets the value of the key in bucket identified by bucketId.
   *
   * @param bucketId identifier of the bucket.
   * @param key     key
   * @param value   value
   */
  void put(long bucketId, Slice key, Slice value);

  /**
   * Returns the value of the key in a bucket identified by bucketId. Fetching a key can be expensive if the key
   * is not in memory and is present on disk. This fetches the key synchronously. <br/>
   * {@link #getAsync(long, Slice)} is recommended for efficiently reading the value of a key.
   *
   * @param bucketId identifier of the bucket.
   * @param key     key
   * @return        value
   */
  Slice getSync(long bucketId, Slice key);

  /**
   * Returns the future using which the value is obtained.<br/>
   * If the key is present in memory, then the future has its value set when constructed.
   *
   * @param key       key
   * @return          value
   */
  Future<Slice> getAsync(long bucketId, Slice key);

}
