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

import com.datatorrent.lib.state.BucketedState;

/**
 * A {@link BucketedState} that defines a limit on the size (optimal size) being used in memory.
 */
public interface ManagedState extends BucketedState
{
  /**
   * Sets the optimal cache size.
   * @param bytes optimal cache bytes.
   */
  void setCacheSize(long bytes);
}
