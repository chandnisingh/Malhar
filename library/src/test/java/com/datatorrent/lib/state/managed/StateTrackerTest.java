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

import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.google.common.collect.ImmutableMap;

public class StateTrackerTest
{
  static class TestMeta extends TestWatcher
  {
    StateTracker tracker;
    ManagedStateImpl managedState;

    @Override
    protected void starting(Description description)
    {
      managedState = new ManagedStateImpl();
      managedState.setDurationPreventingFreeingSpace(Duration.millis(500));
      tracker = new StateTracker(managedState);
    }

    @Override
    protected void finished(Description description)
    {
      tracker.teardown();
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testAdd() throws InterruptedException
  {
    testMeta.tracker.bucketAccessed(1);
    long time1 = testMeta.tracker.getImmutableCopyOfHeap().get(1L);
    Thread.sleep(500);

    testMeta.tracker.bucketAccessed(1);
    ImmutableMap<Long, Long> copy = testMeta.tracker.getImmutableCopyOfHeap();
    Assert.assertEquals("size 1", 1, copy.size());

    Assert.assertTrue("time update", time1 < copy.get(1L));
  }

}
