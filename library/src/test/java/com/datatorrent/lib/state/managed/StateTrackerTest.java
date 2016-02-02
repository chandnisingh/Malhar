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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import javax.validation.constraints.NotNull;

import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.commons.io.FileUtils;

import com.google.common.collect.Lists;

import com.datatorrent.api.Context;
import com.datatorrent.lib.fileaccess.FileAccessFSImpl;
import com.datatorrent.netlet.util.Slice;

public class StateTrackerTest
{
  static class TestMeta extends TestWatcher
  {
    MockManagedStateImpl managedState;
    Context.OperatorContext operatorContext;
    String applicationPath;

    @Override
    protected void starting(Description description)
    {
      managedState = new MockManagedStateImpl();
      applicationPath = "target/" + description.getClassName() + "/" + description.getMethodName();
      ((FileAccessFSImpl)managedState.getFileAccess()).setBasePath(applicationPath + "/" + "bucket_data");

      managedState.setNumBuckets(2);
      managedState.setMaxMemorySize(100);

      operatorContext = ManagedStateTestUtils.getOperatorContext(1, applicationPath);
    }

    @Override
    protected void finished(Description description)
    {
      try {
        FileUtils.deleteDirectory(new File("target/" + description.getClassName()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testEviction() throws InterruptedException
  {
    testMeta.managedState.latch = new CountDownLatch(1);
    testMeta.managedState.setup(testMeta.operatorContext);

    Slice one = ManagedStateTestUtils.getSliceFor("1");
    testMeta.managedState.beginWindow(System.currentTimeMillis());
    testMeta.managedState.put(1, one, one);
    testMeta.managedState.endWindow();

    testMeta.managedState.latch.await();
    testMeta.managedState.teardown();
    Assert.assertEquals("freed bucket", Lists.newArrayList(1L), testMeta.managedState.freedBuckets);
  }

  @Test
  public void testMultipleEvictions() throws InterruptedException
  {
    testMeta.managedState.latch = new CountDownLatch(2);
    testMeta.managedState.setup(testMeta.operatorContext);

    Slice one = ManagedStateTestUtils.getSliceFor("1");
    testMeta.managedState.beginWindow(System.currentTimeMillis());
    testMeta.managedState.put(1, one, one);

    Slice two = ManagedStateTestUtils.getSliceFor("2");
    testMeta.managedState.put(2, two, two);
    testMeta.managedState.endWindow();

    testMeta.managedState.latch.await();
    testMeta.managedState.teardown();
    Assert.assertEquals("freed bucket", Lists.newArrayList(1L, 2L), testMeta.managedState.freedBuckets);
  }

  @Test
  public void testBucketPrevention() throws InterruptedException
  {
    testMeta.managedState.setDurationPreventingFreeingSpace(Duration.standardDays(2));
    testMeta.managedState.setStateTracker(new MockStateTracker(testMeta.managedState));
    testMeta.managedState.latch = new CountDownLatch(1);

    testMeta.managedState.setup(testMeta.operatorContext);
    Slice one = ManagedStateTestUtils.getSliceFor("1");
    testMeta.managedState.beginWindow(System.currentTimeMillis());
    testMeta.managedState.put(1, one, one);

    Slice two = ManagedStateTestUtils.getSliceFor("2");
    testMeta.managedState.put(2, two, two);
    testMeta.managedState.endWindow();

    testMeta.managedState.latch.await();
    testMeta.managedState.teardown();
    Assert.assertEquals("no buckets triggered", 0, testMeta.managedState.freedBuckets.size());
  }

  private static class MockManagedStateImpl extends ManagedStateImpl
  {
    CountDownLatch latch;
    List<Long> freedBuckets = Lists.newArrayList();

    @Override
    protected Bucket newBucket(long bucketId)
    {
      return new MockDefaultBucket(bucketId, this);
    }
  }

  private static class MockDefaultBucket extends Bucket.DefaultBucket
  {
    final MockManagedStateImpl mockManagedState;
    protected MockDefaultBucket(long bucketId, @NotNull ManagedStateContext managedStateContext)
    {
      super(bucketId, managedStateContext);
      mockManagedState = (MockManagedStateImpl)managedStateContext;
    }

    @Override
    public long freeMemory() throws IOException
    {
      long freedBytes = super.freeMemory();
      mockManagedState.freedBuckets.add(getBucketId());
      mockManagedState.latch.countDown();
      return freedBytes;
    }

    @Override
    public long getSizeInBytes()
    {
      return 600;
    }
  }

  private static class MockStateTracker extends StateTracker
  {
    MockStateTracker(@NotNull AbstractManagedStateImpl managedState)
    {
      super(managedState);
    }

    @Override
    public void run()
    {
      super.run();
      ((MockManagedStateImpl)managedState).latch.countDown();
    }
  }

}
