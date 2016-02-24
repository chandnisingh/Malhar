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
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.commons.io.FileUtils;

import com.datatorrent.api.Context;
import com.datatorrent.lib.fileaccess.FileAccessFSImpl;
import com.datatorrent.lib.util.KryoCloneUtils;
import com.datatorrent.netlet.util.Slice;

public class ManagedStateImplTest
{
  class TestMeta extends TestWatcher
  {
    ManagedStateImpl managedState;
    Context.OperatorContext operatorContext;
    String applicationPath;

    @Override
    protected void starting(Description description)
    {
      managedState = new ManagedStateImpl();
      applicationPath = "target/" + description.getClassName() + "/" + description.getMethodName();
      ((FileAccessFSImpl)managedState.getFileAccess()).setBasePath(applicationPath + "/" + "bucket_data");

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
  public void testSerde() throws IOException
  {
    ManagedStateImpl deserialized = KryoCloneUtils.cloneObject(testMeta.managedState);
    Assert.assertEquals("num buckets", deserialized.getNumBuckets(), testMeta.managedState.getNumBuckets());
  }

  @Test
  public void testSimplePutGet()
  {
    Slice one = ManagedStateTestUtils.getSliceFor("1");
    testMeta.managedState.setup(testMeta.operatorContext);
    testMeta.managedState.beginWindow(System.currentTimeMillis());
    testMeta.managedState.put(0, one, one);
    Slice value = testMeta.managedState.getSync(0, one);
    testMeta.managedState.endWindow();

    Assert.assertEquals("value of one", one, value);
    testMeta.managedState.teardown();
  }

  @Test
  public void testAsyncGetFromFlash() throws ExecutionException, InterruptedException
  {
    Slice one = ManagedStateTestUtils.getSliceFor("1");
    testMeta.managedState.setup(testMeta.operatorContext);
    testMeta.managedState.beginWindow(System.currentTimeMillis());
    testMeta.managedState.put(0, one, one);
    Future<Slice> valFuture = testMeta.managedState.getAsync(0, one);
    Slice value = valFuture.get();

    Assert.assertEquals("value of one", one, value);
    testMeta.managedState.teardown();
  }

  @Test
  public void testIncrementalCheckpoint()
  {
    testMeta.managedState.setIncrementalCheckpointWindowCount(1);
    Slice one = ManagedStateTestUtils.getSliceFor("1");
    testMeta.managedState.setup(testMeta.operatorContext);
    long time = System.currentTimeMillis();
    testMeta.managedState.beginWindow(time);
    testMeta.managedState.put(0, one, one);
    testMeta.managedState.endWindow();

    Bucket.DefaultBucket defaultBucket = (Bucket.DefaultBucket)testMeta.managedState.getBucket(0);
    Assert.assertEquals("value of one", one, defaultBucket.getCheckpointedData().get(time).get(one).getValue());

    Slice two = ManagedStateTestUtils.getSliceFor("2");
    testMeta.managedState.beginWindow(time + 1);
    testMeta.managedState.put(0, two, two);
    testMeta.managedState.endWindow();
    Assert.assertEquals("value of two", two, defaultBucket.getCheckpointedData().get(time + 1).get(two).getValue());
    testMeta.managedState.teardown();
  }

  @Test
  public void testAsyncGetFromCheckpoint() throws ExecutionException, InterruptedException
  {
    testMeta.managedState.setIncrementalCheckpointWindowCount(1);
    Slice one = ManagedStateTestUtils.getSliceFor("1");
    testMeta.managedState.setup(testMeta.operatorContext);
    long time = System.currentTimeMillis();
    testMeta.managedState.beginWindow(time);
    testMeta.managedState.put(0, one, one);
    testMeta.managedState.endWindow();

    Future<Slice> valFuture = testMeta.managedState.getAsync(0, one);
    Assert.assertEquals("value of one", one, valFuture.get());
    testMeta.managedState.teardown();
  }

  @Test
  public void testCommitted()
  {
    Slice one = ManagedStateTestUtils.getSliceFor("1");
    Slice two = ManagedStateTestUtils.getSliceFor("2");
    commitHelper(one, two);
    Bucket.DefaultBucket defaultBucket = (Bucket.DefaultBucket)testMeta.managedState.getBucket(0);
    Assert.assertEquals("value of one", one, defaultBucket.getCommittedData().get(one).getValue());

    Assert.assertNull("value of two", defaultBucket.getCommittedData().get(two));
    testMeta.managedState.teardown();
  }

  @Test
  public void testAsyncGetFromCommitted() throws ExecutionException, InterruptedException
  {
    Slice one = ManagedStateTestUtils.getSliceFor("1");
    Slice two = ManagedStateTestUtils.getSliceFor("2");
    commitHelper(one, two);
    Future<Slice> valFuture = testMeta.managedState.getAsync(0, one);
    Assert.assertEquals("value of one", one, valFuture.get());
  }

  private void commitHelper(Slice one, Slice two)
  {
    testMeta.managedState.setIncrementalCheckpointWindowCount(1);

    testMeta.managedState.setup(testMeta.operatorContext);
    long time = System.currentTimeMillis();
    testMeta.managedState.beginWindow(time);
    testMeta.managedState.put(0, one, one);
    testMeta.managedState.endWindow();

    testMeta.managedState.beginWindow(time + 1);
    testMeta.managedState.put(0, two, two);
    testMeta.managedState.endWindow();

    testMeta.managedState.committed(time);
  }

  @Test
  public void testAsyncGetFromReaders() throws IOException, ExecutionException, InterruptedException
  {
    Slice zero = ManagedStateTestUtils.getSliceFor("0");
    long time = System.currentTimeMillis();

    //Save data to the file system.
    BucketsDataManager dataManager = new BucketsDataManager(testMeta.managedState);
    testMeta.managedState.getFileAccess().init();

    Map<Slice, Bucket.BucketedValue> unsavedBucket0 = ManagedStateTestUtils.getTestBucketData(0, time);
    dataManager.transferBucket(time, 0, unsavedBucket0);
    ManagedStateTestUtils.transferBucketHelper(testMeta.managedState.getFileAccess(), 0, unsavedBucket0, 1);

    testMeta.managedState.setDataManager(dataManager);
    testMeta.managedState.setup(testMeta.operatorContext);
    Future<Slice> valFuture = testMeta.managedState.getAsync(0, zero);

    Assert.assertEquals("value of zero", zero, valFuture.get());
    testMeta.managedState.teardown();
  }

  @Test
  public void testPutGetWithTime()
  {
    Slice one = ManagedStateTestUtils.getSliceFor("1");
    testMeta.managedState.setup(testMeta.operatorContext);
    long time = System.currentTimeMillis();
    testMeta.managedState.beginWindow(0);
    testMeta.managedState.put(0, time, one, one);
    Slice value = testMeta.managedState.getSync(0, time, one);
    testMeta.managedState.endWindow();

    Assert.assertEquals("value of one", one, value);
    testMeta.managedState.teardown();
  }

  @Test
  public void testAsyncGetWithTime() throws ExecutionException, InterruptedException
  {
    Slice one = ManagedStateTestUtils.getSliceFor("1");
    testMeta.managedState.setup(testMeta.operatorContext);
    long time = System.currentTimeMillis();
    testMeta.managedState.beginWindow(0);
    testMeta.managedState.put(0, time, one, one);
    Future<Slice> valFuture = testMeta.managedState.getAsync(0, time, one);
    Slice value = valFuture.get();

    Assert.assertEquals("value of one", one, value);
    testMeta.managedState.teardown();
  }

  @Test
  public void testRecovery() throws ExecutionException, InterruptedException
  {
    testMeta.managedState.setIncrementalCheckpointWindowCount(1);

    Slice one = ManagedStateTestUtils.getSliceFor("1");
    testMeta.managedState.setup(testMeta.operatorContext);
    long time = System.currentTimeMillis();
    testMeta.managedState.beginWindow(0);
    testMeta.managedState.put(0, time, one, one);
    testMeta.managedState.endWindow();
    testMeta.managedState.teardown();

    //there is a failure and the operator is re-deployed.
    testMeta.managedState.setStateTracker(new StateTracker(testMeta.managedState));
    testMeta.managedState.setup(testMeta.operatorContext);

    Bucket.DefaultBucket defaultBucket = (Bucket.DefaultBucket)testMeta.managedState.getBucket(0);
    Assert.assertEquals("value of one", one, defaultBucket.get(one, time, Bucket.ReadSource.MEMORY));
  }
}
