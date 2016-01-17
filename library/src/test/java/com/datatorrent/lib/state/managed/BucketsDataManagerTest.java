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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import javax.validation.constraints.NotNull;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Preconditions;

import com.datatorrent.api.Context;
import com.datatorrent.lib.fileaccess.FileAccessFSImpl;
import com.datatorrent.netlet.util.Slice;

public class BucketsDataManagerTest
{
  class TestMeta extends TestWatcher
  {
    BucketsDataManager dataManager;
    String applicationPath;
    int operatorId = 1;
    MockManagedStateContext managedStateContext;
    Context.OperatorContext operatorContext;

    @Override
    protected void starting(Description description)
    {
      managedStateContext = new MockManagedStateContext();
      managedStateContext.setTimeBucketAssigner(new MockTimeBucketsAssigner());
      applicationPath = "target/" + description.getClassName() + "/" + description.getMethodName();
      ((FileAccessFSImpl)managedStateContext.getFileAccess()).setBasePath(applicationPath + "/" + "bucket_data");
      managedStateContext.getFileAccess().init();

      dataManager = new BucketsDataManager(managedStateContext);
      operatorContext = ManagedStateTestUtils.getOperatorContext(operatorId, applicationPath);


      managedStateContext.getTimeBucketAssigner().setup(testMeta.operatorContext);

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
  public void testSerde()
  {
    Kryo kryo = new Kryo();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Output output = new Output(baos);
    kryo.writeObject(output, testMeta.dataManager);
    output.close();

    Input input = new Input(baos.toByteArray());
    BucketsDataManager dataManager2 = kryo.readObject(input, BucketsDataManager.class);
    Assert.assertNotNull("state window data manager", dataManager2);
  }

  @Test
  public void testSave() throws IOException
  {
    testMeta.dataManager.setup(testMeta.operatorContext);
    Map<Long, Map<Slice, Bucket.BucketedValue>> buckets5 = ManagedStateTestUtils.getTestData(0, 5, 0);
    testMeta.dataManager.save(buckets5, testMeta.operatorId, 10);
    testMeta.dataManager.teardown();

    testMeta.dataManager = new BucketsDataManager(testMeta.managedStateContext);

    testMeta.dataManager.setup(testMeta.operatorContext);
    @SuppressWarnings("unchecked")
    Map<Long, Map<Slice, Bucket.BucketedValue>> buckets5After = (Map<Long, Map<Slice, Bucket.BucketedValue>>)
        testMeta.dataManager.load(testMeta.operatorId, 10);

    Assert.assertEquals("saved", buckets5, buckets5After);
    testMeta.dataManager.teardown();
  }

  @Test
  public void testLoad() throws IOException
  {
    testMeta.dataManager.setup(testMeta.operatorContext);

    Map<Long, Map<Slice, Bucket.BucketedValue>> buckets5 = ManagedStateTestUtils.getTestData(0, 5, 0);
    testMeta.dataManager.save(buckets5, testMeta.operatorId, 10);

    Map<Long, Map<Slice, Bucket.BucketedValue>> buckets10 = ManagedStateTestUtils.getTestData(5, 10, 0);
    testMeta.dataManager.save(buckets10, testMeta.operatorId, 20);
    testMeta.dataManager.teardown();

    testMeta.dataManager = new BucketsDataManager(testMeta.managedStateContext);
    testMeta.dataManager.setup(testMeta.operatorContext);

    Map<Long, Map<Slice, Bucket.BucketedValue>> decoded = testMeta.dataManager.load(testMeta.operatorId);
    //combining window 10 and 20
    buckets5.putAll(buckets10);

    Assert.assertEquals("load", buckets5, decoded);
    testMeta.dataManager.teardown();
  }

  @Test
  public void testTransferBucket() throws IOException
  {
    Map<Slice, Bucket.BucketedValue> unsavedBucket0 = ManagedStateTestUtils.getTestBucketData(0);
    testMeta.dataManager.transferBucket(10, 0, unsavedBucket0);

    ManagedStateTestUtils.transferBucketHelper(testMeta.managedStateContext.getFileAccess(), 0, unsavedBucket0, 1);
  }

  @Test
  public void testTransferOfExistingBucket() throws IOException
  {
    Map<Slice, Bucket.BucketedValue> unsavedBucket0 = ManagedStateTestUtils.getTestBucketData(0);
    testMeta.dataManager.transferBucket(10, 0, unsavedBucket0);

    Map<Slice, Bucket.BucketedValue> more = ManagedStateTestUtils.getTestBucketData(50);
    testMeta.dataManager.transferBucket(10, 0, more);

    unsavedBucket0.putAll(more);
    ManagedStateTestUtils.transferBucketHelper(testMeta.managedStateContext.getFileAccess(), 0, unsavedBucket0, 2);
  }

  @Test
  public void testTransferWindowFiles() throws IOException, InterruptedException
  {
    testMeta.dataManager.setup(testMeta.operatorContext);
    Map<Long, Map<Slice, Bucket.BucketedValue>> buckets5 = ManagedStateTestUtils.getTestData(0, 5, 0);
    testMeta.dataManager.save(buckets5, testMeta.operatorId, 10);
    //Need to synchronously call transfer window files so shutting down the other thread.
    testMeta.dataManager.teardown();
    Thread.sleep(500);

    testMeta.dataManager.committed(testMeta.operatorId, 10);
    testMeta.dataManager.transferWindowFiles();

    for (int i = 0; i < 5; i++) {
      ManagedStateTestUtils.transferBucketHelper(testMeta.managedStateContext.getFileAccess(), i, buckets5.get((long)i), 1);
    }
  }

  @Test
  public void testCommitted() throws IOException, InterruptedException
  {
    CountDownLatch latch = new CountDownLatch(5);
    testMeta.dataManager = new MockBucketsDataManager(testMeta.managedStateContext, latch);
    testMeta.dataManager.setup(testMeta.operatorContext);
    Map<Long, Map<Slice, Bucket.BucketedValue>> data = ManagedStateTestUtils.getTestData(0, 5, 0);
    testMeta.dataManager.save(data, testMeta.operatorId, 10);
    testMeta.dataManager.committed(testMeta.operatorId, 10);
    latch.await();
    testMeta.dataManager.teardown();
    Thread.sleep(500);

    for (int i = 0; i < 5; i++) {
      ManagedStateTestUtils.transferBucketHelper(testMeta.managedStateContext.getFileAccess(), i, data.get((long)i), 1);
    }
  }

  @Test
  public void testDeleteFilesBefore() throws IOException, InterruptedException
  {
    testTransferWindowFiles();
    testMeta.dataManager.deleteFilesBefore(200);
    RemoteIterator<LocatedFileStatus> iterator = testMeta.managedStateContext.getFileAccess().listFiles();
    if (iterator.hasNext()) {
      Assert.fail("All buckets should be deleted");
    }
  }

  static class MockBucketsDataManager extends BucketsDataManager
  {
    private final transient CountDownLatch latch;

    public MockBucketsDataManager(@NotNull ManagedStateContext managedStateContext, @NotNull CountDownLatch latch)
    {
      super(managedStateContext);
      this.latch = Preconditions.checkNotNull(latch);
    }

    @Override
    protected void transferBucket(long windowId, long bucketId, Map<Slice, Bucket.BucketedValue> data)
        throws IOException
    {
      super.transferBucket(windowId, bucketId, data);
      if (windowId == 10) {
        latch.countDown();
      }
    }
  }

  static class MockTimeBucketsAssigner extends TimeBucketAssigner
  {
    @Override
    public long getStartTimeFor(long timeBucket)
    {
      return timeBucket;
    }
  }

  private static final transient Logger LOG = LoggerFactory.getLogger(BucketsDataManagerTest.class);
}
