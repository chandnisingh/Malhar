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
import java.util.Iterator;
import java.util.TreeSet;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.commons.io.FileUtils;

import com.datatorrent.lib.fileaccess.FileAccessFSImpl;
import com.datatorrent.netlet.util.Slice;

public class BucketsMetaDataManagerTest
{
  class TestMeta extends TestWatcher
  {
    BucketsMetaDataManager metaDataManager;
    String applicationPath;
    MockManagedStateContext managedStateContext;

    @Override
    protected void starting(Description description)
    {
      managedStateContext = new MockManagedStateContext();
      applicationPath = "target/" + description.getClassName() + "/" + description.getMethodName();
      ((FileAccessFSImpl)managedStateContext.getFileAccess()).setBasePath(applicationPath + "/" + "bucket_data");
      managedStateContext.getFileAccess().init();

      metaDataManager = new BucketsMetaDataManager(managedStateContext);

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
  public void testUpdateBucketMetaDataFile() throws IOException
  {
    BucketsMetaDataManager.MutableTimeBucketMeta mutableTbm = testMeta.metaDataManager.getOrCreateTimeBucketMeta(1, 1);
    mutableTbm.setLastTransferredWindowId(10);
    mutableTbm.setSizeInBytes(100);
    mutableTbm.setFirstKey(new Slice("1".getBytes()));

    testMeta.metaDataManager.updateBucketMetaFile(1);
    BucketsMetaDataManager.ImmutableTimeBucketMeta immutableTbm = testMeta.metaDataManager.getTimeBucketMeta(1, 1);
    Assert.assertNotNull(immutableTbm);
    Assert.assertEquals("last transferred window", 10, immutableTbm.getLastTransferredWindowId());
    Assert.assertEquals("size in bytes", 100, immutableTbm.getSizeInBytes());
    Assert.assertEquals("first key", "1", immutableTbm.getFirstKey().stringValue());
  }

  @Test
  public void testGetTimeBucketMeta() throws IOException
  {
    BucketsMetaDataManager.ImmutableTimeBucketMeta bucketMeta = testMeta.metaDataManager.getTimeBucketMeta(1, 1);
    Assert.assertNull("bucket meta", bucketMeta);

    testMeta.metaDataManager.getOrCreateTimeBucketMeta(1, 1);
    bucketMeta = testMeta.metaDataManager.getTimeBucketMeta(1, 1);
    Assert.assertNotNull("bucket meta not null", bucketMeta);
  }

  @Test
  public void testGetAllTimeBucketMeta() throws IOException
  {
    BucketsMetaDataManager.MutableTimeBucketMeta tbm1 = testMeta.metaDataManager.getOrCreateTimeBucketMeta(1, 1);
    tbm1.setLastTransferredWindowId(10);
    tbm1.setSizeInBytes(100);
    tbm1.setFirstKey(new Slice("1".getBytes()));

    BucketsMetaDataManager.MutableTimeBucketMeta tbm2 = testMeta.metaDataManager.getOrCreateTimeBucketMeta(1, 2);
    tbm2.setLastTransferredWindowId(10);
    tbm2.setSizeInBytes(100);
    tbm2.setFirstKey(new Slice("2".getBytes()));

    testMeta.metaDataManager.updateBucketMetaFile(1);
    TreeSet<BucketsMetaDataManager.ImmutableTimeBucketMeta> timeBucketMetas =
        testMeta.metaDataManager.getAllTimeBuckets(1);

    Iterator<BucketsMetaDataManager.ImmutableTimeBucketMeta> iterator = timeBucketMetas.iterator();
    int i = 2;
    while (iterator.hasNext()) {
      BucketsMetaDataManager.ImmutableTimeBucketMeta tbm = iterator.next();
      Assert.assertEquals("time bucket " + i, i, tbm.getTimeBucketId());
      i--;
    }
  }

  @Test
  public void testInvalidateTimeBucket() throws IOException
  {
    testGetAllTimeBucketMeta();
    testMeta.metaDataManager.invalidateTimeBucket(1, 1);
    BucketsMetaDataManager.ImmutableTimeBucketMeta immutableTbm = testMeta.metaDataManager.getTimeBucketMeta(1,1);
    Assert.assertNull("deleted tbm", immutableTbm);

    TreeSet<BucketsMetaDataManager.ImmutableTimeBucketMeta> timeBucketMetas =
        testMeta.metaDataManager.getAllTimeBuckets(1);

    Assert.assertEquals("only 1 tbm", 1, timeBucketMetas.size());
    immutableTbm = timeBucketMetas.iterator().next();

    Assert.assertEquals("tbm 2", 2, immutableTbm.getTimeBucketId());
  }
}
