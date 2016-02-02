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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.commons.io.FileUtils;

import com.esotericsoftware.kryo.Kryo;

import com.datatorrent.api.Context;
import com.datatorrent.lib.fileaccess.FileAccessFSImpl;
import com.datatorrent.lib.util.TestUtils;
import com.datatorrent.netlet.util.Slice;

public class TimeManagedStateImplTest
{
  class TestMeta extends TestWatcher
  {
    TimeManagedStateImpl managedState;
    Context.OperatorContext operatorContext;
    String applicationPath;

    @Override
    protected void starting(Description description)
    {
      managedState = new TimeManagedStateImpl();
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
    Kryo kryo = new Kryo();
    TimeManagedStateImpl deserialized = TestUtils.clone(kryo, testMeta.managedState);
    Assert.assertEquals("num buckets", deserialized.getNumBuckets(), testMeta.managedState.getNumBuckets());
  }

  @Test
  public void testPutGet()
  {
    Slice one = ManagedStateTestUtils.getSliceFor("1");
    testMeta.managedState.setup(testMeta.operatorContext);
    long time = System.currentTimeMillis();
    testMeta.managedState.beginWindow(0);
    testMeta.managedState.put(time, one, one);
    Slice value = testMeta.managedState.getSync(time, one);
    testMeta.managedState.endWindow();

    Assert.assertEquals("value of one", one, value);
    testMeta.managedState.teardown();
  }

  @Test
  public void testAsyncGet() throws ExecutionException, InterruptedException
  {
    Slice one = ManagedStateTestUtils.getSliceFor("1");
    testMeta.managedState.setup(testMeta.operatorContext);
    long time = System.currentTimeMillis();
    testMeta.managedState.beginWindow(0);
    testMeta.managedState.put(time, one, one);
    Future<Slice> valFuture = testMeta.managedState.getAsync(time, one);
    Slice value = valFuture.get();

    Assert.assertEquals("value of one", one, value);
    testMeta.managedState.teardown();
  }


}
