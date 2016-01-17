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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class TimeBucketAssignerTest
{

  class TestMeta extends TestWatcher
  {
    TimeBucketAssigner timeBucketAssigner;

    @Override
    protected void starting(Description description)
    {
      timeBucketAssigner = new TimeBucketAssigner();
    }

    @Override
    protected void finished(Description description)
    {
      timeBucketAssigner.teardown();
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
    kryo.writeObject(output, testMeta.timeBucketAssigner);
    output.close();

    Input input = new Input(baos.toByteArray());
    TimeBucketAssigner tba1 = kryo.readObject(input, TimeBucketAssigner.class);
    Assert.assertNotNull("time bucket assigner", tba1);
  }

  @Test
  public void testNumBuckets()
  {
    testMeta.timeBucketAssigner.setExpireBefore(Duration.standardHours(1));
    testMeta.timeBucketAssigner.setBucketSpan(Duration.standardMinutes(30));

    testMeta.timeBucketAssigner.setup(ManagedStateTestUtils.getOperatorContext(9));

    Assert.assertEquals("num buckets", 2, testMeta.timeBucketAssigner.getNumBuckets());
  }

  @Test
  public void testTimeBucketKey()
  {
    testMeta.timeBucketAssigner.setExpireBefore(Duration.standardHours(1));
    testMeta.timeBucketAssigner.setBucketSpan(Duration.standardMinutes(30));

    long referenceTime = System.currentTimeMillis();
    testMeta.timeBucketAssigner.setup(ManagedStateTestUtils.getOperatorContext(9));

    long time1 = referenceTime - Duration.standardMinutes(2).getMillis();
    Assert.assertEquals("time bucket", 1, testMeta.timeBucketAssigner.getTimeBucketFor(time1));

    long time0 = referenceTime - Duration.standardMinutes(40).getMillis();
    Assert.assertEquals("time bucket", 0, testMeta.timeBucketAssigner.getTimeBucketFor(time0));

    long expiredTime = referenceTime - Duration.standardMinutes(65).getMillis();
    Assert.assertEquals("time bucket", -1, testMeta.timeBucketAssigner.getTimeBucketFor(expiredTime));
  }

  @Test
  public void testSlidingOnlyBetweenWindow() throws InterruptedException
  {
    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicInteger timesCalled = new AtomicInteger();
    testMeta.timeBucketAssigner.register(new TimeBucketAssigner.Listener()
    {
      @Override
      public void purgeTimeBucketsBefore(long time)
      {
        timesCalled.getAndIncrement();
        latch.countDown();
      }
    });

    testMeta.timeBucketAssigner.setup(ManagedStateTestUtils.getOperatorContext(9));
    testMeta.timeBucketAssigner.beginWindow(0);
    latch.await();
    testMeta.timeBucketAssigner.endWindow();
    int valueBeforeSleep = timesCalled.get();
    Thread.sleep(1000);
    Assert.assertEquals("value should not change", valueBeforeSleep, timesCalled.get());
  }

}
