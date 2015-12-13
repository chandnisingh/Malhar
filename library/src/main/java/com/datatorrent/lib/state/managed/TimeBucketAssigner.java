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

import java.util.Calendar;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import javax.validation.constraints.NotNull;

import org.joda.time.Duration;

import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.google.common.collect.Sets;

import com.datatorrent.api.Context;
import com.datatorrent.api.Operator;

/**
 * Keeps track of time buckets.<br/>
 *
 * The data of a bucket is further divided into time-buckets. This component controls the length of time buckets,
 * which time-bucket an event falls into and sliding the time boundaries.
 * <p/>
 *
 * The configuration {@link #expireBefore} and {@link #bucketSpan}  are used to calculate number of time-buckets.
 * For eg. if <code>expireBefore = 1 hour</code> and <code>bucketSpan = 30 minutes</code>, then <code>
 *   numBuckets = 60 minutes/ 30 minutes = 2 </code>.
 * <p/>
 *
 * The time boundaries- start and end, periodically move by span of a single time-bucket. Any event with time < start
 * is expired. These boundaries slide between application window by another thread and not the operator thread.
 */
public class TimeBucketAssigner implements Operator
{
  @NotNull
  @FieldSerializer.Bind(JavaSerializer.class)
  private Duration expireBefore = Duration.standardDays(2);

  @FieldSerializer.Bind(JavaSerializer.class)
  private Duration bucketSpan;

  private long bucketSpanMillis;

  private long fixedStartTime;
  private long startTime;
  private long endTime;
  private int numBuckets;

  private boolean initialized;

  private transient long spinMillis;
  private transient long lastAdvanceTime = System.currentTimeMillis();
  private transient volatile boolean runExpiryTask;

  private transient boolean isPaused = true;
  private transient ReentrantLock pauseLock = new ReentrantLock();
  private transient Condition unpaused = pauseLock.newCondition();

  @NotNull
  private final transient Set<Listener> listeners = Sets.newHashSet();

  private final transient Runnable expiryTask = new Runnable()
  {
    @Override
    public void run()
    {
      long time;
      while (runExpiryTask) {

        pauseLock.lock();
        try {
          while (isPaused) {
            unpaused.await();
          }
        } catch (InterruptedException e) {
          //interruption is expected by operator thread
        } finally {
          pauseLock.unlock();
        }

        if (runExpiryTask) {
          time = System.currentTimeMillis();
          if (time - lastAdvanceTime >= bucketSpanMillis) {
            synchronized (lock) {
              lastAdvanceTime = time;
              startTime += bucketSpanMillis;
              endTime += bucketSpanMillis;
              for (Listener listener : listeners) {
                listener.purgeTimeBucketsBefore(startTime);
              }
            }
          } else {
            try {
              Thread.sleep(spinMillis);
            } catch (InterruptedException e) {
              //interruption is expected by operator thread
            }
          }
        }
      }
    }
  };

  private final transient Thread expiryThread = new Thread(expiryTask);

  private final transient Object lock = new Object();

  @Override
  public void setup(Context.OperatorContext context)
  {
    spinMillis = context.getValue(Context.OperatorContext.SPIN_MILLIS);
    if (!initialized) {
      if (bucketSpan == null) {
        bucketSpan = Duration.millis(context.getValue(Context.OperatorContext.APPLICATION_WINDOW_COUNT) *
            context.getValue(Context.DAGContext.STREAMING_WINDOW_SIZE_MILLIS));
      }
      Calendar calendar = Calendar.getInstance();
      long now = calendar.getTimeInMillis();
      fixedStartTime = now - expireBefore.getMillis();
      startTime = fixedStartTime;

      bucketSpanMillis = bucketSpan.getMillis();
      numBuckets = (int)Math.ceil((now - fixedStartTime) / (bucketSpanMillis * 1.0));
      endTime = startTime + (numBuckets * bucketSpanMillis);

      initialized = true;
    }
    runExpiryTask = true;
    expiryThread.start();
  }

  @Override
  public void beginWindow(long l)
  {
    pauseLock.lock();
    try {
      isPaused = false;
      unpaused.signalAll();
    } finally {
      pauseLock.unlock();
    }
  }

  @Override
  public void endWindow()
  {
    pauseLock.lock();
    try {
      isPaused = true;
    } finally {
      pauseLock.unlock();
    }
  }

  /**
   * Get the bucket key for the long value.
   *
   * @param value value from which bucket key is derived.
   * @return -1 if value is already expired; bucket key otherwise.
   */
  public long getTimeBucketFor(long value)
  {
    long lstart;
    long lend;
    synchronized (lock) {
      lstart = startTime;
      lend = endTime;
    }
    if (value < lstart) {
      return -1;
    }
    long diffFromStart = value - fixedStartTime;
    long key = diffFromStart / bucketSpanMillis;
    synchronized (lock) {
      if (value > lend) {
        long move = ((value - lend) / bucketSpanMillis + 1) * bucketSpanMillis;
        startTime = lstart + move;
        endTime = lend + move;
      }
    }
    return key;
  }

  public long getStartTimeFor(long timeBucket)
  {
    return (timeBucket * bucketSpanMillis) + fixedStartTime;
  }

  public void register(@NotNull Listener listener)
  {
    if (!listeners.contains(listener)) {
      listeners.add(listener);
    }
  }

  @Override
  public void teardown()
  {
    runExpiryTask = false;
    try {
      expiryThread.join(spinMillis);
    } catch (InterruptedException e) {
      //Current thread is interrupted while waiting for expiry thread to finish.
      throw new RuntimeException("interrupted while waiting for expiry to finish", e);
    }
  }

  /**
   * @return number of buckets.
   */
  public int getNumBuckets()
  {
    return numBuckets;
  }

  /**
   * @return duration before which the data is expired.
   */
  public Duration getExpireBefore()
  {
    return expireBefore;
  }

  /**
   * Sets the duration which denotes expiry. Any event with time before this duration is considered to be expired.
   * @param expireBefore duration
   */
  public void setExpireBefore(Duration expireBefore)
  {
    this.expireBefore = expireBefore;
  }

  /**
   * @return time-bucket span
   */
  public Duration getBucketSpan()
  {
    return bucketSpan;
  }

  /**
   * Sets the length of a time bucket.
   * @param bucketSpan length of time bucket
   */
  public void setBucketSpan(Duration bucketSpan)
  {
    this.bucketSpan = bucketSpan;
  }

  /**
   * The listeners are informed when the start time slides and time buckets which are older than the new start time
   * can be purged.
   */
  public interface Listener
  {
    void purgeTimeBucketsBefore(long time);
  }

}
