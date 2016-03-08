/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.state;

import java.util.concurrent.Future;

import com.datatorrent.netlet.util.Slice;

/**
 * A type of bucketed state where a bucket's data is further divided into time buckets. This requires
 * time per key to figure out which time bucket a particular key belongs to.
 * <p/>
 * The time value eases purging of aged key/value pair.
 */
public interface TimeSlicedBucketedState
{
  /**
   * Sets the value of a key in the bucket identified by bucketId. Time is used to derive which time bucket (within
   * the main bucket) a key belongs to.
   *
   * @param bucketId identifier of the bucket.
   * @param time    time associated with the key.
   * @param key     key
   * @param value   value
   */
  void put(long bucketId, long time, Slice key, Slice value);

  /**
   * Returns the value of the key in the bucket identified by bucketId. This will search for the key in all the
   * time buckets.</br>
   *
   * It retrieves the value synchronously that can be expensive.<br/>
   * {@link #getAsync(long, Slice)} is recommended for efficient reading the value of a key.
   *
   *
   * @param bucketId identifier of the bucket
   * @param key key
   * @return value
   */
  Slice getSync(long bucketId, Slice key);


  /**
   * Returns the value of key in the bucket identified by bucketId. This expects the time value.
   * Time is be used to derive the time bucket (within the main bucket) to which the key belonged and only that
   * time bucket is searched for the key.<br/>.
   *
   * It retrieves the value synchronously which can be expensive.<br/>
   * {@link #getAsync(long, long, Slice)} is recommended for efficiently reading the value of a key.
   *
   * @param bucketId identifier of the bucket.
   * @param time  time associated with the key.
   * @param key   key
   * @return      value
   */
  Slice getSync(long bucketId, long time, Slice key);

  /**
   * Returns the future using which the value is obtained. This searches for value in all time buckets.<br/>
   * This will lookup for the key in all the time-buckets within a bucket.
   * <p/>
   * If the key is present in memory, then the future has its value set when constructed.
   *
   * @param bucketId identifier of the bucket.
   * @param key      key
   * @return value
   */
  Future<Slice> getAsync(long bucketId, Slice key);

  /**
   * Returns the future using which the value is obtained. This expects the time value which is used to derive the
   * time bucket and will search for the key in that particular time bucket.
   *
   * If the key is present in memory, then the future has its value set when constructed.
   *
   * @param bucketId  identifier of the bucket.
   * @param time     time associated with the key.
   * @param key      key
   * @return value
   */
  Future<Slice> getAsync(long bucketId, long time, Slice key);
}
