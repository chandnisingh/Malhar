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

import java.util.Comparator;

import com.datatorrent.lib.fileaccess.FileAccess;
import com.datatorrent.lib.fileaccess.TFileImpl;
import com.datatorrent.lib.util.comparator.SliceComparator;
import com.datatorrent.netlet.util.Slice;

class MockManagedStateContext implements ManagedStateContext
{
  private TFileImpl.DTFileImpl fileAccess = new TFileImpl.DTFileImpl();
  private Comparator<Slice> keyComparator = new SliceComparator();
  private BucketsMetaDataManager bucketsMetaDataManager = new BucketsMetaDataManager(this);
  private TimeBucketAssigner timeBucketAssigner = new TimeBucketAssigner();

  @Override
  public FileAccess getFileAccess()
  {
    return fileAccess;
  }

  @Override
  public Comparator<Slice> getKeyComparator()
  {
    return keyComparator;
  }

  @Override
  public BucketsMetaDataManager getBucketsMetaDataManager()
  {
    return bucketsMetaDataManager;
  }

  @Override
  public TimeBucketAssigner getTimeBucketAssigner()
  {
    return timeBucketAssigner;
  }

  void setFileAccess(TFileImpl.DTFileImpl fileAccess)
  {
    this.fileAccess = fileAccess;
  }

  void setKeyComparator(Comparator<Slice> keyComparator)
  {
    this.keyComparator = keyComparator;
  }

  void setBucketsMetaDataManager(BucketsMetaDataManager bucketsMetaDataManager)
  {
    this.bucketsMetaDataManager = bucketsMetaDataManager;
  }

  void setTimeBucketAssigner(TimeBucketAssigner timeBucketAssigner)
  {
    this.timeBucketAssigner = timeBucketAssigner;
  }
}
