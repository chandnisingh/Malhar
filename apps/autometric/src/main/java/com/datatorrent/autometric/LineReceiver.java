/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.autometric;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;

public class LineReceiver extends BaseOperator
{
  @AutoMetric
  long length;

  @AutoMetric
  long count;

  public final transient DefaultInputPort<String> input = new DefaultInputPort<String>()
  {
    @Override
    public void process(String s)
    {
      length += s.length();
      count++;
    }
  };

  @Override
  public void beginWindow(long windowId)
  {
    length = 0;
    count = 0;
  }
}
