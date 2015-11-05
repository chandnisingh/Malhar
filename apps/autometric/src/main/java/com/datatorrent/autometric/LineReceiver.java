/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.autometric;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.io.block.AbstractBlockReader;

public class LineReceiver extends BaseOperator
{
  @AutoMetric
  int length;

  @AutoMetric
  int count;

  public final transient DefaultInputPort<AbstractBlockReader.ReaderRecord<String>> input =
    new DefaultInputPort<AbstractBlockReader.ReaderRecord<String>>()
    {
      @Override
      public void process(AbstractBlockReader.ReaderRecord<String> s)
      {
        length += s.getRecord().length();
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
