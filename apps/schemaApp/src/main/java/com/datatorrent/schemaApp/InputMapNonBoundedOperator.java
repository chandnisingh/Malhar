/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.schemaApp;

import java.util.Map;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

/**
 * @category schema
 * @param <K>
 * @param <V>
 */
public class InputMapNonBoundedOperator<K,V> implements InputOperator
{
  public final transient DefaultOutputPort<Map<K, V>> output = new DefaultOutputPort<>();
  @Override
  public void emitTuples()
  {

  }

  @Override
  public void beginWindow(long windowId)
  {

  }

  @Override
  public void endWindow()
  {

  }

  @Override
  public void setup(Context.OperatorContext context)
  {

  }

  @Override
  public void teardown()
  {

  }
}
