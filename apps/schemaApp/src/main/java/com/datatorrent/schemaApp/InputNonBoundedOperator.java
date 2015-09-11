/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.schemaApp;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

/**
 * @category schema
 * @param <T>
 */
public class InputNonBoundedOperator<T> implements InputOperator
{

  public final DefaultOutputPort<T> output = new DefaultOutputPort<>();

  @Override
  public void emitTuples()
  {
  }

  @Override
  public void beginWindow(long l)
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
