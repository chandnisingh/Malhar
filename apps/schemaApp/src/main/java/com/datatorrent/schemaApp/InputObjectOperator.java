/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.schemaApp;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

/**
 * Input Object operator<br/>
 * No schema required.
 * @category schema
 */
public class InputObjectOperator implements InputOperator
{

  public final DefaultOutputPort<Object> output = new DefaultOutputPort<>();

  @Override
  public void emitTuples()
  {
    for (int i = 0; i < 5; i++) {
      output.emit(i);
    }
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
