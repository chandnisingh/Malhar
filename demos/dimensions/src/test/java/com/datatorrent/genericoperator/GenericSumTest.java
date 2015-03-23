/*
 *  Copyright (c) 2012-2015 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.genericoperator;

import org.junit.Test;

import junit.framework.Assert;

import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;

/**
 *
 */
public class GenericSumTest
{
  public static class Input
  {
    public int count;
  }

  public static class Output
  {
    public int sum;
  }

  @Test
  public void testOperator()
  {
      GenericSum oper = new GenericSum();
      CollectorTestSink<Output> sink = TestUtils.setSink(oper.output, new CollectorTestSink<Output>());

      oper.inputClass = Input.class;
      oper.outputClass = Output.class;
      oper.expression = "output.sum += input.count";

      oper.setup(null);
      oper.beginWindow(0);

      Input inp = new Input();
      inp.count = 3;

      oper.input.process(inp);

      oper.endWindow();
      Assert.assertEquals(1, sink.collectedTuples.size());

  }
}
