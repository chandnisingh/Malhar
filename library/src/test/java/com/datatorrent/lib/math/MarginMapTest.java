/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.math;

import com.datatorrent.lib.math.MarginMap;
import com.datatorrent.lib.testbench.CountAndLastTupleTestSink;

import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.math.MarginMap}<p>
 *
 */
public class MarginMapTest
{
  private static Logger LOG = LoggerFactory.getLogger(MarginMapTest.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new MarginMap<String, Integer>());
    testNodeProcessingSchema(new MarginMap<String, Double>());
    testNodeProcessingSchema(new MarginMap<String, Float>());
    testNodeProcessingSchema(new MarginMap<String, Short>());
    testNodeProcessingSchema(new MarginMap<String, Long>());
  }

  public void testNodeProcessingSchema(MarginMap oper)
  {
    CountAndLastTupleTestSink marginSink = new CountAndLastTupleTestSink();

    oper.margin.setSink(marginSink);

    oper.beginWindow(0);
    HashMap<String, Number> input = new HashMap<String, Number>();
    input.put("a", 2);
    input.put("b", 20);
    input.put("c", 1000);
    oper.numerator.process(input);

    input.clear();
    input.put("a", 2);
    input.put("b", 40);
    input.put("c", 500);
    oper.denominator.process(input);

    oper.endWindow();

    // One for each key
    Assert.assertEquals("number emitted tuples", 1, marginSink.count);

    HashMap<String, Number> output = (HashMap<String, Number>)marginSink.tuple;
    for (Map.Entry<String, Number> e: output.entrySet()) {
      LOG.debug(String.format("Key, value is %s,%f", e.getKey(), e.getValue().doubleValue()));
      if (e.getKey().equals("a")) {
        Assert.assertEquals("emitted value for 'a' was ", new Double(0), e.getValue().doubleValue());
      }
      else if (e.getKey().equals("b")) {
        Assert.assertEquals("emitted tuple for 'b' was ", new Double(0.5), e.getValue().doubleValue());
      }
      else if (e.getKey().equals("c")) {
        Assert.assertEquals("emitted tuple for 'c' was ", new Double(-1.0), e.getValue().doubleValue());
      }
      else {
        LOG.debug(String.format("key was %s", e.getKey()));
      }
    }
  }
}