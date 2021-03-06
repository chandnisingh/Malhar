/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.math;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.lib.testbench.CountAndLastTupleTestSink;

/**
 * Functional tests for {@link com.datatorrent.lib.math.ExceptMap}
 */
public class ExceptMapTest
{
  /**
   * Test node logic emits correct results
   */
  @Test
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new ExceptMap<String, Integer>());
    testNodeProcessingSchema(new ExceptMap<String, Double>());
    testNodeProcessingSchema(new ExceptMap<String, Float>());
    testNodeProcessingSchema(new ExceptMap<String, Short>());
    testNodeProcessingSchema(new ExceptMap<String, Long>());
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testNodeProcessingSchema(ExceptMap oper)
  {
    CountAndLastTupleTestSink exceptSink = new CountAndLastTupleTestSink();
    oper.except.setSink(exceptSink);
    oper.setKey("a");
    oper.setValue(3.0);
    oper.setTypeEQ();

    oper.beginWindow(0);
    HashMap<String, Number> input = new HashMap<String, Number>();
    input.put("a", 2);
    input.put("b", 20);
    input.put("c", 1000);
    oper.data.process(input);
    input.clear();
    input.put("a", 3);
    oper.data.process(input);
    oper.endWindow();

    // One for each key
    Assert.assertEquals("number emitted tuples", 1, exceptSink.count);
    for (Map.Entry<String, Number> e : ((HashMap<String, Number>) exceptSink.tuple)
      .entrySet()) {
      if (e.getKey().equals("a")) {
        Assert.assertEquals("emitted value for 'a' was ", new Double(2), e
          .getValue().doubleValue(), 0);
      }
      else if (e.getKey().equals("b")) {
        Assert.assertEquals("emitted tuple for 'b' was ", new Double(20), e
          .getValue().doubleValue(), 0);
      }
      else if (e.getKey().equals("c")) {
        Assert.assertEquals("emitted tuple for 'c' was ", new Double(1000), e
          .getValue().doubleValue(), 0);
      }
    }
  }
}
