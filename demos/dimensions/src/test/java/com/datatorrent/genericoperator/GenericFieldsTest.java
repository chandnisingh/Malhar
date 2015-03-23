/*
 *  Copyright (c) 2012-2015 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.genericoperator;

import java.lang.reflect.Method;

import org.codehaus.janino.ExpressionEvaluator;
import org.junit.Test;

import com.esotericsoftware.reflectasm.MethodAccess;

public class GenericFieldsTest
{

  public static class TestBean
  {
    private int intValue = 99;

    public int getIntValue()
    {
      return intValue;
    }

    public void setIntValue(int intValue)
    {
      this.intValue = intValue;
    }
  }

  @Test
  public void test() throws Exception
  {
    int numCalls = 10000000;
    TestBean bean = new TestBean();
    Method m = TestBean.class.getMethod("getIntValue");

    long tms = System.currentTimeMillis();
    for (int i=0; i<numCalls; i++) {
      Object r = m.invoke(bean);
      //System.out.println("" + r);
    }
    System.out.println("reflection took " + (System.currentTimeMillis() - tms) + "ms");

    MethodAccess methodAccess = MethodAccess.get(TestBean.class);
    int methodIndex = methodAccess.getIndex("getIntValue");
    tms = System.currentTimeMillis();
    for (int i=0; i<numCalls; i++) {
      Object r = methodAccess.invoke(bean, methodIndex);
    }
    System.out.println("ASM generated code took " + (System.currentTimeMillis() - tms) + "ms");

    // janino
    ExpressionEvaluator ee = new ExpressionEvaluator(
        "tuple.getIntValue()",                     // expression
        int.class,                           // expressionType
        new String[] { "tuple" },           // parameterNames
        new Class[] { TestBean.class } // parameterTypes
    );
    tms = System.currentTimeMillis();
    Object[] args = new Object[] {bean};
    for (int i=0; i<numCalls; i++) {
      Object r = ee.evaluate(args);
    }
    System.out.println("Janino generated code took " + (System.currentTimeMillis() - tms) + "ms");


    tms = System.currentTimeMillis();
    for (int i=0; i<numCalls; i++) {
      Object r = bean.getIntValue();
    }
    System.out.println("static call took " + (System.currentTimeMillis() - tms) + "ms");

  }


  @Test
  public void janinoTest() throws Exception
  {
 // Compile the expression once; relatively slow.
    ExpressionEvaluator ee = new ExpressionEvaluator(
        "c > d ? c : d",                     // expression
        int.class,                           // expressionType
        new String[] { "c", "d" },           // parameterNames
        new Class[] { int.class, int.class } // parameterTypes
    );

    // Evaluate it with varying parameter values; very fast.
    Integer res = (Integer) ee.evaluate(
        new Object[] {          // parameterValues
            new Integer(10),
            new Integer(11),
        }
    );
    System.out.println("res = " + res);
  }

}
