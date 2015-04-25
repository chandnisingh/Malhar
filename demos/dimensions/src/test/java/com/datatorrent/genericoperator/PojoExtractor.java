/*
 *  Copyright (c) 2012-2015 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.genericoperator;

import java.lang.reflect.Method;
import java.util.List;

import org.codehaus.janino.ExpressionEvaluator;

import com.esotericsoftware.reflectasm.MethodAccess;

/**
 *
 */
public class PojoExtractor
{
  public static String TUPLE = "t";

  private Class<?> tupleClazz;
  private List<String> expressions;
  private ExpressionContext[] compiledExpressions;

  public Class<?> getTupleClazz()
  {
    return tupleClazz;
  }

  public void setTupleClazz(Class<?> tupleClazz)
  {
    this.tupleClazz = tupleClazz;
  }

  public List<String> getExpressions()
  {
    return expressions;
  }

  public void setExpressions(List<String> expressions)
  {
    this.expressions = expressions;
  }

  private class ExpressionContext
  {
    int methodIndex;
    MethodAccess access;
    Object instance;
  }

  private ExpressionContext compile(String expression) throws Exception
  {
    ExpressionEvaluator ee = new ExpressionEvaluator();
    ee.setStaticMethod(false);
    ee.setParameters(new String[] {TUPLE}, new Class[] {tupleClazz});
    ee.setExpressionType(Object.class);
    ee.cook(TUPLE + "." + expression);

    Method method = ee.getMethod();

    ExpressionContext ec = new ExpressionContext();
    ec.access = MethodAccess.get(method.getDeclaringClass());
    ec.instance = method.getDeclaringClass().newInstance();
    ec.methodIndex = ec.access.getIndex(method.getName());
    return ec;
  }

  public void setup()
  {
    this.compiledExpressions = new ExpressionContext[expressions.size()];
    int i = 0;
    for (String expression : expressions) {
      try {
        ExpressionContext ec = compile(expression);
        this.compiledExpressions[i++] = ec;
      } catch (Exception e) {
        throw new RuntimeException("Failed to compile: "+ expression, e);
      }
    }
  }

  public Object[] execute(Object tuple)
  {
    Object[] results = new Object[compiledExpressions.length];
    for (int i=0; i<compiledExpressions.length; i++) {
      ExpressionContext ec = compiledExpressions[i];
      results[i] = ec.access.invoke(ec.instance, ec.methodIndex, tuple);
    }
    return results;
  }

}
