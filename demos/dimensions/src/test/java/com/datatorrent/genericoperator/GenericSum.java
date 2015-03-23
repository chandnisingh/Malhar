/*
 *  Copyright (c) 2012-2015 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.genericoperator;

import java.lang.reflect.InvocationTargetException;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ExpressionEvaluator;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.DTThrowable;

public class GenericSum implements Operator
{
  public DefaultInputPort<Object> input = new DefaultInputPort<Object>() {
    @Override
    public void process(Object input)
    {
      try {
        exprEvaluator.evaluate(new Object[]{input, outputBean});
      } catch (InvocationTargetException e) {
        throw new RuntimeException(e);
      }
    }
  };

  public DefaultOutputPort<Object> output = new DefaultOutputPort<Object>();

  String expression;
  String sourceFieldName;
  Class<?> inputClass;
  String destFieldName;
  Class<?> outputClass;
  Object outputBean;

  private transient ExpressionEvaluator exprEvaluator;

  @Override
  public void setup(OperatorContext arg0)
  {
    try {
      exprEvaluator = new ExpressionEvaluator(expression,
          void.class, // expressionType
          new String[] { "input", "output" }, // parameterNames
          new Class[] { inputClass, outputClass } // parameterTypes
      );
    } catch (CompileException e) {
      throw new RuntimeException("Failed to compile expression " + expression, e);
    }
  }

  @Override
  public void teardown()
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void beginWindow(long arg0)
  {
    try {
      outputBean = outputClass.newInstance();
    } catch (Exception e) {
      DTThrowable.rethrow(e);
    }
  }

  @Override
  public void endWindow()
  {
    output.emit(outputBean);
  }

}
