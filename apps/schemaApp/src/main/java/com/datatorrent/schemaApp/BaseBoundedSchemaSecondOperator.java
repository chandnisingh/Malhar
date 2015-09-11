/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.schemaApp;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;

import com.datatorrent.common.util.BaseOperator;

/**
 * @param <T>
 * @category schema
 */
public class BaseBoundedSchemaSecondOperator<T extends SecondHandwrittenPojo> extends BaseOperator
{
  SecondHandwrittenPojo otherBeanProp;

  @InputPortFieldAnnotation(optional = true, schemaRequired = true)
  public final DefaultInputPort<T> input = new DefaultInputPort<T>()
  {
    @Override
    public void process(T t)
    {
      //
    }
  };

  /**
   * @useSchema $.otherProp1 input.fields[].name
   * @param otherBeanProp
   */
  public void setOtherBeanProp(SecondHandwrittenPojo otherBeanProp)
  {
    this.otherBeanProp = otherBeanProp;
  }

  public SecondHandwrittenPojo getOtherBeanProp()
  {
    return otherBeanProp;
  }
}
