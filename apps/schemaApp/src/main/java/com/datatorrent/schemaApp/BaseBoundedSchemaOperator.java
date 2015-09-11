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
public class BaseBoundedSchemaOperator<T extends HandwrittenPojo> extends BaseOperator
{
  HandwrittenPojo beanProp;

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
   * @useSchema $.prop1 input.fields[].name
   * @param beanProp
   */
  public void setBeanProp(HandwrittenPojo beanProp)
  {
    this.beanProp = beanProp;
  }

  public HandwrittenPojo getBeanProp()
  {
    return beanProp;
  }
}
