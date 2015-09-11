/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.schemaApp;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;

import com.datatorrent.common.util.BaseOperator;

/**
 * @category schema
 */
public class BasePojoArraySchemaOperator extends BaseOperator
{
  private HandwrittenPojo[] pojoArray;

  @InputPortFieldAnnotation(optional = true, schemaRequired = true)
  public final DefaultInputPort<Object> input = new DefaultInputPort<Object>()
  {
    @Override
    public void process(Object o)
    {
      //
    }
  };

  public HandwrittenPojo[] getPojoArray()
  {
    return pojoArray;
  }

  /**
   * @useSchema $[].prop1 input.fields[].name
   * @useSchema $[].prop2 input.fields[].name
   * @param pojoArray
   */
  public void setPojoArray(HandwrittenPojo[] pojoArray)
  {
    this.pojoArray = pojoArray;
  }
}
