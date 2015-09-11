/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.schemaApp;

import java.util.Map;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;

import com.datatorrent.common.util.BaseOperator;

/**
 * @category schema
 */
public class BaseStringArraySchemaOperator extends BaseOperator
{
  private String[] strArray;

  @InputPortFieldAnnotation(optional = true, schemaRequired = true)
  public final DefaultInputPort<Object> input = new DefaultInputPort<Object>()
  {
    @Override
    public void process(Object o)
    {
      //
    }
  };

  public String[] getStrArray()
  {
    return strArray;
  }

  /**
   * @useSchema $[] input.fields[].name
   * @param strArray
   */
  public void setStrArray(String[] strArray)
  {
    this.strArray = strArray;
  }
}
