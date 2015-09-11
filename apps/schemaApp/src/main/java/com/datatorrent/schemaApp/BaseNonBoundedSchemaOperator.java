/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.schemaApp;

import java.util.List;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;

/**
 * Non bounded schema required.<br/>
 * @category schema
 */
public class BaseNonBoundedSchemaOperator<T> extends BaseOperator
{
  private List<String> listProp;

  @InputPortFieldAnnotation(optional = true, schemaRequired = true)
  public final DefaultInputPort<T> input = new DefaultInputPort<T>()
  {
    @Override
    public void process(T t)
    {
      //
    }
  };

  public List<String> getListProp()
  {
    return listProp;
  }

  /**
   * @useSchema $[] input.fields[].name
   * @param listProp
   */
  public void setListProp(List<String> listProp)
  {
    this.listProp = listProp;
  }
}
