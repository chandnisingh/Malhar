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
 * @category schema
 */
public class BaseNonGenSchemaOperator extends BaseOperator
{
  private List<HandwrittenPojo> pojoList;

  @InputPortFieldAnnotation(optional = true, schemaRequired = true)
  public final DefaultInputPort<HandwrittenPojo> input = new DefaultInputPort<HandwrittenPojo>()
  {
    @Override
    public void process(HandwrittenPojo o)
    {
      //
    }
  };

  public List<HandwrittenPojo> getPojoList()
  {
    return pojoList;
  }

  /**
   * @useSchema $[].prop2 input.fields[].name
   * @param pojoList
   */
  public void setPojoList(List<HandwrittenPojo> pojoList)
  {
    this.pojoList = pojoList;
  }
}
