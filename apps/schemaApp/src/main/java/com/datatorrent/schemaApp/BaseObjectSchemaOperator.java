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
public class BaseObjectSchemaOperator extends BaseOperator
{
  private Map<String, HandwrittenPojo> pojoMap;

  @InputPortFieldAnnotation(optional = true, schemaRequired = true)
  public final DefaultInputPort<Object> input = new DefaultInputPort<Object>()
  {
    @Override
    public void process(Object o)
    {
      //
    }
  };

  public Map<String, HandwrittenPojo> getPojoMap()
  {
    return pojoMap;
  }

  /**
   * @useSchema $(value).prop2 input.fields[].name
   * @param pojoMap
   */
  public void setPojoMap(Map<String, HandwrittenPojo> pojoMap)
  {
    this.pojoMap = pojoMap;
  }
}
