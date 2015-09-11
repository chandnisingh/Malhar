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
public class BaseObjectOperator extends BaseOperator
{
  private Map<Map<String, String>, String> multimapProp;

  @InputPortFieldAnnotation(optional = true)
  public final DefaultInputPort<Object> input = new DefaultInputPort<Object>()
  {
    @Override
    public void process(Object o)
    {
      //
    }
  };

  public Map<Map<String, String>, String> getMultimapProp()
  {
    return multimapProp;
  }

  /**
   * @useSchema $(key)(value) input.fields[].name
   * @param multimapProp
   */
  public void setMultimapProp(Map<Map<String, String>, String> multimapProp)
  {
    this.multimapProp = multimapProp;
  }
}
