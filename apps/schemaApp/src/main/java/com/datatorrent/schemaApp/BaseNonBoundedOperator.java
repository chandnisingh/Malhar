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
 * @param <T>
 */
public class BaseNonBoundedOperator<T> extends BaseOperator
{
  private Map<String, String> mapProp;

  @InputPortFieldAnnotation(optional = true)
  public final DefaultInputPort<T> input = new DefaultInputPort<T>()
  {
    @Override
    public void process(T t)
    {
      //
    }
  };

  public Map<String, String> getMapProp()
  {
    return mapProp;
  }

  /**
   * @useSchema $(key) input.fields[].name
   * @param mapProp
   */
  public void setMapProp(Map<String, String> mapProp)
  {
    this.mapProp = mapProp;
  }
}
