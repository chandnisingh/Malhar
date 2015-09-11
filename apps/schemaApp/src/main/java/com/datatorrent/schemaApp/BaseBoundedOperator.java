/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.schemaApp;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;

import com.datatorrent.common.util.BaseOperator;

/**
 *
 * @category schema
 * @param <T>
 */
public class BaseBoundedOperator<T extends HandwrittenPojo> extends BaseOperator
{
  String prop;
  @InputPortFieldAnnotation(optional = true)
  public final DefaultInputPort<T> input = new DefaultInputPort<T>()
  {
    @Override
    public void process(T t)
    {
      //
    }
  };

  /**
   * Sets property
   * @useSchema $ input.fields[].name
   * @param x
   */
  public void setProp(String x)
  {
    this.prop = x;
  }

  public String getProp()
  {
    return prop;
  }
}
