/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.schemaApp;

import com.datatorrent.api.DefaultInputPort;

import com.datatorrent.common.util.BaseOperator;

/**
 * @category debug
 */
public class B extends BaseOperator
{
  private String prop2;

  public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
  {
    @Override
    public void process(Object tuple)
    {

    }
  };

  public String getProp2()
  {
    return prop2;
  }

  /**
   * Sets the property 2 in B
   *
   * @param prop2
   */
  public void setProp2(String prop2)
  {
    this.prop2 = prop2;
  }
}
