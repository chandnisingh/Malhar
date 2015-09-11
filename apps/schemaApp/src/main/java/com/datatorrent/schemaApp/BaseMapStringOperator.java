/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.schemaApp;

import java.util.Map;

import com.datatorrent.api.DefaultInputPort;

import com.datatorrent.common.util.BaseOperator;

/**
 * @category schema
 */
public class BaseMapStringOperator extends BaseOperator
{
  public final transient DefaultInputPort<Map<String, String>> input = new DefaultInputPort<Map<String, String>>()
  {
    @Override
    public void process(Map<String, String> tuple)
    {

    }
  };
}
