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
public class BaseMapNonBoundedOperator<K, V> extends BaseOperator
{
  public final transient DefaultInputPort<Map<K, V>> input = new DefaultInputPort<Map<K, V>>()
  {
    @Override
    public void process(Map<K, V> tuple)
    {

    }
  };
}
