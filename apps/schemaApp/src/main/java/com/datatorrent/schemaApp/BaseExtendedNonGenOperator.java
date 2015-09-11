/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.schemaApp;

import java.util.List;
import java.util.Map;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;

import com.datatorrent.common.util.BaseOperator;

/**
 * @category schema
 */
public class BaseExtendedNonGenOperator extends BaseOperator
{
  private Map<String, List<String>> arrayMultimapProp;

  @InputPortFieldAnnotation(optional = true)
  public final DefaultInputPort<HandwrittenExtendedPojo> input = new DefaultInputPort<HandwrittenExtendedPojo>()
  {
    @Override
    public void process(HandwrittenExtendedPojo o)
    {
      //
    }
  };

  public Map<String, List<String>> getArrayMultimapProp()
  {
    return arrayMultimapProp;
  }

  /**
   * @useSchema $(value)[] input.fields[].name
   * @param arrayMultimapProp
   */
  public void setArrayMultimapProp(Map<String, List<String>> arrayMultimapProp)
  {
    this.arrayMultimapProp = arrayMultimapProp;
  }
}
