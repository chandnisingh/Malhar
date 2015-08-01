/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.helper;

import java.util.Collection;

import javax.validation.constraints.NotNull;

import com.google.common.base.Preconditions;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;

public class TestPortContext implements Context.PortContext
{
  public final Attribute.AttributeMap attributeMap;

  public TestPortContext(@NotNull Attribute.AttributeMap attributeMap)
  {
    this.attributeMap = Preconditions.checkNotNull(attributeMap, "attributes");
  }

  @Override
  public Attribute.AttributeMap getAttributes()
  {
    return attributeMap;
  }

  @Override
  public <T> T getValue(Attribute<T> key)
  {
    return attributeMap.get(key);
  }

  @Override
  public void setCounters(Object counters)
  {

  }

  @Override
  public void sendMetrics(Collection<String> metricNames)
  {

  }
}
