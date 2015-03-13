/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.counters;

import java.util.Collection;

import javax.validation.constraints.NotNull;

import org.apache.commons.lang.mutable.MutableDouble;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.lang.mutable.MutableLong;

import com.google.common.collect.Lists;

public interface Metric<T extends Number>
{
  String getKey();

  /**
   * @return value of the counter
   */
  @NotNull
  T getNumberValue();

  /**
   * @return null if should not be aggregated; list of aggregating methods otherwise.
   */
  Collection<AggregateMethod<? extends Number>> getAggregateMethods();

  public static class IntegerMetric extends MutableInt implements Metric<Integer>
  {
    private final String key;
    private final Collection<AggregateMethod<? extends Number>> aggregateMethods;

    protected IntegerMetric()
    {
      //for kryo
      key = null;
      aggregateMethods = null;
    }

    public IntegerMetric(String key)
    {
      this.key = key;
      this.aggregateMethods = Lists.<AggregateMethod<? extends Number>>newArrayList(new CountAggregateMethod(),
        new SumAggregateMethods.LongSum(),
        new MaxAggregateMethods.IntMax(),
        new MinAggregateMethods.IntMin());
    }

    public IntegerMetric(String key, Collection<AggregateMethod<? extends Number>> aggregateMethods)
    {
      this.key = key;
      this.aggregateMethods = aggregateMethods;
    }

    @Override
    public Integer getNumberValue()
    {
      return intValue();
    }

    @Override
    public Collection<AggregateMethod<? extends Number>> getAggregateMethods()
    {
      return aggregateMethods;
    }

    @Override
    public String getKey()
    {
      return key;
    }
  }

  public static class LongMetric extends MutableLong implements Metric<Long>
  {
    private final String key;
    private final Collection<AggregateMethod<? extends Number>> aggregateMethods;

    protected LongMetric()
    {
      //for kryo
      key = null;
      aggregateMethods = null;
    }

    public LongMetric(String key)
    {
      this.key = key;
      this.aggregateMethods = Lists.<AggregateMethod<? extends Number>>newArrayList(new CountAggregateMethod(),
        new SumAggregateMethods.LongSum(),
        new MaxAggregateMethods.LongMax(),
        new MinAggregateMethods.LongMin());
    }

    public LongMetric(String key, Collection<AggregateMethod<? extends Number>> aggregateMethods)
    {
      this.key = key;
      this.aggregateMethods = aggregateMethods;
    }

    @Override
    public Long getNumberValue()
    {
      return longValue();
    }

    @Override
    public Collection<AggregateMethod<? extends Number>> getAggregateMethods()
    {
      return aggregateMethods;
    }

    @Override
    public String getKey()
    {
      return key;
    }
  }

  public static class DoubleMetric extends MutableDouble implements Metric<Double>
  {
    private final String key;
    private final Collection<AggregateMethod<? extends Number>> aggregateMethods;

    protected DoubleMetric()
    {
      //for kryo
      key = null;
      aggregateMethods = null;
    }

    public DoubleMetric(String key)
    {
      this.key = key;
      this.aggregateMethods = Lists.<AggregateMethod<? extends Number>>newArrayList(new CountAggregateMethod(),
        new SumAggregateMethods.LongSum(),
        new MaxAggregateMethods.LongMax(),
        new MinAggregateMethods.LongMin());
    }

    public DoubleMetric(String key, Collection<AggregateMethod<? extends Number>> aggregateMethods)
    {
      this.key = key;
      this.aggregateMethods = aggregateMethods;
    }

    @Override
    public Double getNumberValue()
    {
      return doubleValue();
    }

    @Override
    public Collection<AggregateMethod<? extends Number>> getAggregateMethods()
    {
      return aggregateMethods;
    }

    @Override
    public String getKey()
    {
      return key;
    }
  }
}
