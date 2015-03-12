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

import java.util.List;

import javax.validation.constraints.NotNull;

import org.apache.commons.lang.mutable.MutableDouble;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.lang.mutable.MutableLong;

import com.google.common.collect.Lists;

public interface NumberCounter
{
  /**
   * @return value of the counter
   */
  @NotNull
  Number getNumberValue();

  /**
   * @return null if should not be aggregated; list of aggregating methods otherwise.
   */
  List<AggregateMethod> getAggregateMethods();

  public static class IntegerCounter extends MutableInt implements NumberCounter
  {
    @Override
    public Number getNumberValue()
    {
      return intValue();
    }

    @Override
    public List<AggregateMethod> getAggregateMethods()
    {
      return Lists.newArrayList(new AggregateMethod.AvgAggregateMethod(), new AggregateMethod.SumAggregateMethod(),
        new AggregateMethod.MaxAggregateMethod(), new AggregateMethod.MinAggregateMethod());
    }
  }

  public static class LongCounter extends MutableLong implements NumberCounter
  {

    @Override
    public Number getNumberValue()
    {
      return longValue();
    }

    @Override
    public List<AggregateMethod> getAggregateMethods()
    {
      return Lists.newArrayList(new AggregateMethod.AvgAggregateMethod(), new AggregateMethod.SumAggregateMethod(),
        new AggregateMethod.MaxAggregateMethod(), new AggregateMethod.MinAggregateMethod());
    }
  }

  public static class DoubleCounter extends MutableDouble implements NumberCounter
  {

    @Override
    public Number getNumberValue()
    {
      return doubleValue();
    }

    @Override
    public List<AggregateMethod> getAggregateMethods()
    {
      return Lists.newArrayList(new AggregateMethod.AvgAggregateMethod(), new AggregateMethod.SumAggregateMethod(),
        new AggregateMethod.MaxAggregateMethod(), new AggregateMethod.MinAggregateMethod());
    }
  }
}
