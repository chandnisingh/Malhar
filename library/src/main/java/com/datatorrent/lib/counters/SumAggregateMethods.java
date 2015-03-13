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

public interface SumAggregateMethods
{
  public static class LongSum implements AggregateMethod<Long>
  {

    @Override
    public String getName()
    {
      return "sum";
    }

    @Override
    public Long aggregate(Collection<Metric<? extends Number>> metrics)
    {
      if (metrics.size() == 0) {
        return null;
      }
      long longResult = 0;
      for (Metric<? extends Number> nc : metrics) {
        longResult += nc.getNumberValue().longValue();
      }
      return longResult;
    }
  }

  public static class DoubleSum implements AggregateMethod<Double>
  {

    @Override
    public String getName()
    {
      return "sum";
    }

    @Override
    public Double aggregate(Collection<Metric<? extends Number>> metrics)
    {
      if (metrics.size() == 0) {
        return null;
      }
      double doubleResult = 0;
      for (Metric<? extends Number> nc : metrics) {
        doubleResult += nc.getNumberValue().longValue();
      }
      return doubleResult;
    }
  }
}
