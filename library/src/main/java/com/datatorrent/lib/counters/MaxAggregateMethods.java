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
import java.util.Iterator;

public interface MaxAggregateMethods
{
  public static class IntMax implements AggregateMethod<Integer>
  {

    @Override
    public String getName()
    {
      return "max";
    }

    @Override
    public Integer aggregate(Collection<Metric<? extends Number>> metrics)
    {
      if (metrics.size() == 0) {
        return null;
      }
      Iterator<Metric<? extends Number>> iterator = metrics.iterator();

      Integer max = iterator.next().getNumberValue().intValue();
      while (iterator.hasNext()) {
        Integer no = iterator.next().getNumberValue().intValue();
        max = Math.max(no, max);
      }
      return max;
    }
  }

  public static class LongMax implements AggregateMethod<Long>
  {

    @Override
    public String getName()
    {
      return "max";
    }

    @Override
    public Long aggregate(Collection<Metric<? extends Number>> metrics)
    {
      if (metrics.size() == 0) {
        return null;
      }
      Iterator<Metric<? extends Number>> iterator = metrics.iterator();

      Long max = iterator.next().getNumberValue().longValue();
      while (iterator.hasNext()) {
        Long no = iterator.next().getNumberValue().longValue();
        max = Math.max(no, max);
      }
      return max;
    }
  }

  public static class FloatMax implements AggregateMethod<Float>
  {

    @Override
    public String getName()
    {
      return "max";
    }

    @Override
    public Float aggregate(Collection<Metric<? extends Number>> metrics)
    {
      if (metrics.size() == 0) {
        return null;
      }
      Iterator<Metric<? extends Number>> iterator = metrics.iterator();

      Float max = iterator.next().getNumberValue().floatValue();

      while (iterator.hasNext()) {
        Float no = iterator.next().getNumberValue().floatValue();
        max = Math.max(no, max);
      }
      return max;
    }
  }

  public static class DoubleMax implements AggregateMethod<Double>
  {

    @Override
    public String getName()
    {
      return "max";
    }

    @Override
    public Double aggregate(Collection<Metric<? extends Number>> metrics)
    {
      if (metrics.size() == 0) {
        return null;
      }
      Iterator<Metric<? extends Number>> iterator = metrics.iterator();

      Double max = iterator.next().getNumberValue().doubleValue();
      while (iterator.hasNext()) {
        Double no = iterator.next().getNumberValue().doubleValue();
        max = Math.max(no, max);
      }
      return max;
    }
  }

}
