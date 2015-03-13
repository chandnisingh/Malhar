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

public interface MinAggregateMethods
{

  public static class IntMin implements AggregateMethod<Integer>
  {

    @Override
    public String getName()
    {
      return "min";
    }

    @Override
    public Integer aggregate(Collection<Metric<? extends Number>> metrics)
    {
      if (metrics.size() == 0) {
        return null;
      }
      Iterator<Metric<? extends Number>> iterator = metrics.iterator();
      Integer min = iterator.next().getNumberValue().intValue();

      while (iterator.hasNext()) {
        Integer no = iterator.next().getNumberValue().intValue();
        min = Math.min(min, no);
      }
      return min;
    }
  }

  public static class LongMin implements AggregateMethod<Long>
  {

    @Override
    public String getName()
    {
      return "min";
    }

    @Override
    public Long aggregate(Collection<Metric<? extends Number>> metrics)
    {
      if (metrics.size() == 0) {
        return null;
      }
      Iterator<Metric<? extends Number>> iterator = metrics.iterator();

      Long min = iterator.next().getNumberValue().longValue();
      while (iterator.hasNext()) {
        Long no = iterator.next().getNumberValue().longValue();
        min = Math.min(min, no);
      }
      return min;
    }
  }

  public static class FloatMin implements AggregateMethod<Float>
  {

    @Override
    public String getName()
    {
      return "min";
    }

    @Override
    public Float aggregate(Collection<Metric<? extends Number>> metrics)
    {
      if (metrics.size() == 0) {
        return null;
      }
      Iterator<Metric<? extends Number>> iterator = metrics.iterator();

      Float min = iterator.next().getNumberValue().floatValue();
      while (iterator.hasNext()) {
        Float no = iterator.next().getNumberValue().floatValue();
        min = Math.min(min, no);
      }
      return min;
    }
  }

  public static class DoubleMin implements AggregateMethod<Double>
  {

    @Override
    public String getName()
    {
      return "min";
    }

    @Override
    public Double aggregate(Collection<Metric<? extends Number>> metrics)
    {
      if (metrics.size() == 0) {
        return null;
      }
      Iterator<Metric<? extends Number>> iterator = metrics.iterator();
      Double min = iterator.next().getNumberValue().doubleValue();

      while (iterator.hasNext()) {
        Double no = iterator.next().getNumberValue().doubleValue();
        min = Math.min(min, no);
      }
      return min;
    }
  }
}
