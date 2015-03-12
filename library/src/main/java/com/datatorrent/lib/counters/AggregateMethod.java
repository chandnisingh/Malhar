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

public interface AggregateMethod
{
  String getName();

  Number aggregate(NumberCounter... numbersCounters);

  public static class SumAggregateMethod implements AggregateMethod
  {

    @Override
    public String getName()
    {
      return "sum";
    }

    @Override
    public Number aggregate(NumberCounter... numbersCounters)
    {
      if (numbersCounters.length == 0) {
        return null;
      }
      long longResult = 0;
      double doubleResult = 0;
      for (NumberCounter nc : numbersCounters) {
        Number no = nc.getNumberValue();
        if (no instanceof Integer || no instanceof Long) {
          longResult += no.longValue();
        }
        else {
          doubleResult += no.doubleValue();
        }
      }
      if (doubleResult == 0) {
        return longResult;
      }
      return doubleResult + longResult;
    }
  }

  public static class MinAggregateMethod implements AggregateMethod
  {

    @Override
    public String getName()
    {
      return "min";
    }

    @Override
    public Number aggregate(NumberCounter... numbersCounters)
    {
      if (numbersCounters.length == 0) {
        return null;
      }

      Number min = numbersCounters[0].getNumberValue();
      for (int i = 1; i < numbersCounters.length; i++) {
        Number no = numbersCounters[i].getNumberValue();
        if ((min instanceof Integer || min instanceof Long) && (no instanceof Integer || no instanceof Long)) {

          min = Math.min(no.longValue(), min.longValue());
        }
        else {
          min = Math.min(no.doubleValue(), min.doubleValue());

        }
      }
      return min;
    }

  }

  public static class MaxAggregateMethod implements AggregateMethod
  {

    @Override
    public String getName()
    {
      return "max";
    }

    @Override
    public Number aggregate(NumberCounter... numbersCounters)
    {
      if (numbersCounters.length == 0) {
        return null;
      }

      Number max = numbersCounters[0].getNumberValue();
      for (int i = 1; i < numbersCounters.length; i++) {
        Number no = numbersCounters[i].getNumberValue();
        if ((max instanceof Integer || max instanceof Long) && (no instanceof Integer || no instanceof Long)) {

          max = Math.max(no.longValue(), max.longValue());
        }
        else {
          max = Math.max(no.doubleValue(), max.doubleValue());

        }
      }
      return max;
    }
  }

  public static class AvgAggregateMethod implements AggregateMethod
  {

    @Override
    public String getName()
    {
      return "avg";
    }

    @Override
    public Number aggregate(NumberCounter... numbersCounters)
    {
      if (numbersCounters.length == 0) {
        return null;
      }
      long longResult = 0;
      double doubleResult = 0;

      for (NumberCounter nc : numbersCounters) {
        Number no = nc.getNumberValue();
        if (no instanceof Integer || no instanceof Long) {
          longResult += no.longValue();
        }
        else {
          doubleResult += no.doubleValue();
        }
      }
      if (doubleResult == 0) {
        return (longResult * 1.0) / numbersCounters.length;
      }
      return (doubleResult + longResult) / numbersCounters.length;
    }
  }
}
