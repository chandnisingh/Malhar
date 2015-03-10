/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import com.datatorrent.api.Context;

/**
 * Collection of {@link NumberCounter}
 */
@JsonSerialize(using = Counters.Serializer.class)
public class Counters
{
  private final Map<String, NumberCounter> cache;

  public Counters()
  {
    cache = Maps.newHashMap();
  }

  /**
   * Returns the counter if it exists; null otherwise
   *
   * @param counterKey
   * @return counter corresponding to the counter key.
   */
  public synchronized NumberCounter getCounter(String counterKey)
  {
    return cache.get(counterKey);
  }

  /**
   * Sets the value of a counter.
   *
   * @param counterKey counter key.
   * @param value      new value.
   */
  public synchronized void setCounter(String counterKey, NumberCounter value)
  {
    cache.put(counterKey, value);
  }

  /**
   * Returns an immutable copy of all the counters.
   *
   * @return an immutable copy of the counters.
   */
  public synchronized ImmutableMap<String, NumberCounter> getCopy()
  {
    return ImmutableMap.copyOf(cache);
  }

  public static class Serializer extends JsonSerializer<Counters>
  {

    @Override
    public void serialize(Counters value, JsonGenerator jgen, SerializerProvider provider)
      throws IOException
    {
      jgen.writeObject(value.getCopy());
    }
  }

  public static class Aggregator implements Context.CountersAggregator, Serializable
  {

    @SuppressWarnings("ConstantConditions")
    @Override
    public Object aggregate(Collection<?> countersList)
    {
      //counter key -> (aggregate method -> aggregated value)
      Map<String, Map<String, Number>> result = Maps.newHashMap();

      NumberCounter viewOfCounters = ((Counters) countersList.iterator().next()).getCopy().values().iterator().next();
      if (viewOfCounters.getAggregateMethods() != null) {

        List<NumberCounter.AggregateMethod> aggregateMethods = viewOfCounters.getAggregateMethods();

        boolean calculateSumOnlyForAvg = aggregateMethods.contains(NumberCounter.AggregateMethod.AVERAGE) &&
          !aggregateMethods.contains(NumberCounter.AggregateMethod.SUM);

        int count = 0;
        for (Object counter : countersList) {
          count++;
          Counters counters = (Counters) counter;

          ImmutableMap<String, NumberCounter> copy = counters.getCopy();

          for (Map.Entry<String, NumberCounter> entry : copy.entrySet()) {

            Map<String, Number> subMap = result.get(entry.getKey());
            if (subMap == null) {
              subMap = Maps.newHashMap();
              result.put(entry.getKey(), subMap);
            }

            for (NumberCounter.AggregateMethod method : aggregateMethods) {
              Number first = subMap.get(method.getDisplayName());
              Number updatedVal = null;

              if (method == NumberCounter.AggregateMethod.MIN) {
                updatedVal = getMinOf(first, entry.getValue().getNumberValue());
              }
              else if (method == NumberCounter.AggregateMethod.MAX) {
                updatedVal = getMaxOf(first, entry.getValue().getNumberValue());
              }
              else if (method == NumberCounter.AggregateMethod.SUM || calculateSumOnlyForAvg) {
                updatedVal = getSumOf(first, entry.getValue().getNumberValue());
              }
              else if (method == NumberCounter.AggregateMethod.AVERAGE && count == countersList.size()) {
                updatedVal = subMap.get(
                  NumberCounter.AggregateMethod.SUM.getDisplayName()).doubleValue() / (count * 1.0);

                if (calculateSumOnlyForAvg) {
                  subMap.remove(method.getDisplayName());
                }
              }
              if (updatedVal != null) {
                subMap.put(method.getDisplayName(), updatedVal);
              }
            }

          }
        }
      }

      return result;
    }

    private Number getMinOf(@Nullable Number first, @NotNull Number second)
    {
      if (first == null) {
        return second;
      }
      if (first instanceof Integer || first instanceof Long) {
        return Math.min(first.longValue(), first.longValue());
      }
      return Math.min(first.doubleValue(), first.doubleValue());
    }

    private Number getMaxOf(@Nullable Number first, @NotNull Number second)
    {
      if (first == null) {
        return second;
      }
      if (first instanceof Integer || first instanceof Long) {
        return Math.max(first.longValue(), first.longValue());
      }
      return Math.max(first.doubleValue(), first.doubleValue());
    }

    private Number getSumOf(@Nullable Number first, @NotNull Number second)
    {
      if (first == null) {
        return second;
      }
      if (first instanceof Integer || first instanceof Long) {
        return first.longValue() + first.longValue();
      }
      return second.doubleValue() + second.doubleValue();
    }

    private static final long serialVersionUID = 201503091713L;

  }
}
