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

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

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
   * @param counterKey counter key
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

    @Override
    public Object aggregate(Collection<?> countersList)
    {
      //counter key -> (aggregate method -> aggregated value)
      Map<String, Map<String, Number>> result = Maps.newHashMap();

      //counter key : list of counter objects of all partitions
      Multimap<String, NumberCounter> counterPool = ArrayListMultimap.create();

      for (Object counter : countersList) {
        Counters counters = (Counters) counter;
        ImmutableMap<String, NumberCounter> copy = counters.getCopy();

        for (Map.Entry<String, NumberCounter> entry : copy.entrySet()) {
          counterPool.put(entry.getKey(), entry.getValue());
        }
      }

      for (String counterKey : counterPool.keySet()) {
        Collection<NumberCounter> ncs = counterPool.get(counterKey);
        List<AggregateMethod> aggregateMethods = ncs.iterator().next().getAggregateMethods();

        Map<String, Number> counterResult = Maps.newHashMap();
        result.put(counterKey, counterResult);

        if (aggregateMethods != null) {
          for (AggregateMethod method : aggregateMethods) {

            Number aggregate = method.aggregate(ncs.toArray(new NumberCounter[ncs.size()]));
            counterResult.put(method.getName(), aggregate);
          }
        }
      }
      return result;
    }
    private static final long serialVersionUID = 201503091713L;

  }
}
