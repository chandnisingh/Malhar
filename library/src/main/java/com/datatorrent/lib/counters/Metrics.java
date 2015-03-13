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
import java.util.Map;

import javax.validation.constraints.NotNull;

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
 * Collection of {@link Metric}
 */
@JsonSerialize(using = Metrics.Serializer.class)
public class Metrics
{
  private final Map<String, Metric<? extends Number>> cache;

  public Metrics()
  {
    cache = Maps.newHashMap();
  }

  /**
   * Returns the metric if it exists; null otherwise
   *
   * @param counterKey counter key
   * @return counter corresponding to the counter key.
   */
  public synchronized Metric<? extends Number> getMetric(String counterKey)
  {
    return cache.get(counterKey);
  }

  /**
   * Sets the value of a metric.
   *
   * @param metric new metric.
   */
  public synchronized void addMetric(Metric<? extends Number> metric)
  {
    cache.put(metric.getKey(), metric);
  }

  /**
   * Returns an immutable copy of all the metrics.
   *
   * @return an immutable copy of the metrics.
   */
  public synchronized ImmutableMap<String, Metric<? extends Number>> getCopy()
  {
    return ImmutableMap.copyOf(cache);
  }

  public static class Serializer extends JsonSerializer<Metrics>
  {

    @Override
    public void serialize(Metrics value, JsonGenerator jgen, SerializerProvider provider)
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
      Multimap<String, Metric<? extends Number>> counterPool = ArrayListMultimap.create();

      for (Object metric : countersList) {
        Metrics metrics = (Metrics) metric;
        ImmutableMap<String, Metric<? extends Number>> copy = metrics.getCopy();

        for (Map.Entry<String, Metric<? extends Number>> entry : copy.entrySet()) {
          counterPool.put(entry.getKey(), entry.getValue());
        }
      }

      for (String counterKey : counterPool.keySet()) {
        Collection<Metric<? extends Number>> ncs = counterPool.get(counterKey);

        Collection<AggregateMethod<? extends Number>> aggregateMethods = ncs.iterator().next().getAggregateMethods();

        Map<String, Number> counterResult = Maps.newHashMap();
        result.put(counterKey, counterResult);

        if (aggregateMethods != null) {
          for (AggregateMethod method : aggregateMethods) {

            Number aggregate = method.aggregate(ncs);
            counterResult.put(method.getName(), aggregate);
          }
        }
      }
      return result;
    }

    private static final long serialVersionUID = 201503091713L;

  }
}
