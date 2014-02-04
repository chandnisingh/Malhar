package com.datatorrent.contrib.accumulo;

import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import com.google.common.collect.Maps;

import com.datatorrent.lib.db.AbstractStoreOutputOperator;
import com.datatorrent.lib.io.AbstractKeyValueStoreOutputOperator;

/**
 * An {@link AbstractKeyValueStoreOutputOperator} which uses Accumulo as its data store.<br/>
 *
 * <p>
 * The tuple could be either:
 * <ul>
 * <li>Any object: the accumulo key/value is parsed from a single tuple.</li>
 * <li>
 * Collection: Each collection item corresponds to an accumulo row.
 * The accumulo key/value is parsed from each item of the collection.
 * </li>
 * <li>
 * Map: Each map entry corresponds to an accumulo row.
 * The accumulo key is parsed from an entry key and accumulo value is parsed from the corresponding entry value.
 * </li>
 * </ul>
 * </p>
 *
 * @param <T> tuple type</T>
 */
public abstract class AbstractAccumuloOutputOperator<T> extends AbstractStoreOutputOperator<T, AccumuloStore>
{
  @SuppressWarnings("unchecked")
  @Override
  public void processTuple(T tuple)
  {
    if (tuple instanceof Collection) {
      for (Object item : (Collection<?>) tuple) {
        store.put(getKeyFrom(item), getValueFrom(item));
      }
    }
    else if (tuple instanceof Map) {
      Map<Object, Object> accumuloKeyValues = Maps.newHashMap();
      for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) tuple).entrySet()) {
        accumuloKeyValues.put(getKeyFrom(entry.getKey()), getValueFrom(entry.getValue()));
      }
      store.putAll(accumuloKeyValues);
    }
    else {
      store.put(getKeyFrom(tuple), getValueFrom(tuple));
    }
  }

  /**
   * Parses the object to get {@link Key}.
   *
   * @param object object from which key is retrieved.
   * @return {@link Key}
   */
  protected abstract Key getKeyFrom(Object object);

  /**
   * Parses the object to get {@link Value}
   *
   * @param object object from which value is retrieved.
   * @return {@link Value}
   */
  protected abstract Value getValueFrom(Object object);
}
