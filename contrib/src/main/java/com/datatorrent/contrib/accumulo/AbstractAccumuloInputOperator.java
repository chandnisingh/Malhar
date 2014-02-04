package com.datatorrent.contrib.accumulo;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;

import com.datatorrent.api.Context;

import com.datatorrent.lib.db.AbstractStoreInputOperator;

/**
 * An {@link AbstractStoreInputOperator} which scans data from Accumulo store and emits each row as a tuple.
 */
public abstract class AbstractAccumuloInputOperator<T> extends AbstractStoreInputOperator<T, AccumuloStore>
{
  private int numTuplesPerWindow;
  private boolean supportMultipleRanges;

  private transient Iterator<Map.Entry<Key, Value>> resultIterator;

  AbstractAccumuloInputOperator()
  {
    numTuplesPerWindow = 100;
    supportMultipleRanges = false;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    if (supportMultipleRanges) {
      resultIterator = store.fetchIteratorFor(getMultipleRanges());
    }
    else {
      resultIterator = store.fetchIteratorFor(getRange());
    }
  }

  @Override
  public void emitTuples()
  {
    int tuplesEmitted = 0;
    while (resultIterator.hasNext() && tuplesEmitted < numTuplesPerWindow) {
      Map.Entry<Key, Value> row = resultIterator.next();
      T tuple = getTuple(row.getKey(), row.getValue());
      outputPort.emit(tuple);
    }
  }

  /**
   * Sets the number of tuples which are emitted every window.
   *
   * @param numTuplesPerWindow number of tuples emitted per window.
   */
  public void setNumTuplesPerWindow(int numTuplesPerWindow)
  {
    this.numTuplesPerWindow = numTuplesPerWindow;
  }

  /**
   * Sets the flag which allows adding multiple key ranges which are scanned to generate tuples.
   *
   * @param supportMultipleRanges true when multiple ranges are supported; false otherwise.
   */
  public void setSupportMultipleRanges(boolean supportMultipleRanges)
  {
    this.supportMultipleRanges = supportMultipleRanges;
  }

  /**
   * Returns the collection of {@link Range}s which are scanned to generate tuples.
   *
   * @return collection of range.
   */
  protected abstract Collection<Range> getMultipleRanges();

  /**
   * Returns a single range which is scanned to generate tuples.
   *
   * @return range of keys.
   */
  protected abstract Range getRange();

  /**
   * Constructs the output tuple from {@link Key} and {@link Value}.
   *
   * @param key   key of the row
   * @param value value
   * @return output tuple
   */
  protected abstract T getTuple(Key key, Value value);

}
