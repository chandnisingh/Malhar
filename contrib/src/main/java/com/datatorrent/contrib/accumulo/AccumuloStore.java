/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.accumulo;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;

import com.datatorrent.api.annotation.ShipContainingJars;

import com.datatorrent.lib.db.KeyValueStore;

/**
 * A {@link KeyValueStore} which uses Accumulo as persistent storage.
 */
@ShipContainingJars(classes = {Connector.class})
public class AccumuloStore implements KeyValueStore
{
  @Nonnull
  private String instanceName;
  @Nonnull
  private String zooServers;
  @Nonnull
  private String userName;
  @Nonnull
  private String password;
  private String table;
  private long maxLatencyInMillis;
  private int numQueryThread;
  @Nonnull
  private String readVisibility;
  private Map<Text, String> columnFamilyToTableMapping;

  private transient boolean isSingleTable;
  private transient Connector connector;
  private transient PasswordToken token;
  private transient MultiTableBatchWriter multiTableBatchWriter;
  private transient BatchWriter singleTableBatchWriter;

  private transient BatchScanner batchScanner;
  private transient Scanner scanner;

  public AccumuloStore()
  {
    maxLatencyInMillis = 500;
    numQueryThread = 5;
    readVisibility = "public";
  }

  @Override
  public Object get(Object key)
  {
    Key accumuloKey = (Key) key;
    Iterator<Map.Entry<Key, Value>> resultIterator = fetchIteratorFor(new Range(accumuloKey, accumuloKey));
    if (resultIterator.hasNext()) {
      return resultIterator.next().getValue();
    }
    return null;
  }

  @Override
  public List<Object> getAll(List<Object> keys)
  {
    throw new UnsupportedOperationException("not supported.");
  }

  @Override
  public void put(Object key, Object value)
  {
    Key accumuloKey = (Key) key;
    Mutation mutation = new Mutation(accumuloKey.getRow());
    mutation.put(accumuloKey.getColumnFamily(), accumuloKey.getColumnQualifier(), accumuloKey.getColumnVisibilityParsed(),
      accumuloKey.getTimestamp(), (Value) value);
    putHelper(accumuloKey, mutation);
  }

  @Override
  public void putAll(Map<Object, Object> m)
  {
    for (Map.Entry<Object, Object> entry : m.entrySet()) {
      Key accumuloKey = (Key) entry.getKey();
      Mutation mutation = new Mutation(accumuloKey.getRow());
      mutation.put(accumuloKey.getColumnFamily(), accumuloKey.getColumnQualifier(), accumuloKey.getColumnVisibilityParsed(),
        System.currentTimeMillis(), (Value) entry.getValue());
      putHelper(accumuloKey, mutation);
    }
  }

  private void putHelper(Key key, Mutation mutation)
  {
    try {
      if (isSingleTable) {
        singleTableBatchWriter.addMutation(mutation);
      }
      else {
        multiTableBatchWriter.getBatchWriter(columnFamilyToTableMapping.get(key.getColumnFamily())).addMutation(mutation);
      }
    }
    catch (Throwable t) {
      Throwables.propagate(t);
    }
  }

  public Iterator<Map.Entry<Key, Value>> fetchIteratorFor(@Nonnull Range keyRange)
  {
    scanner.setRange(keyRange);
    return scanner.iterator();
  }

  public Iterator<Map.Entry<Key, Value>> fetchIteratorFor(@Nonnull Collection<Range> keyRanges)
  {
    batchScanner.setRanges(keyRanges);
    return batchScanner.iterator();
  }

  @Override
  public void remove(Object key)
  {
    throw new UnsupportedOperationException("not supported.");
  }

  @Override
  public void connect() throws IOException
  {
    isSingleTable = table != null;
    Instance inst = new ZooKeeperInstance(instanceName, zooServers);
    try {
      token = new PasswordToken(password);
      connector = inst.getConnector(userName, token);
      BatchWriterConfig writerConfig = new BatchWriterConfig();
      writerConfig.setMaxLatency(maxLatencyInMillis, TimeUnit.MILLISECONDS);

      if (!isSingleTable) {
        multiTableBatchWriter = connector.createMultiTableBatchWriter(writerConfig);
      }
      else {
        singleTableBatchWriter = connector.createBatchWriter(table, writerConfig);
      }
      Authorizations readAuth = new Authorizations(readVisibility);
      batchScanner = connector.createBatchScanner(table, readAuth, numQueryThread);
      scanner = connector.createScanner(table, readAuth);
    }
    catch (Throwable t) {
      Throwables.propagate(t);
    }
  }

  @Override
  public void disconnect() throws IOException
  {
    try {
      if (singleTableBatchWriter != null) {
        singleTableBatchWriter.close();
      }
      if (multiTableBatchWriter != null) {
        multiTableBatchWriter.close();
      }
      token.destroy();
      connector = null;
    }
    catch (Throwable t) {
      Throwables.propagate(t);
    }
  }

  @Override
  public boolean isConnected()
  {
    return !token.isDestroyed();
  }

  /**
   * Sets the accumulo instance name.
   *
   * @param instanceName name of the accumulo instance.
   */
  public void setInstanceName(@Nonnull String instanceName)
  {
    this.instanceName = instanceName;
  }

  /**
   * Sets the user name.
   *
   * @param userName user name.
   */
  public void setUserName(@Nonnull String userName)
  {
    this.userName = userName;
  }

  /**
   * Sets the password.
   *
   * @param password password.
   */
  public void setPassword(@Nonnull String password)
  {
    this.password = password;
  }

  /**
   * Sets the zookeeper servers.
   *
   * @param zooServers comma-separated String of zookeeper servers.
   */
  public void setZooServers(@Nonnull String zooServers)
  {
    this.zooServers = zooServers;
  }

  /**
   * Sets the table that will be used by this store. If the table is set the store would write
   * to a single table otherwise based on the columnFamily of {@link Key} the table is selected from {@link #columnFamilyToTableMapping}.
   *
   * @param table table name.
   */
  public void setTable(@Nonnull String table)
  {
    this.table = table;
  }

  /**
   * Sets the mapping from column family to table which is used when the store writes to multiple tables.
   *
   * @param columnFamilyToTableMapping map of column family to table.
   */
  public void setColumnFamilyToTableMapping(@Nonnull Map<String, String> columnFamilyToTableMapping)
  {
    if (this.columnFamilyToTableMapping == null) {
      columnFamilyToTableMapping = Maps.newHashMap();
    }
    for (Map.Entry<String, String> inputEntry : columnFamilyToTableMapping.entrySet()) {
      this.columnFamilyToTableMapping.put(new Text(inputEntry.getKey()), inputEntry.getValue());
    }
  }

  /**
   * Sets the maximum amount of time to hold the data in memory before flushing it to servers.<br/>
   * <b>Default:</b> 500
   *
   * @param millis maximum latency in milliseconds.
   */
  public void setMaxLatencyInMillis(long millis)
  {
    this.maxLatencyInMillis = millis;
  }

  /**
   * Sets the visibility which is compared against the visibility of each key in order to filter out data.<br/>
   * <b>Default:</b> public
   *
   * @param readVisibility visibility permitted.
   */
  public void setReadVisibility(@Nonnull String readVisibility)
  {
    this.readVisibility = readVisibility;
  }

  /**
   * Sets the number of concurrent threads that will be spun for querying the database when scanning multiple ranges at the
   * same time. <br/>
   * <b>Default:</b> 5
   *
   * @param numQueryThread number of concurrent threads.
   */
  public void setNumQueryThread(int numQueryThread)
  {
    this.numQueryThread = numQueryThread;
  }

}
