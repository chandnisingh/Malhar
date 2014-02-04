package com.datatorrent.contrib.accumulo;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.internal.Lists;
import com.beust.jcommander.internal.Maps;
import com.google.common.collect.ImmutableMap;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;

import com.datatorrent.lib.util.KeyValPair;

/**
 * Tests for {@link AbstractAccumuloInputOperator} and {@link AbstractAccumuloOutputOperator}.
 */
public class AccumuloOperatorTest
{
  private static final String TABLE_NAME = "test";
  private static final String COL_FAMILY = "test_family";
  private static final String COL_QUAL = "test_qual";
  private static final ColumnVisibility COL_VISIBILITY = new ColumnVisibility("public");

  private static AccumuloStore store;

  protected static class InputOperator extends AbstractAccumuloInputOperator<KeyValPair<String, String>>
  {
    @Override
    protected Collection<Range> getMultipleRanges()
    {
      List<Range> ranges = Lists.newArrayList();
      ranges.add(new Range("test1", "test3"));
      ranges.add(new Range("test6", "test9"));
      return ranges;
    }

    @Override
    protected Range getRange()
    {
      return null;
    }

    @Override
    protected KeyValPair<String, String> getTuple(Key key, Value value)
    {
      return new KeyValPair<String, String>(key.getRow().toString(), value.toString());
    }
  }

  protected static class OutputOperator extends AbstractAccumuloOutputOperator<KeyValPair<String, String>>
  {
    @SuppressWarnings("unchecked")
    @Override
    protected Key getKeyFrom(Object object)
    {
      KeyValPair<String, String> keyValPair = (KeyValPair<String, String>) object;
      return new Key(keyValPair.getKey(), COL_FAMILY, COL_QUAL, COL_VISIBILITY, System.currentTimeMillis());
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Value getValueFrom(Object object)
    {
      KeyValPair<String, String> keyValPair = (KeyValPair<String, String>) object;
      return new Value(keyValPair.getValue().getBytes());
    }
  }

  private static Instance testInstance = new ZooKeeperInstance("test", "localhost:2181");
  private static PasswordToken testToken = new PasswordToken("test");
  private static Connector testConnector;
  private static Map<String, String> tuples = Maps.newHashMap();

  @BeforeClass
  public static void setup()
  {
    try {
      testConnector = testInstance.getConnector("root", testToken);
      testConnector.tableOperations().create(TABLE_NAME);

      store = new AccumuloStore();
      store.setUserName("root");
      store.setPassword("test");
      store.setInstanceName("test");
      store.setZooServers("localhost:2181");
      store.setReadVisibility("public");
      store.setTable(TABLE_NAME);
      store.setNumQueryThread(2);

      for (int i = 0; i < 10; i++) {
        tuples.put("test" + i, Integer.toString(i));
      }
    }
    catch (Throwable t) {
      logger.error("setting up accumulo", t);
    }
  }

  @AfterClass
  public static void teardown()
  {
    try {
      testConnector.tableOperations().delete(TABLE_NAME);
      testToken.destroy();
    }
    catch (Throwable t) {
      logger.error("deleting table from accumulo", t);
    }
  }

  @Test
  public void testInputOperator() throws Exception
  {
    store.connect();
    store.put("test_abc", "789");
    store.put("test_def", "456");
    store.put("test_ghi", "123");
    store.disconnect();

    try {
      LocalMode lma = LocalMode.newInstance();
      DAG dag = lma.getDAG();
      @SuppressWarnings("unchecked")
      InputOperator<S> inputOperator = dag.addOperator("input", new InputOperator<S>());
      CollectorModule<Object> collector = dag.addOperator("collector", new CollectorModule<Object>());
      inputOperator.addKey("test_abc");
      inputOperator.addKey("test_def");
      inputOperator.addKey("test_ghi");
      inputOperator.setStore(store);
      dag.addStream("stream", inputOperator.outputPort, collector.inputPort);
      final LocalMode.Controller lc = lma.getController();
      lc.run(3000);
      lc.shutdown();
      Assert.assertEquals(CollectorModule.resultMap.get("test_abc"), "789");
      Assert.assertEquals(CollectorModule.resultMap.get("test_def"), "456");
      Assert.assertEquals(CollectorModule.resultMap.get("test_ghi"), "123");

    }
    finally {
      store.connect();
      store.remove("test_abc");
      store.remove("test_def");
      store.remove("test_ghi");
      store.disconnect();
    }
  }

  public void testOutputOperator() throws IOException
  {
    OutputOperator outputOperator = new OutputOperator();
    try {
      outputOperator.setStore(store);
      outputOperator.setup(null);
      outputOperator.beginWindow(1);
      for (Map.Entry<String, String> entry : tuples.entrySet()) {
        KeyValPair<String, String> tuple = new KeyValPair<String, String>(entry.getKey(), entry.getValue());
        outputOperator.input.process(tuple);
      }
      outputOperator.endWindow();
      outputOperator.teardown();
      store.connect();
      Assert.assertEquals(store.get("test_abc"), "123");
      Assert.assertEquals(store.get("test_def"), "456");
      Assert.assertEquals(store.get("test_ghi"), "789");
    }
    finally {
      store.remove("test_abc");
      store.remove("test_def");
      store.remove("test_ghi");
      store.disconnect();
    }
  }

  protected static transient final Logger logger = LoggerFactory.getLogger(AccumuloOperatorTest.class);

}
