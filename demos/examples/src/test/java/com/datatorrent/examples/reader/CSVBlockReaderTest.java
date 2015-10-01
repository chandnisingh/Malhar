package com.datatorrent.examples.reader;

import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Lists;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.io.block.AbstractBlockReader;
import com.datatorrent.lib.io.fs.FileSplitterInput;

/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
public class CSVBlockReaderTest
{

  public class TestMeta extends TestWatcher
  {
    public String dir = null;

    @Override
    protected void starting(Description description)
    {
      String methodName = description.getMethodName();
      String className = description.getClassName();
      this.dir = "target/" + className + "/" + methodName;
    }

    @Override
    protected void finished(Description description)
    {
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testApplicationSinglePartition()
  {
    Configuration conf = new Configuration(false);
    conf.set("dt.operator.FileSplitter.prop.blockSize", "253");
    conf.set("dt.operator.FileSplitter.prop.scanner.files", "src/test/resources/test.csv");
    conf.set("dt.operator.FileSplitter.prop.scanner.filePatternRegexp", ".*");
    testHelper(conf);
  }

  @Test
  public void testApplicationMultiPartition()
  {
    Configuration conf = new Configuration(false);
    conf.set("dt.operator.CSVBlockReader.attr.PARTITIONER", "com.datatorrent.common.partitioner.StatelessPartitioner:2");
    conf.set("dt.operator.FileSplitter.prop.blockSize", "253");
    conf.set("dt.operator.FileSplitter.prop.scanner.files", "src/test/resources/test.csv");
    conf.set("dt.operator.FileSplitter.prop.scanner.filePatternRegexp", ".*");
    conf.set("dt.operator.Writer.prop.filePath", testMeta.dir);

    testHelper(conf);
  }

  private void testHelper(Configuration conf)
  {

    CSVBlockReaderTestApp application = new CSVBlockReaderTestApp();

    try {
      LocalMode lma = LocalMode.newInstance();
      lma.prepareDAG(application, conf);
      lma.cloneDAG(); // check serialization
      LocalMode.Controller lc = lma.getController();
      lc.setHeartbeatMonitoringEnabled(false);
      lc.run(3000);
      lc.shutdown();
    } catch (Exception ex) {
      LOG.debug("{}", ex.getCause());
      throw new RuntimeException(ex);
    }
  }

  public static class CSVBlockReaderTestApp implements StreamingApplication
  {
    Collector output;

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      FileSplitterInput fileSplitter = dag.addOperator("FileSplitter", new FileSplitterInput());
      CSVBlockReader blockReader = dag.addOperator("CSVBlockReader", new CSVBlockReader());
      output = dag.addOperator("Collector", new Collector());

      dag.addStream("Blocks", fileSplitter.blocksMetadataOutput, blockReader.blocksMetadataInput);
      dag.addStream("Identities", blockReader.messages, output.input);
    }
  }

  public static class Collector extends BaseOperator
  {
    List<Integer> data = Lists.newArrayList();
    public final transient DefaultInputPort<AbstractBlockReader.ReaderRecord<Identity>> input = new DefaultInputPort<AbstractBlockReader.ReaderRecord<Identity>>()
    {
      @Override
      public void process(AbstractBlockReader.ReaderRecord<Identity> tuple)
      {
        LOG.debug("GOT {}", tuple.getRecord().getName());
        data.add(Integer.parseInt(tuple.getRecord().getName()));
      }
    };
  }

  private static final Logger LOG = LoggerFactory.getLogger(CSVBlockReaderTest.class);

}
