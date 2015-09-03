/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.examples;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;

import com.datatorrent.lib.io.fs.AbstractFileInputOperatorTest;
import com.datatorrent.lib.io.fs.AbstractSingleFileOutputOperator;

public class ApplicationTest
{
  public static class TestMeta extends TestWatcher
  {
    public String dir = null;
    public String srcDir;

    @Override
    protected void starting(Description description)
    {
      String methodName = description.getMethodName();
      String className = description.getClassName();
      this.dir = "target/" + className + "/" + methodName;
      this.srcDir = "target/" + className;

      HashSet<String> lines = Sets.newHashSet();
      for (int line = 0; line < 2; line++) {
        lines.add("l" + line);
      }
      File created = new File(this.srcDir, "file1.txt");
      try {
        FileUtils.write(created, StringUtils.join(lines, '\n'));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

    }

    @Override
    protected void finished(Description description)
    {
      FileUtils.deleteQuietly(new File("target/" + description.getClassName()));
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testApplication() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    conf.set("dt.operator.Input.prop.scanner.filePatternRegexp", ".*.txt");
    conf.set("dt.operator.Input.prop.directory", testMeta.srcDir);
    conf.set("dt.operator.Output.prop.filePath", testMeta.dir);

    lma.prepareDAG(new MyStreamingApp(), conf);
    lma.cloneDAG(); // check serialization
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);
    lc.runAsync();

    long now = System.currentTimeMillis();
    File outputFile = new File(testMeta.dir, "outfile.txt");

    while (!outputFile.exists() && System.currentTimeMillis() - now < 30000) {
      Thread.sleep(1000);
      LOG.debug("Waiting for {}", outputFile);
    }

    lc.shutdown();
    Assert.assertTrue("error file exists " + outputFile, outputFile.exists() && outputFile.isFile());
  }

  static class MyStreamingApp implements StreamingApplication
  {

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      AbstractFileInputOperatorTest.TestFileInputOperator input = dag.addOperator("Input", new AbstractFileInputOperatorTest.TestFileInputOperator());
      SimpleFileOutputOperator output = dag.addOperator("Output", new SimpleFileOutputOperator());
      output.setAlwaysWriteToTmp(false);
      output.setOutputFileName("outfile.txt");
      dag.addStream("data", input.output, output.input);
    }
  }

  /**
   * Simple writer which writes to one file.
   */
  private static class SimpleFileOutputOperator extends AbstractSingleFileOutputOperator<String>
  {
    @Override
    protected FileSystem getFSInstance() throws IOException
    {
      return FileSystem.getLocal(new Configuration()).getRaw();
    }

    @Override
    protected byte[] getBytesForTuple(String tuple)
    {
      return (tuple + "\n").getBytes();
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(ApplicationTest.class);
}
