/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.io.fs;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.io.block.FSSliceReader;
import com.datatorrent.lib.io.jms.AbstractJMSInputOperator;

public class ApplicationWithBaseSplitter implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    JMSInput input = dag.addOperator("Input", new JMSInput());
    FileSplitterBase splitter = dag.addOperator("Splitter", new FileSplitterBase());
    FSSliceReader blockReader = dag.addOperator("BlockReader", new FSSliceReader());

    dag.addStream("file-info", input.output, splitter.input);
    dag.addStream("block-metadata", splitter.blocksMetadataOutput, blockReader.blocksMetadataInput);
  }

  public static class JMSInput extends AbstractJMSInputOperator<AbstractFileSplitter.FileInfo>
  {

    public final transient DefaultOutputPort<AbstractFileSplitter.FileInfo> output = new DefaultOutputPort<>();

    @Override
    protected AbstractFileSplitter.FileInfo convert(Message message) throws JMSException
    {
      //assuming the message is a text message containing the absolute path of the file.
      return new AbstractFileSplitter.FileInfo(null, ((TextMessage)message).getText());
    }

    @Override
    protected void emit(AbstractFileSplitter.FileInfo payload)
    {
      output.emit(payload);
    }
  }
}
