/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.examples;

import java.sql.Timestamp;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

import com.datatorrent.lib.io.block.AbstractBlockReader;
import com.datatorrent.lib.io.block.FSSliceReader;
import com.datatorrent.lib.io.fs.AbstractWindowFileOutputOperator;
import com.datatorrent.lib.io.fs.FileSplitterInput;
import com.datatorrent.netlet.util.Slice;

@ApplicationAnnotation(name = "Copy")
public class CopyApplication implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    FileSplitterInput input = dag.addOperator("Input", new FileSplitterInput());
    FSSliceReader reader = dag.addOperator("Reader", new FSSliceReader());
    SliceWriter writer = dag.addOperator("Writer", new SliceWriter());

    dag.addStream("Blocks", input.blocksMetadataOutput, reader.blocksMetadataInput);
    dag.addStream("Slices", reader.messages, writer.input);
  }

  public static class SliceWriter extends AbstractWindowFileOutputOperator<AbstractBlockReader.ReaderRecord<Slice>>
  {
    public SliceWriter()
    {
      this.rotationWindows = 60;
    }

    @Override
    protected byte[] getBytesForTuple(AbstractBlockReader.ReaderRecord<Slice> tuple)
    {
      return tuple.getRecord().toByteArray();
    }

  }
}
