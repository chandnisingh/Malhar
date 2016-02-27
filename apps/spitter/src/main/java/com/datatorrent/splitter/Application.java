/**
 * Put your copyright and license info here.
 */
package com.datatorrent.splitter;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.block.AbstractBlockReader;
import com.datatorrent.lib.io.block.FSSliceReader;
import com.datatorrent.lib.io.fs.FileSplitterInput;
import com.datatorrent.lib.stream.DevNull;
import com.datatorrent.netlet.util.Slice;

@ApplicationAnnotation(name = "SplitterReader")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {

    FileSplitterInput splitter = dag.addOperator("Splitter", new FileSplitterInput());
    splitter.setBlocksThreshold(50);


    FSSliceReader reader = dag.addOperator("Reader", new FSSliceReader());
    DevNull<AbstractBlockReader.ReaderRecord<Slice>> devNull = dag.addOperator("DevNull",
        new DevNull<AbstractBlockReader.ReaderRecord<Slice>>());

    SplitterThresholdRegulator thresholdRegulator = new SplitterThresholdRegulator(splitter.getBlocksThreshold());

    dag.setAttribute(splitter, Context.OperatorContext.STATS_LISTENERS, Arrays.asList(
        new StatsListener[] { thresholdRegulator }));
    dag.setAttribute(reader, Context.OperatorContext.STATS_LISTENERS, Arrays.asList(
        new StatsListener[] { thresholdRegulator }));

    dag.addStream("Block-metadata", splitter.blocksMetadataOutput, reader.blocksMetadataInput);
    dag.addStream("Records", reader.messages, devNull.data);
  }
}
