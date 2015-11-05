/**
 * Put your copyright and license info here.
 */
package com.datatorrent.autometric;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.fs.FileSplitterInput;

@ApplicationAnnotation(name="AutoMetricDemo")
public class AutoMetricDemo implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    FileSplitterInput input = dag.addOperator("FileSplitter", new FileSplitterInput());
    LineReader reader = dag.addOperator("LineReader", new LineReader());
    LineReceiver receiver = dag.addOperator("LineReceiver", new LineReceiver());

    dag.addStream("Blocks", input.blocksMetadataOutput, reader.blocksMetadataInput);
    dag.addStream("Lines", reader.messages, receiver.input);
  }
}
