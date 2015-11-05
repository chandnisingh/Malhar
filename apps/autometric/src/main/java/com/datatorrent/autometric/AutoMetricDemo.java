/**
 * Put your copyright and license info here.
 */
package com.datatorrent.autometric;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.fs.AbstractFileInputOperator;

@ApplicationAnnotation(name = "AutoMetricDemo")
public class AutoMetricDemo implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    AbstractFileInputOperator.FileLineInputOperator input = dag.addOperator("LineInput",
      new AbstractFileInputOperator.FileLineInputOperator());
    LineReceiver receiver = dag.addOperator("LineReceiver", new LineReceiver());

    dag.addStream("Blocks", input.output, receiver.input);
  }
}
