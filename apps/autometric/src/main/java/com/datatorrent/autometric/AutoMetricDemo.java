/**
 * Put your copyright and license info here.
 */
package com.datatorrent.autometric;

import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

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
    LineInputOperator input = dag.addOperator("LineInput", new LineInputOperator());
    LineReceiver receiver = dag.addOperator("LineReceiver", new LineReceiver());

    dag.addStream("Blocks", input.output, receiver.input);
  }

  public static class LineInputOperator extends AbstractFileInputOperator.FileLineInputOperator
  {
    private boolean emit;

    @Override
    public void emitTuples()
    {
      if (emit) {
        emit = false;
        super.emitTuples();
      }
    }

    @Override
    public void beginWindow(long windowId)
    {
      emit = true;
      super.beginWindow(windowId);
    }

    @Override
    protected void scanDirectory()
    {
      if (System.currentTimeMillis() - scanIntervalMillis >= lastScanMillis) {
        Set<Path> newPaths = scanner.scan(fs, filePath, processedFiles);

        for (Path newPath : newPaths) {
          String newPathString = newPath.toString();
          pendingFiles.add(newPathString);
          localProcessedFileCount.increment();
        }
        lastScanMillis = System.currentTimeMillis();
      }
    }
  }
}
