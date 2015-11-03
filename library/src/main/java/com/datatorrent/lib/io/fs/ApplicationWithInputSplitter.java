/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.io.fs;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.io.block.FSSliceReader;

class ApplicationWithInputSplitter implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    FileSplitterInput input = dag.addOperator("Input", new FileSplitterInput());
    FSSliceReader reader = dag.addOperator("Block Reader", new FSSliceReader());

    dag.addStream("block-metadata", input.blocksMetadataOutput, reader.blocksMetadataInput);

  }

}
