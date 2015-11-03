/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.io.block;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;
import com.datatorrent.lib.io.fs.FileSplitterInput;

public class ApplicationWithBlockReader implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    FileSplitterInput input = dag.addOperator("File-splitter", new FileSplitterInput());
    //any concrete implementation of AbstractFSBlockReader based on the use-case can be added here.
    LineReader blockReader = dag.addOperator("Block-reader", new LineReader());
    Filter filter = dag.addOperator("Filter", new Filter());
    RecordOutputOperator recordOutputOperator = dag.addOperator("Record-writer", new RecordOutputOperator());

    dag.addStream("file-block metadata", input.blocksMetadataOutput, blockReader.blocksMetadataInput);
    dag.addStream("records", blockReader.messages, filter.input);
    dag.addStream("filtered-records", filter.output, recordOutputOperator.input);
  }

  /**
   * Concrete implementation of {@link AbstractFSBlockReader} for which a record is a line in the file.
   */
  public static class LineReader extends AbstractFSBlockReader.AbstractFSReadAheadLineReader<String>
  {

    @Override
    protected String convertToRecord(byte[] bytes)
    {
      return new String(bytes);
    }
  }

  /**
   * Considers any line starting with a '.' as invalid. Emits the valid records.
   */
  public static class Filter extends BaseOperator
  {
    public final transient DefaultOutputPort<AbstractBlockReader.ReaderRecord<String>> output = new DefaultOutputPort<>();
    public final transient DefaultInputPort<AbstractBlockReader.ReaderRecord<String>> input = new DefaultInputPort<AbstractBlockReader.ReaderRecord<String>>()
    {
      @Override
      public void process(AbstractBlockReader.ReaderRecord<String> stringRecord)
      {
        //filter records and transform
        //if the string starts with a '.' ignore the string.
        if (!StringUtils.startsWith(stringRecord.getRecord(), ".")) {
          output.emit(stringRecord);
        }
      }
    };
  }

  /**
   * Persists the valid records to corresponding block files.
   */
  public static class RecordOutputOperator extends AbstractFileOutputOperator<AbstractBlockReader.ReaderRecord<String>>
  {
    @Override
    protected String getFileName(AbstractBlockReader.ReaderRecord<String> tuple)
    {
      return Long.toHexString(tuple.getBlockId());
    }

    @Override
    protected byte[] getBytesForTuple(AbstractBlockReader.ReaderRecord<String> tuple)
    {
      return tuple.getRecord().getBytes();
    }
  }
}
