/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.examples.reader;

import java.io.IOException;
import java.io.InputStreamReader;

import org.supercsv.io.CsvBeanReader;
import org.supercsv.prefs.CsvPreference;

import org.apache.hadoop.fs.FSDataInputStream;

import com.datatorrent.lib.io.block.AbstractFSBlockReader;
import com.datatorrent.lib.io.block.BlockMetadata;
import com.datatorrent.lib.io.block.ReaderContext;

public class CSVBlockReader extends AbstractFSBlockReader<Identity>
{
  public CSVBlockReader()
  {
    this.readerContext = new IdentityReadContext();
  }

  @Override
  protected Identity convertToRecord(byte[] bytes)
  {
    return ((IdentityReadContext)readerContext).getLastReadIdentity();
  }

  public static class IdentityReadContext extends ReaderContext.LineReaderContext<FSDataInputStream>
  {
    private transient CsvBeanReader csvReader;
    private transient Identity lastReadIdentity;

    public IdentityReadContext()
    {
    }

    @Override
    public void initialize(FSDataInputStream stream, BlockMetadata blockMetadata, boolean consecutiveBlock)
    {
      super.initialize(stream, blockMetadata, consecutiveBlock);
      //ignore first entity of  all the blocks except the first one because those bytes
      //were used during the parsing of the previous block.
      if (!consecutiveBlock && blockMetadata.getOffset() != 0) {
        try {
          Entity entity = super.readEntity();
          offset += entity.getUsedBytes();
        } catch (IOException e) {
          throw new RuntimeException("when reading first entity", e);
        }
      }

      csvReader = new CsvBeanReader(new InputStreamReader(stream), CsvPreference.STANDARD_PREFERENCE);
      try {
        stream.seek(offset);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    protected Entity readEntity() throws IOException
    {
      entity.clear();
      lastReadIdentity = csvReader.read(Identity.class, Identity.PROPERTY_NAMES);
      entity.setRecord(csvReader.getUntokenizedRow().getBytes());
      entity.setUsedBytes(entity.getRecord().length + 1);
      return entity;
    }

    protected Identity getLastReadIdentity()
    {
      return lastReadIdentity;
    }
  }
}



