/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.autometric;

import com.datatorrent.lib.io.block.AbstractFSBlockReader;

public class LineReader extends AbstractFSBlockReader.AbstractFSReadAheadLineReader<String>
{
  @Override
  protected String convertToRecord(byte[] bytes)
  {
    return new String(bytes);
  }
}
