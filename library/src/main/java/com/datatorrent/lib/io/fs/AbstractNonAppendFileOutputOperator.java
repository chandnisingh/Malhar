/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.io.fs;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;

import com.datatorrent.api.Context;

public abstract class AbstractNonAppendFileOutputOperator<INPUT> extends AbstractFileOutputOperator<INPUT>
{
  private static String APPEND_TMP_FILE = "_APPENDING";

  private transient FileContext fileContext;

  public AbstractNonAppendFileOutputOperator()
  {
    //reduces the frequency of append being invoked.
    setExpireStreamAfterAccessMillis(60 * 60 * 1000L);
    setMaxOpenFiles(1000);
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    try {
      fileContext = FileContext.getFileContext(fs.getUri());
    } catch (UnsupportedFileSystemException e) {
      throw new RuntimeException("unsupported fs", e);
    }
  }

  @Override
  protected FSDataOutputStream openStream(Path filepath, boolean append) throws IOException
  {
    if (append) {
      //Since underlying filesystems do not support append, we have to achieve that behavior by writing to a tmp
      //file and then re-writing the contents back to stream opened in create mode for filepath.
      Path appendTmpFile = new Path(filePath + Path.SEPARATOR + APPEND_TMP_FILE);
      fileContext.rename(filepath, appendTmpFile, Options.Rename.OVERWRITE);
      FSDataInputStream fsIn = fs.open(appendTmpFile);
      FSDataOutputStream fsOut = fs.create(filepath);
      IOUtils.copy(fsIn, fsOut);
      return fsOut;
    }
    return super.openStream(filepath, false);
  }
}
