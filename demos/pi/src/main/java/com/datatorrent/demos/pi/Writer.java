/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.pi;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.io.fs.AbstractFSWriter;
import java.util.Random;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class Writer extends AbstractFSWriter<byte[]>
{
  private transient int operatorId;
  private Random random = new Random();

  @Override
  public void setup(OperatorContext oc)
  {
    operatorId = oc.getId();
    super.setup(oc);
  }

  @Override
  protected String getFileName(byte[] input)
  {
    return String.valueOf(operatorId) + "." + random.nextInt(16);
  }

  @Override
  protected byte[] getBytesForTuple(byte[] input)
  {
    String val = "";
    for (int i = 0; i < random.nextInt(5); i++) {
      val = val + new String(input) + "\n";
    }
    return val.getBytes();
  }

}