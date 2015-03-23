/*
 *  Copyright (c) 2012-2015 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.genericoperator;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator;

public class GenericSum implements Operator
{
  String sourceFieldName;
  Class sourceClass;
  String destFieldName;
  Class destClass;


  @Override
  public void setup(OperatorContext arg0)
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void teardown()
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void beginWindow(long arg0)
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void endWindow()
  {
    // TODO Auto-generated method stub

  }

}
