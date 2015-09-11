/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.schemaApp;

/**
 * @category debug
 */
public class A extends B
{
  @Override
  /**
   * @omitFromUI
   */
  public void setProp2(String prop2)
  {
    super.setProp2(prop2);
  }
}
