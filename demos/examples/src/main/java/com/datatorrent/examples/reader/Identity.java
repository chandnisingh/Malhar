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

import com.datatorrent.lib.io.block.BlockMetadata;
import com.datatorrent.lib.io.block.ReaderContext;

public class Identity
{

  public static String[] PROPERTY_NAMES = {"name", "place"};
  public String name;
  public String place;

  public String getName()
  {
    return name;
  }

  public void setName(String name)
  {
    this.name = name;
  }

  public String getPlace()
  {
    return place;
  }

  public void setPlace(String place)
  {
    this.place = place;
  }
}
