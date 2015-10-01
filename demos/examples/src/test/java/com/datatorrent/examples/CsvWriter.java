/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.examples;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class CsvWriter
{
  public static void main(String[] args) throws IOException
  {
    File file = new File("src/test/resources/test.csv");
    BufferedWriter bw = new BufferedWriter(new FileWriter(file.getAbsolutePath()));
    for (int i = 0; i < 100; i++) {
      bw.write(Integer.toString(i));
      bw.write(',');
      bw.write(Integer.toString(i + 1));
      bw.flush();
      bw.newLine();
    }
    bw.close();
  }
}
