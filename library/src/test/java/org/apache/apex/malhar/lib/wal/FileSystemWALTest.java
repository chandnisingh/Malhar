/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.wal;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.util.TestUtils;

public class FileSystemWALTest
{
  private static final Random RAND = new Random();

  private static byte[] genRandomByteArray(int len)
  {
    byte[] val = new byte[len];
    RAND.nextBytes(val);
    return val;
  }

  private class TestMeta extends TestWatcher
  {
    private String targetDir;
    FileSystemWAL<byte[]> fsWAL = new FileSystemWAL<>();


    @Override
    protected void starting(Description description)
    {
      TestUtils.deleteTargetTestClassFolder(description);
      targetDir = "target/" + description.getClassName() + "/" + description.getMethodName();
      fsWAL = new FileSystemWAL<>();
      fsWAL.setFilePath(targetDir + "/WAL");
      fsWAL.setSerde(new ByteArraySerde());
    }

    @Override
    protected void finished(Description description)
    {
      TestUtils.deleteTargetTestClassFolder(description);
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  /**
   * - Write some data to WAL
   * - Read the data back. The amount of data read should be
   *   same as amount of data written.
   * @throws IOException
   */
  @Test
  public void testWalWriteAndRead() throws IOException
  {
    testMeta.fsWAL.setup();
    int numTuples = 100;

    FileSystemWAL<byte[]>.FileSystemWALWriter fsWALWriter = testMeta.fsWAL.getWriter();
    for (int i = 0; i < numTuples; i++) {
      int len = RAND.nextInt(100);
      fsWALWriter.append(genRandomByteArray(len));
    }
    fsWALWriter.rotate();
    testMeta.fsWAL.beforeCheckpoint(0);
    testMeta.fsWAL.committed(0);
    File walFile = new File(testMeta.fsWAL.getPartFilePath(0));
    Assert.assertEquals("WAL file created ", true, walFile.exists());

    FileSystemWAL<byte[]>.FileSystemWALReader fsWALReader = testMeta.fsWAL.getReader();

    int tuples = 0;
    while (fsWALReader.advance()) {
      fsWALReader.get();
      tuples++;
    }
    Assert.assertEquals("Write and read same number of tuples ", numTuples, tuples);
    testMeta.fsWAL.teardown();
  }

  /**
   * Read WAL from middle of the file by seeking to known valid
   * offset and start reading from that point till the end.
   */
  @Test
  public void testWalSkip() throws IOException
  {
    testMeta.fsWAL.setup();

    FileSystemWAL<byte[]>.FileSystemWALWriter fsWALWriter = testMeta.fsWAL.getWriter();

    int totalTuples = 100;
    int totalBytes = 0;
    long offset = 0;
    for (int i = 0; i < totalTuples; i++) {
      if (i == 30) {
        offset = totalBytes;
      }
      totalBytes += fsWALWriter.append(genRandomByteArray(100));
    }

    fsWALWriter.rotate();
    testMeta.fsWAL.beforeCheckpoint(0);
    testMeta.fsWAL.committed(0);

    File walFile = new File(testMeta.fsWAL.getPartFilePath(0));
    Assert.assertEquals("WAL file size ", totalBytes, walFile.length());

    FileSystemWAL<byte[]>.FileSystemWALReader fsWALReader = testMeta.fsWAL.getReader();
    fsWALReader.seek(new FileSystemWAL.FileSystemWALPointer(offset));

    int read = 0;
    while (fsWALReader.advance()) {
      read++;
      fsWALReader.get();
    }

    Assert.assertEquals("Number of tuples read after skipping", (totalTuples - 30), read);
    testMeta.fsWAL.teardown();
  }

  /**
   * Test WAL rolling functionality, set segment size to 32 * 1024.
   * Write some data which will go over WAL segment size. multiple files
   * should be created.
   * @throws IOException
   */
  @Test
  public void testWalRolling() throws IOException
  {

    testMeta.fsWAL.setMaxLength(32 * 1024);
    testMeta.fsWAL.setup();

    FileSystemWAL<byte[]>.FileSystemWALWriter fsWALWriter = testMeta.fsWAL.getWriter();

    /* write each record of size 1K, each file will have 32 records, last file will have
     * (count % 32) records. */
    int wrote = 100;
    for (int i = 0; i < wrote; i++) {
      fsWALWriter.append(genRandomByteArray(1020));
    }

    fsWALWriter.rotate();
    testMeta.fsWAL.beforeCheckpoint(0);
    testMeta.fsWAL.committed(0);

    /** Read from start till the end */
    FileSystemWAL<byte[]>.FileSystemWALReader reader = testMeta.fsWAL.getReader();

    int read = 0;
    while (reader.advance()) {
      reader.get();
      read++;
    }

    Assert.assertEquals("Number of records read from start ", wrote, read);

    /* skip first file for reading, and then read other entries till the end */
    reader.seek(new FileSystemWAL.FileSystemWALPointer(1, 0));

    read = 0;
    while (reader.advance()) {
      reader.get();
      read++;
    }
    Assert.assertEquals("Number of record read after skipping one file ", wrote - 32, read);

    /* skip first file and few records from the next file, and read till the end */
    reader.seek(new FileSystemWAL.FileSystemWALPointer(1, 16 * 1024));
    read = 0;
    while (reader.advance()) {
      reader.get();
      read++;
    }
    Assert.assertEquals("Number of record read after skipping one file ", wrote - (32 + 16), read);

  }

  private static class ByteArraySerde implements WAL.Serde<byte[]>
  {
    @Override
    public byte[] toBytes(byte[] tuple)
    {
      return tuple;
    }

    @Override
    public byte[] fromBytes(byte[] data)
    {
      return data;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(FileSystemWALTest.class);

}
