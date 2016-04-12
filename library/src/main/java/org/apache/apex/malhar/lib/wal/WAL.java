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

import java.io.IOException;

/**
 * This interface represents a write ahead log that can be used by operator.
 * the WAL is split into two interfaces, a WALWriter which allows writing
 * data, and WALReader which provides iterator like interface to read entries
 * written to the WAL.
 *
 * @param <READER> Type of WAL Reader
 * @param <WRITER> WAL Pointer Type.
 */
public interface WAL<READER extends WAL.WALReader, WRITER extends WAL.WALWriter>
{
  void setup();

  void teardown();

  void beforeCheckpoint(long window);

  void committed(long window);

  READER getReader() throws IOException;

  WRITER getWriter() throws IOException;

  /**
   * Provides iterator like interface to read entries from the WAL.
   * @param <T> type of WAL entries
   * @param <P> type of Pointer in the WAL
   */
  interface WALReader<T, P>
  {
    /**
     * Seek to middle of the WAL. This is used primarily during recovery,
     * when we need to start recovering data from middle of WAL file.
     */
    void seek(P pointer) throws IOException;

    /**
     * Advance WAL by one entry, returns true if it can advance, else false
     * in case of any other error throws an Exception.
     *
     * @return true if next data item is read successfully, false if data can not be read.
     * @throws IOException
     */
    boolean advance() throws IOException;

    /**
     * Return current entry from WAL, returns null if end of file has reached.
     *
     * @return MutableKeyValue
     */
    T get();
  }

  /**
   * Provide method to write entries to the WAL.
   * @param <T>
   * @param <P>
   */
  interface WALWriter<T, P>
  {
    /**
     * Write an entry to the WAL
     */
    int append(T entry) throws IOException;

  }

  /**
   * Serializer interface used while reading and writing entries to the WAL.
   * @param <T>
   */
  interface Serde<T>
  {
    byte[] toBytes(T tuple);

    T fromBytes(byte[] data);
  }
}
