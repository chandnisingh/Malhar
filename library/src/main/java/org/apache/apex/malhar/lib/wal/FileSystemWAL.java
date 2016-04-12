/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.wal;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.utils.FileContextUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;

import com.datatorrent.api.annotation.Stateless;

public class FileSystemWAL<T> implements WAL<FileSystemWAL.FileSystemWALReader, FileSystemWAL.FileSystemWALWriter>
{
  @NotNull
  private Serde<T> serde;

  @NotNull
  private String filePath;

  //max length of the file
  private long maxLength;

  private transient FileContext fileContext;

  @NotNull
  private FileSystemWAL<T>.FileSystemWALReader fileSystemWALReader = new FileSystemWALReader();

  @NotNull
  private FileSystemWAL<T>.FileSystemWALWriter fileSystemWALWriter = new FileSystemWALWriter();

  private long lastCheckpointedWindow = Stateless.WINDOW_ID;

  @Override
  public void setup()
  {
    try {
      fileContext = FileContextUtils.getFileContext(filePath);
      if (maxLength == 0) {
        maxLength = fileContext.getDefaultFileSystem().getServerDefaults().getBlockSize();
      }
      fileSystemWALReader.open();

      fileSystemWALWriter.recover();
      fileSystemWALWriter.open();
    } catch (IOException e) {
      throw new RuntimeException("during setup", e);
    }
  }

  @Override
  public void beforeCheckpoint(long window)
  {
    try {
      lastCheckpointedWindow = window;
      fileSystemWALWriter.flush();
    } catch (IOException e) {
      throw new RuntimeException("during before cp", e);
    }
  }

  @Override
  public void committed(long window)
  {
    try {
      fileSystemWALWriter.finalizeFiles(window);
    } catch (IOException e) {
      throw new RuntimeException("during committed", e);
    }
  }

  @Override
  public void teardown()
  {
    try {
      fileSystemWALReader.close();
      fileSystemWALWriter.close();
    } catch (IOException e) {
      throw new RuntimeException("during teardown", e);
    }
  }

  protected String getPartFilePath(int partNumber)
  {
    return filePath + "_" + partNumber;
  }

  @Override
  public FileSystemWALReader getReader() throws IOException
  {
    return fileSystemWALReader;
  }

  /**
   * Sets the  File System WAL Reader. This can be used to override the default wal reader.
   *
   * @param fileSystemWALReader wal reader.
   */
  public void setFileSystemWALReader(FileSystemWALReader fileSystemWALReader)
  {
    this.fileSystemWALReader = fileSystemWALReader;
  }

  @Override
  public FileSystemWALWriter getWriter() throws IOException
  {
    return fileSystemWALWriter;
  }

  /**
   * Sets the File System WAL Writer. This can be used to override the default wal writer.
   * @param fileSystemWALWriter wal writer.
   */
  public void setFileSystemWALWriter(FileSystemWALWriter fileSystemWALWriter)
  {
    this.fileSystemWALWriter = fileSystemWALWriter;
  }

  /**
   * @return WAL Entry serde
   */
  public Serde<T> getSerde()
  {
    return serde;
  }

  /**
   * Sets the serde which is used for wal entry serialization and de-serialization
   *
   * @param serde serializer/deserializer
   */
  public void setSerde(Serde<T> serde)
  {
    this.serde = serde;
  }

  /**
   * @return WAL file path
   */
  public String getFilePath()
  {
    return filePath;
  }

  /**
   * Sets the WAL file path.
   *
   * @param filePath WAL file path
   */
  public void setFilePath(String filePath)
  {
    this.filePath = filePath;
  }

  /**
   * @return max length of a WAL part file.
   */
  public long getMaxLength()
  {
    return maxLength;
  }

  /**
   * Sets the maximum length of a WAL part file.
   *
   * @param maxLength max length of the WAL part file
   */
  public void setMaxLength(long maxLength)
  {
    this.maxLength = maxLength;
  }

  public static class FileSystemWALPointer implements Comparable<FileSystemWALPointer>
  {
    private final int partNum;
    private long offset;

    public FileSystemWALPointer(long offset)
    {
      this(0, offset);
    }

    public FileSystemWALPointer(int partNum, long offset)
    {
      this.partNum = partNum;
      this.offset = offset;
    }

    @Override
    public int compareTo(@NotNull FileSystemWALPointer o)
    {
      if (this.partNum < o.partNum) {
        return -1;
      }
      if (this.partNum > o.partNum) {
        return 1;
      }
      if (this.offset < o.offset) {
        return -1;
      }
      if (this.offset > o.offset) {
        return 1;
      }
      return 0;
    }

    public int getPartNum()
    {
      return partNum;
    }

    public long getOffset()
    {
      return offset;
    }

    @Override
    public String toString()
    {
      return "FileSystemWalPointer{" + "partNum=" + partNum + ", offset=" + offset + '}';
    }
  }

  public class FileSystemWALReader implements WAL.WALReader<T, FileSystemWALPointer>
  {
    private T entry;
    private FileSystemWALPointer currentPointer = new FileSystemWALPointer(0, 0);

    private transient DataInputStream inputStream;
    private transient Path currentOpenPath;

    protected void open() throws IOException
    {
      //initialize the input stream
      inputStream = getInputStream(currentPointer);
    }

    protected void close() throws IOException
    {
      if (inputStream != null) {
        inputStream.close();
        inputStream = null;
      }
    }

    @Override
    public void seek(FileSystemWALPointer pointer) throws IOException
    {
      if (inputStream != null) {
        inputStream.close();
      }
      inputStream = getInputStream(pointer);
      Preconditions.checkNotNull(inputStream, "invalid pointer " + pointer);
      currentPointer = pointer;
    }

    /**
     * Move to the next WAL segment.
     *
     * @return true if the next part file exists and is opened; false otherwise.
     * @throws IOException
     */
    private boolean nextSegment() throws IOException
    {
      if (inputStream != null) {
        inputStream.close();
        inputStream = null;
      }

      currentPointer = new FileSystemWALPointer(currentPointer.partNum + 1, 0);
      inputStream = getInputStream(currentPointer);

      return inputStream != null;
    }

    private DataInputStream getInputStream(FileSystemWALPointer walPointer) throws IOException
    {
      Path walPartPath = new Path(getPartFilePath(walPointer.partNum));
      if (fileContext.util().exists(walPartPath)) {
        DataInputStream stream = fileContext.open(walPartPath);
        if (walPointer.offset > 0) {
          stream.skip(walPointer.offset);
        }
        currentOpenPath = walPartPath;
        return stream;
      }
      return null;
    }

    @Override
    public boolean advance() throws IOException
    {
      do {
        if (inputStream == null) {
          inputStream = getInputStream(currentPointer);
        }

        if (inputStream != null && currentPointer.offset < fileContext.getFileStatus(currentOpenPath).getLen()) {
          int len = inputStream.readInt();
          Preconditions.checkState(len >= 0, "negative length");

          byte[] data = new byte[len];
          inputStream.readFully(data);

          entry = serde.fromBytes(data);
          currentPointer.offset += data.length + 4;
          return true;
        }
      } while (nextSegment());

      entry = null;
      return false;
    }

    @Override
    public T get()
    {
      return entry;
    }
  }

  public class FileSystemWALWriter implements WAL.WALWriter<T, FileSystemWALPointer>
  {
    private FileSystemWALPointer currentPointer = new FileSystemWALPointer(0, 0);
    private transient DataOutputStream outputStream;

    //windowId => Latest part which can be finalized.
    private final Map<Long, Integer> pendingFinalization = new TreeMap<>();

    //part => tmp file path;
    private final Map<Integer, String> tmpFiles = new TreeMap<>();

    private void recover() throws IOException
    {
      String tmpFilePath = tmpFiles.get(currentPointer.getPartNum());
      if (tmpFilePath != null) {

        Path tmpPath = new Path(tmpFilePath);
        if (fileContext.util().exists(tmpPath)) {
          LOG.debug("tmp path exists {}", tmpPath);

          outputStream = getOutputStream(currentPointer);
          DataInputStream inputStreamOldTmp = fileContext.open(tmpPath);
          IOUtils.copy(inputStreamOldTmp, outputStream);

          //remove old tmp
          inputStreamOldTmp.close();
          fileContext.delete(tmpPath, true);
        }
      }

      //delete any other stray tmp files of parts that are greater than currentPointer.part
      Iterator<Map.Entry<Integer, String>> iterator = tmpFiles.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<Integer, String> entry = iterator.next();
        if (entry.getKey() >= currentPointer.partNum) {
          break;
        }

        fileContext.delete(new Path(entry.getValue()), true);
      }

    }

    protected void open() throws IOException
    {
      outputStream = getOutputStream(currentPointer);
    }

    protected void close() throws IOException
    {
      if (outputStream != null) {
        flush();
        outputStream.close();
        outputStream = null;
      }
    }

    @Override
    public int append(T entry) throws IOException
    {
      byte[] slice = serde.toBytes(entry);
      int entryLength = slice.length + 4;

      // rotate if needed
      if (shouldRotate(entryLength)) {
        rotate();
      }

      outputStream.writeInt(slice.length);
      outputStream.write(slice);
      currentPointer.offset += entryLength;

      return entryLength;
    }

    protected void flush() throws IOException
    {
      if (outputStream != null) {
        outputStream.flush();
        if (outputStream instanceof FSDataOutputStream) {
          ((FSDataOutputStream)outputStream).hflush();
          ((FSDataOutputStream)outputStream).hsync();
        }
      }
    }

    protected boolean shouldRotate(int entryLength)
    {
      if (currentPointer.offset + entryLength > maxLength) {
        return true;
      }
      return false;
    }

    protected void rotate() throws IOException
    {
      close();
      //all parts up to current part can be finalized.
      pendingFinalization.put(lastCheckpointedWindow, currentPointer.partNum);

      //if adding the new entry to the file can cause the current file to exceed the max length then it is rotated.
      FileSystemWALPointer nextPartPointer = new FileSystemWALPointer(currentPointer.partNum + 1, 0);
      outputStream = getOutputStream(nextPartPointer);
      currentPointer = nextPartPointer;
    }

    protected void finalizeFiles(long window) throws IOException
    {
      int largestPartFinalized = -1;

      Iterator<Map.Entry<Long, Integer>> pendingFinalizeIter = pendingFinalization.entrySet().iterator();
      while (pendingFinalizeIter.hasNext()) {
        Map.Entry<Long, Integer> entry = pendingFinalizeIter.next();

        if (entry.getKey() >  window) {
          //finalize files which were requested for finalization in the window <= committed window
          break;
        }
        pendingFinalizeIter.remove();

        int partToFinalizeTill = entry.getValue();
        for (int i = largestPartFinalized + 1; i <= partToFinalizeTill; i++) {
          String tmpToFinalize = tmpFiles.remove(i);
          fileContext.rename(new Path(tmpToFinalize), new Path(getPartFilePath(i)), Options.Rename.OVERWRITE);
        }
      }
    }

    private DataOutputStream getOutputStream(FileSystemWALPointer pointer) throws IOException
    {
      Preconditions.checkArgument(outputStream == null, "output stream is not null");
      String partFile = getPartFilePath(pointer.partNum);
      String tmpFilePath = getTmpFilePath(partFile);
      tmpFiles.put(pointer.partNum, tmpFilePath);

      return fileContext.create(new Path(tmpFilePath), EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE),
          Options.CreateOpts.CreateParent.createParent());
    }

  }

  private static String getTmpFilePath(String filePath)
  {
    return filePath + '.' + System.currentTimeMillis() + TMP_EXTENSION;
  }

  private static final String TMP_EXTENSION = ".tmp";

  private static final Logger LOG = LoggerFactory.getLogger(FileSystemWAL.class);
}
