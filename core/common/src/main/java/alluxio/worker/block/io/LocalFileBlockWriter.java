/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.block.io;

//import alluxio.util.io.BufferUtils;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import io.netty.buffer.ByteBuf;
import lib.llpl.Heap;
import lib.llpl.MemoryBlock;
import lib.llpl.Transactional;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class provides write access to a temp block data file locally stored in managed storage.
 */
@NotThreadSafe
public final class LocalFileBlockWriter implements BlockWriter {
  private final String mFilePath;
  private final RandomAccessFile mLocalFile;
  private final FileChannel mLocalFileChannel;
  private final Closer mCloser = Closer.create();
  private long mPosition;
  private boolean mClosed;
  private Heap mHeap;
  private MemoryBlockArray mHeaderFile;
  private int mArraySlot;

  /**
   * Constructs a Block writer given the file path of the block.
   *
   * @param path file path of the block
   */
  public LocalFileBlockWriter(String path) throws IOException {
    File testfile = new File(path);
    boolean exists = testfile.exists();
    if (exists) {
      System.out.println("AAAAAAA exists");
    }

    mFilePath = Preconditions.checkNotNull(path, "path");
    mLocalFile = mCloser.register(new RandomAccessFile(mFilePath, "rw"));
    mLocalFileChannel = mCloser.register(mLocalFile.getChannel());
    //TODO(): parse path to filename and heap name
    System.out.println(mFilePath);
    File testfile1 = new File(mFilePath);
    exists = testfile1.exists();
    if (exists) {
      System.out.println("exists");
      testfile1.delete();
    }
    mHeap = Heap.getHeap("/mnt/ramdisk/pmemdata", 1048576000L);
    mHeaderFile = new MemoryBlockArray(mFilePath, 100);
    mArraySlot = 0;
  }

  @Override
  public long append(ByteBuffer inputBuf) throws IOException {
    long bytesWritten = write(0, inputBuf.duplicate());
    mPosition += bytesWritten;
    return bytesWritten;
  }

  @Override
  public long append(ByteBuf buf) throws IOException {
    long bytesWritten = buf.readBytes(mLocalFileChannel, buf.readableBytes());
    mPosition += bytesWritten;
    return bytesWritten;
  }

  @Override
  public long getPosition() {
    return mPosition;
  }

  @Override
  public WritableByteChannel getChannel() {
    return mLocalFileChannel;
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    mClosed = true;

    mCloser.close();
    mPosition = -1;
  }

  /**
   * Writes data to the block from an input {@link ByteBuffer}.
   *
   * @param offset starting offset of the block file to write
   * @param inputBuf {@link ByteBuffer} that input data is stored in
   * @return the size of data that was written
   */
  private long write(long offset, ByteBuffer inputBuf) throws IOException {
    int inputBufLength = inputBuf.limit() - inputBuf.position();
    MappedByteBuffer outputBuf = null;
    //    mLocalFileChannel.map(FileChannel.MapMode.READ_WRITE, offset, inputBufLength);
    //outputBuf.put(inputBuf);
    //int bytesWritten = outputBuf.limit();
    //BufferUtils.cleanDirectBuffer(outputBuf);

    MemoryBlock<Transactional> mr = mHeap.allocateMemoryBlock(Transactional.class,
        inputBufLength + Long.BYTES);
    byte[] srcArr = new byte[inputBufLength];
    inputBuf.get(srcArr);
    mr.setLong(0, inputBufLength);
    mr.copyFromArray(srcArr, 0, Long.BYTES, inputBufLength);
    mHeaderFile.set(mArraySlot, mr);
    mArraySlot++;

    return inputBufLength;
  }
}
