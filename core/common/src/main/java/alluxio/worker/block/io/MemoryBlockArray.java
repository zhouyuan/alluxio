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

import lib.llpl.Heap;
import lib.llpl.MemoryBlock;
import lib.llpl.Transactional;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class provides write access to a temp block data file locally stored in managed storage.
 */
@NotThreadSafe
public class MemoryBlockArray {

  static final int HEADER_SIZE = 4;

  private Heap mHeap = null;
  MemoryBlock<Transactional> mBlock;

  /**
   * Constructs a PMEM file The file will get created if not ready.
   *
   * @param path data path of the block
   * @param size of the block
   */
  public MemoryBlockArray(String path, int size) throws IOException {
    mHeap = Heap.getHeap(path, 104857600L);
    mBlock = mHeap.allocateMemoryBlock(Transactional.class, HEADER_SIZE + Long.BYTES * size);
    mBlock.setInt(0, size);
  }

  /**
   * Constructs index file given the address of the block.
   *
   * @param addr address of the block
   * @return block
   */
  public MemoryBlockArray fromAddress(long addr) {
    MemoryBlock<Transactional> block = mHeap.memoryBlockFromAddress(Transactional.class, addr);
    return new MemoryBlockArray(block);
  }

 /**
   * Constructs index file from given block.
   *
   * @param block size of the block
   */
  private MemoryBlockArray(MemoryBlock<Transactional> block) {
    mBlock = block;
  }

 /**
   * Set the memory block of given index.
   *
   * @param index size of the block
   * @param value size of the block
   */
  public void set(int index, MemoryBlock<Transactional> value) {
    if (index < 0 || index >= size()) {
      throw new ArrayIndexOutOfBoundsException();
    }
    mBlock.setLong(HEADER_SIZE + Long.BYTES * index, value == null ? 0 : value.address());
  }

 /**
   * Get the memroy block of given index.
   *
   * @param index size of the block
   * @return block
   */
  public MemoryBlock<Transactional> get(int index) {
    if (index < 0 || index >= size()) {
      throw new ArrayIndexOutOfBoundsException();
    }
    long addr = mBlock.getInt(HEADER_SIZE + Long.BYTES * index);
    return addr == 0 ? null : mHeap.memoryBlockFromAddress(Transactional.class, addr);
  }

 /**
   * Get size of the index file.
   *
   * @return size of the block
   */
  public int size() {
    return mBlock.getInt(0);
  }

 /**
   * Get address of the index file.
   *
   * @return address of the block
   */
  public long address() {
    return mBlock.address();
  }
}
