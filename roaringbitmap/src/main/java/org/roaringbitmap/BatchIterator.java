package org.roaringbitmap;

public interface BatchIterator {

  /**
   * Writes the next batch of integers onto the buffer,
   * and returns how many were written. Aims to fill
   * the buffer.
   * @param buffer - the target to write onto
   * @return how many values were written during the call.
   */
  int nextBatch(int[] buffer);

  /**
   * Returns true is there are more values to get.
   * @return whether the iterator is exhaused or not.
   */
  boolean hasNext();
}
