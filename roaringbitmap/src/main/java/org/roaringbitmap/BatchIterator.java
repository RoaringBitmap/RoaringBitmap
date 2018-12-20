package org.roaringbitmap;

public interface BatchIterator extends Cloneable {

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

  /**
   * Creates a copy of the iterator.
   *
   * @return a clone of the current iterator
   */
  BatchIterator clone();

  /**
   * Creates a wrapper around the iterator so it behaves like an IntIterator
   * @param buffer - array to buffer bits into (size 128-256 should be best).
   * @return the wrapper
   */
  default IntIterator asIntIterator(int[] buffer) {
    return new BatchIntIterator(this, buffer);
  }


}
