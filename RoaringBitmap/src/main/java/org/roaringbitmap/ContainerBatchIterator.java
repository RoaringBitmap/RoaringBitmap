package org.roaringbitmap;

public interface ContainerBatchIterator extends Cloneable {

  /**
   * Fills the buffer with values prefixed by the key,
   * and returns how much of the buffer was used.
   * @param key the prefix of the values
   * @param buffer the buffer to write values onto
   * @return how many values were written.
   */
  int next(int key, int[] buffer);

  /**
   * Whether the underlying container is exhausted or not
   * @return true if there is data remaining
   */
  boolean hasNext();

  /**
   * Creates a copy of the iterator.
   *
   * @return a clone of the current iterator
   */
  ContainerBatchIterator clone();

  /**
   * Discard the reference to the container
   */
  void releaseContainer();

}
