package org.roaringbitmap;

public interface ContainerBatchIterator {

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

}
