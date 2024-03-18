package org.roaringbitmap;

public interface ContainerBatchIterator extends Cloneable {

  /**
   * Fills the buffer with values prefixed by the key,
   * and returns how much of the buffer was used.
   * @param key the prefix of the values
   * @param buffer the buffer to write values onto
   * @param offset the offset into the buffer to write values onto
   * @return how many values were written.
   */
  int next(int key, int[] buffer, int offset);

  /**
   * Fills the buffer with values prefixed by the key,
   * and returns how much of the buffer was used.
   * @param key the prefix of the values
   * @param buffer the buffer to write values onto
   * @return how many values were written.
   */
  default int next(int key, int[] buffer) {
    return next(key, buffer, 0);
  }

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

  /**
   * Advance until the value.
   * @param target the value to advance to.
   */
  void advanceIfNeeded(char target);

}
