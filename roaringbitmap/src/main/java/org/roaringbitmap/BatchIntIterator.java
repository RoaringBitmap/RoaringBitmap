package org.roaringbitmap;

/**
 * Wraps a batch iterator for use as an IntIterator
 */
public class BatchIntIterator implements IntIterator {
  private int i;
  private int mark;
  private int[] buffer;
  private BatchIterator delegate;

  private BatchIntIterator(BatchIterator delegate, int i, int mark, int[] buffer) {
    this.delegate = delegate;
    this.i = i;
    this.mark = mark;
    this.buffer = buffer;
  }

  /**
   * Wraps the batch iterator.
   * @param delegate the batch iterator to do the actual iteration
   * @param buffer the buffer
   */
  BatchIntIterator(BatchIterator delegate, int[] buffer) {
    this(delegate, 0, -1, buffer);
  }

  @Override
  public boolean hasNext() {
    if (i < mark) {
      return true;
    }
    if (!delegate.hasNext() || (mark = delegate.nextBatch(buffer)) == 0) {
      return false;
    }
    i = 0;
    return true;
  }

  @Override
  public int next() {
    return buffer[i++];
  }

  @Override
  public IntIterator clone() {
    try {
      BatchIntIterator it = (BatchIntIterator)super.clone();
      it.delegate = delegate.clone();
      it.buffer = buffer.clone();
      return it;
    } catch (CloneNotSupportedException e) {
      // won't happen
      throw new IllegalStateException();
    }
  }
}
