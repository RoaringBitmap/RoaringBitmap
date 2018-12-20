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
   */
  BatchIntIterator(BatchIterator delegate, int batchSize) {
    this(delegate, 0, -1, new int[batchSize]);
  }

  @Override
  public boolean hasNext() {
    if (i < mark) {
      return true;
    }
    if (!delegate.hasNext()) {
      return false;
    }
    mark = delegate.nextBatch(buffer);
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
