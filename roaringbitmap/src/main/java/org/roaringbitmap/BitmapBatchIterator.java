package org.roaringbitmap;

import static java.lang.Long.numberOfTrailingZeros;

public final class BitmapBatchIterator implements ContainerBatchIterator {

  private int wordIndex = 0;
  private long word;
  private BitmapContainer bitmap;

  public BitmapBatchIterator(BitmapContainer bitmap) {
    wrap(bitmap);
  }

  @Override
  public int next(int key, int[] buffer) {
    int consumed = 0;
    while (consumed < buffer.length) {
      while (word == 0) {
        ++wordIndex;
        if (wordIndex == 1024) {
          return consumed;
        }
        word = bitmap.bitmap[wordIndex];
      }
      buffer[consumed++] = key + (64 * wordIndex) + numberOfTrailingZeros(word);
      word &= (word - 1);
    }
    return consumed;
  }

  @Override
  public boolean hasNext() {
    return wordIndex < 1024;
  }

  @Override
  public ContainerBatchIterator clone() {
    try {
      return (ContainerBatchIterator)super.clone();
    } catch (CloneNotSupportedException e) {
      // won't happen
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void releaseContainer() {
    bitmap = null;
  }

  void wrap(BitmapContainer bitmap) {
    this.bitmap = bitmap;
    word = bitmap.bitmap[0];
    this.wordIndex = 0;
  }
}
