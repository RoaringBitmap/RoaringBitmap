package org.roaringbitmap.buffer;

import org.roaringbitmap.ContainerBatchIterator;

import static java.lang.Long.numberOfTrailingZeros;

public final class BitmapBatchIterator implements ContainerBatchIterator {

  private int wordIndex = 0;
  private long word;
  private MappeableBitmapContainer bitmap;

  public BitmapBatchIterator(MappeableBitmapContainer bitmap) {
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
        word = bitmap.bitmap.get(wordIndex);
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

  void wrap(MappeableBitmapContainer bitmap) {
    this.bitmap = bitmap;
    this.word = bitmap.bitmap.get(0);
    this.wordIndex = 0;
  }

}
