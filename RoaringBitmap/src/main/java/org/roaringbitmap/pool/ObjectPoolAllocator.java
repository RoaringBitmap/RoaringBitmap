package org.roaringbitmap.pool;

import org.roaringbitmap.AllocationManager.DefaultAllocator;

public class ObjectPoolAllocator extends DefaultAllocator {

  public static final ObjectPoolAllocator INSTANCE = new ObjectPoolAllocator();

  @Override
  public char[] allocateChars(int size) {
    return CharArrayPool.INSTANCE.take(size);
  }

  @Override
  public long[] allocateLongs(int size) {
    return LongArrayPool.INSTANCE.take(size);
  }

  @Override
  public long[] copy(long[] longs) {
    long[] copy = allocateLongs(longs.length);
    System.arraycopy(longs, 0, copy, 0, longs.length);
    return copy;
  }

  @Override
  public char[] copy(char[] chars) {
    char[] copy = allocateChars(chars.length);
    System.arraycopy(chars, 0, copy, 0, chars.length);
    return copy;
  }

  @Override
  public char[] copy(char[] chars, int newSize) {
    char[] copy = allocateChars(newSize);
    System.arraycopy(chars, 0, copy, 0, Math.min(newSize, chars.length));
    return copy;
  }

  @Override
  public char[] extend(char[] chars, int newSize) {
    return copy(chars, newSize);
  }

  @Override
  public void free(Object object) {
    if (object instanceof long[]) {
      LongArrayPool.INSTANCE.free((long[]) object);
    } else if (object instanceof char[]) {
      CharArrayPool.INSTANCE.free((char[]) object);
    } else {
      super.free(object);
    }
  }
}
