package org.roaringbitmap.pool;

import java.util.Arrays;

public class LongArrayPool extends ArrayPool<long[]> {

  public static final LongArrayPool INSTANCE = new LongArrayPool();

  private LongArrayPool() {
    super(long[]::new);
  }

  @Override
  protected int getSize(long[] current) {
    return current.length;
  }

  @Override
  protected void reset(long[] current) {
    Arrays.fill(current, 0);
  }
}
