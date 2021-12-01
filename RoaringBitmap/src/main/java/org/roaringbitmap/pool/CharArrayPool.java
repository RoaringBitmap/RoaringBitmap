package org.roaringbitmap.pool;

import java.util.Arrays;

public class CharArrayPool extends ArrayPool<char[]> {

  public static final CharArrayPool INSTANCE = new CharArrayPool();

  private CharArrayPool() {
    super(char[]::new);
  }

  @Override
  protected int getSize(char[] current) {
    return current.length;
  }

  @Override
  protected void reset(char[] current) {
    Arrays.fill(current, (char) 0);
  }
}
