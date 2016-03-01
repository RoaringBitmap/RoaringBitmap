/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap.buffer;

import java.nio.LongBuffer;
import org.junit.Test;

public class TestMappeableBitmapContainer {

  private static MappeableBitmapContainer emptyContainer() {
    return new MappeableBitmapContainer(0, LongBuffer.allocate(1));
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testNextTooLarge() {
    emptyContainer().nextSetBit(Short.MAX_VALUE + 1);
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testNextTooSmall() {
    emptyContainer().nextSetBit(-1);
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testPreviousTooLarge() {
    emptyContainer().prevSetBit(Short.MAX_VALUE + 1);
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testPreviousTooSmall() {
    emptyContainer().prevSetBit(-1);
  }
}
