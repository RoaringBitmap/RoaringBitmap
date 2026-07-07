package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class ArrayContainerNextValueEmptyTest {

  @Test
  public void nextValueOnEmptyArrayContainerMustReturnMinusOne() {
    final ArrayContainer empty = new ArrayContainer();
    assertEquals(0, empty.getCardinality());
    // Contract: "first stored value >= fromValue, or -1 if none".
    // BitmapContainer.nextValue already returns -1 here; ArrayContainer throws.
    assertEquals(-1, empty.nextValue((char) 5));
  }
}
