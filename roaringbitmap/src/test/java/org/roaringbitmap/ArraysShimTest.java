package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class ArraysShimTest {

  @Test
  public void longEqualsComparesOnlyTheRequestedRanges() {
    long[] x = {1L, 2L, 3L, 4L};
    long[] y = {9L, 2L, 3L, 8L};
    assertTrue(ArraysShim.equals(x, 1, 3, y, 1, 3));
    assertFalse(ArraysShim.equals(x, 0, 3, y, 0, 3));
    assertFalse(ArraysShim.equals(x, 1, 4, y, 1, 4));
  }

  @Test
  public void longEqualsHandlesDifferentOffsetsAndLengths() {
    long[] x = {5L, 6L, 7L};
    long[] y = {0L, 5L, 6L, 7L};
    assertTrue(ArraysShim.equals(x, 0, 3, y, 1, 4));
    assertFalse(ArraysShim.equals(x, 0, 3, y, 1, 3));
    assertFalse(ArraysShim.equals(x, 0, 2, y, 0, 3));
  }

  @Test
  public void longEqualsTreatsEmptyRangesAsEqual() {
    long[] x = {1L};
    long[] y = {2L};
    assertTrue(ArraysShim.equals(x, 0, 0, y, 1, 1));
  }

  @Test
  public void charEqualsComparesOnlyTheRequestedRanges() {
    char[] x = {'a', 'b', 'c'};
    char[] y = {'z', 'b', 'c'};
    assertTrue(ArraysShim.equals(x, 1, 3, y, 1, 3));
    assertFalse(ArraysShim.equals(x, 0, 3, y, 0, 3));
    assertFalse(ArraysShim.equals(x, 0, 2, y, 0, 3));
  }
}
