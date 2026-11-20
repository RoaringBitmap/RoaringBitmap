package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

  @Test
  public void mismatchReturnsMinusOneForEqualRanges() {
    byte[] a = {1, 2, 3, 4};
    byte[] b = {9, 2, 3, 8};
    assertEquals(-1, ArraysShim.mismatch(a, 1, 3, b, 1, 3));
    assertEquals(-1, ArraysShim.mismatch(a, 0, 0, b, 3, 3));
  }

  @Test
  public void mismatchReturnsTheRelativeIndexOfTheFirstDifference() {
    byte[] a = {1, 2, 3, 4};
    byte[] b = {0, 1, 2, 9, 4};
    assertEquals(2, ArraysShim.mismatch(a, 0, 4, b, 1, 5));
    assertEquals(0, ArraysShim.mismatch(a, 0, 4, b, 0, 4));
  }

  @Test
  public void mismatchReturnsTheShorterLengthWhenOneRangeIsAPrefix() {
    byte[] a = {1, 2, 3, 4};
    byte[] b = {1, 2, 3};
    assertEquals(3, ArraysShim.mismatch(a, 0, 4, b, 0, 3));
    assertEquals(3, ArraysShim.mismatch(b, 0, 3, a, 0, 4));
    assertEquals(0, ArraysShim.mismatch(a, 0, 4, b, 0, 0));
  }

  @Test
  public void mismatchReturnsMinusOneWhenTheSecondRangeIsInvertedByOne() {
    byte[] a = {1, 2, 3};
    byte[] b = {1, 2, 3};
    assertEquals(-1, ArraysShim.mismatch(a, 0, 3, b, 2, 1));
  }
}
