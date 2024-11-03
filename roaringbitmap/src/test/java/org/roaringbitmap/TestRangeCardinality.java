package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

public class TestRangeCardinality {

  public static Stream<Arguments> data() {
    return Stream.of(
        Arguments.of(new int[] {1, 3, 5, 7, 9}, 3, 8, 3),
        Arguments.of(new int[] {1, 3, 5, 7, 9}, 2, 8, 3),
        Arguments.of(new int[] {1, 3, 5, 7, 9}, 3, 7, 2),
        Arguments.of(new int[] {1, 3, 5, 7, 9}, 0, 7, 3),
        Arguments.of(new int[] {1, 3, 5, 7, 9}, 0, 6, 3),
        Arguments.of(new int[] {1, 3, 5, 7, 9, Short.MAX_VALUE}, 0, Short.MAX_VALUE + 1, 6),
        Arguments.of(new int[] {1, 10000, 25000, Short.MAX_VALUE - 1}, 0, Short.MAX_VALUE, 4),
        Arguments.of(
            new int[] {1 << 3, 1 << 8, 511, 512, 513, 1 << 12, 1 << 14}, 0, Short.MAX_VALUE, 7));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testCardinalityInBitmapWordRange(int[] elements, int begin, int end, int expected) {
    BitmapContainer bc = new BitmapContainer();
    for (int e : elements) {
      bc.add((char) e);
    }
    assertEquals(expected, Util.cardinalityInBitmapRange(bc.bitmap, begin, end));
  }
}
