package org.roaringbitmap.buffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.stream.Stream;

public class TestBufferRangeCardinality {

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

  @ParameterizedTest(name = "{index}: cardinalityInBitmapRange({0},{1},{2})={3}")
  @MethodSource("data")
  public void testCardinalityInBitmapWordRange(int[] elements, int begin, int end, int expected) {
    LongBuffer array =
        ByteBuffer.allocateDirect(MappeableBitmapContainer.MAX_CAPACITY / 8).asLongBuffer();
    MappeableBitmapContainer bc = new MappeableBitmapContainer(array, 0);
    for (int e : elements) {
      bc.add((char) e);
    }
    assertFalse(bc.isArrayBacked());
    assertEquals(expected, BufferUtil.cardinalityInBitmapRange(bc.bitmap, begin, end));
  }
}
