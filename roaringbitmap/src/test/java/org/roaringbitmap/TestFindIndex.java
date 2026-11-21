package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.TreeSet;

class TestFindIndex {

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void sparseArrayIndices(boolean cached) {
    RoaringBitmap bitmap = bitmap(cached);
    assertEquals(-1L, bitmap.findIndex(0));
    assertEquals(-1L, bitmap.findIndex(-1));
    bitmap.add(1, 2, 3, 1000);
    assertEquals(0L, bitmap.findIndex(1));
    assertEquals(1L, bitmap.findIndex(2));
    assertEquals(3L, bitmap.findIndex(1000));
    assertEquals(-1L, bitmap.findIndex(0));
    assertEquals(-1L, bitmap.findIndex(999));
    assertEquals(-1L, bitmap.findIndex(1001));
    assertEquals(-1L, bitmap.findIndex(1 << 16));
  }

  @Test
  void containerIndicesIncludeAbsentValuesAndWordBoundaries() {
    for (Container initial :
        new Container[] {new ArrayContainer(), new BitmapContainer(), new RunContainer()}) {
      Container container = initial;
      assertEquals(-1, container.findIndex((char) 0));
      boolean[] present = new boolean[1 << 16];
      for (int start : new int[] {0, 63, 127, 1024, 32767, 65530}) {
        for (int value = start; value < Math.min(start + 6, present.length); value++) {
          present[value] = true;
          container = container.add((char) value);
        }
      }
      int index = 0;
      for (int value = 0; value < present.length; value++) {
        assertEquals(present[value] ? index++ : -1, container.findIndex((char) value));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void mixedContainersAndUnsignedOrder(boolean cached) {
    RoaringBitmap bitmap = bitmap(cached);
    TreeSet<Integer> expected = new TreeSet<>(Integer::compareUnsigned);
    for (int value : new int[] {0, 63, 64, 4096, 65535, Integer.MAX_VALUE, Integer.MIN_VALUE, -1}) {
      add(bitmap, expected, value);
    }
    for (int low = 0; low < 12000; low += 2) {
      add(bitmap, expected, (1 << 16) | low);
    }
    for (int low = 100; low < 10000; low++) {
      add(bitmap, expected, (3 << 16) | low);
    }
    bitmap.runOptimize();
    assertInstanceOf(ArrayContainer.class, bitmap.highLowContainer.getContainerAtIndex(0));
    assertInstanceOf(BitmapContainer.class, bitmap.highLowContainer.getContainerAtIndex(1));
    assertInstanceOf(RunContainer.class, bitmap.highLowContainer.getContainerAtIndex(2));
    long index = 0;
    for (int value : expected) {
      assertEquals(index++, bitmap.findIndex(value));
      int next = value + 1;
      if (!expected.contains(next)) {
        assertEquals(-1L, bitmap.findIndex(next));
      }
    }
    assertEquals(-1L, bitmap.findIndex(2 << 16));
    assertEquals(-1L, bitmap.findIndex((3 << 16) | 99));
    assertEquals(-1L, bitmap.findIndex((3 << 16) | 10000));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void indicesCanExceedSignedIntegerRange(boolean cached) {
    RoaringBitmap bitmap = bitmap(cached);
    bitmap.add(0L, 1L << 32);
    assertEquals(0L, bitmap.findIndex(0));
    assertEquals(0x7FFFFFFFL, bitmap.findIndex(Integer.MAX_VALUE));
    assertEquals(0x80000000L, bitmap.findIndex(Integer.MIN_VALUE));
    assertEquals(0xFFFFFFFFL, bitmap.findIndex(-1));
    bitmap.remove(0);
    bitmap.remove(Integer.MIN_VALUE);
    assertEquals(-1L, bitmap.findIndex(0));
    assertEquals(-1L, bitmap.findIndex(Integer.MIN_VALUE));
    assertEquals(0xFFFFFFFDL, bitmap.findIndex(-1));
  }

  @Test
  void cachedIndicesFollowMutations() {
    FastRankRoaringBitmap bitmap = new FastRankRoaringBitmap();
    bitmap.add(10, 20, 65536);
    assertEquals(2L, bitmap.findIndex(65536));
    bitmap.add(1);
    assertEquals(3L, bitmap.findIndex(65536));
    bitmap.remove(20);
    assertEquals(2L, bitmap.findIndex(65536));
    bitmap.flip(10);
    assertEquals(1L, bitmap.findIndex(65536));
    bitmap.or(RoaringBitmap.bitmapOf(2, 3));
    assertEquals(3L, bitmap.findIndex(65536));
    bitmap.clear();
    assertEquals(-1L, bitmap.findIndex(65536));
    bitmap.add(65536);
    assertEquals(0L, bitmap.findIndex(65536));
  }

  private static RoaringBitmap bitmap(boolean cached) {
    return cached ? new FastRankRoaringBitmap() : new RoaringBitmap();
  }

  private static void add(RoaringBitmap bitmap, TreeSet<Integer> expected, int value) {
    bitmap.add(value);
    expected.add(value);
  }
}
