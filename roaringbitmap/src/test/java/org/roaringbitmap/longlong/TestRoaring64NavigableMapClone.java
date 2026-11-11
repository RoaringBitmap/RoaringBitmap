package org.roaringbitmap.longlong;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class TestRoaring64NavigableMapClone {
  @Test
  public void testClonePreservesSignedOrderInBothSerializationModes() {
    int previousMode = Roaring64NavigableMap.SERIALIZATION_MODE;
    try {
      for (int mode :
          new int[] {
            Roaring64NavigableMap.SERIALIZATION_MODE_LEGACY,
            Roaring64NavigableMap.SERIALIZATION_MODE_PORTABLE
          }) {
        Roaring64NavigableMap.SERIALIZATION_MODE = mode;
        for (boolean signed : new boolean[] {false, true}) {
          Roaring64NavigableMap original = new Roaring64NavigableMap(signed);
          original.addLong(Long.MIN_VALUE);
          original.addLong(-1L);
          original.addLong(0L);
          original.addLong(Long.MAX_VALUE);
          Roaring64NavigableMap copy = original.clone();
          assertArrayEquals(original.toArray(), copy.toArray());
          assertTrue(copy.validate());
          copy.removeLong(-1L);
          copy.addLong(1L);
          assertFalse(copy.contains(-1L));
          assertTrue(copy.contains(1L));
          assertArrayEquals(
              signed
                  ? new long[] {Long.MIN_VALUE, 0L, 1L, Long.MAX_VALUE}
                  : new long[] {0L, 1L, Long.MAX_VALUE, Long.MIN_VALUE},
              copy.toArray());
          assertTrue(copy.validate());
          assertTrue(original.contains(-1L));
          assertFalse(original.contains(1L));
          assertEquals(mode, Roaring64NavigableMap.SERIALIZATION_MODE);
        }
      }
    } finally {
      Roaring64NavigableMap.SERIALIZATION_MODE = previousMode;
    }
  }

  @Test
  public void testClonePreservesSignedOrderWithOnlyNegativeValues() {
    int previousMode = Roaring64NavigableMap.SERIALIZATION_MODE;
    try {
      Roaring64NavigableMap.SERIALIZATION_MODE = Roaring64NavigableMap.SERIALIZATION_MODE_PORTABLE;
      Roaring64NavigableMap original = new Roaring64NavigableMap(true);
      original.addLong(-1L);
      Roaring64NavigableMap copy = original.clone();
      copy.addLong(0L);
      assertArrayEquals(new long[] {-1L, 0L}, copy.toArray());
      assertArrayEquals(new long[] {-1L}, original.toArray());
    } finally {
      Roaring64NavigableMap.SERIALIZATION_MODE = previousMode;
    }
  }
}
