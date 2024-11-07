package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class TestVeryLargeBitmap {
  @Test
  public void testSelect() {
    RoaringBitmap map = new RoaringBitmap();
    map.add(0L, (1L << 32));
    assertEquals(-2, map.select(-2));
    assertEquals(-1, map.select(-1));
  }

  @Test // this should run fine given enough memory
  public void stupidlyLarge() {
    try {
      RoaringBitmap rb = new RoaringBitmap();
      rb.add(0, 1L << 32);
      for (long k = 1L; k < (1L << 32); k *= 2) {
        assertEquals(rb.rankLong((int) k), k + 1);
      }
      for (long k = (1L << 32) - 10; k < (1L << 32); k++) {
        assertEquals(rb.rankLong((int) k), k + 1);
      }
      assertEquals(rb.getLongCardinality(), 1L << 32);
      // next line should not make things crash
      System.out.println(rb.toString());
    } catch (OutOfMemoryError ome) {
      System.out.println("This is an acceptable OutOfMemoryError:");
      ome.printStackTrace();
    }
  }
}
