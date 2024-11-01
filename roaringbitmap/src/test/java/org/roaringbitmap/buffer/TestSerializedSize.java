package org.roaringbitmap.buffer;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class TestSerializedSize {

  @Test
  public void testEmpty() {
    MutableRoaringBitmap rb = new MutableRoaringBitmap();
    long c = MutableRoaringBitmap.maximumSerializedSize(0, 0);
    long ac = rb.serializedSizeInBytes();
    assertTrue(ac <= c);
    rb.runOptimize();
    long rac = rb.serializedSizeInBytes();
    assertTrue(rac <= c);
  }

  @Test
  public void testOne() {
    for (int k = 0; k < 100000; k += 100) {
      MutableRoaringBitmap rb = new MutableRoaringBitmap();
      rb.add(k);
      long c = MutableRoaringBitmap.maximumSerializedSize(1, k + 1);
      long ac = rb.serializedSizeInBytes();
      assertTrue(ac <= c);
      rb.runOptimize();
      long rac = rb.serializedSizeInBytes();
      assertTrue(rac <= c);
    }
  }

  @Test
  public void testRange() {
    for (int k = 0; k < 100000; k += 100) {
      MutableRoaringBitmap rb = new MutableRoaringBitmap();
      rb.add(0L, k + 1L);
      long c = MutableRoaringBitmap.maximumSerializedSize(rb.getCardinality(), k + 1);
      long ac = rb.serializedSizeInBytes();
      assertTrue(ac <= c);
      rb.runOptimize();
      long rac = rb.serializedSizeInBytes();
      assertTrue(rac <= c);
    }
  }

  @Test
  public void testLarge() {
    for (long scale = 15; scale < 2048; scale *= 15) {
      final int N = 1000000;
      MutableRoaringBitmap rb = new MutableRoaringBitmap();
      int universe_size = 0;
      for (int k = 0; k < N; ++k) {
        int val = (int) (scale * k);
        if (val > universe_size) universe_size = val;
        rb.add((int) (scale * k));
      }
      universe_size++;
      long c = MutableRoaringBitmap.maximumSerializedSize(rb.getCardinality(), universe_size);
      long ac = rb.serializedSizeInBytes();
      assertTrue(ac <= c);
      rb.runOptimize();
      long rac = rb.serializedSizeInBytes();
      assertTrue(rac <= c);
    }
  }

  @Test
  public void testManyRanges() {
    for (int stepsize = 1; stepsize < 32; ++stepsize)
      for (long step = 1; step < 500; ++step) {
        MutableRoaringBitmap rb = new MutableRoaringBitmap();
        int universe_size = 0;

        for (int i = 0; i < step; ++i) {
          final int maxv = i * (1 << 16) + stepsize;
          rb.add(i * (1L << 16), i * (1L << 16) + stepsize);
          if (maxv > universe_size) universe_size = maxv;
        }
        long c = MutableRoaringBitmap.maximumSerializedSize(rb.getCardinality(), universe_size);
        long ac = rb.serializedSizeInBytes();
        assertTrue(ac <= c);
        rb.runOptimize();
        long rac = rb.serializedSizeInBytes();
        assertTrue(rac <= c);
      }
  }
}
