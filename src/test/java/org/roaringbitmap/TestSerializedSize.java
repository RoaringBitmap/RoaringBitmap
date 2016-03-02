package org.roaringbitmap;

import org.junit.Test;

import static org.junit.Assert.*;

public class TestSerializedSize {


  @Test
  public void testEmpty() {
    RoaringBitmap rb = new RoaringBitmap();
    long c = RoaringBitmap.maximumSerializedSize(0, 0);
    long ac = rb.serializedSizeInBytes();
    assertTrue(ac <= c);
    rb.runOptimize();
    long rac = rb.serializedSizeInBytes();
    assertTrue(rac <= c);
  }



  @Test
  public void testOne() {
    for (int k = 0; k < 100000; k += 100) {
      RoaringBitmap rb = new RoaringBitmap();
      rb.add(k);
      long c = RoaringBitmap.maximumSerializedSize(1, k + 1);
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
      RoaringBitmap rb = new RoaringBitmap();
      rb.add(0, k + 1);
      long c = RoaringBitmap.maximumSerializedSize(rb.getCardinality(), k + 1);
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
      RoaringBitmap rb = new RoaringBitmap();
      int universe_size = 0;
      for (int k = 0; k < N; ++k) {
        int val = (int) (scale * k);
        if (val > universe_size)
          universe_size = val;
        rb.add((int) (scale * k));
      }
      universe_size++;
      long c = RoaringBitmap.maximumSerializedSize(rb.getCardinality(), universe_size);
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
        RoaringBitmap rb = new RoaringBitmap();
        int universe_size = 0;
        
        for (int i = 0; i < step; ++i) {
          final int maxv = i * (1 << 16) + stepsize;
          rb.add(i * (1 << 16), i * (1 << 16) + stepsize);
          if (maxv > universe_size)
            universe_size = maxv;
        }
        long c = RoaringBitmap.maximumSerializedSize(rb.getCardinality(),universe_size);
        long ac = rb.serializedSizeInBytes();
        assertTrue(ac <= c);
        rb.runOptimize();
        long rac = rb.serializedSizeInBytes();
        assertTrue(rac <= c);
      }
  }

}
