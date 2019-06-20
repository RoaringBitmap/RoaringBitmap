package org.roaringbitmap;

import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

public class TestMemory {
  @Test
  public void testGCStability() throws Throwable {
    final int N = 10000;
    final int M = 5000000;
    System.out.println("[testGCStability] testing GC stability with " + N + " bitmaps containing ~"
        + M / N + " values each on average");
    System.out.println("Universe size = " + M);
    final RoaringBitmap[] bitmaps = new RoaringBitmap[N];
    for (int i = 0; i < N; i++) {
      bitmaps[i] = new RoaringBitmap();
    }
    final Random random = new Random();
    for (int i = 0; i < M; i++) {
      final int x = random.nextInt(N);
      bitmaps[x].add(i);
    }
    int totalcard = 0;
    for (int i = 0; i < N; i++) {
      totalcard += bitmaps[i].getCardinality();
    }
    Assert.assertEquals(totalcard, M);
  }
}
