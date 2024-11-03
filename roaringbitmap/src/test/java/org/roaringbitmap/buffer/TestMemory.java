package org.roaringbitmap.buffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import java.util.Random;

public class TestMemory {
  @Test
  public void testGCStability() {
    final int N = 10000;
    final int M = 5000000;
    System.out.println(
        "[testGCStability] testing GC stability with "
            + N
            + " bitmaps containing ~"
            + M / N
            + " values each on average");
    System.out.println("Universe size = " + M);
    final MutableRoaringBitmap[] bitmaps = new MutableRoaringBitmap[N];
    for (int i = 0; i < N; i++) {
      bitmaps[i] = new MutableRoaringBitmap();
    }
    final Random random = new Random(1234);
    for (int i = 0; i < M; i++) {
      final int x = random.nextInt(N);
      bitmaps[x].add(i);
    }
    int totalcard = 0;
    for (int i = 0; i < N; i++) {
      totalcard += bitmaps[i].getCardinality();
    }
    assertEquals(totalcard, M);
  }
}
