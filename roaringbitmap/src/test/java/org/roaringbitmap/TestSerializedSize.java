package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class TestSerializedSize {

  @Test
  public void testLucaSize() {
    System.out.println("testLucaSize");
    RoaringBitmap rb =
        RoaringBitmap.bitmapOf(2946000, 2997491, 10478289, 10490227, 10502444, 19866827);
    System.out.println("cardinality = " + rb.getCardinality());
    System.out.println("total size in bytes = " + rb.getSizeInBytes());
    assertTrue(rb.getSizeInBytes() <= 50);
  }

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
      rb.add(0L, (long) (k + 1));
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
        if (val > universe_size) universe_size = val;
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
          rb.add(i * (1L << 16), i * (1L << 16) + stepsize);
          if (maxv > universe_size) universe_size = maxv;
        }
        long c = RoaringBitmap.maximumSerializedSize(rb.getCardinality(), universe_size);
        long ac = rb.serializedSizeInBytes();
        assertTrue(ac <= c);
        rb.runOptimize();
        long rac = rb.serializedSizeInBytes();
        assertTrue(rac <= c);
      }
  }

  private static int[] firstPrimes(int n) {
    int status = 1, num = 3;
    int[] answer = new int[n];
    for (int count = 0; count < n; ) {
      double s = Math.sqrt(num);
      for (int j = 2; j <= s; j++) {
        if (num % j == 0) {
          status = 0;
          break;
        }
      }
      if (status != 0) {
        answer[count] = num;
        count++;
      }
      status = 1;
      num++;
    }
    return answer;
  }

  @Test
  public void testPrimeSerializedSize() {
    System.out.println("[testPrimeSerializedSize]");
    for (int j = 1000; j < 1000 * 1000; j *= 10) {
      int[] primes = firstPrimes(j);
      RoaringBitmap rb = RoaringBitmap.bitmapOf(primes);
      long vagueupperbound =
          RoaringBitmap.maximumSerializedSize(rb.getCardinality(), Integer.MAX_VALUE);
      long upperbound =
          RoaringBitmap.maximumSerializedSize(rb.getCardinality(), primes[primes.length - 1] + 1);

      long actual = rb.serializedSizeInBytes();
      System.out.println(
          "cardinality = "
              + rb.getCardinality()
              + " serialized size = "
              + actual
              + " silly upper bound = "
              + vagueupperbound
              + " better upper bound = "
              + upperbound);
      assertTrue(actual <= vagueupperbound);
      assertTrue(upperbound <= vagueupperbound);
      assertTrue(actual <= upperbound);
    }
  }
}
