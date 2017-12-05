/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */
package org.roaringbitmap;

import java.util.Random;

import org.junit.Assert;
import org.junit.Test;


/**
 * Check FastRankRoaringBitmap dismiss the caches cardinalities when necessary
 */
public class TestRoaringBitmap_FastRank {
  @Test
  public void dismissOnAdd_SingleBucket() {
    FastRankRoaringBitmap b = new FastRankRoaringBitmap();

    b.add(123);
    Assert.assertEquals(1, b.rank(123));

    // Add smaller element
    b.add(12);
    Assert.assertEquals(2, b.rank(123));

    // Remove smaller element
    b.remove(12);
    Assert.assertEquals(1, b.rank(123));
  }

  @Test
  public void dismissOnAdd_MultipleBucket() {
    FastRankRoaringBitmap b = new FastRankRoaringBitmap();

    Random r = new Random(0);

    for (int i = 0; i < 100 * 1000; i++) {
      b.add(r.nextInt());
    }

    for (int i = 0; i < 100 * 1000; i++) {
      b.remove(b.select(0));
    }
    Assert.assertEquals(1, b.getLongCardinality());
  }

  @Test
  public void rankOnEmpty() {
    FastRankRoaringBitmap b = new FastRankRoaringBitmap();

    b.rank(0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void selectOnEmpty() {
    FastRankRoaringBitmap b = new FastRankRoaringBitmap();

    b.select(0);
  }
}
