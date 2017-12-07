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
  public void addSmallRemoveSmaller() {
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
  public void addSmallAddBigRemoveSmall() {
    FastRankRoaringBitmap b = new FastRankRoaringBitmap();

    b.add(123);
    Assert.assertEquals(1, b.rank(123));
    Assert.assertEquals(123, b.select(0));

    // Add smaller element
    b.add(Integer.MAX_VALUE);

    Assert.assertEquals(123, b.select(0));
    Assert.assertEquals(Integer.MAX_VALUE, b.select(1));

    Assert.assertEquals(0, b.rank(123 - 1));
    Assert.assertEquals(1, b.rank(123));
    Assert.assertEquals(1, b.rank(123 + 1));

    Assert.assertEquals(1, b.rank(Integer.MAX_VALUE - 1));
    Assert.assertEquals(2, b.rank(Integer.MAX_VALUE));
    Assert.assertEquals(2, b.rank(Integer.MAX_VALUE + 1));

    // Remove smaller element
    b.remove(123);

    Assert.assertEquals(Integer.MAX_VALUE, b.select(0));

    Assert.assertEquals(0, b.rank(Integer.MAX_VALUE - 1));
    Assert.assertEquals(1, b.rank(Integer.MAX_VALUE));
    Assert.assertEquals(1, b.rank(Integer.MAX_VALUE + 1));
  }

  @Test
  public void addSmallAddBigRemoveBig() {
    FastRankRoaringBitmap b = new FastRankRoaringBitmap();

    b.add(123);

    Assert.assertEquals(123, b.select(0));

    // Add smaller element
    b.add(Integer.MAX_VALUE);

    Assert.assertEquals(123, b.select(0));
    Assert.assertEquals(Integer.MAX_VALUE, b.select(1));

    Assert.assertEquals(0, b.rank(123 - 1));
    Assert.assertEquals(1, b.rank(123));
    Assert.assertEquals(1, b.rank(123 + 1));

    Assert.assertEquals(1, b.rank(Integer.MAX_VALUE - 1));
    Assert.assertEquals(2, b.rank(Integer.MAX_VALUE));
    Assert.assertEquals(2, b.rank(Integer.MAX_VALUE + 1));

    // Remove smaller element
    b.remove(Integer.MAX_VALUE);

    Assert.assertEquals(123, b.select(0));

    Assert.assertEquals(0, b.rank(123 - 1));
    Assert.assertEquals(1, b.rank(123));
    Assert.assertEquals(1, b.rank(123 + 1));
  }

  @Test
  public void addSmallAddNegativeRemoveSmall() {
    FastRankRoaringBitmap b = new FastRankRoaringBitmap();

    b.add(123);
    Assert.assertEquals(1, b.rank(123));
    Assert.assertEquals(123, b.select(0));

    // Add negative element
    b.add(-234);

    Assert.assertEquals(123, b.select(0));
    Assert.assertEquals(-234, b.select(1));

    Assert.assertEquals(0, b.rank(123 - 1));
    Assert.assertEquals(1, b.rank(123));
    Assert.assertEquals(1, b.rank(123 + 1));

    Assert.assertEquals(1, b.rank(-234 - 1));
    Assert.assertEquals(2, b.rank(-234));
    Assert.assertEquals(2, b.rank(-234 + 1));

    // Remove smaller element
    b.remove(123);

    Assert.assertEquals(-234, b.select(0));

    Assert.assertEquals(0, b.rank(-234 - 1));
    Assert.assertEquals(1, b.rank(-234));
    Assert.assertEquals(1, b.rank(-234 + 1));
  }


  @Test
  public void addSmallAddNegativeRemoveNegative() {
    FastRankRoaringBitmap b = new FastRankRoaringBitmap();

    b.add(123);

    Assert.assertEquals(123, b.select(0));

    // Add negative element
    b.add(-234);

    Assert.assertEquals(123, b.select(0));
    Assert.assertEquals(-234, b.select(1));

    Assert.assertEquals(0, b.rank(123 - 1));
    Assert.assertEquals(1, b.rank(123));
    Assert.assertEquals(1, b.rank(123 + 1));

    Assert.assertEquals(1, b.rank(-234 - 1));
    Assert.assertEquals(2, b.rank(-234));
    Assert.assertEquals(2, b.rank(-234 + 1));

    // Remove smaller element
    b.remove(-234);

    Assert.assertEquals(123, b.select(0));

    Assert.assertEquals(0, b.rank(123 - 1));
    Assert.assertEquals(1, b.rank(123));
    Assert.assertEquals(1, b.rank(123 + 1));
  }

  @Test
  public void addManyRandomlyThenRemoveAllRandomly() {
    FastRankRoaringBitmap b = new FastRankRoaringBitmap();

    Random r = new Random(0);

    int problemSize = 10 * 1000;
    int nbReallyAdded = 0;

    // Add randomly
    for (int i = 0; i < problemSize; i++) {
      int added = r.nextInt();
      if (b.checkedAdd(added)) {
        nbReallyAdded++;
      }
    }

    // Remove randomly
    for (int i = 0; i < nbReallyAdded; i++) {
      int selected = b.select(r.nextInt(nbReallyAdded - i));
      b.remove(selected);

    }
    Assert.assertEquals(0, b.getLongCardinality());
  }

  @Test
  public void addAndRemoveRandomly() {
    FastRankRoaringBitmap b = new FastRankRoaringBitmap();

    Random r = new Random(0);

    int problemSize = 10 * 1000;

    int nbReallyAdded = addSelectRemoveRandomly(b, r, problemSize);

    Assert.assertEquals(nbReallyAdded, b.getLongCardinality());
  }

  private int addSelectRemoveRandomly(RoaringBitmap b, Random r, int problemSize) {
    // We count ourselves the cardinality
    int nbReallyAdded = 0;

    // Add randomly
    for (int i = 0; i < problemSize; i++) {
      if (r.nextBoolean()) {
        int added = r.nextInt();
        if (b.checkedAdd(added)) {
          nbReallyAdded++;
        }
      } else {
        // Remove randomly
        if (nbReallyAdded > 0) {
          int selected = b.select(r.nextInt(nbReallyAdded));
          b.remove(selected);

          nbReallyAdded--;
        }
      }
    }
    return nbReallyAdded;
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

  @Test
  public void performanceTest_MixedAddRemove() {
    int problemSize = 1000 * 1000;

    long startFast = System.currentTimeMillis();
    {
      FastRankRoaringBitmap fast = new FastRankRoaringBitmap();
      Random r = new Random(0);
      int nbReallyAdded = addSelectRemoveRandomly(fast, r, problemSize);
      Assert.assertEquals(nbReallyAdded, fast.getLongCardinality());
    }
    long timeFast = System.currentTimeMillis() - startFast;

    long startNormal = System.currentTimeMillis();
    {
      RoaringBitmap normal = new RoaringBitmap();
      Random r = new Random(0);
      int nbReallyAdded = addSelectRemoveRandomly(normal, r, problemSize);
      Assert.assertEquals(nbReallyAdded, normal.getLongCardinality());
    }
    long timeNormal = System.currentTimeMillis() - startNormal;

    // Mixed workload: "fast" is expected slower as it recomputer cardinality regularly
    System.out.println("AddSelectRemoveAreMixed Fast=" + timeFast + "ms");
    System.out.println("AddSelectRemoveAreMixed Normal=" + timeNormal + "ms");
  }

  @Test
  public void performanceTest_AddManyThenSelectMany() {
    int problemSize = 100 * 1000;

    long startFast = System.currentTimeMillis();
    {
      FastRankRoaringBitmap fast = new FastRankRoaringBitmap();
      Random r = new Random(0);

      for (int i = 0; i < problemSize; i++) {
        fast.add(r.nextInt());
      }

      int cardinality = fast.getCardinality();

      for (int i = 0; i < problemSize; i++) {
        fast.select(r.nextInt(cardinality));
      }
    }
    long timeFast = System.currentTimeMillis() - startFast;

    long startNormal = System.currentTimeMillis();
    {
      RoaringBitmap fast = new RoaringBitmap();
      Random r = new Random(0);

      for (int i = 0; i < problemSize; i++) {
        fast.add(r.nextInt());
      }

      int cardinality = fast.getCardinality();

      for (int i = 0; i < problemSize; i++) {
        fast.select(r.nextInt(cardinality));
      }
    }
    long timeNormal = System.currentTimeMillis() - startNormal;

    // And only then select only: "fast" is expected faster as it pre-computes cardinality
    System.out.println("AddOnlyThenSelectOnly Fast=" + timeFast + "ms");
    System.out.println("AddOnlyThenSelectOnly Normal=" + timeNormal + "ms");
  }
}
