/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */
package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.Random;

/**
 * Check FastRankRoaringBitmap dismiss the caches cardinalities when necessary
 */
@Execution(ExecutionMode.CONCURRENT)
public class TestRoaringBitmap_FastRank {

  @Test
  public void addSmallRemoveSmaller() {
    FastRankRoaringBitmap b = new FastRankRoaringBitmap();

    b.add(123);
    assertEquals(1, b.rank(123));

    // Add smaller element
    b.add(12);
    assertEquals(2, b.rank(123));

    // Remove smaller element
    b.remove(12);
    assertEquals(1, b.rank(123));
  }

  @Test
  public void addSmallAddBigRemoveSmall() {
    FastRankRoaringBitmap b = new FastRankRoaringBitmap();

    b.add(123);
    assertEquals(1, b.rank(123));
    assertEquals(123, b.select(0));

    // Add smaller element
    b.add(Integer.MAX_VALUE);

    assertEquals(123, b.select(0));
    assertEquals(Integer.MAX_VALUE, b.select(1));

    assertEquals(0, b.rank(123 - 1));
    assertEquals(1, b.rank(123));
    assertEquals(1, b.rank(123 + 1));

    assertEquals(1, b.rank(Integer.MAX_VALUE - 1));
    assertEquals(2, b.rank(Integer.MAX_VALUE));
    assertEquals(2, b.rank(Integer.MAX_VALUE + 1));

    // Remove smaller element
    b.remove(123);

    assertEquals(Integer.MAX_VALUE, b.select(0));

    assertEquals(0, b.rank(Integer.MAX_VALUE - 1));
    assertEquals(1, b.rank(Integer.MAX_VALUE));
    assertEquals(1, b.rank(Integer.MAX_VALUE + 1));
  }

  @Test
  public void addSmallAddBigRemoveBig() {
    FastRankRoaringBitmap b = new FastRankRoaringBitmap();

    b.add(123);

    assertEquals(123, b.select(0));

    // Add smaller element
    b.add(Integer.MAX_VALUE);

    assertEquals(123, b.select(0));
    assertEquals(Integer.MAX_VALUE, b.select(1));

    assertEquals(0, b.rank(123 - 1));
    assertEquals(1, b.rank(123));
    assertEquals(1, b.rank(123 + 1));

    assertEquals(1, b.rank(Integer.MAX_VALUE - 1));
    assertEquals(2, b.rank(Integer.MAX_VALUE));
    assertEquals(2, b.rank(Integer.MAX_VALUE + 1));

    // Remove smaller element
    b.remove(Integer.MAX_VALUE);

    assertEquals(123, b.select(0));

    assertEquals(0, b.rank(123 - 1));
    assertEquals(1, b.rank(123));
    assertEquals(1, b.rank(123 + 1));
  }

  @Test
  public void addSmallAddNegativeRemoveSmall() {
    FastRankRoaringBitmap b = new FastRankRoaringBitmap();

    b.add(123);
    assertEquals(1, b.rank(123));
    assertEquals(123, b.select(0));

    // Add negative element
    b.add(-234);

    assertEquals(123, b.select(0));
    assertEquals(-234, b.select(1));

    assertEquals(0, b.rank(123 - 1));
    assertEquals(1, b.rank(123));
    assertEquals(1, b.rank(123 + 1));

    assertEquals(1, b.rank(-234 - 1));
    assertEquals(2, b.rank(-234));
    assertEquals(2, b.rank(-234 + 1));

    // Remove smaller element
    b.remove(123);

    assertEquals(-234, b.select(0));

    assertEquals(0, b.rank(-234 - 1));
    assertEquals(1, b.rank(-234));
    assertEquals(1, b.rank(-234 + 1));
  }

  @Test
  public void addSmallAddNegativeRemoveNegative() {
    FastRankRoaringBitmap b = new FastRankRoaringBitmap();

    b.add(123);

    assertEquals(123, b.select(0));

    // Add negative element
    b.add(-234);

    assertEquals(123, b.select(0));
    assertEquals(-234, b.select(1));

    assertEquals(0, b.rank(123 - 1));
    assertEquals(1, b.rank(123));
    assertEquals(1, b.rank(123 + 1));

    assertEquals(1, b.rank(-234 - 1));
    assertEquals(2, b.rank(-234));
    assertEquals(2, b.rank(-234 + 1));

    // Remove smaller element
    b.remove(-234);

    assertEquals(123, b.select(0));

    assertEquals(0, b.rank(123 - 1));
    assertEquals(1, b.rank(123));
    assertEquals(1, b.rank(123 + 1));
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
    assertEquals(0, b.getLongCardinality());
  }

  @Test
  public void addAndRemoveRandomly() {
    FastRankRoaringBitmap b = new FastRankRoaringBitmap();

    Random r = new Random(0);

    int problemSize = 10 * 1000;

    int nbReallyAdded = addSelectRemoveRandomly(b, r, problemSize, 0);

    assertEquals(nbReallyAdded, b.getLongCardinality());
  }

  private int addSelectRemoveRandomly(RoaringBitmap b, Random r, int problemSize, int intBound) {
    // We count ourselves the cardinality
    int nbReallyAdded = 0;

    // Add randomly
    for (int i = 0; i < problemSize; i++) {
      if (r.nextBoolean()) {
        int added = intBound <= 0 ? r.nextInt() : r.nextInt(intBound);
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

  @Test
  public void selectOnEmpty() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          FastRankRoaringBitmap b = new FastRankRoaringBitmap();
          b.select(0);
        });
  }

  @Test
  public void performanceTest_MixedAddRemove_LargeInts() {
    int problemSize = 1000 * 1000;

    long startFast = System.currentTimeMillis();
    {
      FastRankRoaringBitmap fast = new FastRankRoaringBitmap();
      Random r = new Random(0);
      int nbReallyAdded = addSelectRemoveRandomly(fast, r, problemSize, 0);
      assertEquals(nbReallyAdded, fast.getLongCardinality());
    }
    long timeFast = System.currentTimeMillis() - startFast;

    long startNormal = System.currentTimeMillis();
    {
      RoaringBitmap normal = new RoaringBitmap();
      Random r = new Random(0);
      int nbReallyAdded = addSelectRemoveRandomly(normal, r, problemSize, 0);
      assertEquals(nbReallyAdded, normal.getLongCardinality());
    }
    long timeNormal = System.currentTimeMillis() - startNormal;

    // Mixed workload: "fast" is expected slower as it recomputer cardinality regularly
    System.out.println(
        "AddSelectRemoveAreMixed LargeInts FastRankRoaringBitmap=" + timeFast + "ms");
    System.out.println("AddSelectRemoveAreMixed LargeInts RoaringBitmap=" + timeNormal + "ms");
  }

  @Test
  public void performanceTest_MixedAddRemove_SmallInts() {
    int problemSize = 10 * 1000 * 1000;

    long startFast = System.currentTimeMillis();
    {
      FastRankRoaringBitmap fast = new FastRankRoaringBitmap();
      Random r = new Random(0);
      int nbReallyAdded = addSelectRemoveRandomly(fast, r, problemSize, 123);
      assertEquals(nbReallyAdded, fast.getLongCardinality());
    }
    long timeFast = System.currentTimeMillis() - startFast;

    long startNormal = System.currentTimeMillis();
    {
      RoaringBitmap normal = new RoaringBitmap();
      Random r = new Random(0);
      int nbReallyAdded = addSelectRemoveRandomly(normal, r, problemSize, 123);
      assertEquals(nbReallyAdded, normal.getLongCardinality());
    }
    long timeNormal = System.currentTimeMillis() - startNormal;

    // Mixed workload: "fast" is expected slower as it recomputes cardinality regularly
    System.out.println(
        "AddSelectRemoveAreMixed SmallInts FastRankRoaringBitmap=" + timeFast + "ms");
    System.out.println("AddSelectRemoveAreMixed SmallInts RoaringBitmap=" + timeNormal + "ms");
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
    System.out.println("AddOnlyThenSelectOnly FastRankRoaringBitmap=" + timeFast + "ms");
    System.out.println("AddOnlyThenSelectOnly RoaringBitmap=" + timeNormal + "ms");
  }

  private FastRankRoaringBitmap prepareFastWithComputedCache() {
    FastRankRoaringBitmap fast = new FastRankRoaringBitmap();

    fast.add(123);
    fast.select(0);

    // The cache is computed
    assertFalse(fast.isCacheDismissed());

    return fast;
  }

  @Test
  public void testDismissCache_constructor() {
    FastRankRoaringBitmap fast = new FastRankRoaringBitmap();

    assertTrue(fast.isCacheDismissed());
  }

  @Test
  public void testDismissCache_addLongRange() {
    FastRankRoaringBitmap fast = prepareFastWithComputedCache();

    fast.add(0L, 2L);
    assertTrue(fast.isCacheDismissed());
  }

  @Test
  public void testDismissCache_addIntRange() {
    FastRankRoaringBitmap fast = prepareFastWithComputedCache();

    fast.add(0L, 2L);
    assertTrue(fast.isCacheDismissed());
  }

  @Test
  public void testDismissCache_addIntArray() {
    FastRankRoaringBitmap fast = prepareFastWithComputedCache();

    fast.add(0, 2, 3);
    assertTrue(fast.isCacheDismissed());
  }

  @Test
  public void testDismissCache_clear() {
    FastRankRoaringBitmap fast = prepareFastWithComputedCache();

    fast.clear();
    assertTrue(fast.isCacheDismissed());
  }

  @Test
  public void testDismissCache_flip() {
    FastRankRoaringBitmap fast = prepareFastWithComputedCache();

    fast.flip(0);
    assertTrue(fast.isCacheDismissed());
  }

  @Test
  public void testDismissCache_flipIntRange() {
    FastRankRoaringBitmap fast = prepareFastWithComputedCache();

    fast.flip(0L, 2);
    assertTrue(fast.isCacheDismissed());
  }

  @Test
  public void testDismissCache_flipLongRange() {
    FastRankRoaringBitmap fast = prepareFastWithComputedCache();

    fast.flip(0L, 2L);
    assertTrue(fast.isCacheDismissed());
  }

  @Test
  public void testDismissCache_remove() {
    FastRankRoaringBitmap fast = prepareFastWithComputedCache();

    fast.remove(0);
    assertTrue(fast.isCacheDismissed());
  }

  @Test
  public void testDismissCache_removeIntRange() {
    FastRankRoaringBitmap fast = prepareFastWithComputedCache();

    fast.remove(0L, 2);
    assertTrue(fast.isCacheDismissed());
  }

  @Test
  public void testDismissCache_removeLongRange() {
    FastRankRoaringBitmap fast = prepareFastWithComputedCache();

    fast.remove(0L, 2L);
    assertTrue(fast.isCacheDismissed());
  }

  @Test
  public void testDismissCache_checkedAdd() {
    FastRankRoaringBitmap fast = prepareFastWithComputedCache();

    fast.checkedAdd(2);
    assertTrue(fast.isCacheDismissed());
  }

  @Test
  public void testDismissCache_checkedRemove() {
    FastRankRoaringBitmap fast = prepareFastWithComputedCache();

    fast.checkedRemove(2);
    assertTrue(fast.isCacheDismissed());
  }

  @Test
  public void testDismissCache_and() {
    FastRankRoaringBitmap fast = prepareFastWithComputedCache();

    fast.and(new RoaringBitmap());
    assertTrue(fast.isCacheDismissed());
  }

  @Test
  public void testDismissCache_andNot() {
    FastRankRoaringBitmap fast = prepareFastWithComputedCache();

    fast.andNot(new RoaringBitmap());
    assertTrue(fast.isCacheDismissed());
  }

  @Test
  public void testDismissCache_or() {
    FastRankRoaringBitmap fast = prepareFastWithComputedCache();

    fast.or(new RoaringBitmap());
    assertTrue(fast.isCacheDismissed());
  }

  @Test
  public void testDismissCache_xor() {
    FastRankRoaringBitmap fast = prepareFastWithComputedCache();

    fast.xor(new RoaringBitmap());
    assertTrue(fast.isCacheDismissed());
  }

  @Test
  public void testLongSizeInBytes() {
    FastRankRoaringBitmap fast = new FastRankRoaringBitmap();

    // Check when empty
    assertEquals(16, fast.getLongSizeInBytes());

    fast.add(0);
    fast.add(1024);
    fast.add(Integer.MAX_VALUE);

    assertEquals(34, fast.getLongSizeInBytes());

    // Compute a rank: the cache is allocated
    fast.rank(1024);

    // Check the size is bigger once the cache is allocated
    assertEquals(42, fast.getLongSizeInBytes());
  }

  // https://github.com/RoaringBitmap/RoaringBitmap/issues/498
  @Test
  public void testCacheIsNotReset() {
    FastRankRoaringBitmap rankRoaringBitmap = new FastRankRoaringBitmap();
    assertEquals(0, rankRoaringBitmap.rank(3));
    rankRoaringBitmap.add(3);
    rankRoaringBitmap.add(5);
    assertEquals(3, rankRoaringBitmap.select(0));
    assertEquals(5, rankRoaringBitmap.select(1));
  }

  // https://github.com/RoaringBitmap/RoaringBitmap/issues/499
  @Test
  public void testSelectExceptionOutOfBounds() {
    // RoaringBitmap standard behavior
    RoaringBitmap roaringBitmap = new RoaringBitmap();
    roaringBitmap.add(3);
    roaringBitmap.add(5);
    assertEquals(5, roaringBitmap.select(roaringBitmap.getCardinality() - 1));
    assertThrows(
        IllegalArgumentException.class, () -> roaringBitmap.select(roaringBitmap.getCardinality()));
    assertThrows(
        IllegalArgumentException.class,
        () -> roaringBitmap.select(roaringBitmap.getCardinality() + 1));

    // FastRankRoaringBitmap behavior has to be identical
    FastRankRoaringBitmap rankRoaringBitmap = new FastRankRoaringBitmap();
    rankRoaringBitmap.add(3);
    rankRoaringBitmap.add(5);
    assertEquals(5, rankRoaringBitmap.select(rankRoaringBitmap.getCardinality() - 1));
    assertThrows(
        IllegalArgumentException.class,
        () -> rankRoaringBitmap.select(rankRoaringBitmap.getCardinality()));
    assertThrows(
        IllegalArgumentException.class,
        () -> rankRoaringBitmap.select(rankRoaringBitmap.getCardinality() + 1));
  }
}
