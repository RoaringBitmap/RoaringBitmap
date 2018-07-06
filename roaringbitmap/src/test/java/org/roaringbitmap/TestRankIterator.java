package org.roaringbitmap;

import org.junit.Assert;
import org.junit.Test;

public class TestRankIterator {
  private void testBitmapRanksOnNext(FastRankRoaringBitmap bm) {
    PeekableIntRankIterator iterator = bm.getIntRankIterator();
    while (iterator.hasNext()) {
      int bit = iterator.peekNext();
      int rank = iterator.peekNextRank();
      int slowRank = bm.rank(bit);
      Assert.assertEquals(slowRank, rank);

      iterator.next();
    }
  }

  @Test
  public void randomizedTest() {
    RoaringBitmap bm = RandomisedTestData.randomBitmap(1 << 12);
    FastRankRoaringBitmap fast = new FastRankRoaringBitmap();
    fast.or(bm);
    fast.runOptimize();

    Assert.assertTrue(fast.isCacheDismissed());

    testBitmapRanksOnNext(fast);
  }
}
