package org.roaringbitmap;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@RunWith(Parameterized.class)
public class TestRankIterator {

  @Parameterized.Parameters(name = "{index}: advance by {1}")
  public static Collection<Object[]> parameters() {
    RoaringBitmap bm = RandomisedTestData.randomBitmap(1 << 12);
    FastRankRoaringBitmap fast = new FastRankRoaringBitmap();
    fast.or(bm);
    fast.runOptimize();

    Assert.assertTrue(fast.isCacheDismissed());

    List<Integer> primes = Arrays.asList(0, 1, 3, 5, 7, 11);
    List<Integer> powers = IntStream.range(0, 18).mapToObj(i -> 1 << i).collect(Collectors.toList());

    List<Integer> advances = Lists.cartesianProduct(primes, powers).stream()
                                  .map(l -> l.get(0) * l.get(1))
                                  .distinct().sorted()
                                  .collect(Collectors.toList());

    return Lists.cartesianProduct(Collections.singletonList(fast), advances)
                .stream().map(List::toArray).collect(Collectors.toList());
  }

  @Parameterized.Parameter
  public FastRankRoaringBitmap bitmap;
  @Parameterized.Parameter(1)
  public Integer advance;

  @Test
  public void testAdvance() {
    long start = System.nanoTime();
    if (advance == 0) {
      testBitmapRanksOnNext(bitmap);
      long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
      System.out.println("next: " + ms + "ms");
    } else {
      testBitmapRanksOnAdvance(bitmap, advance);
      long end = System.nanoTime();
      long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
      System.out.println("advance by " + advance + ": " + ms + "ms");
    }
  }

  private void testBitmapRanksOnNext(FastRankRoaringBitmap bm) {
    PeekableIntRankIterator iterator = bm.getIntRankIterator();
    long iterations = 0;
    while (iterator.hasNext()) {
      ++iterations;
      int rank = iterator.peekNextRank();
      Assert.assertEquals(iterations, Util.toUnsignedLong(rank));

      iterator.next();
    }
  }

  private void testBitmapRanksOnAdvance(FastRankRoaringBitmap bm, int advance) {
    PeekableIntRankIterator iterator = bm.getIntRankIterator();
    while (iterator.hasNext()) {
      int bit = iterator.peekNext();
      int rank = iterator.peekNextRank();

      Assert.assertEquals(bm.rank(bit), rank);

      if ((Util.toUnsignedLong(bit) + advance) < 0xffffffffL) {
        iterator.advanceIfNeeded(bit + advance);
      } else {
        break;
      }
    }
  }
}
