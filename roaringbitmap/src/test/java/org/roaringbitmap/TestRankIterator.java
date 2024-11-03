package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.roaringbitmap.RoaringBitmapWriter.writer;
import static org.roaringbitmap.SeededTestData.randomBitmap;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@Execution(ExecutionMode.CONCURRENT)
public class TestRankIterator {

  @SuppressWarnings("unchecked")
  public static Stream<Arguments> parameters() throws CloneNotSupportedException {
    FastRankRoaringBitmap fast = getBitmap();
    FastRankRoaringBitmap withFull = new FastRankRoaringBitmap(fast.highLowContainer.clone());
    withFull.add(0L, 262144L);

    assertTrue(fast.isCacheDismissed());
    return Lists.cartesianProduct(Arrays.asList(fast, withFull), computeAdvances()).stream()
        .map(list -> Arguments.of(list.get(0), list.get(1)));
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("parameters")
  public void testAdvance(FastRankRoaringBitmap bitmap, Integer advance) {
    long start = System.nanoTime();
    if (advance == 0) {
      testBitmapRanksOnNext(bitmap);
      long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
      System.out.println("next: " + ms + "ms");
    } else {
      testBitmapRanksOnAdvance(bitmap, advance);
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
      assertEquals(iterations, Util.toUnsignedLong(rank));

      iterator.next();
    }
  }

  private void testBitmapRanksOnAdvance(FastRankRoaringBitmap bm, int advance) {
    PeekableIntRankIterator iterator = bm.getIntRankIterator();
    while (iterator.hasNext()) {
      int bit = iterator.peekNext();
      int rank = iterator.peekNextRank();

      assertEquals(bm.rank(bit), rank);

      if ((Util.toUnsignedLong(bit) + advance) < 0xffffffffL) {
        iterator.advanceIfNeeded(bit + advance);
      } else {
        break;
      }
    }
  }

  private static List<Integer> computeAdvances() {
    return IntStream.of(0, 1, 3, 5, 7, 11)
        .flatMap(i -> IntStream.range(0, 18).map(j -> i * (1 << j)))
        .boxed()
        .distinct()
        .collect(Collectors.toList());
  }

  private static FastRankRoaringBitmap getBitmap() {
    FastRankRoaringBitmap bitmap =
        randomBitmap(50, writer().fastRank().initialCapacity(50).constantMemory().get());
    assertTrue(bitmap.isCacheDismissed());
    return bitmap;
  }
}
