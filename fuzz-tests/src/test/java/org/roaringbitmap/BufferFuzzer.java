package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.roaringbitmap.RandomisedTestData.ITERATIONS;
import static org.roaringbitmap.Util.toUnsignedLong;

import org.roaringbitmap.buffer.BufferFastAggregation;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.BitSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.IntStream;

public class BufferFuzzer {

  @FunctionalInterface
  interface IntBitmapPredicate {
    boolean test(int index, MutableRoaringBitmap bitmap);
  }

  @FunctionalInterface
  interface RangeBitmapPredicate {
    boolean test(long min, long max, ImmutableRoaringBitmap bitmap);
  }

  public static void verifyInvariance(String testName, int maxKeys, RangeBitmapPredicate pred) {
    ThreadLocalRandom random = ThreadLocalRandom.current();
    IntStream.range(0, ITERATIONS)
        .parallel()
        .mapToObj(i -> randomBitmap(maxKeys))
        .forEach(
            bitmap -> {
              long min = random.nextLong(1L << 32);
              long max = random.nextLong(min, 1L << 32);
              try {
                assertTrue(pred.test(min, max, bitmap));
              } catch (Throwable t) {
                Reporter.report(testName, ImmutableMap.of("min", min, "max", max), t, bitmap);
                throw t;
              }
            });
  }

  public static <T> void verifyInvariance(
      String testName,
      Predicate<MutableRoaringBitmap> validity,
      Function<MutableRoaringBitmap, T> left,
      Function<MutableRoaringBitmap, T> right) {
    verifyInvariance(testName, ITERATIONS, 1 << 8, validity, left, right);
  }

  public static <T> void verifyInvariance(
      String testName,
      int count,
      int maxKeys,
      Predicate<MutableRoaringBitmap> validity,
      Function<MutableRoaringBitmap, T> left,
      Function<MutableRoaringBitmap, T> right) {
    IntStream.range(0, count)
        .parallel()
        .mapToObj(i -> randomBitmap(maxKeys))
        .filter(validity)
        .forEach(
            bitmap -> {
              try {
                assertEquals(left.apply(bitmap), right.apply(bitmap));
              } catch (Throwable t) {
                Reporter.report(testName, ImmutableMap.of(), t, bitmap);
                throw t;
              }
            });
  }

  public static void verifyInvariance(
      String testName, int maxKeys, Predicate<MutableRoaringBitmap> pred) {
    IntStream.range(0, ITERATIONS)
        .parallel()
        .mapToObj(i -> randomBitmap(maxKeys))
        .forEach(
            bitmap -> {
              try {
                assertTrue(pred.test(bitmap));
              } catch (Throwable e) {
                Reporter.report(testName, ImmutableMap.of(), e, bitmap);
                throw e;
              }
            });
  }

  public static <T> void verifyInvariance(
      String testName,
      BiFunction<MutableRoaringBitmap, MutableRoaringBitmap, T> left,
      BiFunction<MutableRoaringBitmap, MutableRoaringBitmap, T> right) {
    verifyInvariance(testName, ITERATIONS, 1 << 8, left, right);
  }

  public static <T> void verifyInvariance(
      String testName,
      BiPredicate<MutableRoaringBitmap, MutableRoaringBitmap> validity,
      BiFunction<MutableRoaringBitmap, MutableRoaringBitmap, T> left,
      BiFunction<MutableRoaringBitmap, MutableRoaringBitmap, T> right) {
    verifyInvariance(testName, validity, ITERATIONS, 1 << 8, left, right);
  }

  public static <T> void verifyInvariance(
      String testName,
      int count,
      int maxKeys,
      BiFunction<MutableRoaringBitmap, MutableRoaringBitmap, T> left,
      BiFunction<MutableRoaringBitmap, MutableRoaringBitmap, T> right) {
    verifyInvariance(testName, (l, r) -> true, count, maxKeys, left, right);
  }

  public static <T> void verifyInvariance(
      String testName,
      BiPredicate<MutableRoaringBitmap, MutableRoaringBitmap> validity,
      int count,
      int maxKeys,
      BiFunction<MutableRoaringBitmap, MutableRoaringBitmap, T> left,
      BiFunction<MutableRoaringBitmap, MutableRoaringBitmap, T> right) {
    IntStream.range(0, count)
        .parallel()
        .forEach(
            i -> {
              MutableRoaringBitmap one = randomBitmap(maxKeys);
              MutableRoaringBitmap two = randomBitmap(maxKeys);
              if (validity.test(one, two)) {
                try {
                  assertEquals(left.apply(one, two), right.apply(one, two));
                } catch (Throwable t) {
                  Reporter.report(testName, ImmutableMap.of(), t, one, two);
                  throw t;
                }
              }
            });
  }

  public static void verifyInvariance(
      String testName, Predicate<MutableRoaringBitmap> validity, IntBitmapPredicate predicate) {
    verifyInvariance(testName, validity, ITERATIONS, 1 << 3, predicate);
  }

  public static void verifyInvariance(
      String testName,
      Predicate<MutableRoaringBitmap> validity,
      int count,
      int maxKeys,
      IntBitmapPredicate predicate) {
    IntStream.range(0, count)
        .parallel()
        .mapToObj(i -> randomBitmap(maxKeys))
        .filter(validity)
        .forEach(
            bitmap -> {
              for (int i = 0; i < bitmap.getCardinality(); ++i) {
                try {
                  assertTrue(predicate.test(i, bitmap));
                } catch (Throwable t) {
                  Reporter.report(testName, ImmutableMap.of(), t, bitmap);
                  throw t;
                }
              }
            });
  }

  public static <T> void verifyInvariance(
      String testName, T value, Function<MutableRoaringBitmap, T> func) {
    verifyInvariance(testName, ITERATIONS, 1 << 9, value, func);
  }

  public static <T> void verifyInvariance(
      String testName, int count, int maxKeys, T value, Function<MutableRoaringBitmap, T> func) {
    IntStream.range(0, count)
        .parallel()
        .mapToObj(i -> randomBitmap(maxKeys))
        .forEach(
            bitmap -> {
              try {
                assertEquals(value, func.apply(bitmap));
              } catch (Throwable t) {
                Reporter.report(testName, ImmutableMap.of("value", value), t, bitmap);
                throw t;
              }
            });
  }

  public static <T> void verifyInvariance(
      String testName,
      BiPredicate<MutableRoaringBitmap, MutableRoaringBitmap> validity,
      int maxKeys,
      BiPredicate<MutableRoaringBitmap, MutableRoaringBitmap> test) {
    verifyInvariance(testName, validity, ITERATIONS, maxKeys, test);
  }

  public static <T> void verifyInvariance(
      String testName,
      BiPredicate<MutableRoaringBitmap, MutableRoaringBitmap> validity,
      int count,
      int maxKeys,
      BiPredicate<MutableRoaringBitmap, MutableRoaringBitmap> test) {
    IntStream.range(0, count)
        .parallel()
        .forEach(
            i -> {
              MutableRoaringBitmap one = randomBitmap(maxKeys);
              MutableRoaringBitmap two = randomBitmap(maxKeys);
              if (validity.test(one, two)) {
                try {
                  assertTrue(test.test(one, two));
                } catch (Throwable t) {
                  Reporter.report(testName, ImmutableMap.of(), t, one, two);
                  throw t;
                }
              }
            });
  }

  public static void verifyInvariance(
      Predicate<MutableRoaringBitmap> validity, Consumer<MutableRoaringBitmap> action) {
    verifyInvariance(validity, ITERATIONS, 1 << 3, action);
  }

  public static void verifyInvariance(
      Predicate<MutableRoaringBitmap> validity,
      int count,
      int maxKeys,
      Consumer<MutableRoaringBitmap> action) {
    IntStream.range(0, count)
        .parallel()
        .mapToObj(i -> randomBitmap(maxKeys))
        .filter(validity)
        .forEach(action);
  }

  @Test
  public void rankSelectInvariance() {
    verifyInvariance(
        "rankSelectInvariance",
        bitmap -> !bitmap.isEmpty(),
        (i, rb) -> rb.rank(rb.select(i)) == i + 1);
  }

  @Test
  public void selectContainsInvariance() {
    verifyInvariance(
        "selectContainsInvariance",
        bitmap -> !bitmap.isEmpty(),
        (i, rb) -> rb.contains(rb.select(i)));
  }

  @Test
  public void firstSelect0Invariance() {
    verifyInvariance(
        "firstSelect0Invariance",
        bitmap -> !bitmap.isEmpty(),
        b -> b.first(),
        bitmap -> bitmap.select(0));
  }

  @Test
  public void lastSelectCardinalityInvariance() {
    verifyInvariance(
        "lastSelectCardinalityInvariance",
        bitmap -> !bitmap.isEmpty(),
        bitmap -> bitmap.last(),
        bitmap -> bitmap.select(bitmap.getCardinality() - 1));
  }

  @Test
  public void intersectsRangeFirstLastInvariance() {
    verifyInvariance(
        "intersectsRangeFirstLastInvariance",
        true,
        rb -> rb.intersects(toUnsignedLong(rb.first()), toUnsignedLong(rb.last())));
  }

  @Test
  public void containsRangeFirstLastInvariance() {
    verifyInvariance(
        "containsRangeFirstLastInvariance",
        true,
        rb ->
            MutableRoaringBitmap.add(
                    rb.clone(), toUnsignedLong(rb.first()), toUnsignedLong(rb.last()))
                .contains(toUnsignedLong(rb.first()), toUnsignedLong(rb.last())));
  }

  @Test
  public void andCardinalityInvariance() {
    verifyInvariance(
        "andCardinalityInvariance",
        ITERATIONS,
        1 << 9,
        (l, r) -> MutableRoaringBitmap.and(l, r).getCardinality(),
        (l, r) -> MutableRoaringBitmap.andCardinality(l, r));
  }

  @Test
  public void orCardinalityInvariance() {
    verifyInvariance(
        "orCardinalityInvariance",
        ITERATIONS,
        1 << 9,
        (l, r) -> MutableRoaringBitmap.or(l, r).getCardinality(),
        (l, r) -> MutableRoaringBitmap.orCardinality(l, r));
  }

  @Test
  public void xorCardinalityInvariance() {
    verifyInvariance(
        "xorCardinalityInvariance",
        ITERATIONS,
        1 << 9,
        (l, r) -> MutableRoaringBitmap.xor(l, r).getCardinality(),
        (l, r) -> MutableRoaringBitmap.xorCardinality(l, r));
  }

  @Test
  public void containsContainsInvariance() {
    verifyInvariance(
        "containsContainsInvariance",
        (l, r) -> l.contains(r) && !r.equals(l),
        (l, r) -> false,
        (l, r) -> !r.contains(l));
  }

  @Test
  public void containsAndInvariance() {
    verifyInvariance(
        "containsAndInvariance",
        (l, r) -> l.contains(r),
        (l, r) -> MutableRoaringBitmap.and(l, r).equals(r));
  }

  @Test
  public void andCardinalityContainsInvariance() {
    verifyInvariance(
        "andCardinalityContainsInvariance",
        (l, r) -> MutableRoaringBitmap.andCardinality(l, r) == 0,
        (l, r) -> false,
        (l, r) -> l.contains(r) || r.contains(l));
  }

  @Test
  public void sizeOfUnionOfDisjointSetsEqualsSumOfSizes() {
    verifyInvariance(
        "sizeOfUnionOfDisjointSetsEqualsSumOfSizes",
        (l, r) -> MutableRoaringBitmap.andCardinality(l, r) == 0,
        (l, r) -> l.getCardinality() + r.getCardinality(),
        (l, r) -> MutableRoaringBitmap.orCardinality(l, r));
  }

  @Test
  public void sizeOfDifferenceOfDisjointSetsEqualsSumOfSizes() {
    verifyInvariance(
        "sizeOfDifferenceOfDisjointSetsEqualsSumOfSizes",
        (l, r) -> MutableRoaringBitmap.andCardinality(l, r) == 0,
        (l, r) -> l.getCardinality() + r.getCardinality(),
        (l, r) -> MutableRoaringBitmap.xorCardinality(l, r));
  }

  @Test
  public void equalsSymmetryInvariance() {
    verifyInvariance("equalsSymmetryInvariance", (l, r) -> l.equals(r), (l, r) -> r.equals(l));
  }

  @Test
  public void orOfDisjunction() {
    verifyInvariance(
        "orOfDisjunction",
        ITERATIONS,
        1 << 8,
        (l, r) -> l,
        (l, r) -> MutableRoaringBitmap.or(l, MutableRoaringBitmap.and(l, r)));
  }

  @Test
  public void orCoversXor() {
    verifyInvariance(
        "orCoversXor",
        ITERATIONS,
        1 << 8,
        (l, r) -> MutableRoaringBitmap.or(l, r),
        (l, r) -> MutableRoaringBitmap.or(l, MutableRoaringBitmap.xor(l, r)));
  }

  @Test
  public void xorInvariance() {
    verifyInvariance(
        "xorInvariance",
        ITERATIONS,
        1 << 9,
        (l, r) -> MutableRoaringBitmap.xor(l, r),
        (l, r) ->
            MutableRoaringBitmap.andNot(
                MutableRoaringBitmap.or(l, r), MutableRoaringBitmap.and(l, r)));
  }

  @Test
  public void rangeCardinalityVsMaterialisedRange() {
    verifyInvariance(
        "rangeCardinalityVsMaterialisedRange",
        1 << 9,
        (min, max, bitmap) -> {
          MutableRoaringBitmap range = new MutableRoaringBitmap();
          range.add(min, max);
          return bitmap.rangeCardinality(min, max)
              == ImmutableRoaringBitmap.andCardinality(range, bitmap);
        });
  }

  @Test
  public void intersectsUpperBoundary() {
    verifyInvariance(
        "intersectsUpperBoundary",
        1 << 9,
        bitmap -> {
          long max = bitmap.last() & 0xFFFFFFFFL;
          return max == 0xFFFFFFFFL || bitmap.intersects(max - 1, max + 1);
        });
  }

  @Test
  public void intersectsLowerBoundary() {
    verifyInvariance(
        "intersectsLowerBoundary",
        1 << 9,
        bitmap -> {
          long min = bitmap.first() & 0xFFFFFFFFL;
          return min == 0 || bitmap.intersects(min - 1, min + 1);
        });
  }

  @Test
  public void notIntersectsDisjointUpperBoundary() {
    verifyInvariance(
        "notIntersectsDisjointUpperBoundary",
        1 << 9,
        bitmap -> {
          long max = (bitmap.last() & 0xFFFFFFFFL) + 1;
          return !bitmap.intersects(max, 0xFFFFFFFFL);
        });
  }

  @Test
  public void notIntersectsDisjointLowerBoundary() {
    verifyInvariance(
        "notIntersectsDisjointLowerBoundary",
        1 << 9,
        bitmap -> {
          long min = bitmap.first() & 0xFFFFFFFFL;
          return !bitmap.intersects(0, min);
        });
  }

  @Test
  public void removeIntersection() {
    verifyInvariance(
        "removeIntersection",
        (l, r) -> MutableRoaringBitmap.andCardinality(l, r) > 0,
        1 << 12,
        (l, r) -> {
          int intersection = MutableRoaringBitmap.andCardinality(l, r);
          MutableRoaringBitmap and = MutableRoaringBitmap.and(l, r);
          IntIterator it = and.getBatchIterator().asIntIterator(new int[16]);
          int removed = 0;
          while (it.hasNext()) {
            l.remove(it.next());
            ++removed;
          }
          return removed == intersection;
        });
  }

  @Test
  public void dontContainAfterRemoval() {
    verifyInvariance(
        "dontIntersectAfterRemoval",
        (l, r) -> MutableRoaringBitmap.andCardinality(l, r) > 0,
        1 << 12,
        (l, r) -> {
          int intersection = MutableRoaringBitmap.andCardinality(l, r);
          MutableRoaringBitmap and = MutableRoaringBitmap.and(l, r);
          long first = and.first() & 0xFFFFFFFFL;
          int[] values = and.toArray();
          int removed = 0;
          for (int next : values) {
            if (!l.intersects(toUnsignedLong(next) - 1, toUnsignedLong(next) + 1)) {
              return false;
            }
            l.remove(next);
            if (l.contains(next)) {
              return false;
            }
            if (first != next && l.contains(first, toUnsignedLong(next))) {
              return false;
            }
            ++removed;
          }
          return removed == intersection;
        });
  }

  @Test
  public void intersectsContainsRemove() {
    verifyInvariance(
        "intersectsContainsRemove",
        (l, r) -> MutableRoaringBitmap.andCardinality(l, r) > 0,
        1 << 12,
        (l, r) -> {
          MutableRoaringBitmap and = MutableRoaringBitmap.and(l, r);
          if (!(l.contains(and) && r.contains(and))) {
            return false;
          }
          long first = and.first() & 0xFFFFFFFFL;
          long last = and.last() & 0xFFFFFFFFL;
          IntIterator it = and.getBatchIterator().asIntIterator(new int[16]);
          l.remove(first, last + 1);
          while (it.hasNext()) {
            long next = toUnsignedLong(it.next());
            if (l.intersects(first, next) || (first != next && !r.intersects(first, next))) {
              return false;
            }
            if (l.contains(first, next)) {
              return false;
            }
          }
          return !l.contains(and);
        });
  }

  @Test
  public void absentValuesConsistentWithBitSet() {
    int[] offsets = new int[] {0, 1, -1, 10, -10, 100, -100};

    // Size limit to avoid out of memory errors; r.last() > 0 to avoid bitmaps with last >
    // Integer.MAX_VALUE
    verifyInvariance(
        r -> r.isEmpty() || (r.last() > 0 && r.last() < 1 << 30),
        bitmap -> {
          BitSet reference = new BitSet();
          bitmap.forEach((IntConsumer) reference::set);

          for (int next : bitmap) {
            for (int offset : offsets) {
              int pos = next + offset;
              if (pos >= 0) {
                assertEquals(reference.nextClearBit(pos), bitmap.nextAbsentValue(pos));
                assertEquals(reference.previousClearBit(pos), bitmap.previousAbsentValue(pos));
              }
            }
          }
        });
  }

  @Test
  public void testFastAggregationAnd() {
    IntStream.range(0, ITERATIONS)
        .parallel()
        .forEach(
            i -> {
              MutableRoaringBitmap[] bitmaps =
                  new MutableRoaringBitmap[ThreadLocalRandom.current().nextInt(2, 20)];
              for (int j = 0; j < bitmaps.length; ++j) {
                bitmaps[j] = randomBitmap(512);
              }
              MutableRoaringBitmap naive = BufferFastAggregation.naive_and(bitmaps);
              MutableRoaringBitmap workShy = BufferFastAggregation.and(bitmaps);
              assertEquals(naive, workShy);
            });
  }

  private static MutableRoaringBitmap randomBitmap(int maxKeys) {
    return RandomisedTestData.randomBitmap(maxKeys).toMutableRoaringBitmap();
  }
}
