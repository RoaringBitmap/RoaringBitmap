package org.roaringbitmap;

import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.roaringbitmap.RandomisedTestData.randomBitmap;
import static org.roaringbitmap.Util.toUnsignedLong;

public class Fuzzer {

  @FunctionalInterface
  interface IntBitmapPredicate {
    boolean test(int index, RoaringBitmap bitmap);
  }

  public static <T> void verifyInvarianceArray(Function<RoaringBitmap[], T> left,
                                               Function<RoaringBitmap[], T> right) {
    verifyInvarianceArray(100, 1 << 5, 96, left, right);
  }

  public static <T> void verifyInvarianceArray(int count,
                                               int maxKeys,
                                               int setSize,
                                               Function<RoaringBitmap[], T> left,
                                               Function<RoaringBitmap[], T> right) {
    IntStream.range(0, count)
            .parallel()
            .mapToObj(i -> IntStream.range(0, setSize)
                    .mapToObj(j -> randomBitmap(maxKeys))
                    .toArray(RoaringBitmap[]::new))
            .forEach(bitmap -> Assert.assertEquals(left.apply(bitmap), right.apply(bitmap)));
  }

  public static <T> void verifyInvarianceIterator(Function<Iterator<RoaringBitmap>, T> left,
                                                  Function<Iterator<RoaringBitmap>, T> right) {
    verifyInvarianceIterator(100, 1 << 5, 96, left, right);
  }


  public static <T> void verifyInvarianceIterator(int count,
                                                  int maxKeys,
                                                  int setSize,
                                                  Function<Iterator<RoaringBitmap>, T> left,
                                                  Function<Iterator<RoaringBitmap>, T> right) {
    IntStream.range(0, count)
            .parallel()
            .mapToObj(i -> IntStream.range(0, setSize)
                    .mapToObj(j -> randomBitmap(maxKeys))
                    .collect(toList()))
            .forEach(bitmaps -> Assert.assertEquals(left.apply(bitmaps.iterator()), right.apply(bitmaps.iterator())));
  }

  public static <T> void verifyInvariance(T value, Function<RoaringBitmap, T> func) {
    verifyInvariance(100, 1 << 9, value, func);
  }

  public static <T> void verifyInvariance(int count,
                                          int maxKeys,
                                          T value,
                                          Function<RoaringBitmap, T> func) {
    IntStream.range(0, count)
            .parallel()
            .mapToObj(i -> randomBitmap(maxKeys))
            .forEach(bitmap -> Assert.assertEquals(value, func.apply(bitmap)));
  }

  public static <T> void verifyInvariance(Predicate<RoaringBitmap> validity,
                                          Function<RoaringBitmap, T> left,
                                          Function<RoaringBitmap, T> right) {
    verifyInvariance(100, 1 << 8, validity, left, right);
  }

  public static <T> void verifyInvariance(int count,
                                          int maxKeys,
                                          Predicate<RoaringBitmap> validity,
                                          Function<RoaringBitmap, T> left,
                                          Function<RoaringBitmap, T> right) {
    IntStream.range(0, count)
            .parallel()
            .mapToObj(i -> randomBitmap(maxKeys))
            .filter(validity)
            .forEach(bitmap -> Assert.assertEquals(left.apply(bitmap), right.apply(bitmap)));
  }

  public static <T> void verifyInvariance(BiFunction<RoaringBitmap, RoaringBitmap, T> left,
                                          BiFunction<RoaringBitmap, RoaringBitmap, T> right) {
    verifyInvariance(100, 1 << 8, left, right);
  }

  public static <T> void verifyInvariance(BiPredicate<RoaringBitmap, RoaringBitmap> validity,
                                          BiFunction<RoaringBitmap, RoaringBitmap, T> left,
                                          BiFunction<RoaringBitmap, RoaringBitmap, T> right) {
    verifyInvariance(validity, 100, 1 << 8, left, right);
  }


  public static <T> void verifyInvariance(int count,
                                          int maxKeys,
                                          BiFunction<RoaringBitmap, RoaringBitmap, T> left,
                                          BiFunction<RoaringBitmap, RoaringBitmap, T> right) {
    verifyInvariance((l, r) -> true, count, maxKeys, left, right);
  }

  public static <T> void verifyInvariance(BiPredicate<RoaringBitmap, RoaringBitmap> validity,
                                          int count,
                                          int maxKeys,
                                          BiFunction<RoaringBitmap, RoaringBitmap, T> left,
                                          BiFunction<RoaringBitmap, RoaringBitmap, T> right) {
    IntStream.range(0, count)
            .parallel()
            .forEach(i -> {
              RoaringBitmap one = randomBitmap(maxKeys);
              RoaringBitmap two = randomBitmap(maxKeys);
              if (validity.test(one, two)) {
                Assert.assertEquals(left.apply(one, two), right.apply(one, two));
              }
            });
  }

  public static void verifyInvariance(IntBitmapPredicate predicate) {
    verifyInvariance(rb -> true, predicate);
  }

  public static void verifyInvariance(Predicate<RoaringBitmap> validity,
                                      IntBitmapPredicate predicate) {
    verifyInvariance(validity, 100, 1 << 3, predicate);
  }

  public static void verifyInvariance(Predicate<RoaringBitmap> validity,
                                      int count,
                                      int maxKeys,
                                      IntBitmapPredicate predicate) {
    IntStream.range(0, count)
            .parallel()
            .mapToObj(i -> randomBitmap(maxKeys))
            .filter(validity)
            .forEach(bitmap -> {
              for (int i = 0; i < bitmap.getCardinality(); ++i) {
                Assert.assertTrue(predicate.test(i, bitmap));
              }
            });
  }

  @Test
  public void rankSelectInvariance() {
    verifyInvariance(bitmap -> !bitmap.isEmpty(), (i, rb) -> rb.rank(rb.select(i)) == i + 1);
  }

  @Test
  public void selectContainsInvariance() {
    verifyInvariance(bitmap -> !bitmap.isEmpty(), (i, rb) -> rb.contains(rb.select(i)));
  }

  @Test
  public void firstSelect0Invariance() {
    verifyInvariance(bitmap -> !bitmap.isEmpty(),
            bitmap -> bitmap.first(),
            bitmap -> bitmap.select(0));
  }

  @Test
  public void lastSelectCardinalityInvariance() {
    verifyInvariance(bitmap -> !bitmap.isEmpty(),
            bitmap -> bitmap.last(),
            bitmap -> bitmap.select(bitmap.getCardinality() - 1));
  }

  @Test
  public void andCardinalityInvariance() {
    verifyInvariance(100, 1 << 9,
            (l, r) -> RoaringBitmap.and(l, r).getCardinality(),
            (l, r) -> RoaringBitmap.andCardinality(l, r));
  }

  @Test
  public void orCardinalityInvariance() {
    verifyInvariance(100, 1 << 9,
            (l, r) -> RoaringBitmap.or(l, r).getCardinality(),
            (l, r) -> RoaringBitmap.orCardinality(l, r));
  }

  @Test
  public void xorCardinalityInvariance() {
    verifyInvariance(100, 1 << 9,
            (l, r) -> RoaringBitmap.xor(l, r).getCardinality(),
            (l, r) -> RoaringBitmap.xorCardinality(l, r));
  }

  @Test
  public void containsContainsInvariance() {
    verifyInvariance((l, r) -> l.contains(r) && !r.equals(l),
            (l, r) -> false,
            (l, r) -> !r.contains(l));
  }

  @Test
  public void containsAndInvariance() {
    verifyInvariance((l, r) -> l.contains(r), (l, r) -> RoaringBitmap.and(l, r).equals(r));
  }


  @Test
  public void limitCardinalityEqualsSelf() {
    verifyInvariance(true, rb -> rb.equals(rb.limit(rb.getCardinality())));
  }

  @Test
  public void limitCardinalityXorCardinalityInvariance() {
    verifyInvariance(rb -> true,
            rb -> rb.getCardinality(),
            rb -> rb.getCardinality() / 2
                    + RoaringBitmap.xorCardinality(rb, rb.limit(rb.getCardinality() / 2)));
  }

  @Test
  public void containsRangeFirstLastInvariance() {
    verifyInvariance(true,
            rb -> RoaringBitmap.add(rb.clone(), toUnsignedLong(rb.first()), toUnsignedLong(rb.last()))
                               .contains(toUnsignedLong(rb.first()), toUnsignedLong(rb.last())));
  }

  @Test
  public void intersectsRangeFirstLastInvariance() {
    verifyInvariance(true, rb -> rb.intersects(toUnsignedLong(rb.first()), toUnsignedLong(rb.last())));
  }

  @Test
  public void containsSelf() {
    verifyInvariance(true, rb -> rb.contains(rb.clone()));
  }

  @Test
  public void containsSubset() {
    verifyInvariance(true, rb -> rb.contains(rb.limit(rb.getCardinality() / 2)));
  }

  @Test
  public void andCardinalityContainsInvariance() {
    verifyInvariance((l, r) -> RoaringBitmap.andCardinality(l, r) == 0,
            (l, r) -> false,
            (l, r) -> l.contains(r) || r.contains(l));
  }

  @Test
  public void sizeOfUnionOfDisjointSetsEqualsSumOfSizes() {
    verifyInvariance((l, r) -> RoaringBitmap.andCardinality(l, r) == 0,
            (l, r) -> l.getCardinality() + r.getCardinality(),
            (l, r) -> RoaringBitmap.orCardinality(l, r));
  }

  @Test
  public void sizeOfDifferenceOfDisjointSetsEqualsSumOfSizes() {
    verifyInvariance((l, r) -> RoaringBitmap.andCardinality(l, r) == 0,
            (l, r) -> l.getCardinality() + r.getCardinality(),
            (l, r) -> RoaringBitmap.xorCardinality(l, r));
  }

  @Test
  public void equalsSymmetryInvariance() {
    verifyInvariance((l, r) -> l.equals(r), (l, r) -> r.equals(l));
  }

  @Test
  public void orOfDisjunction() {
    verifyInvariance(100, 1 << 8,
            (l, r) -> l,
            (l, r) -> RoaringBitmap.or(l, RoaringBitmap.and(l, r)));
  }

  @Test
  public void orCoversXor() {
    verifyInvariance(100, 1 << 8,
            (l, r) -> RoaringBitmap.or(l, r),
            (l, r) -> RoaringBitmap.or(l, RoaringBitmap.xor(l, r)));
  }

  @Test
  public void xorInvariance() {
    verifyInvariance(100, 1 << 9,
            (l, r) -> RoaringBitmap.xor(l, r),
            (l, r) -> RoaringBitmap.andNot(RoaringBitmap.or(l, r), RoaringBitmap.and(l, r)));
  }

  @Test
  public void naiveOrPriorityQueueOrInvariance() {
    verifyInvarianceArray(
            bitmaps -> FastAggregation.naive_or(bitmaps),
            bitmaps -> FastAggregation.priorityqueue_or(bitmaps));
  }


  @Test
  public void naiveOrPriorityQueueOrInvarianceIterator() {
    verifyInvarianceIterator(
            bitmaps -> FastAggregation.naive_or(bitmaps),
            bitmaps -> FastAggregation.priorityqueue_or(bitmaps));
  }


  @Test
  public void naiveXorPriorityQueueXorInvariance() {
    verifyInvarianceArray(
            bitmaps -> FastAggregation.naive_xor(bitmaps),
            bitmaps -> FastAggregation.priorityqueue_xor(bitmaps));
  }

  @Test
  public void parallelOrVsFastOr() {
    verifyInvarianceArray(
            bitmaps -> ParallelAggregation.or(bitmaps),
            bitmaps -> FastAggregation.priorityqueue_or(bitmaps));
  }

  @Test
  public void parallelXorVsFastXor() {
    verifyInvarianceArray(
            bitmaps -> ParallelAggregation.xor(bitmaps),
            bitmaps -> FastAggregation.priorityqueue_xor(bitmaps));
  }
}
