package org.roaringbitmap;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.IntStream;

public class Fuzzer {

  @FunctionalInterface
  interface IntBitmapPredicate {
    boolean test(int index, RoaringBitmap bitmap);
  }

  private static final ThreadLocal<long[]> bits = ThreadLocal.withInitial(() -> new long[1 << 10]);

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
                    .iterator())
            .forEach(bitmap -> Assert.assertEquals(left.apply(bitmap), right.apply(bitmap)));
  }

  public static <T> void verifyInvariance(Predicate<RoaringBitmap> validity,
                                          Function<RoaringBitmap, T> left,
                                          Function<RoaringBitmap, T> right) {
    verifyInvariance(1000, 1 << 8, validity, left, right);
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
    verifyInvariance(1000, 1 << 8, left, right);
  }

  public static <T> void verifyInvariance(BiPredicate<RoaringBitmap, RoaringBitmap> validity,
                                          BiFunction<RoaringBitmap, RoaringBitmap, T> left,
                                          BiFunction<RoaringBitmap, RoaringBitmap, T> right) {
    verifyInvariance(validity,1000, 1 << 8, left, right);
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

  private static RoaringBitmap randomBitmap(int maxKeys) {
    int[] keys = createSorted16BitInts(ThreadLocalRandom.current().nextInt(1, maxKeys));
    double rleLimit = ThreadLocalRandom.current().nextDouble();
    double denseLimit = ThreadLocalRandom.current().nextDouble(rleLimit, 1D);
    OrderedWriter writer = new OrderedWriter();
    IntStream.of(keys)
            .forEach(key -> {
              double choice = ThreadLocalRandom.current().nextDouble();
              final IntStream stream;
              if (choice < rleLimit) {
                stream = rleRegion();
              } else if (choice < denseLimit) {
                stream = denseRegion();
              } else {
                stream = sparseRegion();
              }
              stream.map(i -> (key << 16) | i).forEach(writer::add);
            });
    writer.flush();
    return writer.getUnderlying();
  }

  private static IntStream rleRegion() {
    int numRuns = ThreadLocalRandom.current().nextInt(1,2048);
    int[] runs = createSorted16BitInts(numRuns * 2);
    return IntStream.range(0, numRuns)
            .map(i -> i * 2)
            .mapToObj(i -> IntStream.range(runs[i], runs[i + 1]))
            .flatMapToInt(i -> i);
  }

  private static IntStream sparseRegion() {
    return IntStream.of(createSorted16BitInts(ThreadLocalRandom.current().nextInt(1, 4096)));
  }


  private static IntStream denseRegion() {
    return IntStream.of(createSorted16BitInts(ThreadLocalRandom.current().nextInt(4096, 1 << 16)));
  }

  private static int[] createSorted16BitInts(int howMany) {
    // we can have at most 65536 keys in a RoaringBitmap
    long[] bitset = bits.get();
    Arrays.fill(bitset, 0L);
    int consumed = 0;
    while (consumed < howMany) {
      int value = ThreadLocalRandom.current().nextInt(1 << 16);
      long bit = (1L << value);
      consumed += 1 - Long.bitCount(bitset[value >>> 6] & bit);
      bitset[value >>> 6] |= bit;
    }
    int[] keys = new int[howMany];
    int prefix = 0;
    int k = 0;
    for (int i = bitset.length - 1; i >= 0; --i) {
      long word = bitset[i];
      while (word != 0) {
        keys[k++] = prefix + Long.numberOfTrailingZeros(word);
        word ^= Long.lowestOneBit(word);
      }
      prefix += 64;
    }
    return keys;
  }
}
