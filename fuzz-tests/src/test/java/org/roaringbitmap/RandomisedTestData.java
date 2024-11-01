package org.roaringbitmap;

import static java.lang.Integer.parseInt;
import static org.roaringbitmap.RoaringBitmapWriter.writer;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

public class RandomisedTestData {

  public static final int ITERATIONS =
      parseInt(System.getProperty("org.roaringbitmap.fuzz.iterations", "10000"));

  private static final ThreadLocal<long[]> bits = ThreadLocal.withInitial(() -> new long[1 << 10]);
  private static final ThreadLocal<int[]> runs = ThreadLocal.withInitial(() -> new int[4096]);

  public static RoaringBitmap randomBitmap(int maxKeys, double rleLimit, double denseLimit) {
    return randomBitmap(
        maxKeys, rleLimit, denseLimit, writer().initialCapacity(maxKeys).optimiseForArrays().get());
  }

  public static RoaringBitmap randomBitmap(int maxKeys) {
    double rleLimit = ThreadLocalRandom.current().nextDouble();
    double denseLimit = ThreadLocalRandom.current().nextDouble(rleLimit, 1D);
    return randomBitmap(maxKeys, rleLimit, denseLimit);
  }

  public static <T extends BitmapDataProvider> T randomBitmap(
      int maxKeys, RoaringBitmapWriter<T> writer) {
    double rleLimit = ThreadLocalRandom.current().nextDouble();
    double denseLimit = ThreadLocalRandom.current().nextDouble(rleLimit, 1D);
    return randomBitmap(maxKeys, rleLimit, denseLimit, writer);
  }

  private static <T extends BitmapDataProvider> T randomBitmap(
      int maxKeys, double rleLimit, double denseLimit, RoaringBitmapWriter<T> writer) {
    int[] keys = createSorted16BitInts(ThreadLocalRandom.current().nextInt(1, maxKeys));
    IntStream.of(keys)
        .forEach(
            key -> {
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
    return writer.get();
  }

  private static IntStream rleRegion() {
    int maxNumRuns = ThreadLocalRandom.current().nextInt(1, 2048);
    int minRequiredCardinality = maxNumRuns * 2 + 1;
    int[] values = runs.get();
    int totalRuns = 0;
    int start = ThreadLocalRandom.current().nextInt(64);
    int run = 0;
    while (minRequiredCardinality > 0 && start < 0xFFFF && run < 2 * maxNumRuns) {
      int runLength = ThreadLocalRandom.current().nextInt(1, minRequiredCardinality + 1);
      values[run++] = start;
      values[run++] = Math.min(start + runLength, 0x10000 - start);
      start += runLength + ThreadLocalRandom.current().nextInt(64);
      minRequiredCardinality -= runLength;
      ++totalRuns;
    }
    return IntStream.range(0, totalRuns)
        .map(i -> i * 2)
        .mapToObj(i -> IntStream.range(values[i], values[i + 1]))
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
        word &= (word - 1);
      }
      prefix += 64;
    }
    return keys;
  }
}
