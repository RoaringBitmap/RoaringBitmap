package org.roaringbitmap;

import java.util.Arrays;
import java.util.SplittableRandom;
import java.util.stream.IntStream;

public class RandomData {

  private static final SplittableRandom RANDOM = new SplittableRandom(0);

  private static final ThreadLocal<long[]> bits = ThreadLocal.withInitial(() -> new long[1 << 10]);
  private static final ThreadLocal<int[]> runs = ThreadLocal.withInitial(() -> new int[4096]);

  public static RoaringBitmap randomContiguousBitmap(
      int startKey, int numKeys, double rleLimit, double denseLimit) {
    int[] keys = new int[numKeys];
    for (int i = 0; i < numKeys; ++i) {
      keys[i] = i + startKey;
    }
    return forKeys(keys, rleLimit, denseLimit);
  }

  public static RoaringBitmap randomBitmap(int maxKeys, double rleLimit, double denseLimit) {
    return forKeys(createSorted16BitInts(maxKeys), rleLimit, denseLimit);
  }

  public static IntStream rleRegion() {
    int maxNumRuns = RANDOM.nextInt(1, 2048);
    int minRequiredCardinality = maxNumRuns * 2 + 1;
    int[] values = runs.get();
    int totalRuns = 0;
    int start = RANDOM.nextInt(64);
    int run = 0;
    while (minRequiredCardinality > 0 && start < 0xFFFF && run < 2 * maxNumRuns) {
      int runLength = RANDOM.nextInt(1, minRequiredCardinality + 1);
      values[run++] = start;
      values[run++] = Math.min(start + runLength, 0x10000 - start);
      start += runLength + RANDOM.nextInt(64);
      minRequiredCardinality -= runLength;
      ++totalRuns;
    }
    return IntStream.range(0, totalRuns)
        .map(i -> i * 2)
        .mapToObj(i -> IntStream.range(values[i], values[i + 1]))
        .flatMapToInt(i -> i);
  }

  public static IntStream sparseRegion() {
    return IntStream.of(createSorted16BitInts(RANDOM.nextInt(1, 4096)));
  }

  public static IntStream denseRegion() {
    return IntStream.of(createSorted16BitInts(RANDOM.nextInt(4096, 1 << 16)));
  }

  private static int[] createSorted16BitInts(int howMany) {
    long[] bitset = bits.get();
    Arrays.fill(bitset, 0L);
    int consumed = 0;
    while (consumed < howMany) {
      int value = RANDOM.nextInt(1 << 16);
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

  private static RoaringBitmap forKeys(int[] keys, double rleLimit, double denseLimit) {
    RoaringBitmapWriter<RoaringBitmap> writer =
        RoaringBitmapWriter.writer().optimiseForArrays().get();
    IntStream.of(keys)
        .forEach(
            key -> {
              double choice = RANDOM.nextDouble();
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
}
