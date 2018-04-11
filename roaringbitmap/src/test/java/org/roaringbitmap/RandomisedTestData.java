package org.roaringbitmap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

public class RandomisedTestData {

  private static final ThreadLocal<long[]> bits = ThreadLocal.withInitial(() -> new long[1 << 10]);

  public static RoaringBitmap randomBitmap(int maxKeys, double rleLimit, double denseLimit) {
    int[] keys = createSorted16BitInts(ThreadLocalRandom.current().nextInt(1, maxKeys));
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

  public static RoaringBitmap randomBitmap(int maxKeys) {
    double rleLimit = ThreadLocalRandom.current().nextDouble();
    double denseLimit = ThreadLocalRandom.current().nextDouble(rleLimit, 1D);
    return randomBitmap(maxKeys, rleLimit, denseLimit);
  }

  public static IntStream rleRegion() {
    int numRuns = ThreadLocalRandom.current().nextInt(1, 2048);
    int[] runs = createSorted16BitInts(numRuns * 2);
    return IntStream.range(0, numRuns)
            .map(i -> i * 2)
            .mapToObj(i -> IntStream.range(runs[i], runs[i + 1]))
            .flatMapToInt(i -> i);
  }

  public static IntStream sparseRegion() {
    return IntStream.of(createSorted16BitInts(ThreadLocalRandom.current().nextInt(1, 4096)));
  }


  public static IntStream denseRegion() {
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

  public static class TestDataSet {


    public static TestDataSet testCase() {
      return new TestDataSet();
    }

    OrderedWriter writer = new OrderedWriter();
    private List<Long> ranges = new ArrayList<>();

    public TestDataSet withRunAt(int key) {
      assert key < 1 << 16;
      rleRegion().map(i -> (key << 16) | i).forEach(writer::add);
      return this;
    }

    public TestDataSet withArrayAt(int key) {
      assert key < 1 << 16;
      sparseRegion().map(i -> (key << 16) | i).forEach(writer::add);
      return this;
    }

    public TestDataSet withBitmapAt(int key) {
      assert key < 1 << 16;
      denseRegion().map(i -> (key << 16) | i).forEach(writer::add);
      return this;
    }

    public TestDataSet withRange(long minimum, long supremum) {
      ranges.add(minimum);
      ranges.add(supremum);
      return this;
    }

    public RoaringBitmap build() {
      writer.flush();
      RoaringBitmap bitmap = writer.getUnderlying();
      for (int i = 0; i < ranges.size(); i += 2) {
        bitmap.add(ranges.get(i), ranges.get(i + 1));
      }
      return bitmap;
    }
  }
}
