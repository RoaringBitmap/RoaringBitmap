package org.roaringbitmap;

import static org.roaringbitmap.RoaringBitmapWriter.writer;

import java.util.Arrays;
import java.util.SplittableRandom;
import java.util.stream.IntStream;

public class SeededTestData {

  private static final ThreadLocal<long[]> bits = ThreadLocal.withInitial(() -> new long[1 << 10]);

  private static final ThreadLocal<SplittableRandom> RNG =
      ThreadLocal.withInitial(() -> new SplittableRandom(0xfeef1f0));

  public static RoaringBitmap randomBitmap(int maxKeys, double rleLimit, double denseLimit) {
    try {
      return randomBitmap(maxKeys, rleLimit, denseLimit, RNG.get());
    } finally {
      RNG.remove();
    }
  }

  public static RoaringBitmap randomBitmap(int maxKeys) {
    try {
      SplittableRandom random = RNG.get();
      double rleLimit = random.nextDouble();
      double denseLimit = random.nextDouble(rleLimit, 1D);
      return randomBitmap(maxKeys, rleLimit, denseLimit, random);
    } finally {
      RNG.remove();
    }
  }

  public static <T extends BitmapDataProvider> T randomBitmap(
      int maxKeys, RoaringBitmapWriter<T> writer) {
    try {
      SplittableRandom random = RNG.get();
      double rleLimit = random.nextDouble();
      double denseLimit = random.nextDouble(rleLimit, 1D);
      return randomBitmap(maxKeys, rleLimit, denseLimit, random, writer);
    } finally {
      RNG.remove();
    }
  }

  private static RoaringBitmap randomBitmap(
      int maxKeys, double rleLimit, double denseLimit, SplittableRandom random) {
    return randomBitmap(
        maxKeys,
        rleLimit,
        denseLimit,
        random,
        writer().initialCapacity(maxKeys).optimiseForArrays().get());
  }

  private static <T extends BitmapDataProvider> T randomBitmap(
      int maxKeys,
      double rleLimit,
      double denseLimit,
      SplittableRandom random,
      RoaringBitmapWriter<T> writer) {
    int[] keys = createSorted16BitInts(random.nextInt(1, maxKeys), random);
    IntStream.of(keys)
        .forEach(
            key -> {
              double choice = random.nextDouble();
              final IntStream stream;
              if (choice < rleLimit) {
                stream = rleRegion(random);
              } else if (choice < denseLimit) {
                stream = denseRegion(random);
              } else {
                stream = sparseRegion(random);
              }
              stream.map(i -> (key << 16) | i).forEach(writer::add);
            });
    return writer.get();
  }

  public static IntStream rleRegion() {
    try {
      return rleRegion(RNG.get());
    } finally {
      RNG.remove();
    }
  }

  public static IntStream sparseRegion() {
    try {
      return sparseRegion(RNG.get());
    } finally {
      RNG.remove();
    }
  }

  public static IntStream denseRegion() {
    try {
      return denseRegion(RNG.get());
    } finally {
      RNG.remove();
    }
  }

  private static IntStream rleRegion(SplittableRandom random) {
    int numRuns = random.nextInt(1, 2048);
    int[] runs = createSorted16BitInts(numRuns * 2, random);
    return IntStream.range(0, numRuns)
        .map(i -> i * 2)
        .mapToObj(i -> IntStream.range(runs[i], runs[i + 1]))
        .flatMapToInt(i -> i);
  }

  private static IntStream sparseRegion(SplittableRandom random) {
    return IntStream.of(createSorted16BitInts(random.nextInt(1, 4096), random));
  }

  private static IntStream denseRegion(SplittableRandom random) {
    return IntStream.of(createSorted16BitInts(random.nextInt(4096, 1 << 16), random));
  }

  private static int[] createSorted16BitInts(int howMany, SplittableRandom random) {
    // we can have at most 65536 keys in a RoaringBitmap
    long[] bitset = bits.get();
    Arrays.fill(bitset, 0L);
    int consumed = 0;
    while (consumed < howMany) {
      int value = random.nextInt(1 << 16);
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

  public static class TestDataSet {

    public static TestDataSet testCase() {
      return new TestDataSet();
    }

    private final SplittableRandom random = new SplittableRandom(42);

    RoaringBitmapWriter<RoaringBitmap> writer = RoaringBitmapWriter.writer().constantMemory().get();

    public TestDataSet withRunAt(int key) {
      assert key < 1 << 16;
      rleRegion(random).map(i -> (key << 16) | i).forEach(writer::add);
      return this;
    }

    public TestDataSet withArrayAt(int key) {
      assert key < 1 << 16;
      sparseRegion(random).map(i -> (key << 16) | i).forEach(writer::add);
      return this;
    }

    public TestDataSet withBitmapAt(int key) {
      assert key < 1 << 16;
      denseRegion(random).map(i -> (key << 16) | i).forEach(writer::add);
      return this;
    }

    public TestDataSet withRange(long minimum, long supremum) {
      writer.add(minimum, supremum);
      return this;
    }

    public RoaringBitmap build() {
      return writer.get();
    }
  }
}
