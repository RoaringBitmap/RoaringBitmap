package org.roaringbitmap;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static org.roaringbitmap.RoaringBitmapWriter.writer;

public class RandomisedTestData {

  private static final ThreadLocal<long[]> bits = ThreadLocal.withInitial(() -> new long[1 << 10]);

  public static RoaringBitmap randomBitmap(int maxKeys, double rleLimit, double denseLimit) {
    return randomBitmap(maxKeys, rleLimit, denseLimit, writer().initialCapacity(maxKeys).optimiseForArrays().get());
  }

  public static <T extends BitmapDataProvider> T randomBitmap(int maxKeys,
                                                              double rleLimit,
                                                              double denseLimit,
                                                              RoaringBitmapWriter<T> writer) {
    int[] keys = createSorted16BitInts(ThreadLocalRandom.current().nextInt(1, maxKeys));
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
    return writer.get();
  }

  public static RoaringBitmap randomBitmap(int maxKeys) {
    double rleLimit = ThreadLocalRandom.current().nextDouble();
    double denseLimit = ThreadLocalRandom.current().nextDouble(rleLimit, 1D);
    return randomBitmap(maxKeys, rleLimit, denseLimit);
  }

  public static <T extends BitmapDataProvider> T randomBitmap(int maxKeys, RoaringBitmapWriter<T> writer) {
    double rleLimit = ThreadLocalRandom.current().nextDouble();
    double denseLimit = ThreadLocalRandom.current().nextDouble(rleLimit, 1D);
    return randomBitmap(maxKeys, rleLimit, denseLimit, writer);
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

    RoaringBitmapWriter<RoaringBitmap> writer = RoaringBitmapWriter.writer().constantMemory().get();

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
      writer.add(minimum, supremum);
      return this;
    }

    public RoaringBitmap build() {
      writer.flush();
      return writer.getUnderlying();
    }
  }
}
