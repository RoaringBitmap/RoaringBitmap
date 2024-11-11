package org.roaringbitmap.longlong;

import com.google.common.primitives.Ints;
import org.openjdk.jol.info.GraphLayout;

import java.util.Arrays;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

/**
 * This runs benchmarks over {@link Roaring64NavigableMap} memory layout, as suggested in
 * https://github.com/RoaringBitmap/RoaringBitmap/issues/346
 */
// https://github.com/openjdk/jol
// https://github.com/RoaringBitmap/RoaringBitmap/issues/346
public class JolBenchmarksTest {

  @SuppressWarnings("restriction")
  public static void main(String[] args) {
    // distinctHigherRadices();
    // sameHigherRadix();
    statisticsWithGrowingSizes();
  }

  private static void distinctHigherRadices() {
    int valuesPerRadix = 1 << 16;
    int distance = 2000;
    Roaring64NavigableMap bitmapMap = new Roaring64NavigableMap();
    long[] radices = new long[1024];
    for (int i = 0; i < radices.length; ++i) {
      radices[i] = ((long) i) << 48;
    }
    for (int i = 0; i < radices.length; i++) {
      for (int j = 0; j < valuesPerRadix; ++j) {
        bitmapMap.addLong(radices[i] | (j * distance));
      }
    }

    long[] bitmapArray = bitmapMap.toArray();

    Roaring64Bitmap bitmapArt = new Roaring64Bitmap();
    bitmapArt.add(bitmapArray);

    System.out.println("---distinctHigherRadices---");
    System.out.println(GraphLayout.parseInstance(bitmapArray).toFootprint());

    System.out.println("---");
    System.out.println(GraphLayout.parseInstance(bitmapMap).toFootprint());
    System.out.println(bitmapMap.getLongSizeInBytes());
    bitmapMap.runOptimize();
    System.out.println(GraphLayout.parseInstance(bitmapMap).toFootprint());
    System.out.println(bitmapMap.getLongSizeInBytes());

    System.out.println("---");
    System.out.println(GraphLayout.parseInstance(bitmapArt).toFootprint());
    System.out.println(bitmapArt.getLongSizeInBytes());
    bitmapArt.runOptimize();
    System.out.println(GraphLayout.parseInstance(bitmapArt).toFootprint());
    System.out.println(bitmapArt.getLongSizeInBytes());
  }

  private static void sameHigherRadix() {
    int numValues = (1 << 16) * 1024;
    int distance = 2000;
    Roaring64NavigableMap bitmap = new Roaring64NavigableMap();

    long x = 0L;
    for (int i = 0; i < numValues; i++) {
      bitmap.addLong(x);
      x += distance;
    }

    long[] array = bitmap.toArray();

    Roaring64Bitmap bitmapOpt = new Roaring64Bitmap();
    bitmapOpt.add(array);

    System.out.println("---sameHigherRadix---");
    System.out.println(GraphLayout.parseInstance(array).toFootprint());

    System.out.println("---");
    System.out.println(GraphLayout.parseInstance(bitmap).toFootprint());
    bitmap.runOptimize();
    System.out.println(GraphLayout.parseInstance(bitmap).toFootprint());

    System.out.println("---");
    System.out.println(GraphLayout.parseInstance(bitmapOpt).toFootprint());
    bitmapOpt.runOptimize();
    System.out.println(GraphLayout.parseInstance(bitmapOpt).toFootprint());
  }

  public static void statisticsWithGrowingSizes() {
    Random r = new Random();

    // We re-use the bitmaps so that we accumulate the longs into them
    // We then expect to have denser buckets (around 0)
    Roaring64Bitmap bitmap64Art = new Roaring64Bitmap();
    Roaring64NavigableMap bitmap64Map = new Roaring64NavigableMap();

    for (long size = 0; size < 1024; size++) {
      long max = (1L + size * size * size * size * size);
      System.out.println(size + " in [-" + max + ", " + max + "[");

      long fSize = size;
      long[] array =
          IntStream.range(0, Ints.checkedCast(size))
              .mapToLong(i -> i)
              .flatMap(i -> LongStream.generate(() -> r.nextLong() % max).limit(fSize))
              .toArray();

      reportBitmapsMemory(array, bitmap64Art, bitmap64Map);
    }
  }

  private static void reportBitmapsMemory(
      long[] array, LongBitmapDataProvider bitmap, LongBitmapDataProvider bitmap2) {
    reportBitmapsMemory(array, bitmap);
    reportBitmapsMemory(array, bitmap2);

    if (!Arrays.equals(bitmap.toArray(), bitmap.toArray())) {
      throw new IllegalStateException("Issue with: " + Arrays.toString(array));
    }
  }

  private static void reportBitmapsMemory(long[] array, LongBitmapDataProvider bitmap) {
    LongStream.of(array).forEach(bitmap::addLong);
    long jolSize = GraphLayout.parseInstance(bitmap).totalSize();
    long ownEstimation = bitmap.getLongSizeInBytes();
    System.out.println(
        bitmap.getClass().getSimpleName()
            + ": "
            + String.format("%,d", jolSize)
            + "(real) vs "
            + String.format("%,d", ownEstimation)
            + "(estimated) "
            + (ownEstimation < jolSize ? "optimistic" : "pessimistic"));
  }
}
