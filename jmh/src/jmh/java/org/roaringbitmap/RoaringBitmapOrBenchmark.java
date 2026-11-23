package org.roaringbitmap;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

@State(Scope.Benchmark)
public class RoaringBitmapOrBenchmark {

  @Param({"1", "16", "64", "256"})
  int matchingContainers;

  @Param({"0.1", "0.5"})
  double thisDensity;

  @Param({"0.1", "0.5"})
  double thatDensity;

  private RoaringBitmap first;
  private RoaringBitmap second;

  @Setup(Level.Trial)
  public void setup() {
    first = bitmap(matchingContainers, thisDensity, false);
    second = bitmap(matchingContainers, thatDensity, true);
    checkBitmapContainers(first);
    checkBitmapContainers(second);
  }

  @Benchmark
  public RoaringBitmap or() {
    return RoaringBitmap.or(first, second);
  }

  private static RoaringBitmap bitmap(int containerCount, double density, boolean fromEnd) {
    int valuesPerContainer = (int) (0x10000 * density);
    int[] values = new int[containerCount * valuesPerContainer];
    int position = 0;
    for (int key = 0; key < containerCount; ++key) {
      int start = fromEnd ? 0x10000 - valuesPerContainer : 0;
      int base = key << 16;
      for (int value = 0; value < valuesPerContainer; ++value) {
        values[position++] = base | (start + value);
      }
    }
    RoaringBitmap bitmap = new RoaringBitmap();
    bitmap.add(values);
    return bitmap;
  }

  private static void checkBitmapContainers(RoaringBitmap bitmap) {
    for (int i = 0; i < bitmap.highLowContainer.size; ++i) {
      if (!(bitmap.highLowContainer.getContainerAtIndex(i) instanceof BitmapContainer)) {
        throw new IllegalStateException("benchmark input is not a bitmap container");
      }
    }
  }
}
