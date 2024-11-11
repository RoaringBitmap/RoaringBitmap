package org.roaringbitmap.aggregation.or;

import org.roaringbitmap.FastAggregation;
import org.roaringbitmap.RandomData;
import org.roaringbitmap.RoaringBitmap;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class RoaringBitmapBenchmark {

  @Param({"10", "50", "100"})
  int bitmapSize;

  private List<RoaringBitmap> bitmaps = new ArrayList<>();

  @Setup
  public void setup() {
    for (int n = 0; n < bitmapSize; n++) {
      bitmaps.add(RandomData.randomBitmap(1 << 12, 1 / 3.0, 1 / 3.0));
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void or() {
    RoaringBitmap b1 = new RoaringBitmap();
    for (int n = 0; n < bitmapSize; n++) {
      b1.or(bitmaps.get(n));
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void lazyor() {
    FastAggregation.naive_or(bitmaps.iterator());
  }
}
