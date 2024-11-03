package org.roaringbitmap.aggregation.andnot.identical;

import org.roaringbitmap.longlong.Roaring64Bitmap;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class Roaring64BitmapBenchmark {

  private Roaring64Bitmap bitmap1;
  private Roaring64Bitmap bitmap2;

  @Setup
  public void setup() {
    bitmap1 = new Roaring64Bitmap();
    bitmap2 = new Roaring64Bitmap();
    int k = 1 << 16;
    long i = Long.MAX_VALUE / 2;
    long base = i;
    for (; i < base + 10000; ++i) {
      bitmap1.add(i * k);
      bitmap2.add(i * k);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public Roaring64Bitmap inplace_andNot() {
    Roaring64Bitmap b1 = bitmap1.clone();
    b1.andNot(bitmap2);
    return b1;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public Roaring64Bitmap justclone() {
    return bitmap1.clone();
  }
}
