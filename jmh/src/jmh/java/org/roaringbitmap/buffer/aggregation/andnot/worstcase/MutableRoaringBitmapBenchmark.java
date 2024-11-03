package org.roaringbitmap.buffer.aggregation.andnot.worstcase;

import org.roaringbitmap.buffer.MutableRoaringBitmap;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class MutableRoaringBitmapBenchmark {

  private MutableRoaringBitmap bitmap1;
  private MutableRoaringBitmap bitmap2;

  @Setup
  public void setup() {
    bitmap1 = new MutableRoaringBitmap();
    bitmap2 = new MutableRoaringBitmap();
    int k = 1 << 16;
    for (int i = 0; i < 10000; ++i) {
      bitmap1.add(2 * i * k);
      bitmap2.add(2 * i * k + 1);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public MutableRoaringBitmap andNot() {
    return MutableRoaringBitmap.andNot(bitmap1, bitmap2);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public MutableRoaringBitmap inplace_andNot() {
    MutableRoaringBitmap b1 = bitmap1.clone();
    b1.andNot(bitmap2);
    return b1;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public MutableRoaringBitmap justclone() {
    return bitmap1.clone();
  }
}
