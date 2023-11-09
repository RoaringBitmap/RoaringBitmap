package org.roaringbitmap;

import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 3, timeUnit = TimeUnit.MILLISECONDS, time = 1000)
@Measurement(iterations = 5, timeUnit = TimeUnit.MILLISECONDS, time = 1000)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class BitmapOfRangeBenchmark {
  @Param({"0", // from the beginning
      "100000" // from some offset
  })
  int from;

  @Param({"10",
      "100000", // ~ 100 kBi
      "10000000",// ~ 10 MBi
  })
  int length;

  @Benchmark
  public RoaringBitmap original() {
    return bitmapOfRangeOriginal(from, from + length);
  }

  @Benchmark
  public RoaringBitmap optimized() {
    return RoaringBitmap.bitmapOfRange(from, from + length);
  }

  public static RoaringBitmap bitmapOfRangeOriginal(long min, long max) {
    RoaringBitmap bitmap = new RoaringBitmap();
    bitmap.add(min, max);
    return bitmap;
  }
}