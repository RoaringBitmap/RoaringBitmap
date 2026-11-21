package org.roaringbitmap.rangebitmap;

import org.roaringbitmap.RangeBitmap;
import org.roaringbitmap.RoaringBitmap;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(
    value = 2,
    jvmArgsAppend = {"-Xms1g", "-Xmx1g"})
@State(Scope.Benchmark)
public class RangeBitmapTrailingOnesBenchmark {

  @Param({"0", "1", "8", "15"})
  int trailingOnes;

  @Param("1048576")
  int rows;

  private RangeBitmap bitmap;
  private long threshold;

  @Setup
  public void setup() {
    RangeBitmap.Appender appender = RangeBitmap.appender(0x1FFFF);
    SplittableRandom random = new SplittableRandom(42);
    for (int i = 0; i < rows; i++) {
      appender.add(random.nextInt(0x20000));
    }
    bitmap = appender.build();
    threshold = 0x10000L | ((1L << trailingOnes) - 1);
  }

  @Benchmark
  public RoaringBitmap lte() {
    return bitmap.lte(threshold);
  }
}
