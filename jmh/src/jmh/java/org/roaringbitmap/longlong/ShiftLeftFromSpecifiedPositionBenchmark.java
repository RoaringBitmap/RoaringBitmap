package org.roaringbitmap.longlong;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
public class ShiftLeftFromSpecifiedPositionBenchmark {

  public int v = 1234567890; // some value

  @Param({"0", "2"})
  public int pos;

  @Param({"1", "2"})
  public int count;

  @Benchmark
  public void optimized(Blackhole blackhole) {
    blackhole.consume(IntegerUtil.shiftLeftFromSpecifiedPosition(v, pos, count));
  }

  @Benchmark
  public void original(Blackhole blackhole) {
    blackhole.consume(shiftLeftFromSpecifiedPosition(v, pos, count));
  }

  public static int shiftLeftFromSpecifiedPosition(int v, int pos, int count) {
    byte[] initialVal = IntegerUtil.toBDBytes(v);
    System.arraycopy(initialVal, pos + 1, initialVal, pos, count);
    return IntegerUtil.fromBDBytes(initialVal);
  }
}