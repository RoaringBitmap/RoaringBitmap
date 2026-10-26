package org.roaringbitmap;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, timeUnit = TimeUnit.MILLISECONDS, time = 200)
@Measurement(iterations = 10, timeUnit = TimeUnit.MILLISECONDS, time = 200)
@BenchmarkMode(Mode.Throughput)
@Fork(
    value = 1,
    jvmArgsPrepend = {
      "-XX:+UseG1GC",
      "-XX:-TieredCompilation",
      "-XX:+AlwaysPreTouch",
      "-ms4G",
      "-mx4G"
    })
public class CharRangeFillerBenchmark {

  @Param({
    // "11", // set CharRangeFiller.USE_ARRAYCOPY_MIN_SIZE < 12
    "12",
    "16",
    "100",
    "4096",
    "16384",
  })
  public int length;

  public char[] a;

  private static final int DESTINATION_OFFSET = 100;

  @Setup
  public void prepare() {
    CharRangeFiller.allocate(Character.MAX_VALUE);
    a = new char[length + DESTINATION_OFFSET];
  }

  @Benchmark
  public void arraycopy(Blackhole blackhole) {
    CharRangeFiller.fill(a, DESTINATION_OFFSET, 0, length);
    blackhole.consume(a);
  }

  @Benchmark
  public void iteratively(Blackhole blackhole) {
    for (int i = 0, j = DESTINATION_OFFSET; i < length; i++, j++) {
      a[j] = (char) i;
    }
    blackhole.consume(a);
  }
}
