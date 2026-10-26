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
public class CharRangeFillerUsedInArrayContainerBenchmark {

  @Param({"10", "100", "4096"})
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
    blackhole.consume(new ArrayContainer(0, length));
  }

  @Benchmark
  public void iteratively(Blackhole blackhole) {
    blackhole.consume(newArrayContainerOriginal(0, length));
  }

  public ArrayContainer newArrayContainerOriginal(final int firstOfRun, final int lastOfRun) {
    ArrayContainer ac = new ArrayContainer(length);
    final int valuesInRange = lastOfRun - firstOfRun;
    ac.content = new char[valuesInRange];
    for (int i = 0; i < valuesInRange; ++i) {
      ac.content[i] = (char) (firstOfRun + i);
    }
    ac.cardinality = valuesInRange;
    return ac;
  }
}
