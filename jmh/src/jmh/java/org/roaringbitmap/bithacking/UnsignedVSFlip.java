package org.roaringbitmap.bithacking;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class UnsignedVSFlip {
  @Param({"1", "31", "65", "101", "103"})
  public short key;

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public int Time() {
    return key & 0xFFFF;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  public int TimeAvg() {
    return key & 0xFFFF;
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public int flipTime() {
    return key ^ Short.MIN_VALUE;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  public int flipTimeAvg() {
    return key ^ Short.MIN_VALUE;
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public short flipTimeShort() {
    return (short) (key ^ Short.MIN_VALUE);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  public short flipTimeShortAvg() {
    return (short) (key ^ Short.MIN_VALUE);
  }
}
