package org.roaringbitmap;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Fork(
    value = 2,
    jvmArgsAppend = {"-Xms1g", "-Xmx1g"})
@State(Scope.Thread)
public class FindIndexBenchmark {
  private static final int QUERY_COUNT = 1024;

  @Param({"ARRAY", "BITMAP", "RUN"})
  public String container;

  @Param({"false", "true"})
  public boolean cached;

  private RoaringBitmap bitmap;
  private int[] queries;

  @Setup
  public void setup() {
    bitmap = cached ? new FastRankRoaringBitmap() : new RoaringBitmap();
    int cardinality = container.equals("BITMAP") ? 32768 : 512;
    for (int block = 0; block < 64; block++) {
      for (int index = 0; index < cardinality; index++) {
        bitmap.add((block << 26) | lowBits(index));
      }
    }
    if (container.equals("RUN")) {
      bitmap.runOptimize();
    }
    queries = new int[QUERY_COUNT];
    Random random = new Random(627);
    for (int i = 0; i < queries.length; i++) {
      int block = random.nextInt(64);
      int index = random.nextInt(cardinality);
      boolean present = (i & 3) != 0;
      int low = lowBits(index);
      if (!present) {
        low = container.equals("RUN") ? (index / 64) * 1024 + 64 : low + 1;
      }
      queries[i] = (block << 26) | low;
      long expected = present ? (long) block * cardinality + index : -1L;
      long previous = bitmap.contains(queries[i]) ? bitmap.rankLong(queries[i]) - 1 : -1;
      if (previous != expected || bitmap.findIndex(queries[i]) != expected) {
        throw new IllegalStateException("Unexpected index for benchmark query");
      }
    }
  }

  private int lowBits(int index) {
    if (container.equals("ARRAY")) {
      return index * 128 + 1;
    }
    if (container.equals("BITMAP")) {
      return index * 2;
    }
    return (index / 64) * 1024 + index % 64;
  }

  @Benchmark
  @OperationsPerInvocation(QUERY_COUNT)
  public long containsAndRank() {
    long result = 0;
    for (int query : queries) {
      result += bitmap.contains(query) ? bitmap.rankLong(query) - 1 : -1;
    }
    return result;
  }

  @Benchmark
  @OperationsPerInvocation(QUERY_COUNT)
  public long findIndex() {
    long result = 0;
    for (int query : queries) {
      result += bitmap.findIndex(query);
    }
    return result;
  }
}
