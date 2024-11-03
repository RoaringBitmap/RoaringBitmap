package org.roaringbitmap.cardinality64;

import org.roaringbitmap.longlong.Roaring64Bitmap;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class Roaring64BmpCardinalityBenchmark {

  @BenchmarkMode(Mode.Throughput)
  @Benchmark
  public long getCardinality(CacheCardinalitiesBenchmarkState benchmarkState) {
    return benchmarkState.roaring64Bitmap.getLongCardinality();
  }

  static final int SMALL_CARDINALITY = 100;
  // High cardinality: 1000 times the small test
  static final int HIGH_CARDINALITY = SMALL_CARDINALITY * 1000;

  @State(Scope.Benchmark)
  public static class CacheCardinalitiesBenchmarkState {
    final Roaring64Bitmap roaring64Bitmap = new Roaring64Bitmap();

    // Try to generate many buckets with low cardinality bitmaps in order to stress-out the Map
    // overhead
    public CacheCardinalitiesBenchmarkState() {
      for (long i = 0; i < HIGH_CARDINALITY; i++) {
        long toAdd = i + i * Integer.MAX_VALUE;
        roaring64Bitmap.addLong(toAdd);
      }
    }
  }

  public static void main(String... args) throws Exception {
    Options opts =
        new OptionsBuilder()
            .include(".*Roaring64BmpCardinalityBenchmark.*")
            .warmupTime(new TimeValue(1, TimeUnit.SECONDS))
            .warmupIterations(3)
            .measurementTime(new TimeValue(1, TimeUnit.SECONDS))
            .measurementIterations(3)
            .forks(1)
            .build();

    new Runner(opts).run();
  }
}
