// https://github.com/RoaringBitmap/RoaringBitmap/pull/176
package org.roaringbitmap.cardinality64;

import org.roaringbitmap.longlong.Roaring64NavigableMap;

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
public class Cardinality64Benchmark {

  @BenchmarkMode(Mode.Throughput)
  @Benchmark
  public long getCardinalityWithCache_Small(CacheCardinalitiesBenchmarkState benchmarkState) {
    return benchmarkState.smallWithCache.getLongCardinality();
  }

  @BenchmarkMode(Mode.Throughput)
  @Benchmark
  public long getCardinalityWithCache_Big(CacheCardinalitiesBenchmarkState benchmarkState) {
    return benchmarkState.bigWithCache.getLongCardinality();
  }

  @BenchmarkMode(Mode.Throughput)
  @Benchmark
  public long getCardinalityWithoutCache_Small(CacheCardinalitiesBenchmarkState benchmarkState) {
    return benchmarkState.smallWithoutCache.getLongCardinality();
  }

  @BenchmarkMode(Mode.Throughput)
  @Benchmark
  public long getCardinalityWithoutCache_Big(CacheCardinalitiesBenchmarkState benchmarkState) {
    return benchmarkState.bigWithoutCache.getLongCardinality();
  }

  @BenchmarkMode(Mode.Throughput)
  @Benchmark
  public long selectLastWithCache_Small(CacheCardinalitiesBenchmarkState benchmarkState) {
    return benchmarkState.smallWithCache.select(SMALL_CARDINALITY - 1);
  }

  @BenchmarkMode(Mode.Throughput)
  @Benchmark
  public long selectLastWithCache_Big(CacheCardinalitiesBenchmarkState benchmarkState) {
    return benchmarkState.bigWithCache.select(HIGH_CARDINALITY - 1);
  }

  @BenchmarkMode(Mode.Throughput)
  @Benchmark
  public long selectLastWithoutCache_Small(CacheCardinalitiesBenchmarkState benchmarkState) {
    return benchmarkState.smallWithoutCache.select(SMALL_CARDINALITY - 1);
  }

  @BenchmarkMode(Mode.Throughput)
  @Benchmark
  public long selectLastWithoutCache_Big(CacheCardinalitiesBenchmarkState benchmarkState) {
    return benchmarkState.bigWithoutCache.select(HIGH_CARDINALITY - 1);
  }

  static final int SMALL_CARDINALITY = 100;
  // High cardinality: 1000 times the small test
  static final int HIGH_CARDINALITY = SMALL_CARDINALITY * 1000;

  @State(Scope.Benchmark)
  public static class CacheCardinalitiesBenchmarkState {

    final Roaring64NavigableMap smallWithCache = new Roaring64NavigableMap(false, true);
    final Roaring64NavigableMap bigWithCache = new Roaring64NavigableMap(false, true);
    final Roaring64NavigableMap smallWithoutCache = new Roaring64NavigableMap(false, false);
    final Roaring64NavigableMap bigWithoutCache = new Roaring64NavigableMap(false, false);

    // Try to generate many buckets with low cardinality bitmaps in order to stress-out the Map
    // overhead
    public CacheCardinalitiesBenchmarkState() {
      for (long i = 0; i < HIGH_CARDINALITY; i++) {
        long toAdd = i + i * Integer.MAX_VALUE;
        smallWithCache.addLong(toAdd);
        bigWithCache.addLong(toAdd);
        smallWithoutCache.addLong(toAdd);
        bigWithoutCache.addLong(toAdd);
      }

      // This will trigger full cache computation
      smallWithCache.getLongCardinality();
      bigWithCache.getLongCardinality();
    }
  }

  public static final int WARMUP_ITERATIONS = 3;
  public static final int MEASUREMENTS_ITERATIONS = 3;

  public static void main(String... args) throws Exception {
    Options opts =
        new OptionsBuilder()
            .include(".*Cardinality64Benchmark.*")
            .warmupTime(new TimeValue(1, TimeUnit.SECONDS))
            .warmupIterations(3)
            .measurementTime(new TimeValue(1, TimeUnit.SECONDS))
            .measurementIterations(3)
            .forks(1)
            .build();

    new Runner(opts).run();
  }
}
