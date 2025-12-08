package org.roaringbitmap;

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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark comparing rank performance between SuccinctRank and FastRankRoaringBitmap.
 */
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@Threads(1)
@State(Scope.Benchmark)
public class RankBenchmark {

  private static final int LOOKUP_COUNT = 1000;

  @Param({"1000", "10000", "100000", "1000000"})
  private int size;

  @Param({"DENSE", "SPARSE", "RANDOM"})
  private DataDistribution distribution;

  private RoaringBitmap bitmap;
  private SuccinctRank succinctRank;
  private FastRankRoaringBitmap fastRankBitmap;
  private int[] lookupValues;
  private int[] missingValues;

  @Setup
  public void setup() {
    this.bitmap = distribution.createBitmap(size);
    this.succinctRank = SuccinctRank.build(bitmap);

    // Create FastRankRoaringBitmap with same data
    this.fastRankBitmap = new FastRankRoaringBitmap();
    for (int value : bitmap) {
      this.fastRankBitmap.add(value);
    }

    // Pre-generate lookup values for consistent benchmarking
    this.lookupValues = generateLookupValues(bitmap, LOOKUP_COUNT);
    this.missingValues = generateMissingValues(bitmap, LOOKUP_COUNT);
  }

  private int[] generateLookupValues(RoaringBitmap bitmap, int count) {
    int[] values = new int[count];
    Random random = new Random(12345L);
    int bitmapSize = bitmap.getCardinality();

    for (int i = 0; i < count; i++) {
      int index = random.nextInt(bitmapSize);
      values[i] = bitmap.select(index);
    }
    return values;
  }

  private int[] generateMissingValues(RoaringBitmap bitmap, int count) {
    int[] values = new int[count];
    Random random = new Random(54321L);

    // Find the max value in the bitmap to generate values beyond it
    int maxValue = bitmap.isEmpty() ? 0 : bitmap.last();

    // Generate values that are guaranteed to be missing by using values beyond max
    for (int i = 0; i < count; i++) {
      // Generate values starting beyond the max value in the bitmap
      // Add some randomness but ensure they're all missing
      values[i] = maxValue + 1000 + random.nextInt(Integer.MAX_VALUE - maxValue - 1000);
    }
    return values;
  }

  // ==================== Rank Benchmarks - Hit ====================

  @Benchmark
  public void succinctRankLookupHit(Blackhole bh) {
    for (int value : lookupValues) {
      bh.consume(succinctRank.rank(value));
    }
  }

  @Benchmark
  public void fastRankLookupHit(Blackhole bh) {
    for (int value : lookupValues) {
      bh.consume(fastRankBitmap.rankLong(value));
    }
  }

  @Benchmark
  public void roaringBitmapRankLookupHit(Blackhole bh) {
    for (int value : lookupValues) {
      bh.consume(bitmap.rankLong(value));
    }
  }

  // ==================== Rank Benchmarks - Miss ====================

  @Benchmark
  public void succinctRankLookupMiss(Blackhole bh) {
    for (int value : missingValues) {
      bh.consume(succinctRank.rank(value));
    }
  }

  @Benchmark
  public void fastRankLookupMiss(Blackhole bh) {
    for (int value : missingValues) {
      bh.consume(fastRankBitmap.rankLong(value));
    }
  }

  @Benchmark
  public void roaringBitmapRankLookupMiss(Blackhole bh) {
    for (int value : missingValues) {
      bh.consume(bitmap.rankLong(value));
    }
  }

  // ==================== Creation Benchmarks ====================

  @Benchmark
  public void createSuccinctRank(Blackhole bh) {
    bh.consume(SuccinctRank.build(bitmap));
  }

  @Benchmark
  public void createFastRankBitmap(Blackhole bh) {
    FastRankRoaringBitmap frb = new FastRankRoaringBitmap();
    for (int value : bitmap) {
      frb.add(value);
    }
    bh.consume(frb);
  }

  /**
   * Data distribution patterns for benchmark testing.
   */
  public enum DataDistribution {
    /**
     * Dense sequential values (0, 1, 2, ..., n-1).
     * Best case for RoaringBitmap compression.
     */
    DENSE {
      @Override
      RoaringBitmap createBitmap(int size) {
        RoaringBitmap bitmap = new RoaringBitmap();
        for (int i = 0; i < size; i++) {
          bitmap.add(i);
        }
        return bitmap;
      }
    },

    /**
     * Sparse values with gaps (every 100th value).
     * Tests performance with multiple containers.
     */
    SPARSE {
      @Override
      RoaringBitmap createBitmap(int size) {
        RoaringBitmap bitmap = new RoaringBitmap();
        for (int i = 0; i < size; i++) {
          bitmap.add(i * 100);
        }
        return bitmap;
      }
    },

    /**
     * Random values spread across the integer range.
     * Tests worst-case container distribution.
     */
    RANDOM {
      @Override
      RoaringBitmap createBitmap(int size) {
        RoaringBitmap bitmap = new RoaringBitmap();
        Random random = new Random(12345L);
        for (int i = 0; i < size; i++) {
          bitmap.add(random.nextInt(Integer.MAX_VALUE));
        }
        return bitmap;
      }
    };

    abstract RoaringBitmap createBitmap(int size);
  }
}
