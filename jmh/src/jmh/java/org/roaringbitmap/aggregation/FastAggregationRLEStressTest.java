package org.roaringbitmap.aggregation;

import org.roaringbitmap.FastAggregation;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.RoaringBitmapWriter;
import org.roaringbitmap.buffer.BufferFastAggregation;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.Arrays;
import java.util.SplittableRandom;

public class FastAggregationRLEStressTest {

  @State(Scope.Benchmark)
  public static class BitmapState {
    public long[] buffer = new long[1024];

    public enum ConstructionStrategy {
      CONSTANT_MEMORY {
        @Override
        public RoaringBitmapWriter<RoaringBitmap> create() {
          return RoaringBitmapWriter.writer().constantMemory().get();
        }
      },
      CONTAINER_APPENDER {
        @Override
        public RoaringBitmapWriter<RoaringBitmap> create() {
          return RoaringBitmapWriter.writer().get();
        }
      };

      public abstract RoaringBitmapWriter<RoaringBitmap> create();
    }

    // adapted from druid
    // https://github.com/apache/druid/blob/master/benchmarks/src/test/java/org/apache/druid/benchmark/BitmapIterationBenchmark.java

    @Param({"10", "100"})
    int count;

    @Param({"1000000"})
    int size;

    @Param({"0.01", "0.1", "0.5"})
    double probability;

    @Param ConstructionStrategy constructionStrategy;

    @Param("99999")
    long seed;

    SplittableRandom random;
    RoaringBitmap[] bitmaps;
    ImmutableRoaringBitmap[] bufferBitmaps;

    @Setup(Level.Trial)
    public void createBitmaps() {
      random = new SplittableRandom(seed);
      RoaringBitmapWriter<RoaringBitmap> bitmapWriter = constructionStrategy.create();
      bitmaps = new RoaringBitmap[count];
      bufferBitmaps = new ImmutableRoaringBitmap[count];
      double p = Math.pow(probability, 1D / count);
      for (int i = 0; i < count; ++i) {
        for (int j = (int) (Math.log(random.nextDouble()) / Math.log(1 - p));
            j < size;
            j += (int) (Math.log(random.nextDouble()) / Math.log(1 - p)) + 1) {
          bitmapWriter.add(j);
        }
        bitmaps[i] = bitmapWriter.get();
        bufferBitmaps[i] = bitmaps[i].toMutableRoaringBitmap();
        bitmapWriter.reset();
      }
    }
  }

  @Benchmark
  public RoaringBitmap and(BitmapState state) {
    return FastAggregation.and(state.buffer, state.bitmaps);
  }

  @Benchmark
  public ImmutableRoaringBitmap andBuffer(BitmapState state) {
    return BufferFastAggregation.and(state.buffer, state.bufferBitmaps);
  }

  @Benchmark
  public RoaringBitmap andMemoryShy(BitmapState state) {
    Arrays.fill(state.buffer, 0);
    return FastAggregation.workAndMemoryShyAnd(state.buffer, state.bitmaps);
  }

  @Benchmark
  public ImmutableRoaringBitmap andBufferMemoryShy(BitmapState state) {
    Arrays.fill(state.buffer, 0);
    return BufferFastAggregation.workAndMemoryShyAnd(state.buffer, state.bufferBitmaps);
  }

  @Benchmark
  public int andCardinality(BitmapState state) {
    return FastAggregation.andCardinality(state.bitmaps);
  }

  @Benchmark
  public int andCardinalityMaterialize(BitmapState state) {
    return FastAggregation.and(state.bitmaps).getCardinality();
  }

  @Benchmark
  public int andCardinalityBuffer(BitmapState state) {
    return BufferFastAggregation.andCardinality(state.bufferBitmaps);
  }

  @Benchmark
  public int andCardinalityBufferMaterialize(BitmapState state) {
    return BufferFastAggregation.and(state.bufferBitmaps).getCardinality();
  }

  @Benchmark
  public int orCardinality(BitmapState state) {
    return FastAggregation.orCardinality(state.bitmaps);
  }

  @Benchmark
  public int orCardinalityMaterialize(BitmapState state) {
    return FastAggregation.or(state.bitmaps).getCardinality();
  }

  @Benchmark
  public int orCardinalityBuffer(BitmapState state) {
    return BufferFastAggregation.orCardinality(state.bufferBitmaps);
  }

  @Benchmark
  public int orCardinalityBufferMaterialize(BitmapState state) {
    return BufferFastAggregation.or(state.bufferBitmaps).getCardinality();
  }
}
