package org.roaringbitmap.aggregation;

import org.openjdk.jmh.annotations.*;
import org.roaringbitmap.FastAggregation;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.RoaringBitmapWriter;
import org.roaringbitmap.buffer.BufferFastAggregation;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

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
          return RoaringBitmapWriter.writer()
                  .constantMemory()
                  .get();
        }
      },
      CONTAINER_APPENDER {
        @Override
        public RoaringBitmapWriter<RoaringBitmap> create() {
          return RoaringBitmapWriter.writer()
                  .get();
        }
      }
      ;
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

    @Param
    ConstructionStrategy constructionStrategy;

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
      double p = Math.pow(probability, 1D/count);
      for (int i = 0; i < count; ++i) {
        for (int j = (int)(Math.log(random.nextDouble())/Math.log(1-p));
             j < size;
             j += (int)(Math.log(random.nextDouble())/Math.log(1-p)) + 1) {
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
    Arrays.fill(state.buffer,0);
    return FastAggregation.workAndMemoryShyAnd(state.buffer, state.bitmaps);
  }

  @Benchmark
  public ImmutableRoaringBitmap andBufferMemoryShy(BitmapState state) {
    Arrays.fill(state.buffer,0);
    return BufferFastAggregation.workAndMemoryShyAnd(state.buffer, state.bufferBitmaps);
  }

}
