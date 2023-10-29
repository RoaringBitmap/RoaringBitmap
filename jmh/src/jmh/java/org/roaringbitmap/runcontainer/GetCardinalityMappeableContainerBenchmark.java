package org.roaringbitmap.runcontainer;

import org.openjdk.jmh.annotations.*;
import org.roaringbitmap.buffer.MappeableContainer;
import org.roaringbitmap.buffer.MappeableRunContainer;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
public class GetCardinalityMappeableContainerBenchmark {

  @Benchmark
  public int getCardinality_negatedUniformHash(BenchmarkState state) {
    return state.mc1.getCardinality();
  }

  @Benchmark
  public int getCardinality_uniformHash1(BenchmarkState state) {
    return state.mc2.getCardinality();
  }

  @Benchmark
  public int getCardinality_uniformHash2(BenchmarkState state) {
    return state.mc3.getCardinality();
  }

  @Benchmark
  public int getCardinality_crazyRun(BenchmarkState state) {
    return state.mc4.getCardinality();
  }

  @State(Scope.Benchmark)
  public static class BenchmarkState {
    private static final int OFF_VALUES = 32;
    private static final int BIT_SET_PER_WORD2 = 63;
    private static final int BIT_SET_PER_WORD3 = 1;
    MappeableContainer mc1, mc2, mc3, mc4;

    public BenchmarkState() {
      Random rand = new Random();
      final int max = 1 << 16;
      final int howManyWords = (1 << 16) / 64;
      int[] values1 = RandomUtil.negate(RandomUtil.generateUniformHash(rand, OFF_VALUES, max), max);
      int[] values2 = RandomUtil.generateUniformHash(rand, BIT_SET_PER_WORD2 * howManyWords, max);
      int[] values3 = RandomUtil.generateUniformHash(rand, BIT_SET_PER_WORD3 * howManyWords, max);
      int[] values4 = RandomUtil.generateCrazyRun(rand, max);

      mc1 = new MappeableRunContainer();
      mc2 = new MappeableRunContainer();
      mc3 = new MappeableRunContainer();
      mc4 = new MappeableRunContainer();

      for (int i : values1) { mc1.add((char) i); }
      for (int i : values2) { mc2.add((char) i); }
      for (int i : values3) { mc3.add((char) i); }
      for (int i : values4) { mc4.add((char) i); }
    }
  }
}