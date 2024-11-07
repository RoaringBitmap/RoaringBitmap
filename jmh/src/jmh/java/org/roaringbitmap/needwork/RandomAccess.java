package org.roaringbitmap.needwork;

import org.roaringbitmap.needwork.state.NeedWorkBenchmarkState;
import org.roaringbitmap.realdata.wrapper.Bitmap;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class RandomAccess {

  @Benchmark
  public void binarySearch(BenchmarkState bs, Blackhole bh) {
    for (int k : bs.queries) {
      for (Bitmap bitmap : bs.bitmaps) {
        bh.consume(bitmap.contains(k));
      }
    }
  }

  @State(Scope.Benchmark)
  public static class BenchmarkState extends NeedWorkBenchmarkState {

    int[] queries = new int[1024];

    public BenchmarkState() {}

    @Override
    @Setup
    public void setup() throws Exception {
      super.setup();

      int universe = 0;
      for (Bitmap bitmap : bitmaps) {
        int lv = bitmap.last();
        if (lv > universe) universe = lv;
      }
      Random rand = new Random(123);
      for (int k = 0; k < queries.length; ++k) queries[k] = rand.nextInt(universe + 1);
    }
  }
}
