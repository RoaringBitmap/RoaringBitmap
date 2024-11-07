package org.roaringbitmap.needwork;

import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.ZipRealDataRetriever;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class SlowORaggregate1 {

  @Benchmark
  public RoaringBitmap RoaringWithRun(BenchmarkState benchmarkState) {
    RoaringBitmap answer = RoaringBitmap.or(benchmarkState.rc.iterator());
    return answer;
  }

  @State(Scope.Benchmark)
  public static class BenchmarkState {
    @Param({ // putting the data sets in alpha. order
      "wikileaks-noquotes",
    })
    String dataset;

    ArrayList<RoaringBitmap> rc = new ArrayList<RoaringBitmap>();

    public BenchmarkState() {}

    @Setup
    public void setup() throws Exception {
      ZipRealDataRetriever dataRetriever = new ZipRealDataRetriever(dataset);
      System.out.println();
      System.out.println("Loading files from " + dataRetriever.getName());

      for (int[] data : dataRetriever.fetchBitPositions()) {
        RoaringBitmap basic = RoaringBitmap.bitmapOf(data);
        basic.runOptimize();
        rc.add(basic);
      }
      System.out.println("loaded " + rc.size() + " bitmaps");
    }
  }
}
