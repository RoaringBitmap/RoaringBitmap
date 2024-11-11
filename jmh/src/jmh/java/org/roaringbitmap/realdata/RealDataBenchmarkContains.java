package org.roaringbitmap.realdata;

import org.roaringbitmap.realdata.state.RealDataBenchmarkState;
import org.roaringbitmap.realdata.wrapper.Bitmap;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class RealDataBenchmarkContains {

  @Benchmark
  public void contains(BenchmarkState bs, Blackhole bh) {
    for (int k = 0; k < bs.bitmaps.size(); ++k) {
      Bitmap bitmap = bs.bitmaps.get(k);
      int range = bs.ranges.get(k);
      bh.consume(bitmap.contains((int) (range * 1. / 4)));
      bh.consume(bitmap.contains((int) (range * 2. / 4)));
      bh.consume(bitmap.contains((int) (range * 3. / 4)));
    }
  }

  @State(Scope.Benchmark)
  public static class BenchmarkState extends RealDataBenchmarkState {

    List<Integer> ranges;

    public BenchmarkState() {}

    @Override
    @Setup
    public void setup() throws Exception {
      super.setup();

      ranges = new ArrayList<Integer>();
      for (Bitmap bitmap : bitmaps) {
        ranges.add(bitmap.last());
      }
    }
  }
}
