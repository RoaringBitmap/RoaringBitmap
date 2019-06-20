package org.roaringbitmap.spe150271.runroaring;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.roaringbitmap.spe150271.runroaring.state.RealDataBenchmarkState;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class RealDataBenchmarkAnd {

  @Benchmark
  public int pairwiseAnd(RealDataBenchmarkState bs) {
    int total = 0;
    for (int k = 0; k + 1 < bs.bitmaps.size(); ++k) {
      total += bs.bitmaps.get(k).and(bs.bitmaps.get(k + 1)).cardinality();
    }
    return total;
  }

}
