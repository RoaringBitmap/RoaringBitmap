package org.roaringbitmap.realdata;

import org.roaringbitmap.realdata.state.RealDataBenchmarkState;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class RealDataBenchmarkInot {

  @Benchmark
  public int flipLargeRange(RealDataBenchmarkState bs) {
    int total = 0;
    for (int k = 0; k < bs.bitmaps.size(); ++k) {
      total += bs.bitmaps.get(k).flip(30000, 20 * 1000 * 1000).cardinality();
    }
    return total;
  }
}
