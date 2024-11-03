package org.roaringbitmap.realdata;

import org.roaringbitmap.realdata.state.RealDataBenchmarkState;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class RealDataBenchmarkXor {

  @Benchmark
  public int pairwiseXor(RealDataBenchmarkState bs) {
    int total = 0;
    for (int k = 0; k + 1 < bs.bitmaps.size(); ++k) {
      total += bs.bitmaps.get(k).xor(bs.bitmaps.get(k + 1)).cardinality();
    }
    return total;
  }
}
