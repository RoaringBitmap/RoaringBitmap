package org.roaringbitmap.runcontainer;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.RoaringOnlyBenchmarkState;
import org.roaringbitmap.realdata.state.RealDataRoaringOnlyBenchmarkState;

@BenchmarkMode(Mode.Throughput)
public class ArrayContainerAndNotRunContainerBenchmark {

  @Benchmark
  public RoaringBitmap pairwiseACAndNotRC(RealDataRoaringOnlyBenchmarkState bs) {
    RoaringBitmap last = null;
    for (int k = 0; k + 1 < bs.bitmaps.size(); ++k) {
      last = RoaringBitmap.andNot(bs.onlyArrayContainers.get(k), bs.onlyRunContainers.get(k + 1));
    }
    return last;
  }
}
