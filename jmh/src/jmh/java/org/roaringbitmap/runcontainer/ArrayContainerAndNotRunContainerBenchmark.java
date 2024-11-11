package org.roaringbitmap.runcontainer;

import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.realdata.state.RealDataRoaringOnlyBenchmarkState;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;

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

  @Benchmark
  public ImmutableRoaringBitmap immutablePairwiseACAndNotRC(RealDataRoaringOnlyBenchmarkState bs) {
    ImmutableRoaringBitmap last = null;
    for (int k = 0; k + 1 < bs.immutableBitmaps.size(); ++k) {
      last =
          ImmutableRoaringBitmap.andNot(
              bs.immutableOnlyArrayContainers.get(k), bs.immutableOnlyRunContainers.get(k + 1));
    }
    return last;
  }
}
