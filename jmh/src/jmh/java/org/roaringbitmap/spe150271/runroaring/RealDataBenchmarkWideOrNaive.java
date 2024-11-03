package org.roaringbitmap.spe150271.runroaring;

import org.roaringbitmap.realdata.wrapper.BitmapAggregator;
import org.roaringbitmap.spe150271.runroaring.state.RealDataBenchmarkState;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class RealDataBenchmarkWideOrNaive {

  @Benchmark
  public int wideOr_naive(RealDataBenchmarkState bs) {
    BitmapAggregator aggregator = bs.bitmaps.get(0).naiveOrAggregator();
    return aggregator.aggregate(bs.bitmaps).cardinality();
  }
}
