package org.roaringbitmap.realdata;

import org.roaringbitmap.realdata.state.RealDataBenchmarkState;
import org.roaringbitmap.realdata.wrapper.BitmapIterator;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class RealDataBenchmarkOr {

  @Benchmark
  public int pairwiseOr(RealDataBenchmarkState bs) {
    int total = 0;
    for (int k = 0; k + 1 < bs.bitmaps.size(); ++k) {
      total += bs.bitmaps.get(k).or(bs.bitmaps.get(k + 1)).cardinality();
    }
    return total;
  }

  @Benchmark
  public int pairwiseOr_NoCardinality(RealDataBenchmarkState bs) {
    int total = 0;
    for (int k = 0; k + 1 < bs.bitmaps.size(); ++k) {
      BitmapIterator i = bs.bitmaps.get(k).or(bs.bitmaps.get(k + 1)).iterator();
      if (i.hasNext()) {
        total += i.next();
      }
    }
    return total;
  }
}
