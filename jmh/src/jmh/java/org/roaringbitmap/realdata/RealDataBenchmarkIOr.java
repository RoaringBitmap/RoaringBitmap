package org.roaringbitmap.realdata;

import org.roaringbitmap.realdata.state.RealDataBenchmarkState;
import org.roaringbitmap.realdata.wrapper.Bitmap;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class RealDataBenchmarkIOr {

  @Benchmark
  public int pairwiseIOr(RealDataBenchmarkState bs) {
    Bitmap bitmap = bs.bitmaps.get(0).clone();
    for (int k = 1; k < bs.bitmaps.size(); ++k) {
      bitmap.ior(bs.bitmaps.get(k));
    }
    return bitmap.cardinality();
  }
}
