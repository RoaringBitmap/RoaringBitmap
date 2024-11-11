package org.roaringbitmap.realdata;

import static org.roaringbitmap.Util.toUnsignedLong;

import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.realdata.state.RealDataRoaringBitmaps;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class RealDataBenchmarkOrNot {

  @Benchmark
  public void pairwiseOrNot(RealDataRoaringBitmaps state, Blackhole bh) {
    RoaringBitmap[] bitmaps = state.getBitmaps();
    for (int k = 0; k + 1 < bitmaps.length; ++k) {
      RoaringBitmap bitmap = bitmaps[k].clone();
      bitmap.orNot(bitmaps[k + 1], bitmap.last());
      bh.consume(
          RoaringBitmap.orNot(bitmaps[k], bitmaps[k + 1], toUnsignedLong(bitmaps[k].last())));
    }
  }

  @Benchmark
  public void pairwiseOrNotExternal(RealDataRoaringBitmaps state, Blackhole bh) {
    RoaringBitmap[] bitmaps = state.getBitmaps();
    for (int k = 0; k + 1 < bitmaps.length; ++k) {
      long limit = toUnsignedLong(bitmaps[k].last());
      RoaringBitmap range = new RoaringBitmap();
      range.add(0, limit);
      RoaringBitmap bitmap = RoaringBitmap.and(range, bitmaps[k + 1]);
      bitmap.flip(0L, limit);
      bitmap.or(RoaringBitmap.and(range, bitmaps[k]));
      bh.consume(bitmap);
    }
  }
}
