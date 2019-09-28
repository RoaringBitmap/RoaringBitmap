package org.roaringbitmap.first;

import org.openjdk.jmh.annotations.*;
import org.roaringbitmap.BitmapContainer;

import java.util.concurrent.TimeUnit;

@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.Throughput)
public class BitmapContainerFirst {

  @State(Scope.Benchmark)
  public static class BitmapState {

    @Param({"10", "1000", "32000", "50000"})
    int firstBit;

    BitmapContainer bitmap;

    @Setup(Level.Trial)
    public void init() {
      long[] bitmap = new long[1024];
      bitmap[firstBit >>> 6] |= (1L << firstBit);
      this.bitmap = new BitmapContainer(bitmap, 1);
    }
  }

  @Benchmark
  public int first(BitmapState state) {
    return state.bitmap.first();
  }
}
