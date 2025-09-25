package org.roaringbitmap;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 3, timeUnit = TimeUnit.MILLISECONDS, time = 1000)
@Measurement(iterations = 5, timeUnit = TimeUnit.MILLISECONDS, time = 1000)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1)
public class RangeCardinalityBenchmark {

  public RoaringBitmap bitmap = new RoaringBitmap();
  public static int key = 0;

  @Setup(Level.Trial)
  public void prepareBitmap() {
    // many containers, but container with key = 0 is missing
    for (int i = 1; i < 1000; i++) {
      bitmap.add(i * 65537);
    }
  }

  @Benchmark
  public void loopImmediatelyEscaped(Blackhole blackhole) {
    blackhole.consume(bitmap.rangeCardinality(0, 1L + (key << 16)));
  }

  @Benchmark
  public void wholeLoopProcessed(Blackhole blackhole) {
    blackhole.consume(rangeCardinalityOriginal(bitmap, 0, 1L + (key << 16)));
  }

  public long rangeCardinalityOriginal(RoaringBitmap bitmap, long start, long end) {
    if (Long.compareUnsigned(start, end) >= 0) {
      return 0;
    }
    long size = 0;
    int startIndex = bitmap.highLowContainer.getIndex(Util.highbits(start));
    if (startIndex < 0) {
      startIndex = -startIndex - 1;
    } else {
      int inContainerStart = (Util.lowbits(start));
      if (inContainerStart != 0) {
        size -=
            bitmap.highLowContainer
                .getContainerAtIndex(startIndex)
                .rank((char) (inContainerStart - 1));
      }
    }
    char xhigh = Util.highbits(end - 1);
    for (int i = startIndex; i < bitmap.highLowContainer.size(); i++) {
      char key = bitmap.highLowContainer.getKeyAtIndex(i);
      if (key < xhigh) {
        size += bitmap.highLowContainer.getContainerAtIndex(i).getCardinality();
      } else if (key == xhigh) {
        return size
            + bitmap.highLowContainer.getContainerAtIndex(i).rank(Util.lowbits((int) (end - 1)));
      }
    }
    return size;
  }
}
