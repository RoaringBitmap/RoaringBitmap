package org.roaringbitmap.longlong;

import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class AddOffset64Benchmark {
  @Param({"array", "bitmap", "run"})
  public String encoding;

  @Param({"1", "65536", "-1"})
  public long offset;

  private Roaring64Bitmap source;

  @Setup
  public void setup() {
    source = new Roaring64Bitmap();
    long base = 1L << 40;
    for (int block = 0; block < 16; block++) {
      if (encoding.equals("run")) {
        source.addRange(base + block * 65536L, base + (block + 1) * 65536L);
      } else {
        int size = encoding.equals("array") ? 1000 : 10000;
        for (int i = 0; i < size; i++) {
          source.addLong(base + block * 65536L + 2L * i);
        }
      }
    }
    if (encoding.equals("run")) {
      source.runOptimize();
    }
  }

  @Benchmark
  public Roaring64Bitmap compressed() {
    return Roaring64Bitmap.addOffset(source, offset);
  }

  @Benchmark
  public Roaring64Bitmap perValue() {
    Roaring64Bitmap result = new Roaring64Bitmap();
    LongIterator iterator = source.getLongIterator();
    while (iterator.hasNext()) {
      long value = iterator.next();
      long shifted = value + offset;
      if ((offset >= 0 && Long.compareUnsigned(shifted, value) >= 0)
          || (offset < 0 && Long.compareUnsigned(shifted, value) <= 0)) {
        result.addLong(shifted);
      }
    }
    return result;
  }
}
