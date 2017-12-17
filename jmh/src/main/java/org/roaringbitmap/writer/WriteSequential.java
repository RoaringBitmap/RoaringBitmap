package org.roaringbitmap.writer;


import org.openjdk.jmh.annotations.*;
import org.roaringbitmap.OrderedWriter;
import org.roaringbitmap.RoaringBitmap;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class WriteSequential {

  @Param({"10000", "100000", "1000000", "10000000"})
  int size;

  @Param({"0.1", "0.5", "0.9"})
  double randomness;

  int[] data;

  @Setup(Level.Trial)
  public void init() {
    data = generateArray(1D - randomness);
  }

  private int[] generateArray(double runThreshold) {
    Random random = new Random();
    int[] data = new int[size];
    int last = 0;
    int i = 0;
    while (i < size) {
      if (random.nextGaussian() > runThreshold) {
        int runLength = random.nextInt(Math.min(size - i, 1 << 16));
        for (int j = 1; j < runLength; ++j) {
          data[i + j] = last + 1;
          last = data[i + j];
        }
        i += runLength;
      } else {
        data[i] = last + 1 + random.nextInt(999);
        last = data[i];
        ++i;
      }
    }
    Arrays.sort(data);
    return data;
  }

  @Benchmark
  public RoaringBitmap buildRoaringBitmap() {
    return RoaringBitmap.bitmapOf(data);
  }

  @Benchmark
  public RoaringBitmap incrementalNativeAdd() {
    RoaringBitmap bitmap = new RoaringBitmap();
    for (int i = 0; i < data.length; ++i) {
      bitmap.add(data[i]);
    }
    return bitmap;
  }

  @Benchmark
  public RoaringBitmap incrementalUseOrderedWriter() {
    RoaringBitmap bitmap = new RoaringBitmap();
    OrderedWriter writer = new OrderedWriter(bitmap);
    for (int i = 0; i < data.length; ++i) {
      writer.add(data[i]);
    }
    writer.flush();
    return bitmap;
  }
}
