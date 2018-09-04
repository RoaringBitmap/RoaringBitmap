package org.roaringbitmap.writer;


import org.openjdk.jmh.annotations.*;
import org.roaringbitmap.*;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class WriteSequential {

  @Param({"100", "1000", "10000", "100000", "1000000", "10000000"})
  int size;

  @Param({"0.1", "0.5", "0.9"})
  double randomness;

  @Param({"DENSE", "SPARSE", "ADAPTIVE", "UNOPTIMIZED"})
  String writerType;

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
        for (int j = 0; j < runLength; ++j) {
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
    OrderedWriter writer = createWriter(bitmap);
    for (int i = 0; i < data.length; ++i) {
      writer.add(data[i]);
    }
    writer.flush();
    return bitmap;
  }

  private OrderedWriter createWriter(final RoaringBitmap roaringBitmap) {
    switch (writerType) {
      case "SPARSE":
        return new SparseOrderedWriter(roaringBitmap);
      case "DENSE":
        return new DenseOrderedWriter(roaringBitmap);
      case "ADAPTIVE":
        return new AdaptiveOrderedWriter(roaringBitmap);
      case "UNOPTIMIZED":
        return new UnoptimizedOrderedWriter(roaringBitmap);
      default:
        throw new IllegalStateException("Unknown OrderedWriter implementation: " + writerType);
    }
  }

  class UnoptimizedOrderedWriter implements OrderedWriter {

    private final RoaringBitmap roaringBitmap;

    UnoptimizedOrderedWriter(RoaringBitmap roaringBitmap) {
      this.roaringBitmap = roaringBitmap;
    }

    @Override
    public void add(int value) {
      this.roaringBitmap.add(value);
    }

    @Override
    public void flush() {

    }

  }
}
