package org.roaringbitmap.writer;

import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.RoaringBitmapWriter;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class WriteSequential {

  public enum Scenario {
    DENSE {
      @Override
      RoaringBitmapWriter<RoaringBitmap> newWriter() {
        return RoaringBitmapWriter.writer().constantMemory().get();
      }
    },
    SPARSE {
      @Override
      RoaringBitmapWriter<RoaringBitmap> newWriter() {
        return RoaringBitmapWriter.writer().optimiseForArrays().get();
      }
    };

    abstract RoaringBitmapWriter<RoaringBitmap> newWriter();
  }

  @Param({"100", "1000", "10000", "100000", "1000000", "10000000"})
  int size;

  @Param({"0.1", "0.5", "0.9"})
  double randomness;

  @Param({"DENSE", "SPARSE"})
  Scenario scenario;

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
    RoaringBitmapWriter<RoaringBitmap> writer = scenario.newWriter();
    for (int i = 0; i < data.length; ++i) {
      writer.add(data[i]);
    }
    writer.flush();
    return writer.getUnderlying();
  }
}
