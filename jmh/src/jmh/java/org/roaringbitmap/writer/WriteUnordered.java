package org.roaringbitmap.writer;

import org.openjdk.jmh.annotations.*;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.Util;

import java.util.*;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class WriteUnordered {

  @Param({"10000", "100000", "1000000", "10000000"})
  int size;
  @Param({"0.1", "0.5", "0.9"})
  double randomness;

  int[] data;

  @Setup(Level.Trial)
  public void init() {
    data = generateArray();
  }

  private int[] generateArray() {
    Random random = new Random();
    List<Integer> ints = new ArrayList<>(size);
    int last = 0;
    for (int i = 0; i < size; ++i) {
      if (random.nextGaussian() > 1 - randomness) {
        last = last + 1;
      } else {
        last = last + 1 + random.nextInt(99);
      }
      ints.add(last);
    }
    Collections.shuffle(ints);
    int[] data = new int[size];
    int i = 0;
    for (Integer value : ints) {
      data[i++] = value;
    }
    return data;
  }

  @Benchmark
  public int[] setupCost() {
    return Arrays.copyOf(data, data.length);
  }

  @Benchmark
  public RoaringBitmap bitmapOfUnordered() {
    int[] copy = Arrays.copyOf(data, data.length);
    return RoaringBitmap.bitmapOfUnordered(copy);
  }

  @Benchmark
  public RoaringBitmap sortThenBitmapOf() {
    int[] copy = Arrays.copyOf(data, data.length);
    Arrays.sort(copy);
    return RoaringBitmap.bitmapOf(copy);
  }

  @Benchmark
  public RoaringBitmap bitmapOf() {
    int[] copy = Arrays.copyOf(data, data.length);
    return RoaringBitmap.bitmapOf(copy);
  }

  @Benchmark
  public RoaringBitmap partialSortThenBitmapOf() {
    int[] copy = Arrays.copyOf(data, data.length);
    Util.partialRadixSort(copy);
    return RoaringBitmap.bitmapOf(copy);
  }
}

