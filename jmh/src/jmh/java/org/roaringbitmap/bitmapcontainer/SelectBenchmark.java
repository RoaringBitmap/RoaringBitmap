package org.roaringbitmap.bitmapcontainer;

import org.openjdk.jmh.annotations.*;
import org.roaringbitmap.BitmapContainer;
import org.roaringbitmap.buffer.MappeableBitmapContainer;

import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 10, timeUnit = TimeUnit.MILLISECONDS, time = 1000)
@Measurement(iterations = 20, timeUnit = TimeUnit.MILLISECONDS, time = 1000)
public class SelectBenchmark {

  private BitmapContainer bc1;
  private MappeableBitmapContainer mbc1;

  @Param({"100", "5000", "10000"})
  public int valuesCount;

  public char[] indexes;

  @Setup
  public void setup() throws ExecutionException {
    bc1 = new BitmapContainer();
    mbc1 = new MappeableBitmapContainer();
    indexes = new char[valuesCount];
    Random r = new Random(123);
    for (int i = 0; i < valuesCount; i++) {
      char value = (char) r.nextInt(Character.MAX_VALUE);
      bc1.add(value);
      mbc1.add(value);
    }
    int actualCardinality = bc1.getCardinality(); // some values can be inserted multiple times
    for (int i = 0; i < actualCardinality; i++) {
      indexes[i] = (char) r.nextInt(actualCardinality);
    }
  }

  @Benchmark
  public long bitmapContainer_selectOneSide() {
    long accumulator = 0;
    for (int i = 0; i < bc1.getCardinality(); i++) {
      accumulator += bc1.selectOneSide(indexes[i]);
    }
    return accumulator;
  }

  @Benchmark
  public long bitmapContainer_selectBothSides() {
    long accumulator = 0;
    for (int i = 0; i < bc1.getCardinality(); i++) {
      accumulator += bc1.select(indexes[i]);
    }
    return accumulator;
  }

  @Benchmark
  public long mappeableBitmapContainer_selectOneSide() {
    long accumulator = 0;
    for (int i = 0; i < mbc1.getCardinality(); i++) {
      accumulator += mbc1.selectOneSide(indexes[i]);
    }
    return accumulator;
  }

  @Benchmark
  public long mappeableBitmapContainer_selectBothSides() {
    long accumulator = 0;
    for (int i = 0; i < mbc1.getCardinality(); i++) {
      accumulator += mbc1.select(indexes[i]);
    }
    return accumulator;
  }
}
