package org.roaringbitmap;

import org.openjdk.jmh.annotations.*;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 3, timeUnit = TimeUnit.MILLISECONDS, time = 1000)
@Measurement(iterations = 5, timeUnit = TimeUnit.MILLISECONDS, time = 1000)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class SelectTopValuesBenchmark {

  @Param({"200"})
  int n;

  @Param({"10000"})
  public int count;

  public MutableRoaringBitmap bitmap;

  @Setup(Level.Iteration)
  public void setup() {
    bitmap = new MutableRoaringBitmap();
    for (int i = 0; i < count; i++) {
      bitmap.add(i * 100);
    }
  }

  @Benchmark
  public MutableRoaringBitmap limitIncludingAndNot() {
    MutableRoaringBitmap turnoff = bitmap.limit(n);
    bitmap.andNot(turnoff);
    return bitmap;
  }

  @Benchmark
  public MutableRoaringBitmap limit() {
    return bitmap.limit(n);
  }


  @Benchmark
  public MutableRoaringBitmap add() {
    IntIterator it = bitmap.getIntIterator();
    MutableRoaringBitmap turnoff = new MutableRoaringBitmap();
    int i = n;
    while (it.hasNext() && i > 0) {
      turnoff.add(it.next());
      i--;
    }
    return bitmap;
  }

  @Benchmark
  public MutableRoaringBitmap addIncludingAndNot() {
    IntIterator it = bitmap.getIntIterator();
    MutableRoaringBitmap turnoff = new MutableRoaringBitmap();
    int i = n;
    while (it.hasNext() && i > 0) {
      turnoff.add(it.next());
      i--;
    }
    bitmap.andNot(turnoff);
    return bitmap;
  }

  @Benchmark
  public MutableRoaringBitmap remove() {
    IntIterator it = bitmap.getIntIterator();
    int i = n;
    while (it.hasNext() && i > 0) {
      bitmap.remove(it.next());
      i--;
    }
    return bitmap;
  }

  @Benchmark
  public MutableRoaringBitmap batchIterator() {
    BatchIterator it = bitmap.getBatchIterator();
    int[] buffer = new int[n];
    MutableRoaringBitmap turnoff = new MutableRoaringBitmap();
    it.nextBatch(buffer);
    for (int i = 0; i < n; i++) {
      turnoff.add(buffer[i]);
    }
    return turnoff;
  }

  @Benchmark
  public MutableRoaringBitmap batchIteratorIncludingAndNot() {
    BatchIterator it = bitmap.getBatchIterator();
    int[] buffer = new int[n];
    MutableRoaringBitmap turnoff = new MutableRoaringBitmap();
    it.nextBatch(buffer);
    for (int i = 0; i < n; i++) {
      turnoff.add(buffer[i]);
    }
    bitmap.andNot(turnoff);
    return bitmap;
  }

  @Benchmark
  public MutableRoaringBitmap batchIteratorAddAtOnce() {
    BatchIterator it = bitmap.getBatchIterator();
    int[] buffer = new int[n];
    MutableRoaringBitmap turnoff = new MutableRoaringBitmap();
    it.nextBatch(buffer);
    turnoff.add(buffer);
    return turnoff;
  }

  @Benchmark
  public MutableRoaringBitmap batchIteratorAddAtOnceIncludingAndNot() {
    BatchIterator it = bitmap.getBatchIterator();
    int[] buffer = new int[n];
    MutableRoaringBitmap turnoff = new MutableRoaringBitmap();
    it.nextBatch(buffer);
    turnoff.add(buffer);
    bitmap.andNot(turnoff);
    return bitmap;
  }
}