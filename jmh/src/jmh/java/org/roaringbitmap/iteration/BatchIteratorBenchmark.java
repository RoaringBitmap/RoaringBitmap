package org.roaringbitmap.iteration;

import org.roaringbitmap.IntIterator;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RandomData;
import org.roaringbitmap.RoaringBatchIterator;
import org.roaringbitmap.RoaringBitmap;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class BatchIteratorBenchmark {

  @Param({"64", "128", "256", "512"})
  int bufferSize;

  @Param({"8", "64", "128", "256", "512", "1024", "2096", "4096", "8192", "16384"})
  int keys;

  @Param({"0.1", "0.25", "0.5"})
  double runniness;

  @Param({"0.1", "0.25", "0.5"})
  double dirtiness;

  private RoaringBitmap bitmap;
  private int[] buffer;

  @Setup(Level.Trial)
  public void init() {
    this.buffer = new int[bufferSize];
    this.bitmap = RandomData.randomBitmap(keys, runniness, dirtiness);
  }

  @Benchmark
  public int iterate() {
    int blackhole = 0;
    PeekableIntIterator it = bitmap.getIntIterator();
    while (it.hasNext()) {
      blackhole ^= it.next();
    }
    return blackhole;
  }

  @Benchmark
  public int batchIterate() {
    int blackhole = 0;
    RoaringBatchIterator it = bitmap.getBatchIterator();
    while (it.hasNext()) {
      int batch = it.nextBatch(buffer);
      for (int i = 0; i < batch; ++i) {
        blackhole ^= buffer[i];
      }
    }
    return blackhole;
  }

  @Benchmark
  public int batchIterateAsIntIterator() {
    int blackhole = 0;
    IntIterator it = bitmap.getBatchIterator().asIntIterator(buffer);
    while (it.hasNext()) {
      blackhole ^= it.next();
    }
    return blackhole;
  }
}
