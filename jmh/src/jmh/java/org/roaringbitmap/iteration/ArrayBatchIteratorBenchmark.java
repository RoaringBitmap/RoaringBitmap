package org.roaringbitmap.iteration;

import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.roaringbitmap.RandomData;
import org.roaringbitmap.RoaringBatchIterator;
import org.roaringbitmap.RoaringBitmap;

@Warmup(time = 5)
@Measurement(time = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class ArrayBatchIteratorBenchmark {
  @Param({"256"})
  int bufferSize;

  @Param({"4096", "16384"})
  int keys;

  private RoaringBitmap bitmap;

  @Setup(Level.Trial)
  public void init() {
    this.bitmap = RandomData.randomBitmap(keys, 0, 0);
  }

  @Benchmark
  public int array_batch_iterator_benchmark() {
    int blackhole = 0;

    RoaringBatchIterator it = bitmap.getBatchIterator();
    int[] buffer = new int[bufferSize];

    while (it.hasNext()) {
      int read = it.nextBatch(buffer);
      for (int i = 0; i < read; i++) {
        blackhole ^= buffer[i];
      }
    }

    return blackhole;
  }
}
