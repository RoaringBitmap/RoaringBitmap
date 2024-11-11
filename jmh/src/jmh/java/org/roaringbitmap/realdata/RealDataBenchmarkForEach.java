package org.roaringbitmap.realdata;

import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.realdata.state.RealDataBenchmarkState;
import org.roaringbitmap.realdata.wrapper.Bitmap;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class RealDataBenchmarkForEach {

  @Benchmark
  public int forEach(RealDataBenchmarkState bs) {
    Consumer consumer = new Consumer();
    for (int k = 0; k < bs.bitmaps.size(); ++k) {
      Bitmap bitmap = bs.bitmaps.get(k);
      bitmap.forEach(consumer);
    }
    return consumer.total;
  }

  private static class Consumer implements IntConsumer {
    int total = 0;

    @Override
    public void accept(int value) {
      total += value;
    }
  }
}
