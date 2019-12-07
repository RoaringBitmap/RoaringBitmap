package org.roaringbitmap.iteration;


import org.openjdk.jmh.annotations.*;
import org.roaringbitmap.BitmapContainer;
import org.roaringbitmap.CharIterator;
import org.roaringbitmap.Container;
import org.roaringbitmap.PeekableCharIterator;
import org.roaringbitmap.buffer.MappeableBitmapContainer;
import org.roaringbitmap.buffer.MappeableContainer;

import java.nio.LongBuffer;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(jvmArgsPrepend = "-XX:-TieredCompilation")
public class BitmapIteratorBenchmark {


  @Param({"0.1", "0.2", "0.3", "0.4", "0.5"})
  double density;

  private Container container;
  private MappeableContainer bufferContainer;

  @Setup
  public void init() {
    long[] bitmap = new long[1024];
    int cardinality = 0;
    int targetCardinality = (int)(density * 65536);
    ThreadLocalRandom random = ThreadLocalRandom.current();
    while (cardinality < targetCardinality) {
      int index = random.nextInt(65536);
      long before = bitmap[index >>> 6];
      bitmap[index >>> 6] |= (1L << index);
      cardinality += Long.bitCount(before ^ bitmap[index >>> 6]);
    }
    container = new BitmapContainer(bitmap, cardinality);
    bufferContainer = new MappeableBitmapContainer(LongBuffer.wrap(bitmap), cardinality);
  }

  @Benchmark
  public char forwards() {
    PeekableCharIterator it = container.getCharIterator();
    char max = 0;
    while (it.hasNext()) {
      max = it.next();
    }
    return max;
  }

  @Benchmark
  public char backwards() {
    CharIterator it = container.getReverseCharIterator();
    char min = 0;
    while (it.hasNext()) {
      min = it.next();
    }
    return min;
  }

  @Benchmark
  public char forwardsBuffer() {
    PeekableCharIterator it = bufferContainer.getCharIterator();
    char max = 0;
    while (it.hasNext()) {
      max = it.next();
    }
    return max;
  }

  @Benchmark
  public char backwardsBuffer() {
    CharIterator it = bufferContainer.getReverseCharIterator();
    char min = 0;
    while (it.hasNext()) {
      min = it.next();
    }
    return min;
  }
}
