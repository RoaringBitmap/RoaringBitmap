package org.roaringbitmap.iteration;


import org.openjdk.jmh.annotations.*;
import org.roaringbitmap.BitmapContainer;
import org.roaringbitmap.Container;
import org.roaringbitmap.PeekableShortIterator;
import org.roaringbitmap.ShortIterator;
import org.roaringbitmap.buffer.MappeableArrayContainer;
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
  public short forwards() {
    PeekableShortIterator it = container.getShortIterator();
    short max = 0;
    while (it.hasNext()) {
      max = it.next();
    }
    return max;
  }

  @Benchmark
  public short backwards() {
    ShortIterator it = container.getReverseShortIterator();
    short min = 0;
    while (it.hasNext()) {
      min = it.next();
    }
    return min;
  }

  @Benchmark
  public short forwardsBuffer() {
    PeekableShortIterator it = bufferContainer.getShortIterator();
    short max = 0;
    while (it.hasNext()) {
      max = it.next();
    }
    return max;
  }

  @Benchmark
  public short backwardsBuffer() {
    ShortIterator it = bufferContainer.getReverseShortIterator();
    short min = 0;
    while (it.hasNext()) {
      min = it.next();
    }
    return min;
  }
}
