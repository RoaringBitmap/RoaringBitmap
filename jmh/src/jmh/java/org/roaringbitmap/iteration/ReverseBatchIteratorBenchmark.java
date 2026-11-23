package org.roaringbitmap.iteration;

import org.roaringbitmap.*;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import org.openjdk.jmh.annotations.*;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class ReverseBatchIteratorBenchmark {
  @Param({"array", "bitmap", "run"})
  public String shape;

  private RoaringBitmap bitmap;
  private ImmutableRoaringBitmap mapped;
  private int[] buffer = new int[256];

  @Setup
  public void setup() {
    bitmap = new RoaringBitmap();
    for (int h = 0; h < 16; h++) {
      if (shape.equals("run")) bitmap.add(((long) h) << 16, (((long) h) << 16) + 60000);
      else {
        int step = shape.equals("array") ? 101 : 2;
        for (int l = 0; l < 65536; l += step) bitmap.add((h << 16) | l);
      }
    }
    if (shape.equals("run")) bitmap.runOptimize();
    ByteBuffer bytes = ByteBuffer.allocateDirect(bitmap.serializedSizeInBytes());
    bitmap.serialize(bytes);
    bytes.flip();
    mapped = new ImmutableRoaringBitmap(bytes.asReadOnlyBuffer());
  }

  @Benchmark
  public long pageScalar() {
    PeekableIntIterator it = bitmap.getReverseIntIterator();
    it.advanceIfNeeded(550000);
    long sum = 0;
    for (int i = 0; i < 256 && it.hasNext(); i++) sum += it.next();
    return sum;
  }

  @Benchmark
  public long pageBatch() {
    BatchIterator it = bitmap.getReverseBatchIterator();
    it.advanceIfNeeded(550000);
    long sum = 0;
    int n = it.nextBatch(buffer);
    for (int i = 0; i < n; i++) sum += buffer[i];
    return sum;
  }

  @Benchmark
  public long pageMappedScalar() {
    PeekableIntIterator it = mapped.getReverseIntIterator();
    it.advanceIfNeeded(550000);
    long sum = 0;
    for (int i = 0; i < 256 && it.hasNext(); i++) sum += it.next();
    return sum;
  }

  @Benchmark
  public long pageMappedBatch() {
    BatchIterator it = mapped.getReverseBatchIterator();
    it.advanceIfNeeded(550000);
    long sum = 0;
    int n = it.nextBatch(buffer);
    for (int i = 0; i < n; i++) sum += buffer[i];
    return sum;
  }

  @Benchmark
  public long scalar() {
    PeekableIntIterator it = bitmap.getReverseIntIterator();
    long sum = 0;
    while (it.hasNext()) sum += it.next();
    return sum;
  }

  @Benchmark
  public long batch() {
    RoaringReverseBatchIterator it = bitmap.getReverseBatchIterator();
    long sum = 0;
    while (it.hasNext()) {
      int n = it.nextBatch(buffer);
      for (int i = 0; i < n; i++) sum += buffer[i];
    }
    return sum;
  }
}
