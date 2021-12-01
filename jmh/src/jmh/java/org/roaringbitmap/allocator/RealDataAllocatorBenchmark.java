package org.roaringbitmap.allocator;

import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.roaringbitmap.RandomData;
import org.roaringbitmap.RoaringBitmap;


@State(Scope.Benchmark)
public class RealDataAllocatorBenchmark {

  private RoaringBitmap bitmap1;
  private RoaringBitmap bitmap2;
  private RoaringBitmap bitmap3;
  private RoaringBitmap bitmap4;
  private RoaringBitmap bitmap5;
  private RoaringBitmap bitmap6;

  @Setup
  public void setup() {
    //AllocationManager.register(ObjectPoolAllocator.INSTANCE);
    bitmap1 = RandomData.randomBitmap(1 << 12, 0.2, 0.3);
    bitmap2 = RandomData.randomBitmap(1 << 4, 0.2, 0.3);
    bitmap3 = RandomData.randomBitmap(50, 0.5, 0.5);
    bitmap4 = RandomData.randomBitmap(50, 0.5, 0.5);
    bitmap5 = RandomData.randomBitmap(50, 0.5, 0.5);
    bitmap6 = RandomData.randomBitmap(50, 0.5, 0.5);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public RoaringBitmap and() {
    bitmap1.and(bitmap2);
    bitmap1.andNot(bitmap3);
    bitmap1.or(bitmap4);
    bitmap1.xor(bitmap5);
    bitmap1.remove(20L, 20000L);
    bitmap1.flip(4000L, 40000L);

    bitmap1.and(bitmap3);
    bitmap1.andNot(bitmap4);
    bitmap1.or(bitmap5);
    bitmap1.xor(bitmap2);
    return bitmap1;
  }
}
