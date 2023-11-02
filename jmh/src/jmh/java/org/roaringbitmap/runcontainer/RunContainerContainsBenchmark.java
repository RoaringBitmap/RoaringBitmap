package org.roaringbitmap.runcontainer;

import org.openjdk.jmh.annotations.*;
import org.roaringbitmap.buffer.MappeableBitmapContainer;
import org.roaringbitmap.buffer.MappeableRunContainer;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 6, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class RunContainerContainsBenchmark {

  public MappeableRunContainer rc;
  public MappeableBitmapContainer bc;
  @Setup
  public void setup() {
    rc = new MappeableRunContainer();
    bc = new MappeableBitmapContainer();
    Random r = new Random(0);
    int begin, end = 0;
    for (int i = 0; i < 9; i++) {
      begin = end + r.nextInt(10);
      end = begin + r.nextInt(100);
      rc.add(begin, end);
      bc.add((char) (begin & 0xFFFF + r.nextInt(end - begin)));
    }
    System.out.println(rc.contains(bc) + " " + rc.contains2(bc));
    System.err.println("================================");
  }

  @Benchmark
  public boolean optimized() {
    return rc.contains(bc);
  }

  @Benchmark
  public boolean original() {
    return rc.contains2(bc);
  }
}