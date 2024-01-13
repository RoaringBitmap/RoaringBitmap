package org.roaringbitmap.runcontainer;

import org.openjdk.jmh.annotations.*;
import org.roaringbitmap.buffer.MappeableBitmapContainer;
import org.roaringbitmap.buffer.MappeableRunContainer;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
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
    for (int i = 0; i < 50; i++) {
      begin = end + r.nextInt(10);
      end = begin + r.nextInt(100) + 1;
      rc = (MappeableRunContainer) rc.add(begin, end);
      //System.out.println("range added:" + begin + " " + end);
      for (int j = 0; j < 8; j++) {
        bc = (MappeableBitmapContainer) bc.add((char) (begin + r.nextInt(end - begin - 1)));
      }
    }
    //System.out.println("BC: " + bc.toString());
    //System.out.println("RC: " + rc.toString() + " " + rc.numberOfRuns());
    System.out.println("CONTAINS: " + rc.contains(bc) + " " + rc.contains2(bc));
    //System.err.println("================================");
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