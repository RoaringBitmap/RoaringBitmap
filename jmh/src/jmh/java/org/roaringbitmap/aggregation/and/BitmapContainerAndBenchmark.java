package org.roaringbitmap.aggregation.and;

import org.roaringbitmap.BitmapContainer;
import org.roaringbitmap.Container;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.SplittableRandom;

@State(Scope.Benchmark)
public class BitmapContainerAndBenchmark {

  @Param({"0.1", "0.5"})
  double thisDensity;

  @Param({"0.1", "0.5"})
  double thatDensity;

  private BitmapContainer modified;
  private BitmapContainer parameter;

  @Setup(Level.Trial)
  public void setup() {
    SplittableRandom random = new SplittableRandom(42);
    modified = new BitmapContainer();
    parameter = new BitmapContainer();
    while (modified.getCardinality() < 0x10000 * thisDensity) {
      modified.add((char) random.nextInt(0x10000));
    }
    while (parameter.getCardinality() < 0x10000 * thatDensity) {
      parameter.add((char) random.nextInt(0x10000));
    }
  }

  @Benchmark
  public Container stable() {
    return modified.clone().iand(parameter);
  }

  @Benchmark
  public Container tendToZero() {
    return modified.iand(parameter);
  }
}
