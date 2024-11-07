package org.roaringbitmap;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

@BenchmarkMode(Mode.Throughput)
public class RangeOperationBenchmark {

  @Benchmark
  public int computeCardinality(BenchmarkState state) {
    state.bc.computeCardinality();
    return state.bc.getCardinality();
  }

  @Benchmark
  public int updateCardinality(BenchmarkState state) {
    state.bc.updateCardinality(100, 100);
    return state.bc.getCardinality();
  }

  @Benchmark
  public int iadd(BenchmarkState state) {
    Container result = state.emptyBC.iadd(4, 857);
    return result.getCardinality();
  }

  @Benchmark
  public int iandNot(BenchmarkState state) {
    Container result = state.bc.iandNot(state.rc);
    return result.getCardinality();
  }

  @Benchmark
  public int iand(BenchmarkState state) {
    Container result = state.bc.iand(state.rc);
    return result.getCardinality();
  }

  @Benchmark
  public int ior(BenchmarkState state) {
    Container result = state.bc.ior(state.rc);
    return result.getCardinality();
  }

  @Benchmark
  public int ixor(BenchmarkState state) {
    Container result = state.bc.ixor(state.rc);
    return result.getCardinality();
  }

  @Benchmark
  public int iremove(BenchmarkState state) {
    Container result = state.bc.iremove(100, 875);
    return result.getCardinality();
  }

  @Benchmark
  public int inot(BenchmarkState state) {
    Container result = state.bc.inot(100, 875);
    return result.getCardinality();
  }

  @Benchmark
  public int inotFull(BenchmarkState state) {
    Container result = state.bc.inot(0, 1 << 16);
    return result.getCardinality();
  }

  @Benchmark
  public int inotBig(BenchmarkState state) {
    Container result = state.bc.inot(100, 47000);
    return result.getCardinality();
  }

  @State(Scope.Benchmark)
  public static class BenchmarkState {
    BitmapContainer emptyBC = new BitmapContainer();
    BitmapContainer bc = new BitmapContainer();
    RunContainer rc = new RunContainer(new char[] {7, 300, 400, 900, 1400, 2200, 4000, 2700}, 4);

    public BenchmarkState() {
      for (int i = 100; i < 15000; i++) {
        if (i % 5 != 0) bc.add((char) i);
      }
    }
  }
}
