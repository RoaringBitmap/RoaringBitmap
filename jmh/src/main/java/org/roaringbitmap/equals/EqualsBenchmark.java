// https://github.com/RoaringBitmap/RoaringBitmap/issues/161
package org.roaringbitmap.equals;

import org.openjdk.jmh.annotations.*;
import org.roaringbitmap.*;
import org.roaringbitmap.buffer.*;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.TimeUnit;


@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class EqualsBenchmark {


  @BenchmarkMode(Mode.SampleTime)
  @Benchmark
  public boolean runVsArrayEquals_FewRuns(EqualsFewRunsBenchmarkState benchmarkState) {
    return benchmarkState.runContainer.equals(benchmarkState.arrayContainer);
  }

  @BenchmarkMode(Mode.SampleTime)
  @Benchmark
  public boolean runVsArrayEquals_ManyRuns(EqualsManyRunsBenchmarkState benchmarkState) {
    return benchmarkState.runContainer.equals(benchmarkState.arrayContainer);
  }


  @State(Scope.Benchmark)
  public static class EqualsFewRunsBenchmarkState {

    final RunContainer runContainer = new RunContainer();
    final ArrayContainer arrayContainer = new ArrayContainer();

    public EqualsFewRunsBenchmarkState() {
      addRange(arrayContainer, 20, 400);
      addRange(runContainer, 20, 400);

      addRange(arrayContainer, 501, 1500);
      addRange(runContainer, 501, 1500);


      addRange(arrayContainer, 3000, 4500);
      addRange(runContainer, 3000, 4500);
    }

  }


  @State(Scope.Benchmark)
  public static class EqualsManyRunsBenchmarkState {

    final RunContainer runContainer = new RunContainer();
    final ArrayContainer arrayContainer = new ArrayContainer();

    public EqualsManyRunsBenchmarkState() {
      int cardinality = 0;
      int runLength = 20;
      int shift = 5;
      int runStart = 0;
      while (cardinality < 4096 - runLength) {
        cardinality += runLength;
        addRange(runContainer, runStart, runStart + runLength);
        addRange(arrayContainer, runStart, runStart + runLength);
        runStart += runLength + shift;
      }
    }

  }


  static void addRange(Container rc, int min, int sup) {
    rc.iadd(min, sup);
  }

}
