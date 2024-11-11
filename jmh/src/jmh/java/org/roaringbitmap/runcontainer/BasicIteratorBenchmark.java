package org.roaringbitmap.runcontainer;

import org.roaringbitmap.ArrayContainer;
import org.roaringbitmap.CharIterator;
import org.roaringbitmap.Container;
import org.roaringbitmap.RunContainer;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class BasicIteratorBenchmark {

  @Benchmark
  public int iteratorRunContainer(BenchmarkState benchmarkState) {
    if (benchmarkState.rc2.serializedSizeInBytes() > benchmarkState.ac2.serializedSizeInBytes())
      throw new RuntimeException("Can't expect run containers to win if they are larger.");
    CharIterator si = benchmarkState.rc2.getCharIterator();
    int answer = 0;
    while (si.hasNext()) answer += si.next() & 0xFFFF;
    return answer;
  }

  @Benchmark
  public int iteratorBitmapContainer(BenchmarkState benchmarkState) {
    CharIterator si = benchmarkState.ac2.getCharIterator();
    int answer = 0;
    while (si.hasNext()) answer += si.next() & 0xFFFF;
    return answer;
  }

  @State(Scope.Benchmark)
  public static class BenchmarkState {
    public int bitsetperword2 = 63;

    Container rc2, ac2;
    Random rand = new Random();

    public BenchmarkState() {
      final int max = 1 << 16;
      final int howmanywords = (1 << 16) / 64;
      int[] values2 = RandomUtil.generateUniformHash(rand, bitsetperword2 * howmanywords, max);

      rc2 = new RunContainer();
      rc2 = RandomUtil.fillMeUp(rc2, values2);

      ac2 = new ArrayContainer();
      ac2 = RandomUtil.fillMeUp(ac2, values2);

      if (!rc2.equals(ac2)) throw new RuntimeException("second containers do not match");
    }
  }
}
