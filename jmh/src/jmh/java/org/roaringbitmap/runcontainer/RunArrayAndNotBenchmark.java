package org.roaringbitmap.runcontainer;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.roaringbitmap.Container;
import org.roaringbitmap.RoaringBitmap;

import it.uniroma3.mat.extendedset.intset.ConciseSet;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class RunArrayAndNotBenchmark {

  static ConciseSet toConcise(int[] dat) {
    ConciseSet ans = new ConciseSet();
    for (int i : dat) {
      ans.add(i);
    }
    return ans;
  }

  @Benchmark
  public int AndNotRunContainer(BenchmarkState benchmarkState) {
    int answer = 0;
    for (int k = 1; k < benchmarkState.rc.size(); ++k)
      answer += RoaringBitmap.andNot(benchmarkState.rc.get(k - 1), benchmarkState.rc.get(k))
          .getCardinality();
    return answer;
  }

  @Benchmark
  public int AndNotBitmapContainer(BenchmarkState benchmarkState) {
    int answer = 0;
    for (int k = 1; k < benchmarkState.ac.size(); ++k)
      answer += RoaringBitmap.andNot(benchmarkState.ac.get(k - 1), benchmarkState.ac.get(k))
          .getCardinality();
    return answer;
  }

  @Benchmark
  public int AndNotConcise(BenchmarkState benchmarkState) {
    int answer = 0;
    for (int k = 1; k < benchmarkState.cc.size(); ++k)
      answer += benchmarkState.cc.get(k - 1).difference(benchmarkState.cc.get(k)).size();
    return answer;
  }

  @State(Scope.Benchmark)
  public static class BenchmarkState {

    ArrayList<RoaringBitmap> rc = new ArrayList<RoaringBitmap>();
    ArrayList<RoaringBitmap> ac = new ArrayList<RoaringBitmap>();
    ArrayList<ConciseSet> cc = new ArrayList<ConciseSet>();

    Random rand = new Random();
    Container aggregate;


    public BenchmarkState() {
      int N = 30;
      Random rand = new Random(1234);
      for (int k = 0; k < N; ++k) {
        RoaringBitmap rb = new RoaringBitmap();
        int start = rand.nextInt(10000);

        for (int z = 0; z < 50; ++z) {
          int end = start + rand.nextInt(10000);
          rb.add(start, end);
          start = end + rand.nextInt(1000);
        }
        cc.add(toConcise(rb.toArray()));

        ac.add(rb);

        rb = rb.clone();
        rb.runOptimize();
        rc.add(rb);

        rb = new RoaringBitmap();

        for (int z = 0; z < 50; ++z) {
          rb.add(rand.nextInt(100000));
        }
        cc.add(toConcise(rb.toArray()));

        ac.add(rb);

        rb = rb.clone();
        rb.runOptimize();
        rc.add(rb);

      }
    }
  }

}
