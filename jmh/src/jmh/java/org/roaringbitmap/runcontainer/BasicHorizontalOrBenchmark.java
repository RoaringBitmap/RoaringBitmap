package org.roaringbitmap.runcontainer;

import org.roaringbitmap.ArrayContainer;
import org.roaringbitmap.BitmapContainer;
import org.roaringbitmap.Container;
import org.roaringbitmap.RunContainer;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class BasicHorizontalOrBenchmark {

  private static Container horizontal_or(Iterator<Container> i) {
    if (!i.hasNext()) throw new RuntimeException("please provide data");
    Container c = i.next();
    if (!i.hasNext()) throw new RuntimeException("please provide more than one container");
    Container c2 = i.next();
    c = c.lazyOR(c2);
    while (i.hasNext()) c = c.lazyIOR(i.next());
    c = c.repairAfterLazy();
    return c;
  }

  @Benchmark
  public int horizontalOrRunContainer(BenchmarkState benchmarkState) {
    Container answer = horizontal_or(benchmarkState.rc.iterator());
    if (!answer.equals(benchmarkState.aggregate)) throw new RuntimeException("bug");
    return answer.getCardinality();
  }

  @Benchmark
  public int horizontalOrRunContainer_withconversion(BenchmarkState benchmarkState) {
    BitmapContainer c = (BitmapContainer) benchmarkState.ac.get(0);
    Container c2 = benchmarkState.rc.get(1);
    c = (BitmapContainer) c.lazyOR(c2);
    for (int k = 2; k < benchmarkState.rc.size(); ++k) {
      c = (BitmapContainer) c.lazyIOR(benchmarkState.rc.get(k));
    }
    c = (BitmapContainer) c.repairAfterLazy();
    if (!c.equals(benchmarkState.aggregate)) throw new RuntimeException("bug");
    return c.getCardinality();
  }

  @Benchmark
  public int horizontalOrBitmapContainer(BenchmarkState benchmarkState) {
    Container answer = horizontal_or(benchmarkState.ac.iterator());
    if (!answer.equals(benchmarkState.aggregate)) throw new RuntimeException("bug");
    return answer.getCardinality();
  }

  @State(Scope.Benchmark)
  public static class BenchmarkState {
    public int bitsetperword = 63;

    ArrayList<Container> rc = new ArrayList<Container>();
    ArrayList<Container> ac = new ArrayList<Container>();
    Random rand = new Random();
    Container aggregate;

    public BenchmarkState() {
      final int max = 1 << 16;
      final int howmanywords = (1 << 16) / 64;
      int N = 50;
      for (int k = 0; k < N; ++k) {
        int[] values = RandomUtil.generateUniformHash(rand, bitsetperword * howmanywords, max);
        Container rct = new RunContainer();
        rct = RandomUtil.fillMeUp(rct, values);
        if (!(rct instanceof RunContainer)) throw new RuntimeException("unexpected container type");

        Container act = new ArrayContainer();
        act = RandomUtil.fillMeUp(act, values);
        if (!(act instanceof BitmapContainer))
          throw new RuntimeException("unexpected container type");
        if (!act.equals(rct)) throw new RuntimeException("unequal containers");
        if (act.serializedSizeInBytes() < rct.serializedSizeInBytes())
          throw new RuntimeException("You cannot win");
        rc.add(rct);
        ac.add(act);
      }
      Container b1 = rc.get(0);
      Container b2 = ac.get(0);
      if (!b1.equals(b2)) throw new RuntimeException("bug 0");
      for (int k = 1; k < N; ++k) {
        b1 = b1.lazyIOR(rc.get(1));
        b2 = b2.lazyIOR(ac.get(1));
      }

      if (!horizontal_or(rc.iterator()).equals(horizontal_or(ac.iterator())))
        throw new RuntimeException("bug! The ORs do not agree.");
      aggregate = horizontal_or(rc.iterator());
    }
  }
}
