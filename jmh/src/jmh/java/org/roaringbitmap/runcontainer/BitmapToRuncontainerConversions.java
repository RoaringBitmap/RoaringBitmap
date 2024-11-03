package org.roaringbitmap.runcontainer;

import org.roaringbitmap.BitmapContainer;
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
public class BitmapToRuncontainerConversions {

  /*
   * These are commented out in BitmapContainer and soon be be deleted
   *
   * @Benchmark public int numberOfRunsData1(BenchmarkState benchmarkState) { return
   * benchmarkState.ac1.numberOfRuns(); }
   *
   * @Benchmark public int numberOfRunsData2(BenchmarkState benchmarkState) { return
   * benchmarkState.ac2.numberOfRuns(); }
   *
   *
   * @Benchmark public int numberOfRunsOld1(BenchmarkState benchmarkState) { return
   * benchmarkState.ac1.numberOfRuns_old(); }
   *
   * @Benchmark public int numberOfRunOld2(BenchmarkState benchmarkState) { return
   * benchmarkState.ac2.numberOfRuns_old(); }
   *
   * @Benchmark public int numberOfRunsLowerBound1(BenchmarkState benchmarkState) { return
   * benchmarkState.ac1.numberOfRunsLowerBound(); }
   *
   *
   * @Benchmark public int numberOfRunsLowerBound2(BenchmarkState benchmarkState) { return
   * benchmarkState.ac2.numberOfRunsLowerBound(); }
   *
   */
  @Benchmark
  public int numberOfRunsLowerBound1281(BenchmarkState benchmarkState) {
    return benchmarkState.ac1.numberOfRunsLowerBound(1000);
  }

  @Benchmark
  public int numberOfRunsLowerBound1282(BenchmarkState benchmarkState) {
    return benchmarkState.ac2.numberOfRunsLowerBound(1000);
  }

  /*
   * soon to be deleted...
   *
   * @Benchmark public int numberOfRunsLowerBound5121(BenchmarkState benchmarkState) { return
   * benchmarkState.ac1.numberOfRunsLowerBound512(10); }
   *
   *
   * @Benchmark public int numberOfRunsLowerBound5122(BenchmarkState benchmarkState) { return
   * benchmarkState.ac2.numberOfRunsLowerBound512(1000); }
   */

  @Benchmark
  public int numberOfRunsAdjustment(BenchmarkState benchmarkState) {
    return benchmarkState.ac2.numberOfRunsAdjustment();
  }

  /*
   * soon to be deleted...
   *
   * @Benchmark public int numberOfRunsAdjustmentUnrolled(BenchmarkState benchmarkState) { return
   * benchmarkState.ac2.numberOfRunsAdjustmentUnrolled(); }
   *
   *
   * @Benchmark public int numberOfRunsLowerBoundUnrolled(BenchmarkState benchmarkState) { return
   * benchmarkState.ac2.numberOfRunsLowerBoundUnrolled(); }
   *
   *
   * @Benchmark public int numberOfRunsLowerBoundUnrolled2(BenchmarkState benchmarkState) { return
   * benchmarkState.ac2.numberOfRunsLowerBoundUnrolled2(); }
   *
   *
   * @Benchmark public int numberOfRunsLowerBoundUnrolled2threshold1000(BenchmarkState
   * benchmarkState) { return benchmarkState.ac2.numberOfRunsLowerBoundUnrolled2(1000); }
   */

  @Benchmark
  public int numberOfRunsLowerBoundThreshold1000(BenchmarkState benchmarkState) {
    return benchmarkState.ac2.numberOfRunsLowerBound(1000);
  }

  @Benchmark
  public int runOptimize(BenchmarkState benchmarkState) {
    return benchmarkState.ac2.runOptimize() instanceof RunContainer ? 1 : 0;
  }

  /*
   * @Benchmark public int runOptimizeOld(BenchmarkState benchmarkState) { return
   * benchmarkState.ac2.runOptimize_old() instanceof RunContainer ? 1 : 0; }
   *
   */

  @Benchmark
  public int runOptimize1(BenchmarkState benchmarkState) {
    return benchmarkState.ac1.runOptimize() instanceof RunContainer ? 1 : 0;
  }

  /*
   * @Benchmark public int runOptimizeOld1(BenchmarkState benchmarkState) { return
   * benchmarkState.ac1.runOptimize_old() instanceof RunContainer ? 1 : 0; }
   *
   */

  @State(Scope.Benchmark)
  public static class BenchmarkState {
    public int offvalues = 32;
    public int bitsetperword2 = 63;
    public int bitsetperword3 = 1;

    BitmapContainer ac1, ac2;
    Random rand = new Random();

    public BenchmarkState() {
      final int max = 1 << 16;
      final int howmanywords = (1 << 16) / 64;
      int[] values1 = RandomUtil.negate(RandomUtil.generateUniformHash(rand, offvalues, max), max);
      int[] values2 = RandomUtil.generateUniformHash(rand, bitsetperword2 * howmanywords, max);

      ac1 = new BitmapContainer();
      ac1 = (BitmapContainer) RandomUtil.fillMeUp(ac1, values1);

      ac2 = new BitmapContainer();
      ac2 = (BitmapContainer) RandomUtil.fillMeUp(ac2, values2);
    }
  }
}
