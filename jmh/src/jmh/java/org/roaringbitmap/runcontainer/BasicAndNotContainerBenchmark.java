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

import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class BasicAndNotContainerBenchmark {

  @Benchmark
  public int andNotBitmapContainerVSRunContainerContainer(BenchmarkState benchmarkState) {
    if (benchmarkState.rc2.serializedSizeInBytes() > benchmarkState.ac2.serializedSizeInBytes())
      throw new RuntimeException("Can't expect run containers to win if they are larger.");
    return benchmarkState.ac1.andNot(benchmarkState.rc2).getCardinality();
  }

  @Benchmark
  public int andNotBitmapContainerVSBitmapContainer(BenchmarkState benchmarkState) {
    return benchmarkState.ac1.andNot(benchmarkState.ac2).getCardinality();
  }

  @Benchmark
  public int reverseAndNotBitmapContainerVSRunContainerContainer(BenchmarkState benchmarkState) {
    if (benchmarkState.rc2.serializedSizeInBytes() > benchmarkState.ac2.serializedSizeInBytes())
      throw new RuntimeException("Can't expect run containers to win if they are larger.");
    return benchmarkState.rc2.andNot(benchmarkState.ac1).getCardinality();
  }

  @Benchmark
  public int reverseAndNotBitmapContainerVSBitmapContainer(BenchmarkState benchmarkState) {
    return benchmarkState.ac2.andNot(benchmarkState.ac1).getCardinality();
  }

  @Benchmark
  public int part2_andNotRunContainerVSRunContainerContainer(BenchmarkState benchmarkState) {
    if (benchmarkState.rc2.serializedSizeInBytes() > benchmarkState.ac2.serializedSizeInBytes())
      throw new RuntimeException("Can't expect run containers to win if they are larger.");
    if (benchmarkState.rc3.serializedSizeInBytes() > benchmarkState.ac3.serializedSizeInBytes())
      throw new RuntimeException("Can't expect run containers to win if they are larger.");
    return benchmarkState.rc3.andNot(benchmarkState.rc2).getCardinality();
  }

  @Benchmark
  public int part2_andNotBitmapContainerVSBitmapContainer2(BenchmarkState benchmarkState) {
    return benchmarkState.ac3.andNot(benchmarkState.ac2).getCardinality();
  }

  @Benchmark
  public int part2_reverseAndNotRunContainerVSRunContainerContainer(BenchmarkState benchmarkState) {
    if (benchmarkState.rc2.serializedSizeInBytes() > benchmarkState.ac2.serializedSizeInBytes())
      throw new RuntimeException("Can't expect run containers to win if they are larger.");
    if (benchmarkState.rc3.serializedSizeInBytes() > benchmarkState.ac3.serializedSizeInBytes())
      throw new RuntimeException("Can't expect run containers to win if they are larger.");
    return benchmarkState.rc2.andNot(benchmarkState.rc3).getCardinality();
  }

  @Benchmark
  public int part2_reverseAndNotBitmapContainerVSBitmapContainer2(BenchmarkState benchmarkState) {
    return benchmarkState.ac2.andNot(benchmarkState.ac3).getCardinality();
  }

  @Benchmark
  public int part3_andNotArrayContainerVSRunContainerContainer(BenchmarkState benchmarkState) {
    return benchmarkState.ac4.andNot(benchmarkState.rc2).getCardinality();
  }

  @Benchmark
  public int part4_andNotArrayContainerVSRunContainerContainer(BenchmarkState benchmarkState) {
    return benchmarkState.ac4.andNot(benchmarkState.rc4).getCardinality();
  }

  @Benchmark
  public int part3_andNotArrayContainerVSBitmapContainer(BenchmarkState benchmarkState) {
    return benchmarkState.ac4.andNot(benchmarkState.ac2).getCardinality();
  }

  @Benchmark
  public int part3_reverseAndNotArrayContainerVSRunContainerContainer(
      BenchmarkState benchmarkState) {
    if (benchmarkState.rc2.serializedSizeInBytes() > benchmarkState.ac2.serializedSizeInBytes())
      throw new RuntimeException("Can't expect run containers to win if they are larger.");
    return benchmarkState.rc2.andNot(benchmarkState.ac4).getCardinality();
  }

  @Benchmark
  public int part3_reverseAndNotArrayContainerVSBitmapContainer(BenchmarkState benchmarkState) {
    return benchmarkState.ac2.andNot(benchmarkState.ac4).getCardinality();
  }

  @State(Scope.Benchmark)
  public static class BenchmarkState {
    public int offvalues = 32;
    public int bitsetperword2 = 63;
    public int bitsetperword3 = 1;

    Container rc1, rc2, rc3, rc4, ac1, ac2, ac3, ac4;
    Random rand = new Random();

    public BenchmarkState() {
      final int max = 1 << 16;
      final int howmanywords = (1 << 16) / 64;
      int[] values1 = RandomUtil.negate(RandomUtil.generateUniformHash(rand, offvalues, max), max);
      int[] values2 = RandomUtil.generateUniformHash(rand, bitsetperword2 * howmanywords, max);
      int[] values3 = RandomUtil.generateCrazyRun(rand, max);
      int[] values4 = RandomUtil.generateUniformHash(rand, bitsetperword3 * howmanywords, max);

      rc1 = new RunContainer();
      rc1 = RandomUtil.fillMeUp(rc1, values1);

      rc2 = new RunContainer();
      rc2 = RandomUtil.fillMeUp(rc2, values2);

      rc3 = new RunContainer();
      rc3 = RandomUtil.fillMeUp(rc3, values3);

      rc4 =
          new RunContainer(
              new char[] {4, 500, 2000, 1000, 5000, 3000, 16000, 10000, 32000, 600}, 5);

      ac1 = new ArrayContainer();
      ac1 = RandomUtil.fillMeUp(ac1, values1);

      if (!(ac1 instanceof BitmapContainer))
        throw new RuntimeException("expected bitmap container");

      ac2 = new ArrayContainer();
      ac2 = RandomUtil.fillMeUp(ac2, values2);

      if (!(ac2 instanceof BitmapContainer))
        throw new RuntimeException("expected bitmap container");

      ac3 = new ArrayContainer();
      ac3 = RandomUtil.fillMeUp(ac3, values3);

      ac4 = new ArrayContainer();
      ac4 = RandomUtil.fillMeUp(ac4, values4);

      if (!(ac4 instanceof ArrayContainer)) throw new RuntimeException("expected array container");

      if (!rc1.equals(ac1)) throw new RuntimeException("first containers do not match");

      if (!rc2.equals(ac2)) throw new RuntimeException("second containers do not match");

      if (!rc1.andNot(rc2).equals(ac1.andNot(ac2)))
        throw new RuntimeException("andNots do not match");
      if (!ac1.andNot(rc2).equals(ac1.andNot(ac2)))
        throw new RuntimeException("andNots do not match");
      if (!rc2.andNot(rc1).equals(ac2.andNot(ac1)))
        throw new RuntimeException("andNots do not match");
      if (!rc2.andNot(ac1).equals(ac2.andNot(ac1)))
        throw new RuntimeException("andNots do not match");
    }
  }
}
