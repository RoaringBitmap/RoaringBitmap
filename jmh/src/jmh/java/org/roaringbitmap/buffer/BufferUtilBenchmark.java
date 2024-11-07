package org.roaringbitmap.buffer;

import me.lemire.integercompression.synth.ClusteredDataGenerator;
import org.apache.commons.math3.distribution.IntegerDistribution;
import org.apache.commons.math3.distribution.UniformIntegerDistribution;
import org.apache.commons.math3.random.Well19937c;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.nio.CharBuffer;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * The experiment to test the threshold when it is worth to use galloping strategy of intersecting
 * sorted lists. It allows to generate sample lists where first is *param* times bigger than other
 * one. Both lists can be generated used uniform or clustered distribution. The methodology and
 * results are presented in the issue #130 on Github.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class BufferUtilBenchmark {

  @Param({"0"}) // use {"0", "1"} to test both uniform and clustered combinations
  public int smallType; // 0 - uniform, 1 - clustered

  @Param({"0"}) // use {"0", "1"} to test both uniform and clustered combinations
  public int bigType; // 0 - uniform, 1 - clustered

  @Param({"0"}) // use {"0", "1", "2"} for three experiments. Update GENERATE_EXAMPLES if changing
  // this
  public int index;

  @Param({"35"}) // use {"20", "25", "30"} to check different thresholds
  public int param;

  private static final int GENERATE_EXAMPLES = 1;
  public static BenchmarkData data;

  @Setup
  public void setup() {
    data = BenchmarkDataGenerator.generate(param, GENERATE_EXAMPLES, smallType, bigType);
  }

  @Benchmark
  public void galloping() {
    BenchmarkContainer small = data.small[index];
    BenchmarkContainer big = data.big[index];
    BufferUtil.unsignedOneSidedGallopingIntersect2by2(
        small.content, small.length, big.content, big.length, data.dest);
  }

  @Benchmark
  public void local() {
    BenchmarkContainer small = data.small[index];
    BenchmarkContainer big = data.big[index];
    BufferUtil.unsignedLocalIntersect2by2(
        small.content, small.length, big.content, big.length, data.dest);
  }
}

class BenchmarkData {
  BenchmarkData(BenchmarkContainer[] small, BenchmarkContainer[] big) {
    this.small = small;
    this.big = big;
    this.dest = new char[Short.MAX_VALUE];
  }

  final BenchmarkContainer[] small;
  final BenchmarkContainer[] big;
  final char[] dest;
}

class BenchmarkContainer {
  BenchmarkContainer(char[] content) {
    this.content = CharBuffer.wrap(content);
    this.length = content.length;
  }

  final CharBuffer content;
  final int length;
}

/**
 * Deterministic generator for benchmark data. For given *param* it generates *howmany* entries
 */
class BenchmarkDataGenerator {
  static BenchmarkData generate(int param, int howMany, int smallType, int bigType) {
    IntegerDistribution ud =
        new UniformIntegerDistribution(
            new Well19937c(param + 17), Short.MIN_VALUE, Short.MAX_VALUE);
    ClusteredDataGenerator cd = new ClusteredDataGenerator();
    IntegerDistribution p =
        new UniformIntegerDistribution(
            new Well19937c(param + 123), SMALLEST_ARRAY, BIGGEST_ARRAY / param);
    BenchmarkContainer[] smalls = new BenchmarkContainer[howMany];
    BenchmarkContainer[] bigs = new BenchmarkContainer[howMany];
    for (int i = 0; i < howMany; i++) {
      int smallSize = p.sample();
      int bigSize = smallSize * param;
      char[] small =
          smallType == 0 ? generateUniform(ud, smallSize) : generateClustered(cd, smallSize);
      char[] big = bigType == 0 ? generateUniform(ud, bigSize) : generateClustered(cd, bigSize);
      smalls[i] = new BenchmarkContainer(small);
      bigs[i] = new BenchmarkContainer(big);
    }
    return new BenchmarkData(smalls, bigs);
  }

  private static char[] intArrayToShortArraySorted(int[] source) {
    char[] result = new char[source.length];
    for (int i = 0; i < source.length; i++) {
      result[i] = (char) source[i];
    }
    Arrays.sort(result);
    return result;
  }

  private static char[] generateClustered(ClusteredDataGenerator cd, int howMany) {
    int[] half1raw = cd.generateClustered(howMany / 2, Short.MAX_VALUE);
    for (int i = 0; i < half1raw.length; i++) {
      half1raw[i] = -half1raw[i];
    }
    char[] half1 = intArrayToShortArraySorted(half1raw);
    char[] half2 = intArrayToShortArraySorted(cd.generateClustered(howMany / 2, Short.MAX_VALUE));
    char[] result = new char[half1.length + half2.length];
    System.arraycopy(half1, 0, result, 0, half1.length);
    System.arraycopy(half2, 0, result, half1.length, half2.length);
    return result;
  }

  private static char[] generateUniform(IntegerDistribution ud, int howMany) {
    return intArrayToShortArraySorted(ud.sample(howMany));
  }

  private static final int SMALLEST_ARRAY = 2;
  private static final int BIGGEST_ARRAY = 4096;
}
