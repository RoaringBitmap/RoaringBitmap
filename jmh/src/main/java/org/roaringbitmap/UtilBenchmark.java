package org.roaringbitmap;

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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class UtilBenchmark {

    @Param({"0", "1", "2", "3", "4"})
    public int index;
    @Param({"10","15","20","25","30","35", "40", "50","60", "90"})
    public int param;

    public static BenchmarkData data;

    @Setup
    public void setup() {
        data = BenchmarkDataGenerator.generate(param, 5);
    }

    @Benchmark
    public void galloping() {
        BenchmarkContainer small = data.small[index];
        BenchmarkContainer big = data.big[index];
        Util.unsignedOneSidedGallopingIntersect2by2(small.content, small.length, big.content, big.length, data.dest);
    }

    @Benchmark
    public void local() {
        BenchmarkContainer small = data.small[index];
        BenchmarkContainer big = data.big[index];
        Util.unsignedLocalIntersect2by2(small.content, small.length, big.content, big.length, data.dest);
    }

    public static void main(String[] arg) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + UtilBenchmark.class.getSimpleName() + ".*")
                .warmupIterations(12)
                .measurementIterations(7)
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}

class BenchmarkData {
    BenchmarkData(BenchmarkContainer[] small, BenchmarkContainer[] big) {
        this.small = small;
        this.big = big;
        this.dest = new short[Short.MAX_VALUE];
    }
    final BenchmarkContainer[] small;
    final BenchmarkContainer[] big;
    final short[] dest;
}

class BenchmarkContainer {
    BenchmarkContainer(short[] content) {
        this.content = content;
        this.length = content.length;
    }
    final short[] content;
    final int length;
}

/**
 * Deterministic generator for benchmark data.
 * For given *param* it generates *howmany* entries
 */
class BenchmarkDataGenerator {
    static BenchmarkData generate(int param, int howMany) {
        IntegerDistribution ud = new UniformIntegerDistribution(new Well19937c(param + 17), Short.MIN_VALUE, Short.MAX_VALUE);
        IntegerDistribution p = new UniformIntegerDistribution(new Well19937c(param + 123), SMALLEST_ARRAY, BIGGEST_ARRAY / param);
        BenchmarkContainer[] smalls = new BenchmarkContainer[howMany];
        BenchmarkContainer[] bigs =  new BenchmarkContainer[howMany];
        for (int i = 0; i < howMany; i++) {
            int smallSize = p.sample();
            int bigSize = smallSize * param;
            short[] small = intArrayToShortArraySorted(ud.sample(smallSize));
            short[] big = intArrayToShortArraySorted(ud.sample(bigSize));
            smalls[i] = new BenchmarkContainer(small);
            bigs[i] = new BenchmarkContainer(big);
        }
        return new BenchmarkData(smalls, bigs);
    }

    private static short[] intArrayToShortArraySorted(int[] source) {
        short[] result = new short[source.length];
        for (int i = 0; i < source.length; i++) {
            result[i] = (short) source[i];
        }
        Arrays.sort(result);
        return result;
    }

    private final static int SMALLEST_ARRAY = 50;
    private final static int BIGGEST_ARRAY = 2 * Short.MAX_VALUE;
}


