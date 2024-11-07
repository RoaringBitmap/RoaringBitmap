package org.roaringbitmap;

import org.roaringbitmap.longlong.LongUtils;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 3, timeUnit = TimeUnit.MILLISECONDS, time = 1000)
@Measurement(iterations = 5, timeUnit = TimeUnit.MILLISECONDS, time = 1000)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class IntermediateByteArrayBenchmark {

  public long firstV = 0xaa_b3_41_23_22_25_33_43L; // some random value

  @Benchmark
  public int original() {
    byte[] firtBytes = LongUtils.toBDBytes(firstV);
    int unsignedIdx = 0;
    for (int i = 0; i < 8; i++) {
      byte v = firtBytes[i];
      unsignedIdx += Byte.toUnsignedInt(v);
      // some other stuff...
    }
    return unsignedIdx;
  }

  @Benchmark
  public int optimized() {
    int unsignedIdx = 0;
    for (int i = 0; i < 8; i++) {
      unsignedIdx += Byte.toUnsignedInt((byte) (firstV >>> ((7 - i) << 3)));
      // some other stuff...
    }
    return unsignedIdx;
  }
}
