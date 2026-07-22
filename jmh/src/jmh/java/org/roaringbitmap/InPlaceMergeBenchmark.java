package org.roaringbitmap;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.concurrent.TimeUnit;

/**
 * Benchmarks the in-place union/xor/lazy-union merge paths of {@link RoaringBitmap}.
 *
 * <p>The {@code interleaved} pattern (left holds the even high-keys, right the odd ones) forces a
 * source-only container to be inserted between every pair of receiver containers -- the quadratic
 * case a single bulk merge pass should win. The {@code append} pattern (right's keys all follow
 * left's) is a control: it never inserts in the interior, so both strategies take the tail path.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
public class InPlaceMergeBenchmark {

  @Param({"64", "1024", "4096", "16384"})
  public int containers;

  @Param({"interleaved", "append"})
  public String pattern;

  // Templates: cloned per invocation because or/xor mutate the receiver. Clone cost is identical
  // for every merge strategy, so it does not bias the comparison.
  private RoaringBitmap leftTemplate;
  private RoaringBitmap right;

  private static RoaringBitmap withKeys(int start, int count, int step, int low) {
    RoaringBitmap b = new RoaringBitmap();
    for (int i = 0; i < count; i++) {
      int key = start + i * step;
      b.add((key << 16) | low);
    }
    return b;
  }

  @Setup
  public void setup() {
    if ("interleaved".equals(pattern)) {
      // left: 0,2,4,...   right: 1,3,5,...  -> one interior insert per source container
      leftTemplate = withKeys(0, containers, 2, 5);
      right = withKeys(1, containers, 2, 7);
    } else { // append: right's keys all follow left's -> pure tail append, no interior work
      leftTemplate = withKeys(0, containers, 1, 5);
      right = withKeys(containers, containers, 1, 7);
    }
  }

  @Benchmark
  public int or() {
    RoaringBitmap receiver = leftTemplate.clone();
    receiver.or(right);
    return receiver.getCardinality();
  }

  @Benchmark
  public int xor() {
    RoaringBitmap receiver = leftTemplate.clone();
    receiver.xor(right);
    return receiver.getCardinality();
  }

  @Benchmark
  public int naivelazyor() {
    RoaringBitmap receiver = leftTemplate.clone();
    receiver.naivelazyor(right);
    receiver.repairAfterLazy();
    return receiver.getCardinality();
  }
}
