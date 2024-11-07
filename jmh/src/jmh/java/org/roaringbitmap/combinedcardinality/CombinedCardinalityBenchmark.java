package org.roaringbitmap.combinedcardinality;

import org.roaringbitmap.RandomData;
import org.roaringbitmap.RoaringBitmap;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.concurrent.TimeUnit;

@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(
    value = 1,
    jvmArgsPrepend = {
      "-XX:-TieredCompilation",
      "-XX:+UseParallelGC",
      "-mx2G",
      "-ms2G",
      "-XX:+AlwaysPreTouch"
    })
@State(Scope.Benchmark)
public class CombinedCardinalityBenchmark {

  public enum Scenario {
    EQUAL {
      @Override
      RoaringBitmap[] bitmaps() {
        RoaringBitmap bitmap = RandomData.randomBitmap(1 << 12, 0.2, 0.3);
        return new RoaringBitmap[] {bitmap, bitmap.clone()};
      }
    },
    SHIFTED {
      @Override
      RoaringBitmap[] bitmaps() {
        RoaringBitmap bitmap = RandomData.randomBitmap(1 << 12, 0.2, 0.3);
        return new RoaringBitmap[] {bitmap, RoaringBitmap.addOffset(bitmap, 1 << 16)};
      }
    },
    SMALL_LARGE {
      @Override
      RoaringBitmap[] bitmaps() {
        return new RoaringBitmap[] {
          RandomData.randomBitmap(1 << 4, 0.2, 0.3), RandomData.randomBitmap(1 << 12, 0.2, 0.3)
        };
      }
    },
    LARGE_SMALL {
      @Override
      RoaringBitmap[] bitmaps() {
        return new RoaringBitmap[] {
          RandomData.randomBitmap(1 << 12, 0.2, 0.3), RandomData.randomBitmap(1 << 4, 0.2, 0.3)
        };
      }
    };

    abstract RoaringBitmap[] bitmaps();
  }

  @Param Scenario scenario;

  RoaringBitmap left;
  RoaringBitmap right;

  @Setup(Level.Trial)
  public void init() {
    RoaringBitmap[] bitmaps = scenario.bitmaps();
    left = bitmaps[0];
    right = bitmaps[1];
  }

  @Benchmark
  public int xorCardinality() {
    return RoaringBitmap.xorCardinality(left, right);
  }

  @Benchmark
  public int xorCardinalityBaseline() {
    return left.getCardinality()
        + right.getCardinality()
        - 2 * RoaringBitmap.andCardinality(left, right);
  }

  @Benchmark
  public int andNotCardinality() {
    return RoaringBitmap.andNotCardinality(left, right);
  }

  @Benchmark
  public int andNotCardinalityBaseline() {
    return left.getCardinality() - RoaringBitmap.andCardinality(left, right);
  }

  @Benchmark
  public int orCardinality() {
    return RoaringBitmap.orCardinality(left, right);
  }

  @Benchmark
  public int orCardinalityBaseline() {
    return left.getCardinality()
        + right.getCardinality()
        - RoaringBitmap.andCardinality(left, right);
  }
}
