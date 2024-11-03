package org.roaringbitmap.range;

import org.roaringbitmap.RandomData;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.Util;

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
      "-XX:+UseSerialGC",
      "-mx2G",
      "-ms2G",
      "-XX:+AlwaysPreTouch"
    })
@State(Scope.Benchmark)
public class ContainsRange {

  @Param({"8", "32", "8192"})
  int keys;

  @Param({"START", "END"})
  Scenario scenario;

  @Param({"true", "false"})
  boolean match;

  public enum Scenario {
    START {
      @Override
      long getMin(RoaringBitmap bitmap) {
        return Util.toUnsignedLong(bitmap.first());
      }

      @Override
      long getSup(RoaringBitmap bitmap) {
        return Util.toUnsignedLong(bitmap.first()) + bitmap.getLongCardinality() / 10;
      }
    },
    END {
      @Override
      long getMin(RoaringBitmap bitmap) {
        return Util.toUnsignedLong(bitmap.last()) - bitmap.getLongCardinality() / 10;
      }

      @Override
      long getSup(RoaringBitmap bitmap) {
        return Util.toUnsignedLong(bitmap.last()) - 1;
      }
    };

    abstract long getMin(RoaringBitmap bitmap);

    abstract long getSup(RoaringBitmap bitmap);
  }

  private RoaringBitmap bitmap;

  private long min;
  private long sup;

  @Setup(Level.Trial)
  public void init() {
    bitmap = RandomData.randomBitmap(keys, 0.3, 0.2);
    min = scenario.getMin(bitmap);
    sup = scenario.getSup(bitmap);
    if (match) {
      bitmap.add(min, sup);
    } else if (bitmap.contains(min, sup)) {
      bitmap.flip((int) ((min + sup) / 2));
      assert !bitmap.contains(min, sup);
    }
  }

  @Benchmark
  public boolean contains() {
    return bitmap.contains(min, sup);
  }

  @Benchmark
  public boolean containsViaRank() {
    if (!bitmap.contains((int) min) || !bitmap.contains((int) (sup - 1))) {
      return false;
    }
    int startRank = bitmap.rank((int) min);
    int endRank = bitmap.rank((int) (sup - 1));
    return endRank - startRank + 1 == sup - min;
  }
}
