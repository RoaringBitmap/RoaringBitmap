package org.roaringbitmap.rangebitmap;

import org.roaringbitmap.RangeBitmap;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.RoaringBitmapWriter;
import org.roaringbitmap.bsi.BitmapSliceIndex;
import org.roaringbitmap.bsi.RoaringBitmapSliceIndex;

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

import java.util.Arrays;
import java.util.Random;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

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
public class RangeBitmapBenchmark {

  @Param({"NORMAL(100000,10)", "UNIFORM(0,100000)", "UNIFORM(1000000,1100000)", "EXP(0.0001)"})
  String distribution;

  @Param("10000000")
  int rows;

  @Param("42")
  long seed;

  long threshold;

  private RangeBitmap rangeBitmap;
  private Base2ReferenceImplementation referenceImplementation;
  private RoaringBitmapSliceIndex bsi;

  @Setup(Level.Trial)
  public void setup() {
    LongSupplier data = Distribution.parse(seed, distribution);
    long maxValue = Long.MIN_VALUE;
    long[] values = new long[rows];
    for (int i = 0; i < rows; ++i) {
      values[i] = data.getAsLong();
      maxValue = Math.max(values[i], maxValue);
    }
    bsi = new RoaringBitmapSliceIndex();
    Base2ReferenceImplementation.Builder referenceImplementationBuilder =
        Base2ReferenceImplementation.builder();
    RangeBitmap.Appender appender = RangeBitmap.appender(maxValue);
    int lz = Long.numberOfLeadingZeros(maxValue);
    long mask = (1L << lz) - 1;
    int rid = 0;
    for (long value : values) {
      referenceImplementationBuilder.add(value);
      appender.add(value);
      bsi.setValue(rid++, (int) (value & mask));
    }
    this.referenceImplementation = referenceImplementationBuilder.seal();
    this.rangeBitmap = appender.build();
    this.threshold = maxValue >>> 1;
  }

  @Benchmark
  public RoaringBitmap referenceBase2() {
    // this is about as good as a user can do from outside the library
    return referenceImplementation.lessThanOrEqualTo(threshold);
  }

  @Benchmark
  public RoaringBitmap bsi() {
    // this is about as good as a user can do from outside the library
    return bsi.compare(BitmapSliceIndex.Operation.LE, (int) threshold, 0, null);
  }

  @Benchmark
  public RoaringBitmap rangeBitmap() {
    return rangeBitmap.lte(threshold);
  }

  @Benchmark
  public RoaringBitmap rangeBitmapBetweenContextual() {
    return rangeBitmap.gte(threshold - 1, rangeBitmap.lte(threshold + 1));
  }

  @Benchmark
  public RoaringBitmap rangeBitmapBetween() {
    return rangeBitmap.between(threshold - 1, threshold + 1);
  }

  @Benchmark
  public RoaringBitmap rangeBitmapBetweenNonContextual() {
    RoaringBitmap gte = rangeBitmap.gte(threshold - 1);
    RoaringBitmap lte = rangeBitmap.lte(threshold + 1);
    gte.and(lte);
    return gte;
  }

  public static class Base2ReferenceImplementation {

    public static Builder builder() {
      return new Builder();
    }

    Base2ReferenceImplementation(long maxRid, RoaringBitmap[] bitmaps) {
      this.maxRid = maxRid;
      this.bitmaps = bitmaps;
    }

    private final long maxRid;
    private final RoaringBitmap[] bitmaps;

    public RoaringBitmap lessThanOrEqualTo(long threshold) {
      if (63 - Long.numberOfLeadingZeros(threshold) > bitmaps.length) {
        return all();
      }
      RoaringBitmap bitmap = (threshold & 1) == 0 ? bitmaps[0].clone() : all();
      long mask = 2;
      for (int i = 1; i < bitmaps.length; ++i) {
        if ((threshold & mask) != mask) {
          bitmap.and(bitmaps[i]);
        } else {
          bitmap.or(bitmaps[i]);
        }
        mask <<= 1;
      }
      return bitmap;
    }

    private RoaringBitmap all() {
      RoaringBitmap all = new RoaringBitmap();
      all.add(0, maxRid);
      return all;
    }

    private static final class Builder {

      private final RoaringBitmapWriter<RoaringBitmap>[] writers;
      private int rid = 0;
      private long mask = 0;

      @SuppressWarnings("unchecked")
      public Builder() {
        writers = new RoaringBitmapWriter[64];
        Arrays.setAll(writers, i -> RoaringBitmapWriter.writer().runCompress(true).get());
      }

      public void add(long value) {
        long bits = ~value;
        mask |= value;
        while (bits != 0) {
          int index = Long.numberOfTrailingZeros(bits);
          writers[index].add(rid);
          bits &= (bits - 1);
        }
        rid++;
      }

      public Base2ReferenceImplementation seal() {
        int numDiscarded = Long.numberOfLeadingZeros(mask);
        RoaringBitmap[] bitmaps = new RoaringBitmap[writers.length - numDiscarded];
        for (int i = 0; i < bitmaps.length; ++i) {
          bitmaps[i] = writers[i].get();
        }
        return new Base2ReferenceImplementation(rid, bitmaps);
      }
    }
  }

  public enum Distribution {
    UNIFORM {
      LongSupplier of(long seed, double... params) {
        long min = (long) params[0];
        long max = (long) params[1];
        SplittableRandom random = new SplittableRandom(seed);
        return () -> random.nextLong(min, max);
      }
    },
    NORMAL {
      LongSupplier of(long seed, double... params) {
        double mean = params[0];
        double stddev = params[1];
        Random random = new Random(seed);
        return () -> (long) (stddev * random.nextGaussian() + mean);
      }
    },
    EXP {
      LongSupplier of(long seed, double... params) {
        double rate = params[0];
        SplittableRandom random = new SplittableRandom(seed);
        return () -> (long) -(Math.log(random.nextDouble()) / rate);
      }
    };

    abstract LongSupplier of(long seed, double... params);

    public static LongSupplier parse(long seed, String spec) {
      int paramsStart = spec.indexOf('(');
      int paramsEnd = spec.indexOf(')');
      double[] params =
          Arrays.stream(spec.substring(paramsStart + 1, paramsEnd).split(","))
              .mapToDouble(s -> Double.parseDouble(s.trim()))
              .toArray();
      String dist = spec.substring(0, paramsStart).toUpperCase();
      return Distribution.valueOf(dist).of(seed, params);
    }
  }
}
