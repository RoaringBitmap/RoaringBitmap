package org.roaringbitmap.realdata;

import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.ZipRealDataRetriever;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class RoaringBenchmarkQuick {

  private static final String CENSUS_INCOME = "census-income";
  private static final String CENSUS1881 = "census1881";
  private static final String DIMENSION_008 = "dimension_008";
  private static final String DIMENSION_003 = "dimension_003";
  private static final String DIMENSION_033 = "dimension_033";
  private static final String USCENSUS2000 = "uscensus2000";
  private static final String WEATHER_SEPT_85 = "weather_sept_85";
  private static final String WIKILEAKS_NOQUOTES = "wikileaks-noquotes";
  private static final String CENSUS_INCOME_SRT = "census-income_srt";
  private static final String CENSUS1881_SRT = "census1881_srt";
  private static final String WEATHER_SEPT_85_SRT = "weather_sept_85_srt";
  private static final String WIKILEAKS_NOQUOTES_SRT = "wikileaks-noquotes_srt";

  @State(Scope.Benchmark)
  public static class BenchmarkState {

    @Param({
      CENSUS_INCOME,
      CENSUS1881,
      DIMENSION_008,
      DIMENSION_003,
      DIMENSION_033,
      USCENSUS2000,
      WEATHER_SEPT_85,
      WIKILEAKS_NOQUOTES,
      CENSUS_INCOME_SRT,
      CENSUS1881_SRT,
      WEATHER_SEPT_85_SRT,
      WIKILEAKS_NOQUOTES_SRT
    })
    public String dataset;

    @Param({"128"})
    public int maxBitmaps;

    List<RoaringBitmap> roaring;
    List<ImmutableRoaringBitmap> immutable;
    int maxValue;

    @Setup(Level.Trial)
    public void setup() throws Exception {
      ZipRealDataRetriever zip = new ZipRealDataRetriever(dataset);
      List<int[]> data = zip.fetchBitPositions();

      int limit = Math.min(maxBitmaps, data.size());
      roaring = new ArrayList<RoaringBitmap>(limit);
      immutable = new ArrayList<ImmutableRoaringBitmap>(limit);

      int max = 0;
      for (int i = 0; i < limit; ++i) {
        int[] values = data.get(i);
        int last = values[values.length - 1];
        if (last > max) {
          max = last;
        }

        RoaringBitmap rb = RoaringBitmap.bitmapOf(values);
        rb.runOptimize();
        roaring.add(rb);

        MutableRoaringBitmap mb = MutableRoaringBitmap.bitmapOf(values);
        mb.runOptimize();
        immutable.add(mb);
      }
      maxValue = max;
    }
  }

  @Benchmark
  public void successiveAndRoaring(BenchmarkState state, Blackhole bh) {
    long total = 0;
    List<RoaringBitmap> bitmaps = state.roaring;
    for (int i = 0; i + 1 < bitmaps.size(); ++i) {
      total += RoaringBitmap.and(bitmaps.get(i), bitmaps.get(i + 1)).getCardinality();
    }
    bh.consume(total);
  }

  @Benchmark
  public void successiveAndImmutable(BenchmarkState state, Blackhole bh) {
    long total = 0;
    List<ImmutableRoaringBitmap> bitmaps = state.immutable;
    for (int i = 0; i + 1 < bitmaps.size(); ++i) {
      total += ImmutableRoaringBitmap.and(bitmaps.get(i), bitmaps.get(i + 1)).getCardinality();
    }
    bh.consume(total);
  }

  @Benchmark
  public void successiveOrRoaring(BenchmarkState state, Blackhole bh) {
    long total = 0;
    List<RoaringBitmap> bitmaps = state.roaring;
    for (int i = 0; i + 1 < bitmaps.size(); ++i) {
      total += RoaringBitmap.or(bitmaps.get(i), bitmaps.get(i + 1)).getCardinality();
    }
    bh.consume(total);
  }

  @Benchmark
  public void successiveOrImmutable(BenchmarkState state, Blackhole bh) {
    long total = 0;
    List<ImmutableRoaringBitmap> bitmaps = state.immutable;
    for (int i = 0; i + 1 < bitmaps.size(); ++i) {
      total += ImmutableRoaringBitmap.or(bitmaps.get(i), bitmaps.get(i + 1)).getCardinality();
    }
    bh.consume(total);
  }

  @Benchmark
  public void wideOrRoaring(BenchmarkState state, Blackhole bh) {
    int total = RoaringBitmap.or(state.roaring.iterator()).getCardinality();
    bh.consume(total);
  }

  @Benchmark
  public void wideOrImmutable(BenchmarkState state, Blackhole bh) {
    int total = ImmutableRoaringBitmap.or(state.immutable.iterator()).getCardinality();
    bh.consume(total);
  }

  @Benchmark
  public void containsRoaring(BenchmarkState state, Blackhole bh) {
    int quartcount = 0;
    int q1 = state.maxValue / 4;
    int q2 = state.maxValue / 2;
    int q3 = 3 * state.maxValue / 4;
    for (RoaringBitmap rb : state.roaring) {
      if (rb.contains(q1)) {
        ++quartcount;
      }
      if (rb.contains(q2)) {
        ++quartcount;
      }
      if (rb.contains(q3)) {
        ++quartcount;
      }
    }
    bh.consume(quartcount);
  }

  @Benchmark
  public void containsImmutable(BenchmarkState state, Blackhole bh) {
    int quartcount = 0;
    int q1 = state.maxValue / 4;
    int q2 = state.maxValue / 2;
    int q3 = 3 * state.maxValue / 4;
    for (ImmutableRoaringBitmap rb : state.immutable) {
      if (rb.contains(q1)) {
        ++quartcount;
      }
      if (rb.contains(q2)) {
        ++quartcount;
      }
      if (rb.contains(q3)) {
        ++quartcount;
      }
    }
    bh.consume(quartcount);
  }
}
