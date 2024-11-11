package org.roaringbitmap.realdata;

import static org.roaringbitmap.RealDataset.CENSUS1881;
import static org.roaringbitmap.RealDataset.CENSUS1881_SRT;
import static org.roaringbitmap.RealDataset.CENSUS_INCOME;
import static org.roaringbitmap.RealDataset.CENSUS_INCOME_SRT;
import static org.roaringbitmap.RealDataset.DIMENSION_003;
import static org.roaringbitmap.RealDataset.DIMENSION_008;
import static org.roaringbitmap.RealDataset.DIMENSION_033;
import static org.roaringbitmap.RealDataset.USCENSUS2000;
import static org.roaringbitmap.RealDataset.WEATHER_SEPT_85;
import static org.roaringbitmap.RealDataset.WEATHER_SEPT_85_SRT;
import static org.roaringbitmap.RealDataset.WIKILEAKS_NOQUOTES;
import static org.roaringbitmap.RealDataset.WIKILEAKS_NOQUOTES_SRT;

import org.roaringbitmap.FastAggregation;
import org.roaringbitmap.ParallelAggregation;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.ZipRealDataRetriever;
import org.roaringbitmap.buffer.BufferFastAggregation;
import org.roaringbitmap.buffer.BufferParallelAggregation;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class ParallelAggregatorBenchmark {

  private static final Cache<String, RoaringBitmap[]> DATASET_CACHE =
      CacheBuilder.newBuilder().maximumSize(1).build();

  @Param({ // putting the data sets in alpha. order
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

  RoaringBitmap[] bitmaps;
  ImmutableRoaringBitmap[] immutableRoaringBitmaps;

  @Setup(Level.Trial)
  public void setup() throws Exception {
    bitmaps =
        DATASET_CACHE.get(
            dataset,
            () -> {
              System.out.println("Loading" + dataset);
              ZipRealDataRetriever dataRetriever = new ZipRealDataRetriever(dataset);
              return StreamSupport.stream(dataRetriever.fetchBitPositions().spliterator(), false)
                  .map(RoaringBitmap::bitmapOf)
                  .toArray(RoaringBitmap[]::new);
            });
    immutableRoaringBitmaps =
        Arrays.stream(bitmaps)
            .map(RoaringBitmap::toMutableRoaringBitmap)
            .toArray(ImmutableRoaringBitmap[]::new);
  }

  @Benchmark
  public RoaringBitmap parallelOr() {
    return ParallelAggregation.or(bitmaps);
  }

  @Benchmark
  public RoaringBitmap parallelXor() {
    return ParallelAggregation.xor(bitmaps);
  }

  @Benchmark
  public Object groupByKey() {
    return ParallelAggregation.groupByKey(bitmaps);
  }

  @Benchmark
  public RoaringBitmap fastOr() {
    return FastAggregation.or(bitmaps);
  }

  @Benchmark
  public RoaringBitmap fastAnd() {
    return FastAggregation.and(bitmaps);
  }

  @Benchmark
  public RoaringBitmap fastXor() {
    return FastAggregation.xor(bitmaps);
  }

  @Benchmark
  public MutableRoaringBitmap bufferParallelOr() {
    return BufferParallelAggregation.or(immutableRoaringBitmaps);
  }

  @Benchmark
  public MutableRoaringBitmap bufferParallelXor() {
    return BufferParallelAggregation.xor(immutableRoaringBitmaps);
  }

  @Benchmark
  public Object bufferGroupByKey() {
    return BufferParallelAggregation.groupByKey(immutableRoaringBitmaps);
  }

  @Benchmark
  public MutableRoaringBitmap bufferFastOr() {
    return BufferFastAggregation.or(immutableRoaringBitmaps);
  }

  @Benchmark
  public MutableRoaringBitmap bufferFastXor() {
    return BufferFastAggregation.xor(immutableRoaringBitmaps);
  }
}
