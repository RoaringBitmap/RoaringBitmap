package org.roaringbitmap.arraycontainer;

import org.roaringbitmap.ArrayContainer;
import org.roaringbitmap.ZipRealDataRangeRetriever;
import org.roaringbitmap.buffer.MappeableArrayContainer;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class AddBenchmark {

  private ArrayContainer ac1;
  private MappeableArrayContainer mac1;
  private Cache<String, List<int[][]>> DATASET_CACHE;
  private final String dataset = "random_range";
  private List<int[][]> ints;

  @Setup
  public void setup() throws ExecutionException {
    ac1 = new ArrayContainer();
    mac1 = new MappeableArrayContainer();
    DATASET_CACHE = CacheBuilder.newBuilder().maximumSize(1).build();
    ints =
        DATASET_CACHE.get(
            dataset,
            new Callable<List<int[][]>>() {

              @Override
              public List<int[][]> call() throws Exception {
                System.out.println("Loading" + dataset);
                ZipRealDataRangeRetriever<int[][]> dataRetriever =
                    new ZipRealDataRangeRetriever<>(dataset, "/random-generated-data/");

                return Lists.newArrayList(dataRetriever.fetchNextRange());
              }
            });
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public ArrayContainer add() throws ExecutionException {

    for (int[][] i : ints) {
      for (int[] j : i) {
        ac1.iadd(j[0], j[1]);
      }
    }
    return ac1;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public MappeableArrayContainer addBuffer() throws ExecutionException {

    for (int[][] i : ints) {
      for (int[] j : i) {
        mac1.iadd(j[0], j[1]);
      }
    }
    return mac1;
  }
}
