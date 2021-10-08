package org.roaringbitmap;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.concurrent.ForkJoinPool;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.roaringbitmap.SeededTestData.TestDataSet.testCase;

@Execution(ExecutionMode.CONCURRENT)
public class ParallelAggregationBigPoolTest {

  private static ForkJoinPool BIG_POOL;

  @BeforeAll
  public static void init() {
    BIG_POOL = new ForkJoinPool(8);
  }

  @AfterAll
  public static void teardown() {
    BIG_POOL.shutdownNow();
  }

  @Test
  public void regressionTest() {
    RoaringBitmap[] input = IntStream.range(0, 513)
            .mapToObj(i -> testCase().withBitmapAt(0).withArrayAt(1).withRunAt(2).build())
            .toArray(RoaringBitmap[]::new);
    assertEquals(FastAggregation.or(input),
            BIG_POOL.submit(() -> ParallelAggregation.or(input)).join());
  }
}
