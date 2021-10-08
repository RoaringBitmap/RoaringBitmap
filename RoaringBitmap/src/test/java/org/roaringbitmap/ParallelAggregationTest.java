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
public class ParallelAggregationTest {

  private static ForkJoinPool POOL;

  private static ForkJoinPool NO_PARALLELISM_AVAILABLE;

  @BeforeAll
  public static void init() {
    POOL = new ForkJoinPool(8);
    NO_PARALLELISM_AVAILABLE = new ForkJoinPool(1);
  }

  @AfterAll
  public static void teardown() {
    POOL.shutdownNow();
    NO_PARALLELISM_AVAILABLE.shutdownNow();
  }

  @Test
  public void singleContainerOR() {
    RoaringBitmap one = testCase().withRunAt(0).build();
    RoaringBitmap two = testCase().withBitmapAt(0).build();
    RoaringBitmap three = testCase().withArrayAt(0).build();
    assertEquals(FastAggregation.or(one, two, three), ParallelAggregation.or(one, two, three));
  }

  @Test
  public void twoContainerOR() {
    RoaringBitmap one = testCase().withRunAt(0).withArrayAt(1).build();
    RoaringBitmap two = testCase().withBitmapAt(1).build();
    RoaringBitmap three = testCase().withArrayAt(1).build();
    assertEquals(FastAggregation.or(one, two, three), ParallelAggregation.or(one, two, three));
  }

  @Test
  public void disjointOR() {
    RoaringBitmap one = testCase().withRunAt(0).withArrayAt(2).build();
    RoaringBitmap two = testCase().withBitmapAt(1).build();
    RoaringBitmap three = testCase().withArrayAt(3).build();
    assertEquals(FastAggregation.or(one, two, three), ParallelAggregation.or(one, two, three));
  }


  @Test
  public void wideOr() {
    RoaringBitmap[] input = IntStream.range(0, 20)
            .mapToObj(i -> testCase().withBitmapAt(0).withArrayAt(1).withRunAt(2).build())
            .toArray(RoaringBitmap[]::new);
    assertEquals(FastAggregation.or(input), ParallelAggregation.or(input));
  }

  @Test
  public void hugeOr1() {
    RoaringBitmap[] input = IntStream.range(0, 513)
            .mapToObj(i -> testCase().withBitmapAt(0).withArrayAt(1).withRunAt(2).build())
            .toArray(RoaringBitmap[]::new);
    assertEquals(FastAggregation.or(input), ParallelAggregation.or(input));
  }


  @Test
  public void hugeOr2() {
    RoaringBitmap[] input = IntStream.range(0, 1999)
            .mapToObj(i -> testCase().withBitmapAt(0).withArrayAt(1).withRunAt(2).build())
            .toArray(RoaringBitmap[]::new);
    assertEquals(FastAggregation.or(input), ParallelAggregation.or(input));
  }

  @Test
  public void hugeOr3() {
    RoaringBitmap[] input = IntStream.range(0, 4096)
            .mapToObj(i -> testCase().withBitmapAt(0).withArrayAt(1).withRunAt(2).build())
            .toArray(RoaringBitmap[]::new);
    assertEquals(FastAggregation.or(input), ParallelAggregation.or(input));
  }

  @Test
  public void hugeOrNoParallelismAvailable1() {
    RoaringBitmap[] input = IntStream.range(0, 513)
            .mapToObj(i -> testCase().withBitmapAt(0).withArrayAt(1).withRunAt(2).build())
            .toArray(RoaringBitmap[]::new);
    assertEquals(FastAggregation.or(input),
            NO_PARALLELISM_AVAILABLE.submit(() -> ParallelAggregation.or(input)).join());
  }


  @Test
  public void hugeOrNoParallelismAvailable2() {
    RoaringBitmap[] input = IntStream.range(0, 2000)
            .mapToObj(i -> testCase().withBitmapAt(0).withArrayAt(1).withRunAt(2).build())
            .toArray(RoaringBitmap[]::new);
    assertEquals(FastAggregation.or(input),
            NO_PARALLELISM_AVAILABLE.submit(() -> ParallelAggregation.or(input)).join());
  }

  @Test
  public void hugeOrNoParallelismAvailable3() {
    RoaringBitmap[] input = IntStream.range(0, 4096)
            .mapToObj(i -> testCase().withBitmapAt(0).withArrayAt(1).withRunAt(2).build())
            .toArray(RoaringBitmap[]::new);
    assertEquals(FastAggregation.or(input),
            NO_PARALLELISM_AVAILABLE.submit(() -> ParallelAggregation.or(input)).join());
  }

  @Test
  public void hugeOrInFJP1() {
    RoaringBitmap[] input = IntStream.range(0, 513)
            .mapToObj(i -> testCase().withBitmapAt(0).withArrayAt(1).withRunAt(2).build())
            .toArray(RoaringBitmap[]::new);
    assertEquals(FastAggregation.or(input),
            POOL.submit(() -> ParallelAggregation.or(input)).join());
  }

  @Test
  public void hugeOrInFJP2() {
    RoaringBitmap[] input = IntStream.range(0, 2000)
            .mapToObj(i -> testCase().withBitmapAt(0).withArrayAt(1).withRunAt(2).build())
            .toArray(RoaringBitmap[]::new);
    assertEquals(FastAggregation.or(input),
            POOL.submit(() -> ParallelAggregation.or(input)).join());
  }

  @Test
  public void hugeOrInFJP3() {
    RoaringBitmap[] input = IntStream.range(0, 4096)
            .mapToObj(i -> testCase().withBitmapAt(0).withArrayAt(1).withRunAt(2).build())
            .toArray(RoaringBitmap[]::new);
    assertEquals(FastAggregation.or(input),
            POOL.submit(() -> ParallelAggregation.or(input)).join());
  }

  @Test
  public void disjointBigKeysOR() {
    RoaringBitmap one = testCase().withRunAt(0).withArrayAt(2).withBitmapAt((1 << 15) | 1).build();
    RoaringBitmap two = testCase().withBitmapAt(1).withRunAt((1 << 15) | 2).build();
    RoaringBitmap three = testCase().withArrayAt(3).withRunAt((1 << 15) | 3).build();
    assertEquals(FastAggregation.or(one, two, three), ParallelAggregation.or(one, two, three));
  }

  @Test
  public void singleContainerXOR() {
    RoaringBitmap one = testCase().withRunAt(0).build();
    RoaringBitmap two = testCase().withBitmapAt(0).build();
    RoaringBitmap three = testCase().withArrayAt(0).build();
    assertEquals(FastAggregation.xor(one, two, three), ParallelAggregation.xor(one, two, three));
  }


  @Test
  public void missingMiddleContainerXOR() {
    RoaringBitmap one = testCase().withRunAt(0).withBitmapAt(1).withArrayAt(2).build();
    RoaringBitmap two = testCase().withBitmapAt(0).withArrayAt(2).build();
    RoaringBitmap three = testCase().withArrayAt(0).withBitmapAt(1).withArrayAt(2).build();
    assertEquals(FastAggregation.xor(one, two, three), ParallelAggregation.xor(one, two, three));
  }

  @Test
  public void twoContainerXOR() {
    RoaringBitmap one = testCase().withRunAt(0).withArrayAt(1).build();
    RoaringBitmap two = testCase().withBitmapAt(1).build();
    RoaringBitmap three = testCase().withArrayAt(1).build();
    assertEquals(FastAggregation.xor(one, two, three), ParallelAggregation.xor(one, two, three));
  }

  @Test
  public void disjointXOR() {
    RoaringBitmap one = testCase().withRunAt(0).withArrayAt(2).build();
    RoaringBitmap two = testCase().withBitmapAt(1).build();
    RoaringBitmap three = testCase().withArrayAt(3).build();
    assertEquals(FastAggregation.xor(one, two, three), ParallelAggregation.xor(one, two, three));
  }

  @Test
  public void disjointBigKeysXOR() {
    RoaringBitmap one = testCase().withRunAt(0).withArrayAt(2).withBitmapAt((1 << 15) | 1).build();
    RoaringBitmap two = testCase().withBitmapAt(1).withRunAt((1 << 15) | 2).build();
    RoaringBitmap three = testCase().withArrayAt(3).withRunAt((1 << 15) | 3).build();
    assertEquals(FastAggregation.xor(one, two, three), ParallelAggregation.xor(one, two, three));
  }


}
