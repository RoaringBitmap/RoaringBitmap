package org.roaringbitmap.buffer;

import static java.util.Arrays.copyOfRange;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.roaringbitmap.ContainerBatchIterator;
import org.roaringbitmap.SeededTestData;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@Execution(ExecutionMode.CONCURRENT)
public class BufferContainerBatchIteratorTest {

  private static int[][] DATA;

  @BeforeAll
  public static void setup() {
    DATA =
        Stream.of(
                IntStream.range(0, 20000).toArray(),
                IntStream.range(0, 1 << 16).toArray(),
                IntStream.range(0, 1 << 16)
                    .filter(i -> i < 500 || i > 2000)
                    .filter(i -> i < (1 << 15) || i > ((1 << 15) | (1 << 8)))
                    .toArray(),
                IntStream.range(0, 1 << 16).filter(i -> ((i >>> 12) & 1) == 0).toArray(),
                IntStream.range(0, 1 << 16).filter(i -> ((i >>> 11) & 1) == 0).toArray(),
                IntStream.range(0, 1 << 16).filter(i -> ((i >>> 10) & 1) == 0).toArray(),
                IntStream.range(0, 1 << 16).filter(i -> ((i >>> 9) & 1) == 0).toArray(),
                IntStream.range(0, 1 << 16).filter(i -> ((i >>> 8) & 1) == 0).toArray(),
                IntStream.range(0, 1 << 16).filter(i -> ((i >>> 7) & 1) == 0).toArray(),
                IntStream.range(0, 1 << 16).filter(i -> ((i >>> 6) & 1) == 0).toArray(),
                IntStream.range(0, 1 << 16).filter(i -> ((i >>> 5) & 1) == 0).toArray(),
                IntStream.range(0, 1 << 16).filter(i -> ((i >>> 4) & 1) == 0).toArray(),
                IntStream.range(0, 1 << 16).filter(i -> ((i >>> 3) & 1) == 0).toArray(),
                IntStream.range(0, 1 << 16).filter(i -> ((i >>> 2) & 1) == 0).toArray(),
                IntStream.range(0, 1 << 16).filter(i -> ((i >>> 1) & 1) == 0).toArray(),
                IntStream.range(0, 1 << 16).filter(i -> (i & 1) == 0).toArray(),
                IntStream.range(0, 1 << 16).filter(i -> (i & 1) == 0).toArray(),
                IntStream.range(0, 1 << 16).filter(i -> (i % 3) == 0).toArray(),
                IntStream.range(0, 1 << 16).filter(i -> (i % 5) == 0).toArray(),
                IntStream.range(0, 1 << 16).filter(i -> (i % 7) == 0).toArray(),
                IntStream.range(0, 1 << 16).filter(i -> (i % 9) == 0).toArray(),
                IntStream.range(0, 1 << 16).filter(i -> (i % 271) == 0).toArray(),
                IntStream.range(0, 1 << 16).filter(i -> (i % 1000) == 0).toArray(),
                IntStream.empty().toArray(),
                SeededTestData.sparseRegion().toArray(),
                SeededTestData.sparseRegion().toArray(),
                SeededTestData.sparseRegion().toArray(),
                SeededTestData.sparseRegion().toArray(),
                SeededTestData.sparseRegion().toArray(),
                SeededTestData.sparseRegion().toArray(),
                SeededTestData.sparseRegion().toArray(),
                SeededTestData.sparseRegion().toArray(),
                SeededTestData.sparseRegion().toArray(),
                SeededTestData.sparseRegion().toArray(),
                SeededTestData.sparseRegion().toArray(),
                SeededTestData.sparseRegion().toArray(),
                SeededTestData.sparseRegion().toArray(),
                SeededTestData.sparseRegion().toArray(),
                SeededTestData.sparseRegion().toArray(),
                SeededTestData.denseRegion().toArray(),
                SeededTestData.denseRegion().toArray(),
                SeededTestData.denseRegion().toArray(),
                SeededTestData.denseRegion().toArray(),
                SeededTestData.denseRegion().toArray(),
                SeededTestData.denseRegion().toArray(),
                SeededTestData.denseRegion().toArray(),
                SeededTestData.denseRegion().toArray(),
                SeededTestData.denseRegion().toArray(),
                SeededTestData.denseRegion().toArray(),
                SeededTestData.denseRegion().toArray(),
                SeededTestData.denseRegion().toArray(),
                SeededTestData.denseRegion().toArray(),
                SeededTestData.denseRegion().toArray(),
                SeededTestData.denseRegion().toArray(),
                SeededTestData.rleRegion().toArray(),
                SeededTestData.rleRegion().toArray(),
                SeededTestData.rleRegion().toArray(),
                SeededTestData.rleRegion().toArray(),
                SeededTestData.rleRegion().toArray(),
                SeededTestData.rleRegion().toArray(),
                SeededTestData.rleRegion().toArray(),
                SeededTestData.rleRegion().toArray(),
                SeededTestData.rleRegion().toArray(),
                SeededTestData.rleRegion().toArray(),
                SeededTestData.rleRegion().toArray(),
                SeededTestData.rleRegion().toArray(),
                SeededTestData.rleRegion().toArray(),
                SeededTestData.rleRegion().toArray(),
                SeededTestData.rleRegion().toArray())
            .toArray(int[][]::new);
  }

  @AfterAll
  public static void clear() {
    DATA = null;
  }

  public static Stream<Arguments> params() {
    return Stream.of(DATA)
        .flatMap(
            array ->
                IntStream.concat(
                        IntStream.of(512, 1024, 2048, 4096, 8192, 65536),
                        IntStream.range(0, 100)
                            .map(i -> ThreadLocalRandom.current().nextInt(1, 65536)))
                    .mapToObj(i -> Arguments.of(array, i)));
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("params")
  public void test(int[] expectedValues, int batchSize) {
    int[] buffer = new int[batchSize];
    MappeableContainer container = createContainer(expectedValues);
    ContainerBatchIterator it = container.getBatchIterator();
    int cardinality = 0;
    while (it.hasNext()) {
      int from = cardinality;
      cardinality += it.next(0, buffer);
      assertArrayEquals(
          copyOfRange(expectedValues, from, cardinality),
          copyOfRange(buffer, 0, cardinality - from),
          "Failure with batch size " + batchSize);
    }
    assertEquals(expectedValues.length, cardinality);
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("params")
  public void testAdvanceIfNeeded(int[] expectedValues, int batchSize) {
    if (expectedValues.length < 2) {
      return;
    }
    int[] buffer = new int[batchSize];
    MappeableContainer container = createContainer(expectedValues);
    ContainerBatchIterator it = container.getBatchIterator();
    int cardinality = expectedValues.length / 2;
    int advanceUntil = expectedValues[cardinality];
    it.advanceIfNeeded((char) advanceUntil);
    while (it.hasNext()) {
      int from = cardinality;
      cardinality += it.next(0, buffer);
      assertArrayEquals(
          copyOfRange(expectedValues, from, cardinality),
          copyOfRange(buffer, 0, cardinality - from),
          "Failure with batch size "
              + batchSize
              + " and container type "
              + container.getContainerName());
    }
    assertEquals(expectedValues.length, cardinality);
  }

  private MappeableContainer createContainer(int[] expectedValues) {
    MappeableContainer container = new MappeableArrayContainer();
    for (int value : expectedValues) {
      container = container.add((char) value);
    }
    return container.runOptimize();
  }
}
