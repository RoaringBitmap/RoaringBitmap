package org.roaringbitmap;


import static java.util.Arrays.copyOfRange;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@Execution(ExecutionMode.CONCURRENT)
public class ContainerBatchIteratorTest {


  public static Stream<Arguments> params() {
    return Stream.of(
        IntStream.range(0, 20000).toArray(),
        IntStream.range(0, 1 << 16).toArray(),
        IntStream.range(0, 1 << 16).filter(i -> i < 500 || i > 2000)
            .filter(i -> i < (1 << 15) || i > ((1 << 15) | (1 << 8))).toArray(),
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
        .flatMap(array -> IntStream.concat(IntStream.of(
            512, 1024, 2048, 4096, 8192, 65536
        ), IntStream.range(0, 100).map(i -> ThreadLocalRandom.current().nextInt(1, 65536)))
            .mapToObj(i -> Arguments.of(array, i)));
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("params")
  public void test(int[] expectedValues, int batchSize) {
    int[] buffer = new int[batchSize];
    Container container = createContainer(expectedValues);
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

  private Container createContainer(int[] expectedValues) {
    Container container = new ArrayContainer();
    for (int value : expectedValues) {
      container = container.add((char) value);
    }
    return container.runOptimize();
  }

  @ParameterizedTest
  @MethodSource("streamOfContainersImpl")
  void testEmpty(Container container) {
    int[] buffer = new int[64];

    ContainerBatchIterator it = createContainerBatchIterator(container);

    assertEquals(0, it.next(0, buffer));
    assertRange(0, buffer, 0, 64);
  }

  public static Stream<Arguments> streamOfContainersImpl() {
    return Stream.of(new RunContainer(), new ArrayContainer(), new BitmapContainer())
        .map(Arguments::of);
  }

  private ContainerBatchIterator createContainerBatchIterator(Container container, int... bits) {
    for (int b : bits) {
      container.add((char) b);
    }

    if (container instanceof BitmapContainer) {
      return new BitmapBatchIterator((BitmapContainer) container);
    } else if (container instanceof ArrayContainer) {
      return new ArrayBatchIterator((ArrayContainer) container);
    } else if (container instanceof RunContainer) {
      return new RunBatchIterator((RunContainer) container);
    } else {
      throw new IllegalArgumentException("Unsupported container");
    }
  }

  private void assertRange(int expected, int[] arr, int start, int end) {
    for (int i = start; i < end; i++) {
      assertEquals(expected, arr[i]);
    }
  }

  @ParameterizedTest
  @MethodSource("streamOfContainersImpl")
  void testCollectSingleBitIntoBuffer(Container container) {
    int[] buffer = new int[64];

    ContainerBatchIterator it = createContainerBatchIterator(container, 3);

    assertEquals(1, it.next(0, buffer));
    assertEquals(3, buffer[0]);
    assertRange(0, buffer, 1, 64);
  }

  @ParameterizedTest
  @MethodSource("streamOfContainersImpl")
  void testCollectManyBitsIntoBuffer(Container container) {
    int[] expectedBits = new int[]{0, 3, 5, 500, 1000, 65535};
    int[] buffer = new int[64];

    ContainerBatchIterator it = createContainerBatchIterator(container, expectedBits);

    assertEquals(expectedBits.length, it.next(0, buffer));
    assertArrayEquals(expectedBits, Arrays.copyOfRange(buffer, 0, expectedBits.length));
    assertRange(0, buffer, expectedBits.length, 64);
  }

  @ParameterizedTest
  @MethodSource("streamOfContainersImpl")
  void testConsumeIteratorInMultipleBatches(Container container) {
    container.iadd(50, 100);
    container.iadd(200, 300);
    container.iadd(500, 1000);

    int[] result = new int[650];
    int[] buffer = new int[64];

    ContainerBatchIterator it = createContainerBatchIterator(container);

    int ptr = 0;
    int read;
    while (it.hasNext() && (read = it.next(0, buffer)) > 0) {
      for (int i = 0; i < read; i++) {
        result[ptr++] = buffer[i];
      }
    }

    assertEquals(650, ptr);

    final int[] expected = IntStream
        .concat(IntStream.concat(IntStream.range(50, 100), IntStream.range(200, 300)),
            IntStream.range(500, 1000)).toArray();
    assertArrayEquals(expected, result);
  }

  @ParameterizedTest
  @MethodSource("streamOfContainersImpl")
  void testAdvanceIfNeeded(Container container) {
    ContainerBatchIterator empty = createContainerBatchIterator(container); // no op
    advanceIfNeeded(empty, 0);
    advanceIfNeeded(empty, 1);
    assertNextBit(empty, 0);

    container.clear();

    ContainerBatchIterator before = createContainerBatchIterator(container, 5);
    advanceIfNeeded(before, 4);
    assertEquals(5, assertNextBit(before, 1));

    container.clear();

    ContainerBatchIterator mid = createContainerBatchIterator(container, 5);
    advanceIfNeeded(mid, 5);
    assertEquals(5, assertNextBit(mid, 1));

    container.clear();

    ContainerBatchIterator after = createContainerBatchIterator(container, 5);
    advanceIfNeeded(after, 6);
    assertNextBit(after, 0);
  }

  private void advanceIfNeeded(ContainerBatchIterator iterator, int minval) {
    if (iterator instanceof BitmapBatchIterator) {
      ((BitmapBatchIterator) iterator).advanceIfNeeded((char) minval);
    } else if (iterator instanceof ArrayBatchIterator) {
      ((ArrayBatchIterator) iterator).advanceIfNeeded((char) minval);
    } else if (iterator instanceof RunBatchIterator) {
      ((RunBatchIterator) iterator).advanceIfNeeded((char) minval);
    } else {
      throw new IllegalArgumentException("Unsupported container");
    }
  }

  private int assertNextBit(ContainerBatchIterator it, int read) {
    int[] intbuffer = new int[1];
    assertEquals(read, it.next(0, intbuffer));
    return intbuffer[0];
  }

  @ParameterizedTest
  @MethodSource("streamOfContainersImpl")
  void testAdvanceIfNeededWithinContiguousSegment(Container container) {
    container.iadd(1000, 1500);

    ContainerBatchIterator it = createContainerBatchIterator(container);
    advanceIfNeeded(it, 1400);

    int[] buffer = new int[128];
    int read = it.next(0, buffer);

    assertEquals(100, read);
    assertArrayEquals(IntStream.range(1400, 1500).toArray(), Arrays.copyOfRange(buffer, 0, 100));
    assertRange(0, buffer, 100, 128);
  }

  @ParameterizedTest
  @MethodSource("streamOfContainersImpl")
  void testAdvanceIfNeededSkipsMultipleRanges(Container container) {
    container.iadd(90, 101);
    container.iadd(200, 203);
    container.iadd(64000, 64070);

    ContainerBatchIterator it = createContainerBatchIterator(container);
    advanceIfNeeded(it, 100);

    int[] result = new int[3];
    int read = it.next(0, result);

    assertEquals(read, 3);
    assertArrayEquals(new int[]{100, 200, 201}, result);

    advanceIfNeeded(it, 64050);

    int[] result2 = new int[30];
    int read2 = it.next(0, result2);

    assertEquals(20, read2);
    assertArrayEquals(IntStream.range(64050, 64070).toArray(),
        Arrays.copyOfRange(result2, 0, 20));

    assertFalse(it.hasNext());
  }

  @ParameterizedTest
  @MethodSource("streamOfContainersImpl")
  void testThatAdvanceIfNeededDontGoBackInTime(Container container) {
    ContainerBatchIterator it = createContainerBatchIterator(container, 5, 10, 20);
    advanceIfNeeded(it, 10);
    assertEquals(10, assertNextBit(it, 1));

    advanceIfNeeded(it, 5);
    assertEquals(20, assertNextBit(it, 1));

    assertNextBit(it, 0);
  }

  @ParameterizedTest
  @MethodSource("streamOfContainersImpl")
  void testAdvanceIfNeededOverSparseContainer(Container container) {
    int[] bits = new int[]{5, 1000, 4096, 40000, 65535};

    ContainerBatchIterator it = createContainerBatchIterator(container, bits);
    for (int i = 0; i < 2; i++) {
      advanceIfNeeded(it, bits[i]);
      assertEquals(bits[i], assertNextBit(it, 1));
    }

    advanceIfNeeded(it, 4097);

    int[] buf = new int[2];
    int read = it.next(0, buf);

    assertEquals(buf.length, read);
    assertArrayEquals(Arrays.copyOfRange(bits, 3, 5), buf);

    assertNextBit(it, 0);
  }

  @ParameterizedTest
  @MethodSource("streamOfContainersImpl")
  void testAdvanceIfNeededToStalePositionDontClearCurrentBits(Container container) {
    container.iadd(20, 40);
    container.iadd(84, 104);

    ContainerBatchIterator it = createContainerBatchIterator(container);

    advanceIfNeeded(it, 84);
    advanceIfNeeded(it, 30);

    int[] buff = new int[20];
    assertEquals(20, it.next(0, buff));
    assertArrayEquals(IntStream.range(84, 104).toArray(), buff);
  }

  @ParameterizedTest
  @MethodSource("streamOfContainersImpl")
  void testAdvanceIfNeededDontGoBackInTimeForContiguousSegment(Container container) {
    container.iadd(0, 5);

    ContainerBatchIterator it = createContainerBatchIterator(container);

    advanceIfNeeded(it, 2);
    assertEquals(2, assertNextBit(it, 1));

    advanceIfNeeded(it, 2);
    assertEquals(3, assertNextBit(it, 1));
  }

  @ParameterizedTest
  @MethodSource("streamOfContainersImpl")
  void testReadBeyondUpperBoundIsNOP(Container container) {
    container.add(Character.MAX_VALUE);

    ContainerBatchIterator it = createContainerBatchIterator(container);
    assertEquals(Character.MAX_VALUE, assertNextBit(it, 1));

    for (int k = 0; k < 8; k++) {
      assertNextBit(it, 0);
    }
  }
}