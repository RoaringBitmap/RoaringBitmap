package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.roaringbitmap.SeededTestData.TestDataSet.testCase;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

@Execution(ExecutionMode.CONCURRENT)
public class TestFastAggregation {

  @Test
  public void horizontal_or() {
    RoaringBitmap rb1 = RoaringBitmap.bitmapOf(0, 1, 2);
    RoaringBitmap rb2 = RoaringBitmap.bitmapOf(0, 5, 6);
    RoaringBitmap rb3 = RoaringBitmap.bitmapOf(1 << 16, 2 << 16);
    RoaringBitmap result = FastAggregation.horizontal_or(Arrays.asList(rb1, rb2, rb3));
    RoaringBitmap expected = RoaringBitmap.bitmapOf(0, 1, 2, 5, 6, 1 << 16, 2 << 16);
    assertEquals(expected, result);
  }

  @Test
  public void or() {
    RoaringBitmap rb1 = RoaringBitmap.bitmapOf(0, 1, 2);
    RoaringBitmap rb2 = RoaringBitmap.bitmapOf(0, 5, 6);
    RoaringBitmap rb3 = RoaringBitmap.bitmapOf(1 << 16, 2 << 16);
    RoaringBitmap result = FastAggregation.or(rb1, rb2, rb3);
    RoaringBitmap expected = RoaringBitmap.bitmapOf(0, 1, 2, 5, 6, 1 << 16, 2 << 16);
    assertEquals(expected, result);
  }

  @Test
  public void horizontal_or2() {
    RoaringBitmap rb1 = RoaringBitmap.bitmapOf(0, 1, 2);
    RoaringBitmap rb2 = RoaringBitmap.bitmapOf(0, 5, 6);
    RoaringBitmap rb3 = RoaringBitmap.bitmapOf(1 << 16, 2 << 16);
    RoaringBitmap result = FastAggregation.horizontal_or(rb1, rb2, rb3);
    RoaringBitmap expected = RoaringBitmap.bitmapOf(0, 1, 2, 5, 6, 1 << 16, 2 << 16);
    assertEquals(expected, result);
  }

  @Test
  public void priorityqueue_or() {
    RoaringBitmap rb1 = RoaringBitmap.bitmapOf(0, 1, 2);
    RoaringBitmap rb2 = RoaringBitmap.bitmapOf(0, 5, 6);
    RoaringBitmap rb3 = RoaringBitmap.bitmapOf(1 << 16, 2 << 16);
    RoaringBitmap result =
        FastAggregation.priorityqueue_or(Arrays.asList(rb1, rb2, rb3).iterator());
    RoaringBitmap expected = RoaringBitmap.bitmapOf(0, 1, 2, 5, 6, 1 << 16, 2 << 16);
    assertEquals(expected, result);
  }

  @Test
  public void priorityqueue_or2() {
    RoaringBitmap rb1 = RoaringBitmap.bitmapOf(0, 1, 2);
    RoaringBitmap rb2 = RoaringBitmap.bitmapOf(0, 5, 6);
    RoaringBitmap rb3 = RoaringBitmap.bitmapOf(1 << 16, 2 << 16);
    RoaringBitmap result = FastAggregation.priorityqueue_or(rb1, rb2, rb3);
    RoaringBitmap expected = RoaringBitmap.bitmapOf(0, 1, 2, 5, 6, 1 << 16, 2 << 16);
    assertEquals(expected, result);
  }

  private static class ExtendedRoaringBitmap extends RoaringBitmap {}

  @Test
  public void testWorkShyAnd() {
    final RoaringBitmap b1 = RoaringBitmap.bitmapOf(1, 2, 0x10001, 0x20001, 0x30001);
    final RoaringBitmap b2 = RoaringBitmap.bitmapOf(2, 3, 0x20002, 0x30001);
    final RoaringBitmap bResult = FastAggregation.workShyAnd(new long[1024], b1, b2);
    assertFalse(bResult.contains(1));
    assertTrue(bResult.contains(2));
    assertFalse(bResult.contains(3));
  }

  @Test
  public void testAndWithIterator() {
    final RoaringBitmap b1 = RoaringBitmap.bitmapOf(1, 2);
    final RoaringBitmap b2 = RoaringBitmap.bitmapOf(2, 3);
    final RoaringBitmap bResult = FastAggregation.and(Arrays.asList(b1, b2).iterator());
    assertFalse(bResult.contains(1));
    assertTrue(bResult.contains(2));
    assertFalse(bResult.contains(3));

    final ExtendedRoaringBitmap eb1 = new ExtendedRoaringBitmap();
    eb1.add(1);
    eb1.add(2);
    final ExtendedRoaringBitmap eb2 = new ExtendedRoaringBitmap();
    eb2.add(2);
    eb2.add(3);
    final RoaringBitmap ebResult = FastAggregation.and(Arrays.asList(b1, b2).iterator());
    assertFalse(ebResult.contains(1));
    assertTrue(ebResult.contains(2));
    assertFalse(ebResult.contains(3));
  }

  @Test
  public void testNaiveAndWithIterator() {
    final RoaringBitmap b1 = RoaringBitmap.bitmapOf(1, 2);
    final RoaringBitmap b2 = RoaringBitmap.bitmapOf(2, 3);
    final RoaringBitmap bResult = FastAggregation.naive_and(Arrays.asList(b1, b2).iterator());
    assertFalse(bResult.contains(1));
    assertTrue(bResult.contains(2));
    assertFalse(bResult.contains(3));

    final ExtendedRoaringBitmap eb1 = new ExtendedRoaringBitmap();
    eb1.add(1);
    eb1.add(2);
    final ExtendedRoaringBitmap eb2 = new ExtendedRoaringBitmap();
    eb2.add(2);
    eb2.add(3);
    final RoaringBitmap ebResult = FastAggregation.naive_and(Arrays.asList(b1, b2).iterator());
    assertFalse(ebResult.contains(1));
    assertTrue(ebResult.contains(2));
    assertFalse(ebResult.contains(3));
  }

  @Test
  public void testOrWithIterator() {
    final RoaringBitmap b1 = RoaringBitmap.bitmapOf(1, 2);
    final RoaringBitmap b2 = RoaringBitmap.bitmapOf(2, 3);
    final RoaringBitmap bItResult = FastAggregation.or(Arrays.asList(b1, b2).iterator());
    assertTrue(bItResult.contains(1));
    assertTrue(bItResult.contains(2));
    assertTrue(bItResult.contains(3));

    final ExtendedRoaringBitmap eb1 = new ExtendedRoaringBitmap();
    eb1.add(1);
    eb1.add(2);
    final ExtendedRoaringBitmap eb2 = new ExtendedRoaringBitmap();
    eb2.add(2);
    eb2.add(3);
    final RoaringBitmap ebItResult = FastAggregation.or(Arrays.asList(b1, b2).iterator());
    assertTrue(ebItResult.contains(1));
    assertTrue(ebItResult.contains(2));
    assertTrue(ebItResult.contains(3));
  }

  @Test
  public void testNaiveOrWithIterator() {
    final RoaringBitmap b1 = RoaringBitmap.bitmapOf(1, 2);
    final RoaringBitmap b2 = RoaringBitmap.bitmapOf(2, 3);
    final RoaringBitmap bResult = FastAggregation.naive_or(Arrays.asList(b1, b2).iterator());
    assertTrue(bResult.contains(1));
    assertTrue(bResult.contains(2));
    assertTrue(bResult.contains(3));

    final ExtendedRoaringBitmap eb1 = new ExtendedRoaringBitmap();
    eb1.add(1);
    eb1.add(2);
    final ExtendedRoaringBitmap eb2 = new ExtendedRoaringBitmap();
    eb2.add(2);
    eb2.add(3);
    final RoaringBitmap ebResult = FastAggregation.naive_or(Arrays.asList(b1, b2).iterator());
    assertTrue(ebResult.contains(1));
    assertTrue(ebResult.contains(2));
    assertTrue(ebResult.contains(3));
  }

  @Test
  public void testNaiveXorWithIterator() {
    final RoaringBitmap b1 = RoaringBitmap.bitmapOf(1, 2);
    final RoaringBitmap b2 = RoaringBitmap.bitmapOf(2, 3);
    final RoaringBitmap bResult = FastAggregation.naive_xor(Arrays.asList(b1, b2).iterator());
    assertTrue(bResult.contains(1));
    assertFalse(bResult.contains(2));
    assertTrue(bResult.contains(3));

    final ExtendedRoaringBitmap eb1 = new ExtendedRoaringBitmap();
    eb1.add(1);
    eb1.add(2);
    final ExtendedRoaringBitmap eb2 = new ExtendedRoaringBitmap();
    eb2.add(2);
    eb2.add(3);
    final RoaringBitmap ebResult = FastAggregation.naive_xor(Arrays.asList(b1, b2).iterator());
    assertTrue(ebResult.contains(1));
    assertFalse(ebResult.contains(2));
    assertTrue(ebResult.contains(3));
  }

  public static Stream<Arguments> bitmaps() {
    return Stream.of(
        Arguments.of(
            Arrays.asList(
                testCase().withBitmapAt(0).withArrayAt(1).withRunAt(2).build(),
                testCase().withBitmapAt(0).withArrayAt(1).withRunAt(2).build(),
                testCase().withBitmapAt(0).withArrayAt(1).withRunAt(2).build())),
        Arguments.of(
            Arrays.asList(
                testCase().withBitmapAt(0).withRunAt(1).withArrayAt(2).build(),
                testCase().withBitmapAt(0).withRunAt(1).withArrayAt(2).build(),
                testCase().withBitmapAt(0).withRunAt(1).withArrayAt(2).build())),
        Arguments.of(
            Arrays.asList(
                testCase().withArrayAt(0).withRunAt(1).withBitmapAt(2).build(),
                testCase().withArrayAt(0).withRunAt(1).withBitmapAt(2).build(),
                testCase().withArrayAt(0).withRunAt(1).withBitmapAt(2).build())),
        Arguments.of(
            Arrays.asList(
                testCase().withBitmapAt(0).withArrayAt(1).withRunAt(2).build(),
                testCase().withBitmapAt(0).withArrayAt(3).withRunAt(4).build(),
                testCase().withBitmapAt(0).withArrayAt(1).withRunAt(2).build())),
        Arguments.of(
            Arrays.asList(
                testCase().withArrayAt(0).withBitmapAt(1).withRunAt(2).build(),
                testCase().withRunAt(0).withArrayAt(1).withBitmapAt(2).build(),
                testCase().withBitmapAt(0).withRunAt(1).withArrayAt(2).build())),
        Arguments.of(
            Arrays.asList(
                testCase().withBitmapAt(0).withArrayAt(1).withRunAt(2).build(),
                testCase().withBitmapAt(0).withArrayAt(2).withRunAt(4).build(),
                testCase().withBitmapAt(0).withArrayAt(1).withRunAt(2).build())),
        Arguments.of(
            Arrays.asList(
                testCase().withArrayAt(0).withArrayAt(1).withArrayAt(2).build(),
                testCase().withBitmapAt(0).withBitmapAt(2).withBitmapAt(4).build(),
                testCase().withRunAt(0).withRunAt(1).withRunAt(2).build())),
        Arguments.of(
            Arrays.asList(
                testCase().withArrayAt(0).withArrayAt(1).withArrayAt(2).build(),
                testCase().withBitmapAt(0).withBitmapAt(2).withArrayAt(4).build(),
                testCase().withRunAt(0).withRunAt(1).withArrayAt(2).build())),
        Arguments.of(
            Arrays.asList(
                testCase().withArrayAt(0).withArrayAt(1).withBitmapAt(2).build(),
                testCase().withBitmapAt(0).withBitmapAt(2).withBitmapAt(4).build(),
                testCase().withRunAt(0).withRunAt(1).withBitmapAt(2).build())),
        Arguments.of(
            Arrays.asList(
                testCase().withArrayAt(20).build(),
                testCase().withBitmapAt(0).withBitmapAt(1).withBitmapAt(4).build(),
                testCase().withRunAt(0).withRunAt(1).withBitmapAt(3).build())));
  }

  @MethodSource("bitmaps")
  @ParameterizedTest(name = "testWorkShyAnd")
  public void testWorkShyAnd(List<RoaringBitmap> list) {
    RoaringBitmap[] bitmaps = list.toArray(new RoaringBitmap[0]);
    long[] buffer = new long[1024];
    RoaringBitmap result = FastAggregation.and(buffer, bitmaps);
    RoaringBitmap expected = FastAggregation.naive_and(bitmaps);
    assertEquals(expected, result);
    result = FastAggregation.and(bitmaps);
    assertEquals(expected, result);
    result = FastAggregation.workAndMemoryShyAnd(buffer, bitmaps);
    assertEquals(expected, result);
  }

  @MethodSource("bitmaps")
  @ParameterizedTest(name = "testAndCardinality")
  public void testAndCardinality(List<RoaringBitmap> list) {
    RoaringBitmap[] bitmaps = list.toArray(new RoaringBitmap[0]);
    for (int length = 0; length <= bitmaps.length; length++) {
      RoaringBitmap[] subset = Arrays.copyOf(bitmaps, length);
      RoaringBitmap and = FastAggregation.and(subset);
      int andCardinality = FastAggregation.andCardinality(subset);
      assertEquals(and.getCardinality(), andCardinality);
    }
  }

  @MethodSource("bitmaps")
  @ParameterizedTest(name = "testIntersects")
  public void testIntersects(List<RoaringBitmap> list) {
    RoaringBitmap[] bitmaps = list.toArray(new RoaringBitmap[0]);
    for (int length = 0; length <= bitmaps.length; length++) {
      RoaringBitmap[] subset = Arrays.copyOf(bitmaps, length);
      RoaringBitmap and = FastAggregation.and(subset);
      boolean intersects = FastAggregation.intersects(subset);
      assertEquals(!and.isEmpty(), intersects);
    }
  }

  @MethodSource("bitmaps")
  @ParameterizedTest(name = "testOrCardinality")
  public void testOrCardinality(List<RoaringBitmap> list) {
    RoaringBitmap[] bitmaps = list.toArray(new RoaringBitmap[0]);
    for (int length = 0; length <= bitmaps.length; length++) {
      RoaringBitmap[] subset = Arrays.copyOf(bitmaps, length);
      RoaringBitmap or = FastAggregation.or(subset);
      int orCardinality = FastAggregation.orCardinality(subset);
      assertEquals(or.getCardinality(), orCardinality);
    }
  }

  @Test
  public void horizontal_xorDropsBlocksThatCancelOut() {
    RoaringBitmap a = RoaringBitmap.bitmapOf(1, 3, 5, 70_000);
    RoaringBitmap b = RoaringBitmap.bitmapOf(1, 3, 5);
    RoaringBitmap result = FastAggregation.horizontal_xor(a, b);
    assertEquals(RoaringBitmap.bitmapOf(70_000), result);
    assertTrue(result.validate());
    assertTrue(FastAggregation.horizontal_xor(a, a.clone()).isEmpty());
  }

  @Test
  public void horizontal_xorDoesNotShareContainersWithItsInputs() {
    RoaringBitmap a = RoaringBitmap.bitmapOf(1, 3, 5, 7, 9, 11, 13, 15, 17, 19);
    RoaringBitmap runs = new RoaringBitmap();
    runs.add(100L, 105L);
    runs.runOptimize();
    RoaringBitmap result = FastAggregation.horizontal_xor(a, a.clone(), runs);
    assertEquals(runs, result);

    result.or(runs);
    result.add(5_000);
    assertEquals(RoaringBitmap.bitmapOf(100, 101, 102, 103, 104, 5_000), result);
    assertEquals(RoaringBitmap.bitmapOf(100, 101, 102, 103, 104), runs);
  }

  private static List<RoaringBitmap> readBitmapsPerLine(String resourceName) throws IOException {
    List<RoaringBitmap> bitmaps = new ArrayList<>();
    try (BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(TestAdversarialInputs.openInputstream(resourceName), "UTF-8"))) {
      for (String line = reader.readLine(); line != null; line = reader.readLine()) {
        if (!line.isEmpty()) {
          bitmaps.add(
              RoaringBitmap.bitmapOf(
                  Arrays.stream(line.split(",")).mapToInt(Integer::parseInt).toArray()));
        }
      }
    }
    return bitmaps;
  }

  @Test
  public void testIssue330() throws IOException {
    // from the Go library: sparse inputs over keys 16-24 with runs
    List<RoaringBitmap> bitmaps = readBitmapsPerLine("/testdata/issue330.txt");
    assertEquals(3, bitmaps.size());
    for (RoaringBitmap bitmap : bitmaps) {
      bitmap.runOptimize();
      assertTrue(bitmap.validate());
    }
    RoaringBitmap b0 = bitmaps.get(0);
    RoaringBitmap b1 = bitmaps.get(1);
    RoaringBitmap b2 = bitmaps.get(2);
    assertEquals(0, FastAggregation.and(b0, b1).getCardinality());
    assertEquals(0, FastAggregation.and(b0, b2).getCardinality());
    assertEquals(0, FastAggregation.and(b1, b2).getCardinality());
    RoaringBitmap chained = RoaringBitmap.or(b0, b1);
    chained.or(b2);
    assertEquals(1040, chained.getCardinality());
    assertTrue(chained.validate());
    RoaringBitmap[][] orders = {{b0, b1, b2}, {b2, b1, b0}};
    for (RoaringBitmap[] order : orders) {
      for (RoaringBitmap result :
          new RoaringBitmap[] {
            FastAggregation.or(order),
            FastAggregation.horizontal_or(order),
            FastAggregation.priorityqueue_or(order),
            FastAggregation.naive_or(order),
            ParallelAggregation.or(order)
          }) {
        assertEquals(chained, result);
        assertTrue(result.validate());
      }
    }
  }

  @Test
  public void testIssue178() {
    // from the Go library: one value per container
    RoaringBitmap r1 =
        RoaringBitmap.bitmapOf(
            3585028, 65901253, 143441994, 211160474, 286511937, 356744840, 434332509, 502812785,
            576097614, 646557334, 714794241, 775083485, 833704249, 889329147, 941367043);
    RoaringBitmap r2 =
        RoaringBitmap.bitmapOf(
            17883, 54494426, 113908938, 174519827, 235465665, 296685741, 357644666, 420192495,
            476104304, 523046142, 577855081, 634889665, 692460635, 751350463, 809989192, 863494316,
            919127240);
    assertEquals(32, FastAggregation.or(r1, r2).getCardinality());
    assertEquals(32, FastAggregation.horizontal_or(r1, r2).getCardinality());
    assertEquals(32, FastAggregation.priorityqueue_or(r1, r2).getCardinality());
    assertEquals(32, ParallelAggregation.or(r1, r2).getCardinality());
  }

  @Test
  public void unionThatFillsAContainerBecomesARunContainer() {
    // from CRoaring's test_full_bitset_union_becomes_run
    RoaringBitmap evens = new RoaringBitmap();
    RoaringBitmap odds = new RoaringBitmap();
    for (int i = 0; i < 65536; i += 2) {
      evens.add(i);
      odds.add(i + 1);
    }
    RoaringBitmap inPlace = evens.clone();
    inPlace.or(odds);
    RoaringBitmap[] results = {
      inPlace,
      RoaringBitmap.or(evens, odds),
      FastAggregation.or(evens, odds, odds),
      FastAggregation.horizontal_or(evens, odds, odds),
      FastAggregation.priorityqueue_or(evens, odds, odds),
      ParallelAggregation.or(evens, odds, odds)
    };
    for (RoaringBitmap result : results) {
      assertEquals(65536, result.getLongCardinality());
      assertEquals(1, result.getContainerCount());
      assertTrue(result.getContainerPointer().isRunContainer());
      assertTrue(result.validate());
    }
  }
}
