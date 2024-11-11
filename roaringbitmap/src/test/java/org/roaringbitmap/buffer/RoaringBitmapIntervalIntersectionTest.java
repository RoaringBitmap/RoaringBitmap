package org.roaringbitmap.buffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.roaringbitmap.SeededTestData.TestDataSet.testCase;

import org.roaringbitmap.RoaringBitmap;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

@Execution(ExecutionMode.CONCURRENT)
public class RoaringBitmapIntervalIntersectionTest {

  private static Arguments[] ARGS;

  @BeforeAll
  public static void before() {
    ARGS =
        new Arguments[] {
          Arguments.of(RoaringBitmap.bitmapOf(1, 2, 3).toMutableRoaringBitmap(), 0, 1 << 16),
          Arguments.of(RoaringBitmap.bitmapOf(1, 2, 3).toMutableRoaringBitmap(), 1, 1),
          Arguments.of(
              RoaringBitmap.bitmapOf(1 << 31 | 1 << 30).toMutableRoaringBitmap(), 0, 1 << 16),
          Arguments.of(RoaringBitmap.bitmapOf(1 << 31 | 1 << 30).toMutableRoaringBitmap(), 0, 256),
          Arguments.of(
              RoaringBitmap.bitmapOf(1, 1 << 31 | 1 << 30).toMutableRoaringBitmap(), 0, 256),
          Arguments.of(
              RoaringBitmap.bitmapOf(1, 1 << 16, 1 << 31 | 1 << 30).toMutableRoaringBitmap(),
              0,
              1L << 32),
          Arguments.of(
              testCase()
                  .withArrayAt(10)
                  .withBitmapAt(20)
                  .withRunAt(30)
                  .withRange(70000L, 150000L)
                  .build()
                  .toMutableRoaringBitmap(),
              70000L,
              150000L),
          Arguments.of(
              testCase()
                  .withArrayAt(10)
                  .withBitmapAt(20)
                  .withRunAt(30)
                  .withRange(70000L, 150000L)
                  .build()
                  .toMutableRoaringBitmap(),
              71000L,
              140000L),
          Arguments.of(
              testCase()
                  .withArrayAt(0)
                  .withBitmapAt(1)
                  .withRunAt(20)
                  .build()
                  .toMutableRoaringBitmap(),
              67000,
              150000),
          Arguments.of(
              testCase()
                  .withBitmapAt(0)
                  .withArrayAt(1)
                  .withRunAt(20)
                  .build()
                  .toMutableRoaringBitmap(),
              67000,
              150000),
          Arguments.of(
              testCase()
                  .withBitmapAt(0)
                  .withRunAt(1)
                  .withArrayAt(20)
                  .build()
                  .toMutableRoaringBitmap(),
              67000,
              150000),
          Arguments.of(
              testCase()
                  .withArrayAt(0)
                  .withArrayAt(1)
                  .withArrayAt(2)
                  .withBitmapAt(200)
                  .withRunAt(205)
                  .build()
                  .toMutableRoaringBitmap(),
              199 * (1 << 16),
              200 * (1 << 16) + (1 << 14))
        };
  }

  @AfterAll
  public static void after() {
    ARGS = null;
  }

  public static Stream<Arguments> params() {
    return Stream.of(ARGS);
  }

  @ParameterizedTest
  @MethodSource("params")
  public void test(MutableRoaringBitmap bitmap, long minimum, long supremum) {
    MutableRoaringBitmap test = new MutableRoaringBitmap();
    test.add(minimum, supremum);
    assertEquals(
        ImmutableRoaringBitmap.intersects(bitmap, test), bitmap.intersects(minimum, supremum));
  }

  @ParameterizedTest
  @MethodSource("params")
  public void testIntersects(MutableRoaringBitmap bitmap, long minimum, long supremum) {
    MutableRoaringBitmap test = new MutableRoaringBitmap();
    test.add(minimum, supremum);
    assertEquals(
        MutableRoaringBitmap.intersects(bitmap, test), bitmap.intersects(minimum, supremum));
  }

  @ParameterizedTest
  @MethodSource("params")
  public void testContains(MutableRoaringBitmap bitmap, long minimum, long supremum) {
    MutableRoaringBitmap test = new MutableRoaringBitmap();
    test.add(minimum, supremum);
    assertEquals(!test.isEmpty() && bitmap.contains(test), bitmap.contains(minimum, supremum));
    assertTrue(test.isEmpty() || test.contains(minimum, supremum));
  }

  @ParameterizedTest
  @MethodSource("params")
  public void ifContainsThenIntersects(MutableRoaringBitmap bitmap, long minimum, long supremum) {
    boolean contains = bitmap.contains(minimum, supremum);
    boolean intersects = bitmap.intersects(minimum, supremum);
    assertTrue(!contains || intersects);
  }
}
