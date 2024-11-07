package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.roaringbitmap.SeededTestData.TestDataSet.testCase;

import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.function.Consumer;
import java.util.stream.Stream;

@Execution(ExecutionMode.CONCURRENT)
public class OrNotTruncationTest {

  private static final Consumer<RoaringBitmap> NO_OP = x -> {};

  public static Stream<Arguments> params() {
    return Stream.of(
        Arguments.of(new RoaringBitmap(), NO_OP),
        Arguments.of(new RoaringBitmap(), (Consumer<RoaringBitmap>) x -> x.add(2)),
        Arguments.of(new RoaringBitmap(), (Consumer<RoaringBitmap>) x -> x.add(2L, 5L)),
        Arguments.of(new RoaringBitmap(), (Consumer<RoaringBitmap>) x -> x.add(3L, 5L)),
        Arguments.of(
            new RoaringBitmap(),
            (Consumer<RoaringBitmap>)
                x -> {
                  x.add(1L, 10L);
                  x.remove(2L, 10L);
                }),
        Arguments.of(
            new RoaringBitmap(),
            (Consumer<RoaringBitmap>)
                x -> {
                  for (int i : new int[] {0, 1, 2, 3, 4, 5, 6}) x.add(i);
                }),
        Arguments.of(RoaringBitmap.bitmapOf(2), NO_OP),
        Arguments.of(RoaringBitmap.bitmapOf(2, 3, 4), NO_OP),
        Arguments.of(testCase().withArrayAt(0).build(), NO_OP),
        Arguments.of(testCase().withRunAt(0).build(), NO_OP),
        Arguments.of(testCase().withBitmapAt(0).build(), NO_OP),
        Arguments.of(testCase().withArrayAt(0).withRunAt(1).build(), NO_OP),
        Arguments.of(testCase().withRunAt(0).withRunAt(1).build(), NO_OP),
        Arguments.of(testCase().withBitmapAt(0).withRunAt(1).build(), NO_OP),
        Arguments.of(testCase().withArrayAt(1).build(), NO_OP),
        Arguments.of(testCase().withRunAt(1).build(), NO_OP),
        Arguments.of(testCase().withBitmapAt(1).build(), NO_OP),
        Arguments.of(testCase().withArrayAt(1).withRunAt(2).build(), NO_OP),
        Arguments.of(testCase().withRunAt(1).withRunAt(2).build(), NO_OP),
        Arguments.of(testCase().withBitmapAt(1).withRunAt(2).build(), NO_OP));
  }

  @ParameterizedTest
  @MethodSource("params")
  public void testTruncation(RoaringBitmap other, Consumer<RoaringBitmap> init) {
    RoaringBitmap one = new RoaringBitmap();
    one.add(0);
    one.add(10);
    init.accept(other);
    one.orNot(other, 7);
    assertTrue(one.contains(10));
  }
}
