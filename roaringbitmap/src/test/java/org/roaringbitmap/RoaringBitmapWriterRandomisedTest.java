package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.roaringbitmap.RoaringBitmapWriter.writer;
import static org.roaringbitmap.Util.toUnsignedLong;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Random;
import java.util.stream.Stream;

@Execution(ExecutionMode.CONCURRENT)
public class RoaringBitmapWriterRandomisedTest {

  private static int[][] ARRAYS;

  @BeforeAll
  public static void init() {
    ARRAYS =
        new int[][] {
          new int[] {0, 1, 2, 3},
          randomArray(0),
          randomArray(10),
          randomArray(100),
          randomArray(1000),
          randomArray(10_000),
          randomArray(100_000),
          randomArray(1000_000),
          randomArray(10_000_000)
        };
  }

  @AfterAll
  public static void clear() {
    ARRAYS = null;
  }

  public static Stream<Arguments> tests() {
    return Stream.of(ARRAYS).map(Arguments::of);
  }

  @ParameterizedTest(name = "-")
  @MethodSource("tests")
  public void shouldBuildSameBitmapAsBitmapOf(int[] values) {
    RoaringBitmapWriter<RoaringBitmap> writer =
        writer().expectedRange(toUnsignedLong(min()), toUnsignedLong(max(values))).get();
    for (int i : values) {
      writer.add(i);
    }
    writer.flush();
    verify(writer.getUnderlying(), values);
  }

  @ParameterizedTest(name = "-")
  @MethodSource("tests")
  public void shouldBuildSameBitmapAsBitmapOfWithAddMany(int[] values) {
    RoaringBitmapWriter<RoaringBitmap> writer =
        writer().expectedRange(toUnsignedLong(min()), toUnsignedLong(max(values))).get();
    writer.addMany(values);
    writer.flush();
    verify(writer.getUnderlying(), values);
  }

  @ParameterizedTest(name = "-")
  @MethodSource("tests")
  public void getShouldFlushToTheUnderlyingBitmap(int[] values) {
    RoaringBitmapWriter<RoaringBitmap> writer =
        writer().expectedRange(toUnsignedLong(min()), toUnsignedLong(max(values))).get();
    writer.addMany(values);
    verify(writer.get(), values);
  }

  @ParameterizedTest(name = "-")
  @MethodSource("tests")
  public void getShouldFlushToTheUnderlyingBitmap_ConstantMemory(int[] values) {
    RoaringBitmapWriter<RoaringBitmap> writer = writer().constantMemory().get();
    writer.addMany(values);
    verify(writer.get(), values);
  }

  @ParameterizedTest(name = "-")
  @MethodSource("tests")
  public void shouldBuildSameBitmapAsBitmapOf_ConstantMemory(int[] values) {
    RoaringBitmapWriter<RoaringBitmap> writer =
        writer()
            .constantMemory()
            .expectedRange(toUnsignedLong(min()), toUnsignedLong(max(values)))
            .get();
    for (int i : values) {
      writer.add(i);
    }
    writer.flush();
    verify(writer.getUnderlying(), values);
  }

  @ParameterizedTest(name = "-")
  @MethodSource("tests")
  public void shouldBuildSameBitmapAsBitmapOfWithAddMany_ConstantMemory(int[] values) {
    RoaringBitmapWriter<RoaringBitmap> writer =
        writer()
            .constantMemory()
            .expectedRange(toUnsignedLong(min()), toUnsignedLong(max(values)))
            .get();
    writer.addMany(values);
    writer.flush();
    verify(writer.getUnderlying(), values);
  }

  private void verify(RoaringBitmap rb, int[] values) {
    RoaringBitmap baseline = RoaringBitmap.bitmapOf(values);
    RoaringArray baselineHLC = baseline.highLowContainer;
    RoaringArray rbHLC = rb.highLowContainer;
    assertEquals(baselineHLC.size, rbHLC.size);
    for (int i = 0; i < baselineHLC.size; ++i) {
      Container baselineContainer = baselineHLC.getContainerAtIndex(i);
      Container rbContainer = rbHLC.getContainerAtIndex(i);
      assertEquals(baselineContainer, rbContainer);
    }
    assertEquals(baseline, rb);
  }

  private static int[] randomArray(int size) {
    Random random = new Random(1234);
    int[] data = new int[size];
    int last = 0;
    int i = 0;
    while (i < size) {
      if (random.nextGaussian() > 0.1) {
        int runLength = random.nextInt(Math.min(size - i, 1 << 16));
        for (int j = 1; j < runLength; ++j) {
          data[i + j] = last + 1;
          last = data[i + j];
        }
        i += runLength;
      } else {
        data[i] = last + 1 + random.nextInt(999);
        last = data[i];
        ++i;
      }
    }
    Arrays.sort(data);
    return data;
  }

  private int max(int[] values) {
    return values.length == 0 ? 0 : values[values.length - 1];
  }

  private int min() {
    return 0;
  }
}
