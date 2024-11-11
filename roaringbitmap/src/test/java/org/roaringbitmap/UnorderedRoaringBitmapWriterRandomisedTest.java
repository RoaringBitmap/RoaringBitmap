package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

@Execution(ExecutionMode.CONCURRENT)
public class UnorderedRoaringBitmapWriterRandomisedTest {

  public static Stream<Arguments> tests() {
    return Stream.of(
        Arguments.of(generateUnorderedArray(0)),
        Arguments.of(generateUnorderedArray(10)),
        Arguments.of(generateUnorderedArray(100)),
        Arguments.of(generateUnorderedArray(1000)),
        Arguments.of(generateUnorderedArray(10000)),
        Arguments.of(generateUnorderedArray(100000)),
        Arguments.of(generateUnorderedArray(1000000)));
  }

  @ParameterizedTest
  @MethodSource("tests")
  public void bitmapOfUnorderedShouldBuildSameBitmapAsBitmapOf(int[] data) {
    RoaringBitmap baseline = RoaringBitmap.bitmapOf(data);
    RoaringBitmap test = RoaringBitmap.bitmapOfUnordered(data);
    RoaringArray baselineHLC = baseline.highLowContainer;
    RoaringArray testHLC = test.highLowContainer;
    assertEquals(baselineHLC.size, testHLC.size);
    for (int i = 0; i < baselineHLC.size; ++i) {
      Container baselineContainer = baselineHLC.getContainerAtIndex(i);
      Container rbContainer = testHLC.getContainerAtIndex(i);
      assertEquals(baselineContainer, rbContainer);
    }
    assertEquals(baseline, test);
  }

  private static int[] generateUnorderedArray(int size) {
    if (size == 0) {
      return new int[0];
    }
    Random random = new Random();
    List<Integer> ints = new ArrayList<>(size);
    int last = 0;
    for (int i = 0; i < size; ++i) {
      if (random.nextGaussian() > 0.1) {
        last = last + 1;
      } else {
        last = last + 1 + random.nextInt(99);
      }
      ints.add(last);
    }
    Collections.shuffle(ints);
    int[] data = new int[size];
    int i = 0;
    for (Integer value : ints) {
      data[i++] = value;
    }
    return data;
  }
}
