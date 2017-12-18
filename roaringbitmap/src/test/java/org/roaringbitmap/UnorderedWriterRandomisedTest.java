package org.roaringbitmap;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class UnorderedWriterRandomisedTest {


  @Parameterized.Parameters
  public static Object[][] tests() {
    return new Object[][] {
            {generateUnorderedArray(10)},
            {generateUnorderedArray(100)},
            {generateUnorderedArray(1000)},
            {generateUnorderedArray(10000)},
            {generateUnorderedArray(100000)},
            {generateUnorderedArray(1000000)},
    };
  }


  private final int[] data;

  public UnorderedWriterRandomisedTest(int[] data) {
    this.data = data;
  }

  @Test
  public void bitmapOfUnorderedShouldBuildSameBitmapAsBitmapOf() {
    RoaringBitmap baseline = RoaringBitmap.bitmapOf(data);
    RoaringBitmap test = RoaringBitmap.bitmapOfUnordered(data);
    RoaringArray baselineHLC = baseline.highLowContainer;
    RoaringArray testHLC = test.highLowContainer;
    Assert.assertEquals(baselineHLC.size, testHLC.size);
    for (int i = 0; i < baselineHLC.size; ++i) {
      Container baselineContainer = baselineHLC.getContainerAtIndex(i);
      Container rbContainer = testHLC.getContainerAtIndex(i);
      assertEquals(baselineContainer, rbContainer);
    }
    assertEquals(baseline, test);
  }

  private static int[] generateUnorderedArray(int size) {
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
