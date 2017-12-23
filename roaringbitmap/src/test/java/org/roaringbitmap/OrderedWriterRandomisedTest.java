package org.roaringbitmap;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class OrderedWriterRandomisedTest {

  @Parameterized.Parameters
  public static Object[][] tests() {
    return new Object[][]
            {
                    {new int[]{0, 1, 2, 3}},
                    {randomArray(10)},
                    {randomArray(100)},
                    {randomArray(1000)},
                    {randomArray(10_000)},
                    {randomArray(100_000)},
                    {randomArray(1000_000)},
                    {randomArray(10_000_000)}
            };
  }


  private final int[] values;

  public OrderedWriterRandomisedTest(int[] values) {
    this.values = values;
  }

  @Test
  public void shouldBuildSameBitmapAsBitmapOf() {
    RoaringBitmap baseline = RoaringBitmap.bitmapOf(values);
    RoaringBitmap rb = new RoaringBitmap();
    OrderedWriter writer = new OrderedWriter(rb);
    for (int i : values) {
      writer.add(i);
    }

    writer.flush();
    RoaringArray baselineHLC = baseline.highLowContainer;
    RoaringArray rbHLC = rb.highLowContainer;
    Assert.assertEquals(baselineHLC.size, rbHLC.size);
    for (int i = 0; i < baselineHLC.size; ++i) {
      Container baselineContainer = baselineHLC.getContainerAtIndex(i);
      Container rbContainer = rbHLC.getContainerAtIndex(i);
      assertEquals(baselineContainer, rbContainer);
    }
    assertEquals(baseline, rb);
  }

  private static int[] randomArray(int size) {
    Random random = new Random();
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
}