package org.roaringbitmap;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.roaringbitmap.RandomisedTestData.TestDataSet.testCase;

@RunWith(Parameterized.class)
public class RoaringBitmapIntervalIntersectionTest {

  @Parameterized.Parameters
  public static Object[][] params() {
    return new Object[][] {
            {RoaringBitmap.bitmapOf(1, 2, 3), 0, 1 << 16},
            {RoaringBitmap.bitmapOf(1 << 31 | 1 << 30), 0, 1 << 16},
            {RoaringBitmap.bitmapOf(1 << 31 | 1 << 30), 0, 256},
            {RoaringBitmap.bitmapOf(1, 1 << 31 | 1 << 30), 0, 256},
            {RoaringBitmap.bitmapOf(1, 1 << 16, 1 << 31 | 1 << 30), 0, 1L << 32},
            {testCase().withArrayAt(10).withBitmapAt(20).withRunAt(30)
                    .withRange(70000L, 150000L).build(), 70000L, 150000L},
            {testCase().withArrayAt(10).withBitmapAt(20).withRunAt(30)
                    .withRange(70000L, 150000L).build(), 71000L, 140000L},
            {testCase().withArrayAt(0).withBitmapAt(1).withRunAt(20).build(), 67000, 150000},
            {testCase().withBitmapAt(0).withArrayAt(1).withRunAt(20).build(), 67000, 150000},
            {testCase().withBitmapAt(0).withRunAt(1).withArrayAt(20).build(), 67000, 150000},
            {testCase().withArrayAt(0)
                       .withArrayAt(1)
                       .withArrayAt(2)
                       .withBitmapAt(200)
                       .withRunAt(205).build(), 199 * (1 << 16), 200 * (1 << 16) + (1 << 14)},
    };
  }


  private final RoaringBitmap bitmap;
  private final long minimum;
  private final long supremum;

  public RoaringBitmapIntervalIntersectionTest(RoaringBitmap bitmap, long minimum, long supremum) {
    this.bitmap = bitmap;
    this.minimum = minimum;
    this.supremum = supremum;
  }


  @Test
  public void testIntersects() {
    RoaringBitmap test = new RoaringBitmap();
    test.add(minimum, supremum);
    Assert.assertEquals(RoaringBitmap.intersects(bitmap, test), bitmap.intersects(minimum, supremum));
  }

  @Test
  public void testContains() {
    RoaringBitmap test = new RoaringBitmap();
    test.add(minimum, supremum);
    Assert.assertEquals(bitmap.contains(test), bitmap.contains(minimum, supremum));
    Assert.assertTrue(test.contains(minimum, supremum));
  }

  @Test
  public void ifContainsThenIntersects() {
    boolean contains = bitmap.contains(minimum, supremum);
    boolean intersects = bitmap.intersects(minimum, supremum);
    Assert.assertTrue(!contains || intersects);
  }
}
