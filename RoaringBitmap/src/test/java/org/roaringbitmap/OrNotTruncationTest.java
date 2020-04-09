package org.roaringbitmap;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.function.Consumer;

import static org.roaringbitmap.SeededTestData.TestDataSet.testCase;

@RunWith(Parameterized.class)
public class OrNotTruncationTest {

  private static final Consumer<RoaringBitmap> NO_OP = x -> {
  };

  @Parameterized.Parameters
  public static Object[] params() {
    return new Object[][]{
            {new RoaringBitmap(), NO_OP},
            {new RoaringBitmap(), (Consumer<RoaringBitmap>) x -> x.add(2)},
            {new RoaringBitmap(), (Consumer<RoaringBitmap>) x -> x.add(2L, 5L)},
            {new RoaringBitmap(), (Consumer<RoaringBitmap>) x -> x.add(3L, 5L)},
            {new RoaringBitmap(), (Consumer<RoaringBitmap>) x -> {
              x.add(1L, 10L);
              x.remove(2L, 10L);
            }},
            {new RoaringBitmap(), (Consumer<RoaringBitmap>) x -> {
              for (int i : new int[]{0, 1, 2, 3, 4, 5, 6})
                x.add(i);
            }},
            {RoaringBitmap.bitmapOf(2), NO_OP},
            {RoaringBitmap.bitmapOf(2, 3, 4), NO_OP},
            {testCase().withArrayAt(0).build(), NO_OP},
            {testCase().withRunAt(0).build(), NO_OP},
            {testCase().withBitmapAt(0).build(), NO_OP},
            {testCase().withArrayAt(0).withRunAt(1).build(), NO_OP},
            {testCase().withRunAt(0).withRunAt(1).build(), NO_OP},
            {testCase().withBitmapAt(0).withRunAt(1).build(), NO_OP},
            {testCase().withArrayAt(1).build(), NO_OP},
            {testCase().withRunAt(1).build(), NO_OP},
            {testCase().withBitmapAt(1).build(), NO_OP},
            {testCase().withArrayAt(1).withRunAt(2).build(), NO_OP},
            {testCase().withRunAt(1).withRunAt(2).build(), NO_OP},
            {testCase().withBitmapAt(1).withRunAt(2).build(), NO_OP},
    };
  }

  private final RoaringBitmap other;
  private final Consumer<RoaringBitmap> init;

  public OrNotTruncationTest(RoaringBitmap other, Consumer<RoaringBitmap> init) {
    this.other = other;
    this.init = init;
  }

  @Test
  public void testTruncation() {
    RoaringBitmap one = new RoaringBitmap();
    one.add(0);
    one.add(10);
    init.accept(other);
    one.orNot(other, 7);
    Assert.assertTrue(one.contains(10));
  }

}
