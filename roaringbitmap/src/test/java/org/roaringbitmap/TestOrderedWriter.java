package org.roaringbitmap;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestOrderedWriter {

  private final String writerType;

  public TestOrderedWriter(String writerType) {
    this.writerType = writerType;
  }

  @Parameterized.Parameters
  public static Object[][] params() {
    return new Object[][]
        {
            {"DENSE"},
            {"SPARSE"},
            {"ADAPTIVE"}
        };
  }

  private OrderedWriter createWriter(final RoaringBitmap roaringBitmap) {
    switch (writerType) {
      case "SPARSE":
        return new SparseOrderedWriter(roaringBitmap);
      case "DENSE":
        return new DenseOrderedWriter(roaringBitmap);
      case "ADAPTIVE":
        return new AdaptiveOrderedWriter(roaringBitmap);
      default:
        throw new IllegalStateException("Unknown OrderedWriter implementation: " + writerType);
    }
  }

  @Test(expected = IllegalStateException.class)
  public void shouldThrowWhenAddingInReverseOrder() {
    OrderedWriter writer = createWriter(new RoaringBitmap());
    writer.add(1 << 17);
    writer.add(0);
  }

  @Test
  public void bitmapShouldContainAllValuesAfterFlush() {
    RoaringBitmap bitmap = new RoaringBitmap();
    OrderedWriter writer = createWriter(bitmap);
    writer.add(0);
    writer.add(1 << 17);
    writer.flush();
    Assert.assertTrue(bitmap.contains(0));
    Assert.assertTrue(bitmap.contains(1 << 17));
  }

  @Test
  public void newKeyShouldTriggerFlush() {
    RoaringBitmap bitmap = new RoaringBitmap();
    OrderedWriter writer = createWriter(bitmap);
    writer.add(0);
    writer.add(1 << 17);
    Assert.assertTrue(bitmap.contains(0));
    writer.add(1 << 18);
    Assert.assertTrue(bitmap.contains(1 << 17));
  }

  @Test(expected = IllegalStateException.class)
  public void shouldThrowIfSameKeyIsWrittenToAfterManualFlush() {
    OrderedWriter writer = createWriter(new RoaringBitmap());
    writer.add(0);
    writer.flush();
    writer.add(1);
    writer.flush();
  }
}
