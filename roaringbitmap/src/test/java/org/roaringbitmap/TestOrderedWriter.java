package org.roaringbitmap;

import org.junit.Assert;
import org.junit.Test;

public class TestOrderedWriter {

  @Test(expected = IllegalStateException.class)
  public void shouldThrowWhenAddingInReverseOrder() {
    OrderedWriter writer = new OrderedWriter(new RoaringBitmap());
    writer.add(1 << 17);
    writer.add(0);
  }

  @Test
  public void bitmapShouldContainAllValuesAfterFlush() {
    RoaringBitmap bitmap = new RoaringBitmap();
    OrderedWriter writer = new OrderedWriter(bitmap);
    writer.add(0);
    writer.add(1 << 17);
    writer.flush();
    Assert.assertTrue(bitmap.contains(0));
    Assert.assertTrue(bitmap.contains(1 << 17));
  }

  @Test
  public void newKeyShouldTriggerFlush() {
    RoaringBitmap bitmap = new RoaringBitmap();
    OrderedWriter writer = new OrderedWriter(bitmap);
    writer.add(0);
    writer.add(1 << 17);
    Assert.assertTrue(bitmap.contains(0));
    writer.add(1 << 18);
    Assert.assertTrue(bitmap.contains(1 << 17));
  }

  @Test(expected = IllegalStateException.class)
  public void shouldThrowIfSameKeyIsWrittenToAfterManualFlush() {
    OrderedWriter writer = new OrderedWriter(new RoaringBitmap());
    writer.add(0);
    writer.flush();
    writer.add(1);
    writer.flush();
  }
}
