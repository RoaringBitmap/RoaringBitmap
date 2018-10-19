package org.roaringbitmap;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestOrderedWriter {

  @Test
  public void shouldPermitAddingInReverseOrder_Sparse() {
    addInReverseOrder(new SparseOrderedWriter(new RoaringBitmap()));
  }

  @Test
  public void shouldPermitAddingInReverseOrder_Dense() {
    addInReverseOrder(new DenseOrderedWriter(new RoaringBitmap()));
  }

  public void addInReverseOrder(OrderedWriter writer) {
    writer.add(1 << 17);
    writer.add(0);
    writer.flush();
    assertEquals(RoaringBitmap.bitmapOf(0, 1 << 17), writer.getUnderlying());
  }

  @Test
  public void bitmapShouldContainAllValuesAfterFlush_Sparse() {
    bitmapShouldContainAllValuesAfterFlush(new SparseOrderedWriter());
  }

  @Test
  public void bitmapShouldContainAllValuesAfterFlush_Dense() {
    bitmapShouldContainAllValuesAfterFlush(new DenseOrderedWriter());
  }

  public void bitmapShouldContainAllValuesAfterFlush(OrderedWriter writer) {
    writer.add(0);
    writer.add(1 << 17);
    writer.flush();
    Assert.assertTrue(writer.getUnderlying().contains(0));
    Assert.assertTrue(writer.getUnderlying().contains(1 << 17));
  }

  @Test
  public void newKeyShouldTriggerFlush_Dense() {
    newKeyShouldTriggerFlush(new DenseOrderedWriter());
  }

  @Test
  public void newKeyShouldTriggerFlush_Sparse() {
    newKeyShouldTriggerFlush(new SparseOrderedWriter());
  }

  public void newKeyShouldTriggerFlush(OrderedWriter writer) {
    writer.add(0);
    writer.add(1 << 17);
    Assert.assertTrue(writer.getUnderlying().contains(0));
    writer.add(1 << 18);
    Assert.assertTrue(writer.getUnderlying().contains(1 << 17));
  }


  @Test
  public void shouldPermitWritingToSameKeyAfterManualFlush_Dense() {
    writeSameKeyAfterManualFlush(new DenseOrderedWriter());
  }

  @Test
  public void shouldPermitWritingToSameKeyAfterManualFlush_Sparse() {
    writeSameKeyAfterManualFlush(new DenseOrderedWriter());
  }

  public void writeSameKeyAfterManualFlush(OrderedWriter writer) {
    writer.add(0);
    writer.flush();
    writer.add(1);
    writer.flush();
    assertEquals(RoaringBitmap.bitmapOf(0, 1), writer.getUnderlying());
  }
}
