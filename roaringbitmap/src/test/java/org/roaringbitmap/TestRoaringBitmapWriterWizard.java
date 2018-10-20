package org.roaringbitmap;

import org.junit.Test;

import static org.junit.Assert.*;

public class TestRoaringBitmapWriterWizard {

  @Test
  public void whenConstantMemoryIsSelectedWizardCreatesConstantMemoryWriter() {
    assertTrue(RoaringBitmapWriter.writer().constantMemory().get() instanceof ConstantMemoryContainerAppender);
  }

  @Test
  public void whenFastRankIsSelectedWizardCreatesFastRankRoaringBitmap() {
    assertNotNull(RoaringBitmapWriter.writer().fastRank().get().getUnderlying());
  }

  @Test(expected = IllegalStateException.class)
  public void whenFastRankIsSelectedBufferWizardThrows() {
    RoaringBitmapWriter.bufferWriter().fastRank().get().getUnderlying();
  }

  @Test
  public void shouldRespectProvidedStorageSizeHint() {
    assertEquals(20, RoaringBitmapWriter.writer().initialCapacity(20).get().getUnderlying().highLowContainer.keys.length);
  }

}
