package org.roaringbitmap;

import org.junit.Test;

import static org.junit.Assert.*;
import static org.roaringbitmap.RoaringBitmapWriter.bufferWriter;
import static org.roaringbitmap.RoaringBitmapWriter.writer;

public class TestRoaringBitmapWriterWizard {

  @Test
  public void whenConstantMemoryIsSelectedWizardCreatesConstantMemoryWriter() {
    assertTrue(writer().constantMemory().get() instanceof ConstantMemoryContainerAppender);
  }

  @Test
  public void whenFastRankIsSelectedWizardCreatesFastRankRoaringBitmap() {
    assertNotNull(writer().fastRank().get().getUnderlying());
  }

  @Test(expected = IllegalStateException.class)
  public void whenFastRankIsSelectedBufferWizardThrows() {
    bufferWriter().fastRank().get().getUnderlying();
  }

  @Test
  public void shouldRespectProvidedStorageSizeHint() {
    assertEquals(20, writer().initialCapacity(20).get().getUnderlying().highLowContainer.keys.length);
  }

}
