package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.roaringbitmap.RoaringBitmapWriter.bufferWriter;
import static org.roaringbitmap.RoaringBitmapWriter.writer;

import org.junit.jupiter.api.Test;

public class TestRoaringBitmapWriterWizard {

  @Test
  public void whenConstantMemoryIsSelectedWizardCreatesConstantMemoryWriter() {
    assertTrue(writer().constantMemory().get() instanceof ConstantMemoryContainerAppender);
  }

  @Test
  public void whenFastRankIsSelectedWizardCreatesFastRankRoaringBitmap() {
    assertNotNull(writer().fastRank().get().getUnderlying());
  }

  @Test
  public void whenFastRankIsSelectedBufferWizardThrows() {
    assertThrows(
        IllegalStateException.class, () -> bufferWriter().fastRank().get().getUnderlying());
  }

  @Test
  public void shouldRespectProvidedStorageSizeHint() {
    assertEquals(
        20, writer().initialCapacity(20).get().getUnderlying().highLowContainer.keys.length);
  }
}
