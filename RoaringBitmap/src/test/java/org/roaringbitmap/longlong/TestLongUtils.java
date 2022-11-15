package org.roaringbitmap.longlong;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestLongUtils {
  @Test
  public void testHighPart() {
      Assertions.assertFalse(LongUtils.isMaxHigh(LongUtils.highPart(0)));
      Assertions.assertFalse(LongUtils.isMaxHigh(LongUtils.highPart(Long.MAX_VALUE)));
      Assertions.assertFalse(LongUtils.isMaxHigh(LongUtils.highPart(Long.MIN_VALUE)));

      Assertions.assertTrue(LongUtils.isMaxHigh(LongUtils.highPart(-1L)));
  }
}
