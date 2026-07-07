package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class ReverseBitmapIteratorWordZeroTest {

  @Test
  public void reverseAdvanceKeepsWordZeroValueAcrossEmptyWords() {
    final long[] words = new long[1024]; // 1024 * 64 = 65536 bits
    words[2] = 1L << 2;   // value 2 * 64 + 2 = 130 (word 2)
    words[0] = 1L << 40;  // value 40           (word 0)
    final BitmapContainer bc = new BitmapContainer(words, 2);

    final PeekableCharIterator it = bc.getReverseCharIterator();
    // 130 > 128 must be skipped; 40 <= 128 is the highest remaining value.
    it.advanceIfNeeded((char) 128);

    assertTrue(it.hasNext());
    assertEquals(40, it.peekNext());
    assertEquals(40, it.next());
    assertFalse(it.hasNext());
  }
}
