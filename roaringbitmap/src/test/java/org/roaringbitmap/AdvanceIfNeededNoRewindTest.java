package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.function.Supplier;

/**
 * advanceIfNeeded must never move an iterator backwards: once values have been consumed, a target
 * that lies behind the current position is a no-op.
 */
public class AdvanceIfNeededNoRewindTest {

  private static RoaringBitmap bitmapWithRun() {
    RoaringBitmap bitmap = RoaringBitmap.bitmapOf();
    bitmap.add(36000L, 41001L);
    bitmap.add(0x30000L, 0x30000L + 100);
    bitmap.runOptimize();
    return bitmap;
  }

  private static ImmutableRoaringBitmap direct(RoaringBitmap bitmap) {
    ByteBuffer buffer = ByteBuffer.allocateDirect(bitmap.serializedSizeInBytes());
    bitmap.serialize(buffer);
    buffer.flip();
    return new ImmutableRoaringBitmap(buffer);
  }

  private static void checkForward(Supplier<PeekableIntIterator> iterators) {
    PeekableIntIterator it = iterators.get();
    it.advanceIfNeeded(40000);
    for (int i = 0; i < 10; i++) {
      assertEquals(40000 + i, it.next());
    }
    it.advanceIfNeeded(36500);
    assertTrue(it.hasNext());
    assertEquals(40010, it.peekNext());
    assertEquals(40010, it.next());
  }

  private static void checkReverse(Supplier<PeekableIntIterator> iterators) {
    PeekableIntIterator it = iterators.get();
    it.advanceIfNeeded(36311);
    for (int i = 0; i < 10; i++) {
      assertEquals(36311 - i, it.next());
    }
    it.advanceIfNeeded(40892);
    assertTrue(it.hasNext());
    assertEquals(36301, it.peekNext());
    assertEquals(36301, it.next());
  }

  @Test
  public void heapRunIteratorsDoNotRewind() {
    RoaringBitmap bitmap = bitmapWithRun();
    checkForward(bitmap::getIntIterator);
    checkReverse(bitmap::getReverseIntIterator);
  }

  @Test
  public void heapFlyweightRunIteratorsDoNotRewind() {
    RoaringBitmap bitmap = bitmapWithRun();
    checkForward(() -> new IntIteratorFlyweight(bitmap));
    checkReverse(() -> new ReverseIntIteratorFlyweight(bitmap));
  }

  @Test
  public void arrayBackedMappeableRunIteratorsDoNotRewind() {
    MutableRoaringBitmap bitmap = bitmapWithRun().toMutableRoaringBitmap();
    checkForward(bitmap::getIntIterator);
    checkReverse(bitmap::getReverseIntIterator);
  }

  @Test
  public void directMappeableRunIteratorsDoNotRewind() {
    ImmutableRoaringBitmap bitmap = direct(bitmapWithRun());
    checkForward(bitmap::getIntIterator);
    checkReverse(bitmap::getReverseIntIterator);
  }

  @Test
  public void reverseAdvanceStillMovesWithinCurrentRun() {
    RoaringBitmap bitmap = bitmapWithRun();
    PeekableIntIterator it = bitmap.getReverseIntIterator();
    it.advanceIfNeeded(40000);
    assertEquals(40000, it.next());
    it.advanceIfNeeded(36000);
    assertEquals(36000, it.next());
    assertFalse(it.hasNext());
  }
}
