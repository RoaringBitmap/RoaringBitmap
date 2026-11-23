package org.roaringbitmap.buffer;

import static org.junit.jupiter.api.Assertions.*;

import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Random;

class ReverseBatchIteratorTest {
  @Test
  void mappedAndMutableMixedContainers() {
    Random r = new Random(767);
    for (int trial = 0; trial < 10; trial++) {
      RoaringBitmap heap = new RoaringBitmap();
      for (int i = 0; i < 1000; i++) heap.add(r.nextInt());
      heap.add(0L, 60000L);
      heap.add(0x80000000L, 0x8000f000L);
      for (int i = 0; i < 65536; i += 2) heap.add(0x10000 + i);
      heap.add(-1);
      heap.runOptimize();
      MutableRoaringBitmap mutable = heap.toMutableRoaringBitmap();
      ByteBuffer bytes = ByteBuffer.allocateDirect(mutable.serializedSizeInBytes());
      mutable.serialize(bytes);
      bytes.flip();
      int originalPosition = bytes.position();
      for (ImmutableRoaringBitmap b :
          new ImmutableRoaringBitmap[] {
            mutable, new ImmutableRoaringBitmap(bytes.asReadOnlyBuffer())
          }) {
        for (int size : new int[] {1, 63, 64, 65, 256, 4096}) {
          RoaringReverseBatchIterator it =
              new RoaringReverseBatchIterator(
                  b.highLowContainer.getContainerPointer(b.highLowContainer.size() - 1));
          PeekableIntIterator expected = b.getReverseIntIterator();
          int[] values = new int[size];
          while (expected.hasNext()) {
            assertTrue(it.hasNext());
            assertEquals(0, it.nextBatch(new int[0]));
            int n = it.nextBatch(values);
            assertTrue(n > 0);
            for (int i = 0; i < n; i++) assertEquals(expected.next(), values[i]);
          }
          assertFalse(it.hasNext());
          assertEquals(0, it.nextBatch(values));
        }
        RoaringReverseBatchIterator it =
            new RoaringReverseBatchIterator(
                b.highLowContainer.getContainerPointer(b.highLowContainer.size() - 1));
        PeekableIntIterator expected = b.getReverseIntIterator();
        int[] one = new int[1];
        for (int i = 0; i < 1000 && expected.hasNext(); i++) {
          int target = r.nextInt();
          it.advanceIfNeeded(target);
          while (expected.hasNext() && Integer.compareUnsigned(expected.peekNext(), target) > 0)
            expected.next();
          assertEquals(expected.hasNext(), it.hasNext());
          if (expected.hasNext()) {
            it.nextBatch(one);
            assertEquals(expected.next(), one[0]);
          }
        }
      }
      assertEquals(originalPosition, bytes.position());
    }
  }

  @Test
  void clonedPointerIsIndependent() {
    MutableRoaringBitmap b = new MutableRoaringBitmap();
    b.add(0L, 65536L);
    b.add(0xffff0000L, 0x100000000L);
    b.runOptimize();
    RoaringReverseBatchIterator it =
        new RoaringReverseBatchIterator(
            b.highLowContainer.getContainerPointer(b.highLowContainer.size() - 1));
    int[] first = new int[100];
    it.nextBatch(first);
    RoaringReverseBatchIterator clone = it.clone();
    int[] second = new int[100];
    assertEquals(it.nextBatch(first), clone.nextBatch(second));
    assertArrayEquals(first, second);
    clone.advanceIfNeeded(0);
    clone.nextBatch(second);
    assertEquals(0, second[0]);
    assertFalse(clone.hasNext());
    assertTrue(it.hasNext());
    MutableRoaringBitmap empty = new MutableRoaringBitmap();
    assertFalse(
        new RoaringReverseBatchIterator(empty.highLowContainer.getContainerPointer(-1)).hasNext());
  }

  @Test
  void withinContainerThresholdsAndClones() {
    for (int shape = 0; shape < 3; shape++) {
      RoaringBitmap b = new RoaringBitmap();
      if (shape == 0) for (int v = 0; v < 65536; v += 101) b.add(v);
      if (shape == 1) for (int v = 0; v < 65536; v += 2) b.add(v);
      if (shape == 2) {
        b.add(0L, 101L);
        b.add(200L, 500L);
        b.add(1000L, 65536L);
        b.runOptimize();
      }
      for (int target :
          new int[] {
            0, 1, 63, 64, 65, 99, 100, 101, 199, 200, 499, 500, 999, 1000, 32767, 65534, 65535
          }) {
        for (int consumed : new int[] {0, 1, 63, 64, 65, 256}) {
          MutableRoaringBitmap mutable = b.toMutableRoaringBitmap();
          ByteBuffer data = ByteBuffer.allocateDirect(mutable.serializedSizeInBytes());
          mutable.serialize(data);
          data.flip();
          ImmutableRoaringBitmap mapped = new ImmutableRoaringBitmap(data.asReadOnlyBuffer());
          RoaringReverseBatchIterator it =
              new RoaringReverseBatchIterator(
                  mapped.highLowContainer.getContainerPointer(mapped.highLowContainer.size() - 1));
          PeekableIntIterator expected = b.getReverseIntIterator();
          int[] prefix = new int[consumed];
          int n = it.nextBatch(prefix);
          for (int j = 0; j < n; j++) assertEquals(expected.next(), prefix[j]);
          it.advanceIfNeeded(target);
          while (expected.hasNext() && Integer.compareUnsigned(expected.peekNext(), target) > 0)
            expected.next();
          RoaringReverseBatchIterator clone = it.clone();
          int[] one = new int[1], other = new int[1];
          while (expected.hasNext()) {
            int v = expected.next();
            assertEquals(1, it.nextBatch(one));
            assertEquals(
                v, one[0], "shape=" + shape + " consumed=" + consumed + " target=" + target);
            assertEquals(1, clone.nextBatch(other));
            assertEquals(v, other[0]);
          }
          assertFalse(it.hasNext());
          assertFalse(clone.hasNext());
        }
      }
    }
  }
}
