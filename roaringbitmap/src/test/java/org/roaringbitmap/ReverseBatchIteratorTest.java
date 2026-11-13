package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import java.util.Random;

class ReverseBatchIteratorTest {
  @Test
  void mixedContainersMatchScalarAndSkipWithoutRewinding() {
    Random r = new Random(767);
    for (int trial = 0; trial < 20; trial++) {
      RoaringBitmap b = new RoaringBitmap();
      for (int i = 0; i < 1000; i++) b.add(r.nextInt());
      b.add(0L, 60000L);
      b.add(0x80000000L, 0x8000f000L);
      for (int i = 0; i < 65536; i += 2) b.add(0x10000 + i);
      b.add(-1);
      b.add(Integer.MIN_VALUE);
      b.runOptimize();
      for (int size : new int[] {1, 2, 63, 64, 65, 127, 256, 4096}) {
        RoaringReverseBatchIterator it = new RoaringReverseBatchIterator(b.highLowContainer);
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
      RoaringReverseBatchIterator it = new RoaringReverseBatchIterator(b.highLowContainer);
      PeekableIntIterator expected = b.getReverseIntIterator();
      int[] one = new int[1];
      for (int i = 0; i < 1000 && expected.hasNext(); i++) {
        int target = r.nextInt();
        it.advanceIfNeeded(target);
        while (expected.hasNext() && Integer.compareUnsigned(expected.peekNext(), target) > 0)
          expected.next();
        assertEquals(expected.hasNext(), it.hasNext());
        if (expected.hasNext()) {
          assertEquals(1, it.nextBatch(one));
          assertEquals(expected.next(), one[0]);
        }
      }
    }
  }

  @Test
  void cloneAndFullRunBoundaries() {
    RoaringBitmap b = new RoaringBitmap();
    b.add(0L, 65536L);
    b.add(0xffff0000L, 0x100000000L);
    b.runOptimize();
    RoaringReverseBatchIterator it = new RoaringReverseBatchIterator(b.highLowContainer);
    int[] batch = new int[100];
    assertEquals(100, it.nextBatch(batch));
    RoaringReverseBatchIterator clone = it.clone();
    int[] other = new int[100];
    assertEquals(it.nextBatch(batch), clone.nextBatch(other));
    assertArrayEquals(batch, other);
    clone.advanceIfNeeded(0);
    assertTrue(clone.hasNext());
    assertEquals(1, clone.nextBatch(other));
    assertEquals(0, other[0]);
    assertFalse(clone.hasNext());
    assertTrue(it.hasNext());
    assertFalse(new RoaringReverseBatchIterator(new RoaringBitmap().highLowContainer).hasNext());
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
          RoaringReverseBatchIterator it = new RoaringReverseBatchIterator(b.highLowContainer);
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

  @Test
  void publicFactoriesAndIntAdapter() {
    RoaringBitmap bitmap = RoaringBitmap.bitmapOf(0, 1, 63, 64, 65, 65536, Integer.MIN_VALUE, -1);
    BatchIterator[] batches = {
      bitmap.getReverseBatchIterator(), bitmap.toMutableRoaringBitmap().getReverseBatchIterator()
    };
    for (BatchIterator batch : batches) {
      IntIterator wrapped = batch.asIntIterator(new int[3]);
      assertTrue(wrapped.hasNext());
      assertEquals(-1, wrapped.next());
      IntIterator clone = wrapped.clone();
      int[] expected = {Integer.MIN_VALUE, 65536, 65, 64, 63, 1, 0};
      for (int value : expected) {
        assertTrue(wrapped.hasNext());
        assertTrue(clone.hasNext());
        assertEquals(value, wrapped.next());
        assertEquals(value, clone.next());
      }
      assertFalse(wrapped.hasNext());
      assertFalse(clone.hasNext());
    }
  }

  @Test
  void pagesMatchFilteredUnsignedValues() {
    Random r = new Random(1767);
    RoaringBitmap b = new RoaringBitmap();
    for (int i = 0; i < 100000; i++) b.add(r.nextInt());
    b.add(100L, 10000L);
    b.add(0xffff0000L, 0x100000000L);
    b.runOptimize();
    for (int trial = 0; trial < 300; trial++) {
      int target = r.nextInt();
      int pageSize = 1 + r.nextInt(512);
      BatchIterator[] pages = {
        b.getReverseBatchIterator(), b.toMutableRoaringBitmap().getReverseBatchIterator()
      };
      for (BatchIterator page : pages) {
        PeekableIntIterator expected = b.getReverseIntIterator();
        while (expected.hasNext() && Integer.compareUnsigned(expected.peekNext(), target) > 0)
          expected.next();
        page.advanceIfNeeded(target);
        int[] buffer = new int[pageSize];
        int count = page.nextBatch(buffer);
        for (int i = 0; i < count; i++) {
          assertTrue(expected.hasNext());
          assertEquals(expected.next(), buffer[i]);
        }
        assertTrue(count == pageSize || !expected.hasNext());
        if (expected.hasNext()) {
          page.advanceIfNeeded(-1);
          assertEquals(1, page.nextBatch(new int[1]));
        }
      }
    }
  }

  @Test
  void mixedOperationsFollowUnsignedArrayOracle() {
    Random random = new Random(176767);
    for (int trial = 0; trial < 100; trial++) {
      RoaringBitmap bitmap = new RoaringBitmap();
      for (int v = 0; v < 2000; v++) bitmap.add(random.nextInt());
      for (int run = 0; run < 30; run++) {
        long start = Integer.toUnsignedLong(random.nextInt());
        bitmap.add(start, Math.min(0x100000000L, start + 50 + random.nextInt(1000)));
      }
      if ((trial & 1) != 0) bitmap.runOptimize();
      int[] sorted = bitmap.toArray();
      BatchIterator[] iterators = {
        bitmap.getReverseBatchIterator(), bitmap.toMutableRoaringBitmap().getReverseBatchIterator()
      };
      for (BatchIterator iterator : iterators) {
        int position = sorted.length - 1;
        for (int operation = 0; operation < 1000; operation++) {
          if (random.nextBoolean()) {
            int target =
                position < 0
                    ? random.nextInt()
                    : sorted[Math.max(0, position - random.nextInt(40))];
            if (random.nextInt(4) == 0) target = -1;
            iterator.advanceIfNeeded(target);
            while (position >= 0 && Integer.compareUnsigned(sorted[position], target) > 0)
              position--;
          } else {
            int[] buffer = new int[random.nextInt(130)];
            int count = iterator.nextBatch(buffer);
            assertEquals(Math.min(buffer.length, position + 1), count);
            for (int j = 0; j < count; j++) assertEquals(sorted[position--], buffer[j]);
          }
          assertEquals(position >= 0, iterator.hasNext());
          if (operation % 50 == 0) {
            BatchIterator clone = iterator.clone();
            int[] buffer = new int[65];
            int count = clone.nextBatch(buffer);
            assertEquals(Math.min(buffer.length, position + 1), count);
            for (int j = 0; j < count; j++) assertEquals(sorted[position - j], buffer[j]);
          }
        }
      }
    }
  }
}
