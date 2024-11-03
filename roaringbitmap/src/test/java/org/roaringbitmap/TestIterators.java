/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class TestIterators {
  private static List<Integer> asList(IntIterator ints) {
    int[] values = new int[10];
    int size = 0;
    while (ints.hasNext()) {
      if (!(size < values.length)) {
        values = Arrays.copyOf(values, values.length * 2);
      }
      values[size++] = ints.next();
    }
    return Ints.asList(Arrays.copyOf(values, size));
  }

  private static List<Integer> asList(final CharIterator shorts) {
    return asList(
        new IntIterator() {
          @Override
          public IntIterator clone() {
            throw new UnsupportedOperationException();
          }

          @Override
          public boolean hasNext() {
            return shorts.hasNext();
          }

          @Override
          public int next() {
            return shorts.next();
          }
        });
  }

  private static int[] takeSortedAndDistinct(
      Random source, int count, Comparator<Integer> comparator) {
    HashSet<Integer> ints = new HashSet<Integer>(count);
    for (int size = 0; size < count; size++) {
      int next;
      do {
        next = source.nextInt();
      } while (!ints.add(next));
    }
    ArrayList<Integer> list = new ArrayList<Integer>(ints);
    list.sort(comparator);
    return Ints.toArray(list);
  }

  @Test
  public void testBitmapIteration() {
    final BitmapContainer bits = new BitmapContainer(new long[] {0x1l, 1l << 63}, 2);

    assertEquals(asList(bits.getCharIterator()), ImmutableList.of(0, 127));
    assertEquals(asList(bits.getReverseCharIterator()), ImmutableList.of(127, 0));
  }

  @Test
  public void testEmptyIteration() {
    assertFalse(RoaringBitmap.bitmapOf().iterator().hasNext());
    assertFalse(RoaringBitmap.bitmapOf().getIntIterator().hasNext());
    assertFalse(RoaringBitmap.bitmapOf().getSignedIntIterator().hasNext());
    assertFalse(RoaringBitmap.bitmapOf().getReverseIntIterator().hasNext());
  }

  @Test
  public void testIteration() {
    final Random source = new Random(0xcb000a2b9b5bdfb6l);
    final int[] data = takeSortedAndDistinct(source, 450000, Integer::compareUnsigned);
    RoaringBitmap bitmap = RoaringBitmap.bitmapOf(data);

    final List<Integer> iteratorCopy = ImmutableList.copyOf(bitmap.iterator());
    final List<Integer> intIteratorCopy = asList(bitmap.getIntIterator());
    final List<Integer> signedIntIteratorCopy = asList(bitmap.getSignedIntIterator());
    final List<Integer> reverseIntIteratorCopy = asList(bitmap.getReverseIntIterator());

    assertEquals(bitmap.getCardinality(), iteratorCopy.size());
    assertEquals(bitmap.getCardinality(), intIteratorCopy.size());
    assertEquals(bitmap.getCardinality(), signedIntIteratorCopy.size());
    assertEquals(bitmap.getCardinality(), reverseIntIteratorCopy.size());
    assertEquals(Ints.asList(data), iteratorCopy);
    assertEquals(Ints.asList(data), intIteratorCopy);
    assertEquals(
        Ints.asList(data).stream().sorted().collect(Collectors.toList()), signedIntIteratorCopy);
    assertEquals(Lists.reverse(Ints.asList(data)), reverseIntIteratorCopy);
  }

  @Test
  public void testSmallIteration() {
    RoaringBitmap bitmap = RoaringBitmap.bitmapOf(0, 1, 2, 3, -1, 2147483647, -2147483648);

    final List<Integer> iteratorCopy = ImmutableList.copyOf(bitmap.iterator());
    final List<Integer> intIteratorCopy = asList(bitmap.getIntIterator());
    final List<Integer> signedIntIteratorCopy = asList(bitmap.getSignedIntIterator());
    final List<Integer> reverseIntIteratorCopy = asList(bitmap.getReverseIntIterator());

    assertEquals(ImmutableList.of(0, 1, 2, 3, 2147483647, -2147483648, -1), iteratorCopy);
    assertEquals(ImmutableList.of(0, 1, 2, 3, 2147483647, -2147483648, -1), intIteratorCopy);
    assertEquals(ImmutableList.of(-2147483648, -1, 0, 1, 2, 3, 2147483647), signedIntIteratorCopy);
    assertEquals(ImmutableList.of(-1, -2147483648, 2147483647, 3, 2, 1, 0), reverseIntIteratorCopy);
  }

  @Test
  public void testSkips() {
    final Random source = new Random(0xcb000a2b9b5bdfb6L);
    final int[] data = takeSortedAndDistinct(source, 45000, Integer::compareUnsigned);
    RoaringBitmap bitmap = RoaringBitmap.bitmapOf(data);
    PeekableIntIterator pii = bitmap.getIntIterator();
    for (int i = 0; i < data.length; ++i) {
      pii.advanceIfNeeded(data[i]);
      assertEquals(data[i], pii.peekNext());
    }
    pii = bitmap.getIntIterator();
    for (int i = 0; i < data.length; ++i) {
      pii.advanceIfNeeded(data[i]);
      assertEquals(data[i], pii.next());
    }
    pii = bitmap.getIntIterator();
    for (int i = 1; i < data.length; ++i) {
      pii.advanceIfNeeded(data[i - 1]);
      pii.next();
      assertEquals(data[i], pii.peekNext());
    }
    bitmap.getIntIterator().advanceIfNeeded(-1); // should not crash
  }

  @Test
  public void testSkipsSignedIterator() {
    final Random source = new Random(0xcb000a2b9b5bdfb6L);
    int[] data = takeSortedAndDistinct(source, 45000, Integer::compare);
    RoaringBitmap bitmap = RoaringBitmap.bitmapOf(data);

    PeekableIntIterator pii = bitmap.getSignedIntIterator();
    for (int i = 0; i < data.length; ++i) {
      pii.advanceIfNeeded(data[i]);
      assertEquals(data[i], pii.peekNext());
    }
    pii = bitmap.getSignedIntIterator();
    for (int i = data.length - 1; i >= 0; --i) { // no backward advancing
      pii.advanceIfNeeded(data[i]);
      assertEquals(data[data.length - 1], pii.peekNext());
    }
    pii = bitmap.getSignedIntIterator();
    for (int i = 0; i < data.length; ++i) {
      pii.advanceIfNeeded(data[i]);
      assertEquals(data[i], pii.next());
    }
    pii = bitmap.getSignedIntIterator();
    for (int i = 1; i < data.length; ++i) {
      pii.advanceIfNeeded(data[i - 1]);
      pii.next();
      assertEquals(data[i], pii.peekNext());
    }
  }

  @Test
  public void testSkipsDense() {
    RoaringBitmap bitmap = new RoaringBitmap();
    int N = 100000;
    for (int i = 0; i < N; ++i) {
      bitmap.add(2 * i);
    }
    for (int i = 0; i < N; ++i) {
      PeekableIntIterator pii = bitmap.getIntIterator();
      pii.advanceIfNeeded(2 * i);
      assertEquals(pii.peekNext(), 2 * i);
      assertEquals(pii.next(), 2 * i);
    }
  }

  // https://github.com/RoaringBitmap/RoaringBitmap/issues/475
  @Test
  public void testCorruptionInfiniteLoop() {
    RoaringBitmap bitmap = new RoaringBitmap();
    bitmap.add(Integer.MAX_VALUE - 0);
    bitmap.add(Integer.MAX_VALUE - 1);
    bitmap.add(Integer.MAX_VALUE - 2);
    // Adding this one leads to the issue
    bitmap.add(Integer.MAX_VALUE - 3);
    bitmap.forEach(
        (org.roaringbitmap.IntConsumer)
            e -> {
              if (!bitmap.contains(e)) {
                throw new IllegalStateException("Not expecting to find: " + e);
              }
            });

    bitmap.runOptimize(); // This is the line causing the issue
    bitmap.forEach(
        (org.roaringbitmap.IntConsumer)
            e -> {
              if (!bitmap.contains(e)) {
                throw new IllegalStateException("Not expecting to find: " + e);
              }
            });
  }

  @Test
  public void testSkipsRun() {
    RoaringBitmap bitmap = new RoaringBitmap();
    bitmap.add(4L, 100000L);
    bitmap.runOptimize();
    for (int i = 4; i < 100000; ++i) {
      PeekableIntIterator pii = bitmap.getIntIterator();
      pii.advanceIfNeeded(i);
      assertEquals(pii.peekNext(), i);
      assertEquals(pii.next(), i);
    }
  }

  @Test
  public void testIndexIterator4() throws Exception {
    RoaringBitmap b = new RoaringBitmap();
    for (int i = 0; i < 4096; i++) {
      b.add(i);
    }
    PeekableIntIterator it = b.getIntIterator();
    it.advanceIfNeeded(4096);
    while (it.hasNext()) {
      it.next();
    }
  }

  @Test
  public void testEmptySkips() {
    RoaringBitmap bitmap = new RoaringBitmap();
    PeekableIntIterator it = bitmap.getIntIterator();
    it.advanceIfNeeded(0);
  }

  @Test
  public void testSkipIntoGaps() {
    RoaringBitmap bitset = new RoaringBitmap();

    bitset.add(2000000L, 2200000L);
    bitset.add(4000000L, 4300000L);

    PeekableIntIterator bitIt = bitset.getIntIterator();

    assertEquals(2000000, bitIt.peekNext());
    assertEquals(2000000, bitIt.next());

    assertTrue(bitset.contains(2100000));
    bitIt.advanceIfNeeded(2100000);
    assertEquals(2100000, bitIt.peekNext());
    assertEquals(2100000, bitIt.next());

    // advancing to a value not in either range should go to the first value of second range
    assertFalse(bitset.contains(2300000));
    bitIt.advanceIfNeeded(2300000);

    assertEquals(4000000, bitIt.peekNext());

    assertTrue(bitset.contains(4000000));
    bitIt.advanceIfNeeded(4000000);
    assertEquals(4000000, bitIt.peekNext());
    assertEquals(4000000, bitIt.next());
  }

  @Test
  public void testSkipIntoFarAwayGaps() {
    RoaringBitmap bitset = new RoaringBitmap();

    bitset.add(2000000L, 2200000L);
    bitset.add(4000000L, 4300000L);
    bitset.add(6000000L, 6400000L);

    PeekableIntIterator bitIt = bitset.getIntIterator();

    assertEquals(2000000, bitIt.peekNext());
    assertEquals(2000000, bitIt.next());

    assertTrue(bitset.contains(2100000));
    bitIt.advanceIfNeeded(2100000);
    assertEquals(2100000, bitIt.peekNext());
    assertEquals(2100000, bitIt.next());

    // advancing to a value not in any range but beyond second range
    // should go to the first value of third range
    assertFalse(bitset.contains(4325376 - 5)); // same container
    bitIt.advanceIfNeeded(4325376 - 5);

    assertEquals(6000000, bitIt.peekNext());

    assertTrue(bitset.contains(6000000));
    bitIt.advanceIfNeeded(6000000);
    assertEquals(6000000, bitIt.peekNext());
    assertEquals(6000000, bitIt.next());

    // reset
    bitIt = bitset.getIntIterator();

    assertEquals(2000000, bitIt.peekNext());
    assertEquals(2000000, bitIt.next());

    bitIt.advanceIfNeeded(2100000);
    assertEquals(2100000, bitIt.peekNext());
    assertEquals(2100000, bitIt.next());

    // advancing to a value not in any range but beyond second range
    // should go to the first value of third range
    assertFalse(bitset.contains(4325376 + 5)); // next container
    bitIt.advanceIfNeeded(4325376 + 5);

    assertEquals(6000000, bitIt.peekNext());

    bitIt.advanceIfNeeded(6000000);
    assertEquals(6000000, bitIt.peekNext());
    assertEquals(6000000, bitIt.next());
  }
}
