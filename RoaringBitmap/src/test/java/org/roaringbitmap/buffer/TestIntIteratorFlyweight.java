/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */


package org.roaringbitmap.buffer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.PeekableIntIterator;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class TestIntIteratorFlyweight {
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

  private static int[] takeSortedAndDistinct(Random source, int count) {
    LinkedHashSet<Integer> ints = new LinkedHashSet<>(count);
    for (int size = 0; size < count; size++) {
      int next;
      do {
        next = Math.abs(source.nextInt());
      } while (!ints.add(next));
    }
    // we add a range of continuous values
    for(int k = 1000; k < 10000; ++k) {
      ints.add(k);
    }
    int[] unboxed = Ints.toArray(ints);
    Arrays.sort(unboxed);
    return unboxed;
  }



  @Test
  public void testEmptyIteration() {
    BufferIntIteratorFlyweight iter = new BufferIntIteratorFlyweight();
    BufferReverseIntIteratorFlyweight reverseIter = new BufferReverseIntIteratorFlyweight();

    MutableRoaringBitmap bitmap = MutableRoaringBitmap.bitmapOf();
    iter.wrap(bitmap);
    reverseIter.wrap(bitmap);
    assertFalse(iter.hasNext());

    assertFalse(reverseIter.hasNext());
  }


  @Test
  public void testIteration() {
    final Random source = new Random(0xcb000a2b9b5bdfb6L);
    final int[] data = takeSortedAndDistinct(source, 450000);
    MutableRoaringBitmap bitmap = MutableRoaringBitmap.bitmapOf(data);

    BufferIntIteratorFlyweight iter = new BufferIntIteratorFlyweight();
    iter.wrap(bitmap);

    BufferIntIteratorFlyweight iter2 = new BufferIntIteratorFlyweight(bitmap);
    PeekableIntIterator j = bitmap.getIntIterator();
    for(int k = 0; k < data.length; k+=3) {
      iter2.advanceIfNeeded(data[k]);
      iter2.advanceIfNeeded(data[k]);
      j.advanceIfNeeded(data[k]);
      j.advanceIfNeeded(data[k]);
      assertEquals(j.peekNext(),data[k]);
      assertEquals(iter2.peekNext(),data[k]);
    }
    new BufferIntIteratorFlyweight(bitmap).advanceIfNeeded(-1);
    bitmap.getIntIterator().advanceIfNeeded(-1);// should not crash


    BufferReverseIntIteratorFlyweight reverseIter = new BufferReverseIntIteratorFlyweight();
    reverseIter.wrap(bitmap);

    final List<Integer> intIteratorCopy = asList(iter);
    final List<Integer> reverseIntIteratorCopy = asList(reverseIter);

    assertEquals(bitmap.getCardinality(), intIteratorCopy.size());
    assertEquals(bitmap.getCardinality(), reverseIntIteratorCopy.size());

    assertEquals(Ints.asList(data), intIteratorCopy);
    assertEquals(Lists.reverse(Ints.asList(data)), reverseIntIteratorCopy);
  }


  @Test
  public void testIterationFromBitmap() {
    final Random source = new Random(0xcb000a2b9b5bdfb6L);
    final int[] data = takeSortedAndDistinct(source, 450000);
    MutableRoaringBitmap bitmap = MutableRoaringBitmap.bitmapOf(data);

    BufferIntIteratorFlyweight iter = new BufferIntIteratorFlyweight(bitmap);
    assertEquals(iter.peekNext(),data[0]);
    assertEquals(iter.peekNext(),data[0]);

    BufferIntIteratorFlyweight iter2 = new BufferIntIteratorFlyweight(bitmap);
    PeekableIntIterator j = bitmap.getIntIterator();
    for(int k = 0; k < data.length; k+=3) {
      iter2.advanceIfNeeded(data[k]);
      iter2.advanceIfNeeded(data[k]);
      j.advanceIfNeeded(data[k]);
      j.advanceIfNeeded(data[k]);
      assertEquals(data[k], j.peekNext());
      assertEquals(data[k], iter2.peekNext());
    }



    BufferReverseIntIteratorFlyweight reverseIter = new BufferReverseIntIteratorFlyweight(bitmap);

    final List<Integer> intIteratorCopy = asList(iter);
    final List<Integer> reverseIntIteratorCopy = asList(reverseIter);

    assertEquals(bitmap.getCardinality(), intIteratorCopy.size());
    assertEquals(bitmap.getCardinality(), reverseIntIteratorCopy.size());

    assertEquals(Ints.asList(data), intIteratorCopy);
    assertEquals(Lists.reverse(Ints.asList(data)), reverseIntIteratorCopy);
  }


  @Test
  public void testIterationFromBitmapClone() {
    final Random source = new Random(0xcb000a2b9b5bdfb6L);
    final int[] data = takeSortedAndDistinct(source, 450000);
    MutableRoaringBitmap bitmap = MutableRoaringBitmap.bitmapOf(data);

    BufferIntIteratorFlyweight iter = new BufferIntIteratorFlyweight(bitmap);

    BufferReverseIntIteratorFlyweight reverseIter = (BufferReverseIntIteratorFlyweight) new BufferReverseIntIteratorFlyweight(bitmap).clone();

    final List<Integer> intIteratorCopy = asList(iter);
    final List<Integer> reverseIntIteratorCopy = asList(reverseIter);

    assertEquals(bitmap.getCardinality(), intIteratorCopy.size());
    assertEquals(bitmap.getCardinality(), reverseIntIteratorCopy.size());

    assertEquals(Ints.asList(data), intIteratorCopy);
    assertEquals(Lists.reverse(Ints.asList(data)), reverseIntIteratorCopy);
  }

  @Test
  public void testIteration1() {
    final Random source = new Random(0xcb000a2b9b5bdfb6L);
    final int[] data1 = takeSortedAndDistinct(source, 450000);
    final int[] data = Arrays.copyOf(data1, data1.length + 50000);

    LinkedHashSet<Integer> data1Members = new LinkedHashSet<>();
    for (int i : data1) {
      data1Members.add(i);
    }

    int counter = 77777;
    for (int i = data1.length; i < data.length; ++i) {
      // ensure uniqueness
      while (data1Members.contains(counter)) {
        ++counter;
      }
      data[i] = counter; // must be unique
      counter++;
      if (i % 15 == 0) {
        counter += 10; // runs of length 15 or so, with gaps of 10
      }
    }
    Arrays.sort(data);

    MutableRoaringBitmap bitmap = MutableRoaringBitmap.bitmapOf(data);

    BufferIntIteratorFlyweight iter = new BufferIntIteratorFlyweight();
    iter.wrap(bitmap);

    BufferReverseIntIteratorFlyweight reverseIter = new BufferReverseIntIteratorFlyweight();
    reverseIter.wrap(bitmap);

    final List<Integer> intIteratorCopy = asList(iter);
    final List<Integer> reverseIntIteratorCopy = asList(reverseIter);

    assertEquals(bitmap.getCardinality(), intIteratorCopy.size());
    assertEquals(bitmap.getCardinality(), reverseIntIteratorCopy.size());

    assertEquals(Ints.asList(data), intIteratorCopy);
    assertEquals(Lists.reverse(Ints.asList(data)), reverseIntIteratorCopy);
  }

  @Test
  public void testSmallIteration() {
    MutableRoaringBitmap bitmap = MutableRoaringBitmap.bitmapOf(1, 2, 3);

    BufferIntIteratorFlyweight iter = new BufferIntIteratorFlyweight();
    iter.wrap(bitmap);

    BufferReverseIntIteratorFlyweight reverseIter = new BufferReverseIntIteratorFlyweight();
    reverseIter.wrap(bitmap);

    final List<Integer> intIteratorCopy = asList(iter);
    final List<Integer> reverseIntIteratorCopy = asList(reverseIter);
    assertEquals(ImmutableList.of(1, 2, 3), intIteratorCopy);
    assertEquals(ImmutableList.of(3, 2, 1), reverseIntIteratorCopy);
  }


}
