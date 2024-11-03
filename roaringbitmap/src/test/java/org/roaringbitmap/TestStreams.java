/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class TestStreams {
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

  private static int[] takeSortedAndDistinct(Random source, int count) {
    LinkedHashSet<Integer> ints = new LinkedHashSet<Integer>(count);
    for (int size = 0; size < count; size++) {
      int next;
      do {
        next = Math.abs(source.nextInt());
      } while (!ints.add(next));
    }
    int[] unboxed = Ints.toArray(ints);
    Arrays.sort(unboxed);
    return unboxed;
  }

  @Test
  public void testEmptyViaIteration() {
    assertFalse(RoaringBitmap.bitmapOf().stream().iterator().hasNext());
    assertFalse(RoaringBitmap.bitmapOf().reverseStream().iterator().hasNext());
  }

  @Test
  public void testViaIteration() {
    final Random source = new Random(0xcb000a2b9b5bdfb6l);
    final int[] data = takeSortedAndDistinct(source, 450000);
    RoaringBitmap bitmap = RoaringBitmap.bitmapOf(data);

    final List<Integer> iteratorCopy = ImmutableList.copyOf(bitmap.iterator());
    final List<Integer> intIteratorCopy =
        bitmap.stream().mapToObj(Integer::valueOf).collect(Collectors.toList());
    final List<Integer> reverseIntIteratorCopy =
        bitmap.reverseStream().mapToObj(Integer::valueOf).collect(Collectors.toList());

    assertEquals(bitmap.getCardinality(), iteratorCopy.size());
    assertEquals(bitmap.getCardinality(), intIteratorCopy.size());
    assertEquals(bitmap.getCardinality(), reverseIntIteratorCopy.size());
    assertEquals(Ints.asList(data), iteratorCopy);
    assertEquals(Ints.asList(data), intIteratorCopy);
    assertEquals(Lists.reverse(Ints.asList(data)), reverseIntIteratorCopy);
  }

  @Test
  public void testSmallIteration() {
    RoaringBitmap bitmap = RoaringBitmap.bitmapOf(1, 2, 3);

    final List<Integer> iteratorCopy = ImmutableList.copyOf(bitmap.iterator());
    final List<Integer> intIteratorCopy =
        bitmap.stream().mapToObj(Integer::valueOf).collect(Collectors.toList());
    final List<Integer> reverseIntIteratorCopy =
        bitmap.reverseStream().mapToObj(Integer::valueOf).collect(Collectors.toList());

    assertEquals(ImmutableList.of(1, 2, 3), iteratorCopy);
    assertEquals(ImmutableList.of(1, 2, 3), intIteratorCopy);
    assertEquals(ImmutableList.of(3, 2, 1), reverseIntIteratorCopy);
    assertEquals(bitmap.last(), bitmap.reverseStream().max().getAsInt());
    assertEquals(bitmap.last(), bitmap.stream().max().getAsInt());
  }
}
