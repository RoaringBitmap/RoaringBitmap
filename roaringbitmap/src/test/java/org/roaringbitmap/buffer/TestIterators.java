/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap.buffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.roaringbitmap.CharIterator;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.PeekableIntIterator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

@Execution(ExecutionMode.CONCURRENT)
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

  // https://github.com/RoaringBitmap/RoaringBitmap/issues/475
  @Test
  public void testCorruptionInfiniteLoop() {
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
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
  public void testBitmapIteration() {
    final MappeableBitmapContainer bits =
        new MappeableBitmapContainer(2, LongBuffer.allocate(2).put(0x1l).put(1l << 63));

    assertEquals(asList(bits.getCharIterator()), ImmutableList.of(0, 127));
    assertEquals(asList(bits.getReverseCharIterator()), ImmutableList.of(127, 0));
  }

  @Test
  public void testEmptyIteration() {
    assertFalse(MutableRoaringBitmap.bitmapOf().iterator().hasNext());
    assertFalse(MutableRoaringBitmap.bitmapOf().getIntIterator().hasNext());
    assertFalse(MutableRoaringBitmap.bitmapOf().getSignedIntIterator().hasNext());
    assertFalse(MutableRoaringBitmap.bitmapOf().getReverseIntIterator().hasNext());
  }

  @Test
  public void testIteration() {
    final Random source = new Random(0xcb000a2b9b5bdfb6l);
    final int[] data = takeSortedAndDistinct(source, 450000, Integer::compareUnsigned);
    MutableRoaringBitmap bitmap = MutableRoaringBitmap.bitmapOf(data);

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
  public void testIteration1() {
    final Random source = new Random(0xcb000a2b9b5bdfb6l);
    final int[] data1 = takeSortedAndDistinct(source, 450000, Integer::compareUnsigned);

    HashSet<Integer> data1Members = new HashSet<Integer>(data1.length);
    for (int i : data1) {
      data1Members.add(i);
    }
    final List<Integer> data = new ArrayList<>(data1.length + 50000);
    data.addAll(data1Members);

    int counter = 77777;
    for (int i = data1.length; i < data.size(); ++i) {
      // ensure uniqueness
      while (data1Members.contains(counter)) {
        ++counter;
      }
      data.set(i, counter); // must be unique
      counter++;
      if (i % 15 == 0) {
        counter += 10; // runs of length 15 or so, with gaps of 10
      }
    }
    data.sort(Integer::compareUnsigned);

    MutableRoaringBitmap bitmap = MutableRoaringBitmap.bitmapOf(Ints.toArray(data));
    bitmap.runOptimize(); // result should have some runcontainers and some non.

    final List<Integer> iteratorCopy = ImmutableList.copyOf(bitmap.iterator());
    final List<Integer> intIteratorCopy = asList(bitmap.getIntIterator());
    final List<Integer> signedIntIteratorCopy = asList(bitmap.getSignedIntIterator());
    final List<Integer> reverseIntIteratorCopy = asList(bitmap.getReverseIntIterator());

    assertEquals(bitmap.getCardinality(), iteratorCopy.size());
    assertEquals(bitmap.getCardinality(), intIteratorCopy.size());
    assertEquals(bitmap.getCardinality(), signedIntIteratorCopy.size());
    assertEquals(bitmap.getCardinality(), reverseIntIteratorCopy.size());
    assertEquals(data, iteratorCopy);
    assertEquals(data, intIteratorCopy);
    assertEquals(data.stream().sorted().collect(Collectors.toList()), signedIntIteratorCopy);
    assertEquals(Lists.reverse(data), reverseIntIteratorCopy);
  }

  @Test
  public void testSmallIteration() {
    MutableRoaringBitmap bitmap =
        MutableRoaringBitmap.bitmapOf(1, 2, 3, -1, -2147483648, 2147483647, 0);

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
  public void testSmallIteration1() {
    MutableRoaringBitmap bitmap = MutableRoaringBitmap.bitmapOf(1, 2, 3, -1);
    bitmap.runOptimize();

    final List<Integer> iteratorCopy = ImmutableList.copyOf(bitmap.iterator());
    final List<Integer> intIteratorCopy = asList(bitmap.getIntIterator());
    final List<Integer> signedIntIteratorCopy = asList(bitmap.getSignedIntIterator());
    final List<Integer> reverseIntIteratorCopy = asList(bitmap.getReverseIntIterator());

    assertEquals(ImmutableList.of(1, 2, 3, -1), iteratorCopy);
    assertEquals(ImmutableList.of(1, 2, 3, -1), intIteratorCopy);
    assertEquals(ImmutableList.of(-1, 1, 2, 3), signedIntIteratorCopy);
    assertEquals(ImmutableList.of(-1, 3, 2, 1), reverseIntIteratorCopy);
  }

  @Test
  public void testSkips() {
    final Random source = new Random(0xcb000a2b9b5bdfb6L);
    final int[] data = takeSortedAndDistinct(source, 45000, Integer::compareUnsigned);
    MutableRoaringBitmap bitmap = MutableRoaringBitmap.bitmapOf(data);
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
    bitmap.getIntIterator().advanceIfNeeded(-1);
  }

  @Test
  public void testSkipsSignedIterator() {
    final Random source = new Random(0xcb000a2b9b5bdfb6L);
    final int[] data = takeSortedAndDistinct(source, 45000, Integer::compare);
    MutableRoaringBitmap bitmap = MutableRoaringBitmap.bitmapOf(data);
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
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
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

  @Test
  public void testIndexIterator4() throws Exception {
    MutableRoaringBitmap b = new MutableRoaringBitmap();
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
  public void testSkipsRun() {
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
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
  public void testEmptySkips() {
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    PeekableIntIterator it = bitmap.getIntIterator();
    it.advanceIfNeeded(0);
  }

  @Test
  public void testIteratorsOnLargeBitmap() throws IOException {
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();

    int inc = Short.MAX_VALUE;

    for (long i = -Integer.MIN_VALUE; i < Integer.MAX_VALUE; i += inc) {
      bitmap.add((int) i);
    }

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    bitmap.serialize(dos);
    dos.close();
    ByteBuffer bb = ByteBuffer.wrap(bos.toByteArray());
    ImmutableRoaringBitmap rrback1 = new ImmutableRoaringBitmap(bb);
    int j = 0;

    // we can iterate over the mutable bitmap
    for (int i : bitmap) {
      j += i;
    }

    int jj = 0;

    // we can iterate over the immutable bitmap
    for (int i : rrback1) {
      jj += i;
    }
    assertEquals(j, jj);
  }
}
