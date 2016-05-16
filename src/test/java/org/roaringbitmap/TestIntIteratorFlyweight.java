/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */


package org.roaringbitmap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;

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
    LinkedHashSet<Integer> ints = new LinkedHashSet<Integer>(count);
    for (int size = 0; size < count; size++) {
      int next;
      do {
        next = Math.abs(source.nextInt());
      } while (!ints.add(next));
    }
    // we add a range of continuous values
    for(int k = 1000; k < 10000; ++k) {
      if(!ints.contains(k))
        ints.add(k);
    }
    int[] unboxed = Ints.toArray(ints);
    Arrays.sort(unboxed);
    return unboxed;
  }

  @Test
  public void testEmptyIteration() {
    IntIteratorFlyweight iter = new IntIteratorFlyweight();
    ReverseIntIteratorFlyweight reverseIter = new ReverseIntIteratorFlyweight();

    RoaringBitmap bitmap = RoaringBitmap.bitmapOf();
    iter.wrap(bitmap);
    reverseIter.wrap(bitmap);
    Assert.assertFalse(iter.hasNext());

    Assert.assertFalse(reverseIter.hasNext());
  }



  @Test
  public void testIteration() {
    final Random source = new Random(0xcb000a2b9b5bdfb6l);
    final int[] data = takeSortedAndDistinct(source, 450000);

    // make at least one long run
    for (int i = 0; i < 25000; ++i) {
      data[70000 + i] = data[70000] + i;
    }

    RoaringBitmap bitmap = RoaringBitmap.bitmapOf(data);
    bitmap.runOptimize();

    IntIteratorFlyweight iter = new IntIteratorFlyweight();
    iter.wrap(bitmap);
    
    IntIteratorFlyweight iter2 = new IntIteratorFlyweight(bitmap);
    PeekableIntIterator j = bitmap.getIntIterator();
    for(int k = 0; k < data.length; k+=3) {
      iter2.advanceIfNeeded(data[k]);
      iter2.advanceIfNeeded(data[k]);
      j.advanceIfNeeded(data[k]);
      j.advanceIfNeeded(data[k]);
      Assert.assertEquals(j.peekNext(),data[k]);            
      Assert.assertEquals(iter2.peekNext(),data[k]);
    }
    new IntIteratorFlyweight(bitmap).advanceIfNeeded(-1);
    bitmap.getIntIterator().advanceIfNeeded(-1);// should not crash

    ReverseIntIteratorFlyweight reverseIter = new ReverseIntIteratorFlyweight();
    reverseIter.wrap(bitmap);

    final List<Integer> intIteratorCopy = asList(iter);
    final List<Integer> reverseIntIteratorCopy = asList(reverseIter);


    Assert.assertEquals(bitmap.getCardinality(), intIteratorCopy.size());
    Assert.assertEquals(bitmap.getCardinality(), reverseIntIteratorCopy.size());

    Assert.assertEquals(Ints.asList(data), intIteratorCopy);
    Assert.assertEquals(Lists.reverse(Ints.asList(data)), reverseIntIteratorCopy);
  }

  @Test
  public void testIterationFromBitmap() {
    final Random source = new Random(0xcb000a2b9b5bdfb6l);
    final int[] data = takeSortedAndDistinct(source, 450000);

    // make at least one long run
    for (int i = 0; i < 25000; ++i) {
      data[70000 + i] = data[70000] + i;
    }

    RoaringBitmap bitmap = RoaringBitmap.bitmapOf(data);
    bitmap.runOptimize();

    IntIteratorFlyweight iter = new IntIteratorFlyweight(bitmap);
    Assert.assertEquals(iter.peekNext(),data[0]);
    Assert.assertEquals(iter.peekNext(),data[0]);
    
    IntIteratorFlyweight iter2 = new IntIteratorFlyweight(bitmap);
    PeekableIntIterator j = bitmap.getIntIterator();
    for(int k = 0; k < data.length; k+=3) {
      iter2.advanceIfNeeded(data[k]);
      iter2.advanceIfNeeded(data[k]);
      j.advanceIfNeeded(data[k]);
      j.advanceIfNeeded(data[k]);
      Assert.assertEquals(j.peekNext(),data[k]);
      Assert.assertEquals(iter2.peekNext(),data[k]);
    }
    
    ReverseIntIteratorFlyweight reverseIter = new ReverseIntIteratorFlyweight(bitmap);

    final List<Integer> intIteratorCopy = asList(iter);
    final List<Integer> reverseIntIteratorCopy = asList(reverseIter);


    Assert.assertEquals(bitmap.getCardinality(), intIteratorCopy.size());
    Assert.assertEquals(bitmap.getCardinality(), reverseIntIteratorCopy.size());

    Assert.assertEquals(Ints.asList(data), intIteratorCopy);
    Assert.assertEquals(Lists.reverse(Ints.asList(data)), reverseIntIteratorCopy);
  }

  @Test
  public void testIterationFromBitmapClone() {
    final Random source = new Random(0xcb000a2b9b5bdfb6l);
    final int[] data = takeSortedAndDistinct(source, 450000);

    // make at least one long run
    for (int i = 0; i < 25000; ++i) {
      data[70000 + i] = data[70000] + i;
    }

    RoaringBitmap bitmap = RoaringBitmap.bitmapOf(data);
    bitmap.runOptimize();

    IntIteratorFlyweight iter = (IntIteratorFlyweight) new IntIteratorFlyweight(bitmap).clone();

    ReverseIntIteratorFlyweight reverseIter = (ReverseIntIteratorFlyweight) new ReverseIntIteratorFlyweight(bitmap).clone();

    final List<Integer> intIteratorCopy = asList(iter);
    final List<Integer> reverseIntIteratorCopy = asList(reverseIter);


    Assert.assertEquals(bitmap.getCardinality(), intIteratorCopy.size());
    Assert.assertEquals(bitmap.getCardinality(), reverseIntIteratorCopy.size());

    Assert.assertEquals(Ints.asList(data), intIteratorCopy);
    Assert.assertEquals(Lists.reverse(Ints.asList(data)), reverseIntIteratorCopy);
  }  
  @Test
  public void testIterationSmall() {

    final int[] data = new int[] {1, 2, 3, 4, 5, 6, 100, 101, 102, 103, 104, 105, 50000, 50001,
        50002, 1000000, 1000005, 1000007}; // runcontainer then arraycontainer
    RoaringBitmap bitmap = RoaringBitmap.bitmapOf(data);
    bitmap.runOptimize();

    IntIteratorFlyweight iter = new IntIteratorFlyweight();
    iter.wrap(bitmap);

    ReverseIntIteratorFlyweight reverseIter = new ReverseIntIteratorFlyweight();
    reverseIter.wrap(bitmap);

    final List<Integer> intIteratorCopy = asList(iter);
    final List<Integer> reverseIntIteratorCopy = asList(reverseIter);


    Assert.assertEquals(bitmap.getCardinality(), intIteratorCopy.size());
    Assert.assertEquals(bitmap.getCardinality(), reverseIntIteratorCopy.size());

    Assert.assertEquals(Ints.asList(data), intIteratorCopy);
    Assert.assertEquals(Lists.reverse(Ints.asList(data)), reverseIntIteratorCopy);
  }

  @Test
  public void testSmallIteration() {
    RoaringBitmap bitmap = RoaringBitmap.bitmapOf(1, 2, 3);

    IntIteratorFlyweight iter = new IntIteratorFlyweight();
    iter.wrap(bitmap);

    ReverseIntIteratorFlyweight reverseIter = new ReverseIntIteratorFlyweight();
    reverseIter.wrap(bitmap);

    final List<Integer> intIteratorCopy = asList(iter);
    final List<Integer> reverseIntIteratorCopy = asList(reverseIter);
    Assert.assertEquals(ImmutableList.of(1, 2, 3), intIteratorCopy);
    Assert.assertEquals(ImmutableList.of(3, 2, 1), reverseIntIteratorCopy);
  }

  @Test
  public void testClone() {
    RoaringBitmap bitmap = RoaringBitmap.bitmapOf(1, 2, 3, 4, 5);
    IntIteratorFlyweight iter = new IntIteratorFlyweight(bitmap);
    PeekableIntIterator iterClone = iter.clone();
    final List<Integer> iterList = asList(iter);
    final List<Integer> iterCloneList = asList(iterClone);
    Assert.assertEquals(iterList.toString(), iterCloneList.toString());
  }
}
