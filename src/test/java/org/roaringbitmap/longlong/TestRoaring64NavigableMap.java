package org.roaringbitmap.longlong;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.Util;

public class TestRoaring64NavigableMap {


  private Roaring64NavigableMap newBuffered() {
    return new Roaring64NavigableMap(true, new MutableRoaringBitmapSupplier());
  }

  @Test
  public void testEmpty() {
    Roaring64NavigableMap map = new Roaring64NavigableMap();

    Assert.assertFalse(map.getLongIterator().hasNext());

    Assert.assertEquals(0, map.getLongCardinality());
    Assert.assertTrue(map.isEmpty());
    Assert.assertFalse(map.contains(0));

    Assert.assertEquals(0, map.rankLong(Long.MIN_VALUE));
    Assert.assertEquals(0, map.rankLong(Long.MIN_VALUE + 1));
    Assert.assertEquals(0, map.rankLong(-1));
    Assert.assertEquals(0, map.rankLong(0));
    Assert.assertEquals(0, map.rankLong(1));
    Assert.assertEquals(0, map.rankLong(Long.MAX_VALUE - 1));
    Assert.assertEquals(0, map.rankLong(Long.MAX_VALUE));
  }

  @Test
  public void testZero() {
    Roaring64NavigableMap map = new Roaring64NavigableMap();

    map.addLong(0);

    {
      LongIterator iterator = map.getLongIterator();

      Assert.assertTrue(iterator.hasNext());
      Assert.assertEquals(0, iterator.next());
      Assert.assertEquals(0, map.select(0));
      Assert.assertTrue(map.contains(0));

      Assert.assertFalse(iterator.hasNext());
    }

    Assert.assertEquals(1, map.getLongCardinality());
    Assert.assertFalse(map.isEmpty());

    Assert.assertEquals(0, map.rankLong(Long.MIN_VALUE));
    Assert.assertEquals(0, map.rankLong(Integer.MIN_VALUE - 1L));
    Assert.assertEquals(0, map.rankLong(-1));
    Assert.assertEquals(1, map.rankLong(0));
    Assert.assertEquals(1, map.rankLong(1));
    Assert.assertEquals(1, map.rankLong(Integer.MAX_VALUE + 1L));
    Assert.assertEquals(1, map.rankLong(Long.MAX_VALUE));
  }



  @Test
  public void testAddNotAddLong() {
    Roaring64NavigableMap map = new Roaring64NavigableMap();

    // Use BitmapProvider.add instead of .addLong
    map.add(0);

    {
      LongIterator iterator = map.getLongIterator();

      Assert.assertTrue(iterator.hasNext());
      Assert.assertEquals(0, iterator.next());
      Assert.assertEquals(0, map.select(0));
      Assert.assertTrue(map.contains(0));

      Assert.assertFalse(iterator.hasNext());
    }

    Assert.assertEquals(1, map.getLongCardinality());
    Assert.assertFalse(map.isEmpty());

    Assert.assertEquals(0, map.rankLong(Long.MIN_VALUE));
    Assert.assertEquals(0, map.rankLong(Integer.MIN_VALUE - 1L));
    Assert.assertEquals(0, map.rankLong(-1));
    Assert.assertEquals(1, map.rankLong(0));
    Assert.assertEquals(1, map.rankLong(1));
    Assert.assertEquals(1, map.rankLong(Integer.MAX_VALUE + 1L));
    Assert.assertEquals(1, map.rankLong(Long.MAX_VALUE));
  }

  @Test
  public void testSimpleIntegers() {
    Roaring64NavigableMap map = new Roaring64NavigableMap();

    map.addLong(123);
    map.addLong(234);

    {
      LongIterator iterator = map.getLongIterator();

      Assert.assertTrue(iterator.hasNext());
      Assert.assertEquals(123, iterator.next());
      Assert.assertEquals(123, map.select(0));
      Assert.assertTrue(map.contains(123));

      Assert.assertTrue(iterator.hasNext());
      Assert.assertEquals(234, iterator.next());
      Assert.assertEquals(234, map.select(1));
      Assert.assertTrue(map.contains(234));

      Assert.assertFalse(iterator.hasNext());
    }

    Assert.assertFalse(map.contains(345));

    Assert.assertEquals(2, map.getLongCardinality());

    Assert.assertEquals(0, map.rankLong(0));
    Assert.assertEquals(1, map.rankLong(123));
    Assert.assertEquals(1, map.rankLong(233));
    Assert.assertEquals(2, map.rankLong(234));
    Assert.assertEquals(2, map.rankLong(235));
    Assert.assertEquals(2, map.rankLong(Integer.MAX_VALUE + 1L));
    Assert.assertEquals(2, map.rankLong(Long.MAX_VALUE));

    Assert.assertArrayEquals(new long[] {123L, 234L}, map.toArray());
  }



  @Test
  public void testHashCodeEquals() {
    Roaring64NavigableMap left = new Roaring64NavigableMap();

    left.addLong(123);
    left.addLong(Long.MAX_VALUE);

    Roaring64NavigableMap right = Roaring64NavigableMap.bitmapOf(123, Long.MAX_VALUE);

    Assert.assertEquals(left.hashCode(), right.hashCode());
    Assert.assertEquals(left, right);
    Assert.assertEquals(right, left);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAddOneSelect2() {
    Roaring64NavigableMap map = new Roaring64NavigableMap();

    map.addLong(123);

    map.select(1);
  }

  @Test
  public void testIterator_NextWithoutHasNext_Filled() {
    Roaring64NavigableMap map = new Roaring64NavigableMap();

    map.addLong(0);

    Assert.assertTrue(map.getLongIterator().hasNext());
    Assert.assertEquals(0, map.getLongIterator().next());
  }

  @Test(expected = IllegalStateException.class)
  public void testIterator_NextWithoutHasNext_Empty() {
    Roaring64NavigableMap map = new Roaring64NavigableMap();

    map.getLongIterator().next();
  }

  @Test
  public void testLongMaxValue() {
    Roaring64NavigableMap map = new Roaring64NavigableMap();

    map.addLong(Long.MAX_VALUE);

    {
      LongIterator iterator = map.getLongIterator();
      Assert.assertTrue(iterator.hasNext());
      Assert.assertEquals(Long.MAX_VALUE, iterator.next());
      Assert.assertEquals(Long.MAX_VALUE, map.select(0));
      Assert.assertFalse(iterator.hasNext());
    }

    Assert.assertEquals(1, map.getLongCardinality());

    Assert.assertEquals(0, map.rankLong(Long.MIN_VALUE));
    Assert.assertEquals(0, map.rankLong(Long.MIN_VALUE + 1));
    Assert.assertEquals(0, map.rankLong(-1));
    Assert.assertEquals(0, map.rankLong(0));
    Assert.assertEquals(0, map.rankLong(1));
    Assert.assertEquals(0, map.rankLong(Long.MAX_VALUE - 1));
    Assert.assertEquals(1, map.rankLong(Long.MAX_VALUE));
  }

  @Test
  public void testLongMinValue() {
    Roaring64NavigableMap map = new Roaring64NavigableMap();

    map.addLong(Long.MIN_VALUE);

    {
      LongIterator iterator = map.getLongIterator();
      Assert.assertTrue(iterator.hasNext());
      Assert.assertEquals(Long.MIN_VALUE, iterator.next());
      Assert.assertEquals(Long.MIN_VALUE, map.select(0));
      Assert.assertFalse(iterator.hasNext());
    }

    Assert.assertEquals(1, map.getLongCardinality());

    Assert.assertEquals(1, map.rankLong(Long.MIN_VALUE));
    Assert.assertEquals(1, map.rankLong(Long.MIN_VALUE + 1));
    Assert.assertEquals(1, map.rankLong(-1));
    Assert.assertEquals(1, map.rankLong(0));
    Assert.assertEquals(1, map.rankLong(1));
    Assert.assertEquals(1, map.rankLong(Long.MAX_VALUE - 1));
    Assert.assertEquals(1, map.rankLong(Long.MAX_VALUE));
  }

  @Test
  public void testLongMinValueZeroOneMaxValue() {
    Roaring64NavigableMap map = new Roaring64NavigableMap();

    map.addLong(Long.MIN_VALUE);
    map.addLong(0);
    map.addLong(1);
    map.addLong(Long.MAX_VALUE);

    {
      LongIterator iterator = map.getLongIterator();
      Assert.assertTrue(iterator.hasNext());
      Assert.assertEquals(Long.MIN_VALUE, iterator.next());
      Assert.assertEquals(Long.MIN_VALUE, map.select(0));
      Assert.assertEquals(0, iterator.next());
      Assert.assertEquals(0, map.select(1));
      Assert.assertEquals(1, iterator.next());
      Assert.assertEquals(1, map.select(2));
      Assert.assertEquals(Long.MAX_VALUE, iterator.next());
      Assert.assertEquals(Long.MAX_VALUE, map.select(3));
      Assert.assertFalse(iterator.hasNext());
    }

    Assert.assertEquals(4, map.getLongCardinality());

    Assert.assertEquals(1, map.rankLong(Long.MIN_VALUE));
    Assert.assertEquals(1, map.rankLong(Long.MIN_VALUE + 1));
    Assert.assertEquals(1, map.rankLong(-1));
    Assert.assertEquals(2, map.rankLong(0));
    Assert.assertEquals(3, map.rankLong(1));
    Assert.assertEquals(3, map.rankLong(2));
    Assert.assertEquals(3, map.rankLong(Long.MAX_VALUE - 1));
    Assert.assertEquals(4, map.rankLong(Long.MAX_VALUE));

    final List<Long> foreach = new ArrayList<>();
    map.forEach(new LongConsumer() {

      @Override
      public void accept(long value) {
        foreach.add(value);
      }
    });
    Assert.assertEquals(Arrays.asList(Long.MIN_VALUE, 0L, 1L, Long.MAX_VALUE), foreach);
  }

  @Test
  public void testReverseIterator_SingleBuket() {
    Roaring64NavigableMap map = new Roaring64NavigableMap();

    map.addLong(123);
    map.addLong(234);

    {
      LongIterator iterator = map.getReverseLongIterator();
      Assert.assertTrue(iterator.hasNext());
      Assert.assertEquals(234, iterator.next());
      Assert.assertTrue(iterator.hasNext());
      Assert.assertEquals(123, iterator.next());
      Assert.assertFalse(iterator.hasNext());
    }
  }

  @Test
  public void testReverseIterator_MultipleBuket() {
    Roaring64NavigableMap map = new Roaring64NavigableMap();

    map.addLong(123);
    map.addLong(Long.MAX_VALUE);

    {
      LongIterator iterator = map.getReverseLongIterator();
      Assert.assertTrue(iterator.hasNext());
      Assert.assertEquals(Long.MAX_VALUE, iterator.next());
      Assert.assertTrue(iterator.hasNext());
      Assert.assertEquals(123, iterator.next());
      Assert.assertFalse(iterator.hasNext());
    }
  }

  @Test
  public void testRemove_Signed() {
    Roaring64NavigableMap map = new Roaring64NavigableMap(true);

    // Add a value
    map.addLong(123);
    Assert.assertEquals(1L, map.getLongCardinality());

    // Remove it
    map.remove(123L);
    Assert.assertEquals(0L, map.getLongCardinality());
    Assert.assertTrue(map.isEmpty());

    // Add it back
    map.addLong(123);
    Assert.assertEquals(1L, map.getLongCardinality());
  }

  @Test
  public void testRemove_Unsigned() {
    Roaring64NavigableMap map = new Roaring64NavigableMap(false);

    // Add a value
    map.addLong(123);
    Assert.assertEquals(1L, map.getLongCardinality());

    // Remove it
    map.remove(123L);
    Assert.assertEquals(0L, map.getLongCardinality());
    Assert.assertTrue(map.isEmpty());

    // Add it back
    map.addLong(123);
    Assert.assertEquals(1L, map.getLongCardinality());
  }

  @Test
  public void testRemoveDifferentBuckets() {
    Roaring64NavigableMap map = new Roaring64NavigableMap();

    // Add two values
    map.addLong(123);
    map.addLong(Long.MAX_VALUE);
    Assert.assertEquals(2L, map.getLongCardinality());

    // Remove biggest
    map.remove(Long.MAX_VALUE);
    Assert.assertEquals(1L, map.getLongCardinality());

    Assert.assertEquals(123L, map.select(0));

    // Add back to different bucket
    map.add(Long.MAX_VALUE);
    Assert.assertEquals(2L, map.getLongCardinality());

    Assert.assertEquals(123L, map.select(0));
    Assert.assertEquals(Long.MAX_VALUE, map.select(1));
  }

  @Test
  public void testRemoveDifferentBuckets_RemoveBigAddIntermediate() {
    Roaring64NavigableMap map = new Roaring64NavigableMap();

    // Add two values
    map.addLong(123);
    map.addLong(Long.MAX_VALUE);
    Assert.assertEquals(2L, map.getLongCardinality());

    // Remove biggest
    map.remove(Long.MAX_VALUE);
    Assert.assertEquals(1L, map.getLongCardinality());

    Assert.assertEquals(123L, map.select(0));

    // Add back to different bucket
    map.add(Long.MAX_VALUE / 2L);
    Assert.assertEquals(2L, map.getLongCardinality());

    Assert.assertEquals(123L, map.select(0));
    Assert.assertEquals(Long.MAX_VALUE / 2L, map.select(1));
  }

  @Test
  public void testRemoveDifferentBuckets_RemoveIntermediateAddBug() {
    Roaring64NavigableMap map = new Roaring64NavigableMap();

    // Add two values
    map.addLong(123);
    map.addLong(Long.MAX_VALUE / 2L);
    Assert.assertEquals(2L, map.getLongCardinality());

    // Remove biggest
    map.remove(Long.MAX_VALUE / 2L);
    Assert.assertEquals(1L, map.getLongCardinality());

    Assert.assertEquals(123L, map.select(0));

    // Add back to different bucket
    map.add(Long.MAX_VALUE);
    Assert.assertEquals(2L, map.getLongCardinality());

    Assert.assertEquals(123L, map.select(0));
    Assert.assertEquals(Long.MAX_VALUE, map.select(1));
  }

  @Test
  public void testPerfManyDifferentBuckets_WithCache() {
    Roaring64NavigableMap map = new Roaring64NavigableMap(true, true);

    long problemSize = 1000 * 1000L;
    for (long i = 1; i <= problemSize; i++) {
      map.addLong(i * Integer.MAX_VALUE + 1L);
    }

    long cardinality = map.getLongCardinality();
    Assert.assertEquals(problemSize, cardinality);

    long last = map.select(cardinality - 1);
    Assert.assertEquals(problemSize * Integer.MAX_VALUE + 1L, last);
    Assert.assertEquals(cardinality, map.rankLong(last));
  }

  @Test
  public void testPerfManyDifferentBuckets_NoCache() {
    Roaring64NavigableMap map = new Roaring64NavigableMap(true, false);

    long problemSize = 100 * 1000L;
    for (long i = 1; i <= problemSize; i++) {
      map.addLong(i * Integer.MAX_VALUE + 1L);
    }

    long cardinality = map.getLongCardinality();
    Assert.assertEquals(problemSize, cardinality);

    long last = map.select(cardinality - 1);
    Assert.assertEquals(problemSize * Integer.MAX_VALUE + 1L, last);
    Assert.assertEquals(cardinality, map.rankLong(last));
  }

  @Test
  public void testComparator() {
    Comparator<Integer> natural = new Comparator<Integer>() {

      @Override
      public int compare(Integer o1, Integer o2) {
        return Integer.compare(o1, o2);
      }
    };
    Comparator<Integer> unsigned = RoaringIntPacking.unsignedComparator();

    // Comparator a negative and a positive differs from natural comparison
    Assert.assertTrue(natural.compare(-1, 1) < 0);
    Assert.assertFalse(unsigned.compare(-1, 1) < 0);

    // Comparator Long.MAX_VALUE and Long.MAX_VALUE + 1 differs
    Assert.assertTrue(natural.compare(Integer.MAX_VALUE, Integer.MAX_VALUE + 1) > 0);
    Assert.assertFalse(unsigned.compare(Integer.MAX_VALUE, Integer.MAX_VALUE + 1) > 0);

    // 'Integer.MAX_VALUE+1' is lower than 'Integer.MAX_VALUE+2'
    Assert.assertTrue(unsigned.compare(Integer.MAX_VALUE + 1, Integer.MAX_VALUE + 2) < 0);
  }

  @Test
  public void testLargeSelectLong_signed() {
    long positive = 1;
    long negative = -1;
    Roaring64NavigableMap map = new Roaring64NavigableMap(true);
    map.addLong(positive);
    map.addLong(negative);
    long first = map.select(0);
    long last = map.select(1);

    // signed: positive is after negative
    Assert.assertEquals(negative, first);
    Assert.assertEquals(positive, last);
  }

  @Test
  public void testLargeSelectLong_unsigned() {
    long positive = 1;
    long negative = -1;
    Roaring64NavigableMap map = new Roaring64NavigableMap(false);
    map.addLong(positive);
    map.addLong(negative);
    long first = map.select(0);
    long last = map.select(1);

    // unsigned: negative means bigger than Long.MAX_VALUE
    Assert.assertEquals(positive, first);
    Assert.assertEquals(negative, last);
  }

  @Test
  public void testLargeRankLong_signed() {
    long positive = 1;
    long negative = -1;
    Roaring64NavigableMap map = new Roaring64NavigableMap(true);
    map.addLong(positive);
    map.addLong(negative);
    Assert.assertEquals(1, map.rankLong(negative));
  }

  @Test
  public void testLargeRankLong_unsigned() {
    long positive = 1;
    long negative = -1;
    Roaring64NavigableMap map = new Roaring64NavigableMap(false);
    map.addLong(positive);
    map.addLong(negative);
    Assert.assertEquals(2, map.rankLong(negative));
  }

  @Test
  public void testIterationOrder_signed() {
    long positive = 1;
    long negative = -1;
    Roaring64NavigableMap map = new Roaring64NavigableMap(true);
    map.addLong(positive);
    map.addLong(negative);
    LongIterator it = map.getLongIterator();
    long first = it.next();
    long last = it.next();
    Assert.assertEquals(negative, first);
    Assert.assertEquals(positive, last);
  }

  @Test
  public void testIterationOrder_unsigned() {
    long positive = 1;
    long negative = -1;
    Roaring64NavigableMap map = new Roaring64NavigableMap(false);
    map.addLong(positive);
    map.addLong(negative);
    LongIterator it = map.getLongIterator();
    long first = it.next();
    long last = it.next();
    Assert.assertEquals(positive, first);
    Assert.assertEquals(negative, last);
  }

  @Test
  public void testAddingLowValueAfterHighValue() {
    Roaring64NavigableMap map = new Roaring64NavigableMap();
    map.addLong(Long.MAX_VALUE);
    Assert.assertEquals(Long.MAX_VALUE, map.select(0));
    map.addLong(666);
    Assert.assertEquals(666, map.select(0));
    Assert.assertEquals(Long.MAX_VALUE, map.select(1));
  }

  @Test
  public void testSerialization_Empty() throws IOException, ClassNotFoundException {
    final Roaring64NavigableMap map = new Roaring64NavigableMap();

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(map);
    }

    final Roaring64NavigableMap clone;
    try (ObjectInputStream ois =
        new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
      clone = (Roaring64NavigableMap) ois.readObject();
    }

    // Check the test has not simply copied the ref
    Assert.assertNotSame(map, clone);
    Assert.assertEquals(0, clone.getLongCardinality());
  }

  @Test
  public void testSerialization_OneValue() throws IOException, ClassNotFoundException {
    final Roaring64NavigableMap map = new Roaring64NavigableMap();
    map.addLong(123);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(map);
    }

    final Roaring64NavigableMap clone;
    try (ObjectInputStream ois =
        new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
      clone = (Roaring64NavigableMap) ois.readObject();
    }

    // Check the test has not simply copied the ref
    Assert.assertNotSame(map, clone);
    Assert.assertEquals(1, clone.getLongCardinality());
    Assert.assertEquals(123, clone.select(0));
  }


  @Test
  public void testSerialization_MultipleBuckets() throws IOException, ClassNotFoundException {
    final Roaring64NavigableMap map = new Roaring64NavigableMap();
    map.addLong(123);
    map.addLong(Long.MAX_VALUE);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(map);
    }

    final Roaring64NavigableMap clone;
    try (ObjectInputStream ois =
        new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
      clone = (Roaring64NavigableMap) ois.readObject();
    }

    // Check the test has not simply copied the ref
    Assert.assertNotSame(map, clone);
    Assert.assertEquals(2, clone.getLongCardinality());
    Assert.assertEquals(123, clone.select(0));
    Assert.assertEquals(Long.MAX_VALUE, clone.select(1));
  }

  @Test
  public void testSerializationSizeInBytes() throws IOException, ClassNotFoundException {
    final Roaring64NavigableMap map = new Roaring64NavigableMap();
    map.addLong(123);
    // map.addLong(Long.MAX_VALUE);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (DataOutputStream oos = new DataOutputStream(baos)) {
      map.serialize(oos);
    }

    Assert.assertEquals(baos.toByteArray().length, map.serializedSizeInBytes());
  }

  @Test
  public void testOr_SameBucket() {
    Roaring64NavigableMap left = new Roaring64NavigableMap();
    Roaring64NavigableMap right = new Roaring64NavigableMap();

    left.addLong(123);
    right.addLong(234);

    left.or(right);

    Assert.assertEquals(2, left.getLongCardinality());

    Assert.assertEquals(123, left.select(0));
    Assert.assertEquals(234, left.select(1));
  }

  @Test
  public void testOr_MultipleBuckets() {
    Roaring64NavigableMap left = new Roaring64NavigableMap();
    Roaring64NavigableMap right = new Roaring64NavigableMap();

    left.addLong(123);
    left.addLong(Long.MAX_VALUE);
    right.addLong(234);

    left.or(right);

    Assert.assertEquals(3, left.getLongCardinality());

    Assert.assertEquals(123, left.select(0));
    Assert.assertEquals(234, left.select(1));
    Assert.assertEquals(Long.MAX_VALUE, left.select(2));
  }

  @Test
  public void testOr_DifferentBucket_NotBuffer() {
    Roaring64NavigableMap left = new Roaring64NavigableMap(true, new RoaringBitmapSupplier());
    Roaring64NavigableMap right = new Roaring64NavigableMap(true, new RoaringBitmapSupplier());

    left.addLong(123);
    right.addLong(Long.MAX_VALUE / 2);

    left.or(right);

    Assert.assertEquals(2, left.getLongCardinality());

    Assert.assertEquals(123, left.select(0));
    Assert.assertEquals(Long.MAX_VALUE / 2, left.select(1));
  }


  @Test
  public void testOr_SameBucket_NotBuffer() {
    Roaring64NavigableMap left = new Roaring64NavigableMap(true, new RoaringBitmapSupplier());
    Roaring64NavigableMap right = new Roaring64NavigableMap(true, new RoaringBitmapSupplier());

    left.addLong(123);
    right.addLong(234);

    left.or(right);

    Assert.assertEquals(2, left.getLongCardinality());

    Assert.assertEquals(123, left.select(0));
    Assert.assertEquals(234, left.select(1));
  }

  @Test
  public void testOr_DifferentBucket_Buffer() {
    Roaring64NavigableMap left = newBuffered();
    Roaring64NavigableMap right = newBuffered();

    left.addLong(123);
    right.addLong(Long.MAX_VALUE);

    left.or(right);

    Assert.assertEquals(2, left.getLongCardinality());

    Assert.assertEquals(123, left.select(0));
    Assert.assertEquals(Long.MAX_VALUE, left.select(1));
  }

  @Test
  public void testOr_SameBucket_Buffer() {
    Roaring64NavigableMap left = newBuffered();
    Roaring64NavigableMap right = newBuffered();

    left.addLong(123);
    right.addLong(234);

    left.or(right);

    Assert.assertEquals(2, left.getLongCardinality());

    Assert.assertEquals(123, left.select(0));
    Assert.assertEquals(234, left.select(1));
  }

  @Test
  public void testOr_CloneInput() {
    Roaring64NavigableMap left = new Roaring64NavigableMap();
    Roaring64NavigableMap right = new Roaring64NavigableMap();

    right.addLong(123);

    // We push in left a bucket which does not exist
    left.or(right);

    // Then we mutate left: ensure it does not impact right as it should remain unchanged
    left.addLong(234);

    Assert.assertEquals(2, left.getLongCardinality());
    Assert.assertEquals(123, left.select(0));
    Assert.assertEquals(234, left.select(1));

    Assert.assertEquals(1, right.getLongCardinality());
    Assert.assertEquals(123, right.select(0));
  }


  @Test
  public void testXor_SingleBucket() {
    Roaring64NavigableMap left = new Roaring64NavigableMap();
    Roaring64NavigableMap right = new Roaring64NavigableMap();

    left.addLong(123);
    left.addLong(234);
    right.addLong(234);
    right.addLong(345);

    // We have 1 shared value: 234
    left.xor(right);

    Assert.assertEquals(2, left.getLongCardinality());
    Assert.assertEquals(123, left.select(0));
    Assert.assertEquals(345, left.select(1));
  }



  @Test
  public void testXor_Buffer() {
    Roaring64NavigableMap left = newBuffered();
    Roaring64NavigableMap right = newBuffered();

    left.addLong(123);
    right.addLong(234);

    left.xor(right);

    Assert.assertEquals(2, left.getLongCardinality());
    Assert.assertEquals(123, left.select(0));
    Assert.assertEquals(234, left.select(1));
  }

  @Test
  public void testXor_DifferentBucket() {
    Roaring64NavigableMap left = new Roaring64NavigableMap();
    Roaring64NavigableMap right = new Roaring64NavigableMap();

    left.addLong(123);
    right.addLong(Long.MAX_VALUE);

    // We have 1 shared value: 234
    left.xor(right);

    Assert.assertEquals(2, left.getLongCardinality());
    Assert.assertEquals(123, left.select(0));
    Assert.assertEquals(Long.MAX_VALUE, left.select(1));
  }

  @Test
  public void testXor_MultipleBucket() {
    Roaring64NavigableMap left = new Roaring64NavigableMap();
    Roaring64NavigableMap right = new Roaring64NavigableMap();

    left.addLong(123);
    left.addLong(Long.MAX_VALUE);
    right.addLong(Long.MAX_VALUE);

    // We have 1 shared value: 234
    left.xor(right);

    Assert.assertEquals(1, left.getLongCardinality());
    Assert.assertEquals(123, left.select(0));
  }


  @Test
  public void testAnd_SingleBucket() {
    Roaring64NavigableMap left = new Roaring64NavigableMap();
    Roaring64NavigableMap right = new Roaring64NavigableMap();

    left.addLong(123);
    left.addLong(234);
    right.addLong(234);
    right.addLong(345);

    // We have 1 shared value: 234
    left.and(right);

    Assert.assertEquals(1, left.getLongCardinality());
    Assert.assertEquals(234, left.select(0));
  }

  @Test
  public void testAnd_Buffer() {
    Roaring64NavigableMap left = newBuffered();
    Roaring64NavigableMap right = newBuffered();

    left.addLong(123);
    right.addLong(123);

    // We have 1 shared value: 234
    left.and(right);

    Assert.assertEquals(1, left.getLongCardinality());
    Assert.assertEquals(123, left.select(0));
  }

  @Test
  public void testAnd_DifferentBucket() {
    Roaring64NavigableMap left = new Roaring64NavigableMap();
    Roaring64NavigableMap right = new Roaring64NavigableMap();

    left.addLong(123);
    right.addLong(Long.MAX_VALUE);

    // We have 1 shared value: 234
    left.and(right);

    Assert.assertEquals(0, left.getLongCardinality());
  }

  @Test
  public void testAnd_MultipleBucket() {
    Roaring64NavigableMap left = new Roaring64NavigableMap();
    Roaring64NavigableMap right = new Roaring64NavigableMap();

    left.addLong(123);
    left.addLong(Long.MAX_VALUE);
    right.addLong(Long.MAX_VALUE);

    // We have 1 shared value: 234
    left.and(right);

    Assert.assertEquals(1, left.getLongCardinality());
    Assert.assertEquals(Long.MAX_VALUE, left.select(0));
  }


  @Test
  public void testAndNot_SingleBucket() {
    Roaring64NavigableMap left = new Roaring64NavigableMap();
    Roaring64NavigableMap right = new Roaring64NavigableMap();

    left.addLong(123);
    left.addLong(234);
    right.addLong(234);
    right.addLong(345);

    // We have 1 shared value: 234
    left.andNot(right);

    Assert.assertEquals(1, left.getLongCardinality());
    Assert.assertEquals(123, left.select(0));
  }

  @Test
  public void testAndNot_Buffer() {
    Roaring64NavigableMap left = newBuffered();
    Roaring64NavigableMap right = newBuffered();

    left.addLong(123);
    right.addLong(234);

    // We have 1 shared value: 234
    left.andNot(right);

    Assert.assertEquals(1, left.getLongCardinality());
    Assert.assertEquals(123, left.select(0));
  }

  @Test
  public void testAndNot_DifferentBucket() {
    Roaring64NavigableMap left = new Roaring64NavigableMap();
    Roaring64NavigableMap right = new Roaring64NavigableMap();

    left.addLong(123);
    right.addLong(Long.MAX_VALUE);

    // We have 1 shared value: 234
    left.andNot(right);

    Assert.assertEquals(1, left.getLongCardinality());
    Assert.assertEquals(123, left.select(0));
  }

  @Test
  public void testAndNot_MultipleBucket() {
    Roaring64NavigableMap left = new Roaring64NavigableMap();
    Roaring64NavigableMap right = new Roaring64NavigableMap();

    left.addLong(123);
    left.addLong(Long.MAX_VALUE);
    right.addLong(Long.MAX_VALUE);

    // We have 1 shared value: 234
    left.andNot(right);

    Assert.assertEquals(1, left.getLongCardinality());
    Assert.assertEquals(123, left.select(0));
  }

  @Test
  public void testToString_signed() {
    Roaring64NavigableMap map = new Roaring64NavigableMap(true);

    map.addLong(123);
    map.addLong(Long.MAX_VALUE);
    map.addLong(Long.MAX_VALUE + 1L);

    Assert.assertEquals("{-9223372036854775808,123,9223372036854775807}", map.toString());
  }

  @Test
  public void testToString_unsigned() {
    Roaring64NavigableMap map = new Roaring64NavigableMap(false);

    map.addLong(123);
    map.addLong(Long.MAX_VALUE);
    map.addLong(Long.MAX_VALUE + 1L);

    Assert.assertEquals("{123,9223372036854775807,9223372036854775808}", map.toString());
  }

  @Test
  public void testAddRange_SingleBucket_NotBuffer() {
    Roaring64NavigableMap map = new Roaring64NavigableMap();

    map.add(5L, 12L);
    Assert.assertEquals(7L, map.getLongCardinality());

    Assert.assertEquals(5L, map.select(0));
    Assert.assertEquals(11L, map.select(6L));
  }


  @Test
  public void testAddRange_SingleBucket_Buffer() {
    Roaring64NavigableMap map = newBuffered();

    map.add(5L, 12L);
    Assert.assertEquals(7L, map.getLongCardinality());

    Assert.assertEquals(5L, map.select(0));
    Assert.assertEquals(11L, map.select(6L));
  }

  // Edge case: the last high is excluded and should not lead to a new bitmap. However, it may be
  // seen only while trying to add for high=1
  @Test
  public void testAddRange_EndExcludingNextBitmapFirstLow() {
    Roaring64NavigableMap map = new Roaring64NavigableMap();

    long end = Util.toUnsignedLong(-1) + 1;

    map.add(end - 2, end);
    Assert.assertEquals(2, map.getLongCardinality());

    Assert.assertEquals(end - 2, map.select(0));
    Assert.assertEquals(end - 1, map.select(1));
  }


  @Test
  public void testAddRange_MultipleBuckets() {
    Roaring64NavigableMap map = new Roaring64NavigableMap();

    int enableTrim = 5;

    long from = RoaringIntPacking.pack(0, -1 - enableTrim);
    long to = from + 2 * enableTrim;
    map.add(from, to);
    int nbItems = (int) (to - from);
    Assert.assertEquals(nbItems, map.getLongCardinality());

    Assert.assertEquals(from, map.select(0));
    Assert.assertEquals(to - 1, map.select(nbItems - 1));
  }


  public static final long outOfRoaringBitmapRange = 2L * Integer.MAX_VALUE + 3L;

  // Check this range is not handled by RoaringBitmap
  @Test(expected = IllegalArgumentException.class)
  public void testCardinalityAboveIntegerMaxValue_RoaringBitmap() {
    RoaringBitmap map = new RoaringBitmap();

    map.add(0L, outOfRoaringBitmapRange);
  }

  @Test
  public void testCardinalityAboveIntegerMaxValue() {
    Roaring64NavigableMap map = new Roaring64NavigableMap();

    long outOfSingleRoaring = outOfRoaringBitmapRange - 3;

    // This should fill entirely one bitmap,and add one in the next bitmap
    map.add(0, outOfSingleRoaring);
    Assert.assertEquals(outOfSingleRoaring, map.getLongCardinality());

    Assert.assertEquals(outOfSingleRoaring, map.getLongCardinality());

    Assert.assertEquals(0, map.select(0));
    Assert.assertEquals(outOfSingleRoaring - 1, map.select(outOfSingleRoaring - 1));

  }

  @Test
  public void testRoaringBitmap_SelectAboveIntegerMaxValue() {
    RoaringBitmap map = new RoaringBitmap();

    long maxForRoaringBitmap = Util.toUnsignedLong(-1) + 1;
    map.add(0L, maxForRoaringBitmap);

    Assert.assertEquals(maxForRoaringBitmap, map.getLongCardinality());
    Assert.assertEquals(-1, map.select(-1));
  }

  @Test
  public void testRoaringBitmap_SelectAboveIntegerMaxValuePlusOne() {
    RoaringBitmap map = new RoaringBitmap();

    long maxForRoaringBitmap = Util.toUnsignedLong(-1) + 1;
    map.add(0L, maxForRoaringBitmap);

    Assert.assertEquals(maxForRoaringBitmap, map.getLongCardinality());
    Assert.assertEquals(-1, map.select(-1));
  }

  @Test
  public void testTrim() {
    Roaring64NavigableMap map = new Roaring64NavigableMap(true);

    // How many contiguous values do we have to set to enable .trim?
    int enableTrim = 100;

    long from = RoaringIntPacking.pack(0, -1 - enableTrim);
    long to = from + 2 * enableTrim;

    // Check we cover different buckets
    Assert.assertNotEquals(RoaringIntPacking.high(to), RoaringIntPacking.high(from));

    for (long i = from; i <= to; i++) {
      map.addLong(i);
    }

    map.trim();
  }

  @Test
  public void testAutoboxedIterator() {
    Roaring64NavigableMap map = new Roaring64NavigableMap();

    map.addLong(123);
    map.addLong(234);

    Iterator<Long> it = map.iterator();

    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(123L, it.next().longValue());
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(234, it.next().longValue());
    Assert.assertFalse(it.hasNext());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testAutoboxedIterator_CanNotRemove() {
    Roaring64NavigableMap map = new Roaring64NavigableMap();

    map.addLong(123);
    map.addLong(234);

    Iterator<Long> it = map.iterator();

    Assert.assertTrue(it.hasNext());

    // Should throw a UnsupportedOperationException
    it.remove();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSelect_Empty() {
    Roaring64NavigableMap map = new Roaring64NavigableMap();

    map.select(0);
  }


  @Test(expected = IllegalArgumentException.class)
  public void testSelect_OutOfBounds_MatchCardinality() {
    Roaring64NavigableMap map = new Roaring64NavigableMap();

    map.addLong(123);

    map.select(1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSelect_OutOfBounds_OtherCardinality() {
    Roaring64NavigableMap map = new Roaring64NavigableMap();

    map.addLong(123);

    map.select(2);
  }

  @Test
  public void testRank_NoCache_HighNotPresent() {
    Roaring64NavigableMap map = new Roaring64NavigableMap(true, false);

    map.addLong(123);
    map.addLong(Long.MAX_VALUE);

    Assert.assertEquals(1, map.rankLong(Long.MAX_VALUE / 2L));
  }

  @Test
  public void testRunOptimize_NotBuffer() {
    Roaring64NavigableMap map = new Roaring64NavigableMap(true, new RoaringBitmapSupplier());

    map.addLong(123);
    map.addLong(234);

    map.runOptimize();
  }

  @Test
  public void testRunOptimize_Buffer() {
    Roaring64NavigableMap map = newBuffered();

    map.addLong(123);
    map.addLong(234);

    map.runOptimize();
  }


  @Test
  public void testFlip_NotBuffer() {
    Roaring64NavigableMap map = new Roaring64NavigableMap();

    map.add(0);
    map.flip(0);

    Assert.assertFalse(map.contains(0));
  }

  @Test
  public void testFlip_Buffer() {
    Roaring64NavigableMap map = newBuffered();

    map.add(0);
    map.flip(0);

    Assert.assertFalse(map.contains(0));
  }

}
