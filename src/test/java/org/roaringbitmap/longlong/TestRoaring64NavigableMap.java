package org.roaringbitmap.longlong;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;

public class TestRoaring64NavigableMap {
  @Test
  public void testEmpty() {
    Roaring64NavigableMap map = new Roaring64NavigableMap();

    Assert.assertFalse(map.getLongIterator().hasNext());

    Assert.assertEquals(0, map.getLongCardinality());

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
      Assert.assertFalse(iterator.hasNext());
    }

    Assert.assertEquals(1, map.getLongCardinality());

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
      Assert.assertTrue(iterator.hasNext());
      Assert.assertEquals(234, iterator.next());
      Assert.assertEquals(234, map.select(1));
      Assert.assertFalse(iterator.hasNext());
    }

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


  // TODO
  // FIXME
  // @Ignore("TODO FIXME")
  @Test
  public void testRemove() {
    Roaring64NavigableMap map = new Roaring64NavigableMap();

    // Add a value
    map.addLong(123);
    Assert.assertEquals(1L, map.getLongCardinality());

    // Remove it
    map.remove(123L);
    Assert.assertEquals(0L, map.getLongCardinality());

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
  }

  @Test
  public void testPerfManyDifferentBuckets() {
    Roaring64NavigableMap map = new Roaring64NavigableMap();

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
  public void testOr_DifferentBucket_NotBuffer() {
    Roaring64NavigableMap left = new Roaring64NavigableMap(true, true, new RoaringBitmapSupplier());
    Roaring64NavigableMap right =
        new Roaring64NavigableMap(true, true, new RoaringBitmapSupplier());

    left.addLong(123);
    right.addLong(Long.MAX_VALUE / 2);

    left.or(right);

    Assert.assertEquals(2, left.getLongCardinality());

    Assert.assertEquals(123, left.select(0));
    Assert.assertEquals(Long.MAX_VALUE / 2, left.select(1));
  }


  @Test
  public void testOr_SameBucket_NotBuffer() {
    Roaring64NavigableMap left = new Roaring64NavigableMap(true, true, new RoaringBitmapSupplier());
    Roaring64NavigableMap right =
        new Roaring64NavigableMap(true, true, new RoaringBitmapSupplier());

    left.addLong(123);
    right.addLong(234);

    left.or(right);

    Assert.assertEquals(2, left.getLongCardinality());

    Assert.assertEquals(123, left.select(0));
    Assert.assertEquals(234, left.select(1));
  }

  @Test
  public void testOr_DifferentBucket_Buffer() {
    Roaring64NavigableMap left =
        new Roaring64NavigableMap(true, true, new MutableRoaringBitmapSupplier());
    Roaring64NavigableMap right =
        new Roaring64NavigableMap(true, true, new MutableRoaringBitmapSupplier());

    left.addLong(123);
    right.addLong(Long.MAX_VALUE / 2);

    left.or(right);

    Assert.assertEquals(2, left.getLongCardinality());

    Assert.assertEquals(123, left.select(0));
    Assert.assertEquals(Long.MAX_VALUE / 2, left.select(1));
  }

  @Test
  public void testOr_SameBucket_Buffer() {
    Roaring64NavigableMap left =
        new Roaring64NavigableMap(true, true, new MutableRoaringBitmapSupplier());
    Roaring64NavigableMap right =
        new Roaring64NavigableMap(true, true, new MutableRoaringBitmapSupplier());

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

  public static final long outOfRoaringBitmapRange = 2L * Integer.MAX_VALUE + 3L;

  // TODO
  // FIXME
  @Ignore("TODO FIXME")
  @Test
  public void testCardinalityAboveIntegerMaxValue() {
    Roaring64NavigableMap map = new Roaring64NavigableMap();

    // This should fill entirely one bitmap,and add one in the next bitmap
    map.add(0, outOfRoaringBitmapRange);

    Assert.assertEquals(0, map.select(0));
    Assert.assertEquals(outOfRoaringBitmapRange, map.select(outOfRoaringBitmapRange - 1));

    Assert.assertEquals(outOfRoaringBitmapRange, map.getLongCardinality());

  }

  @Test(expected = IllegalArgumentException.class)
  public void testCardinalityAboveIntegerMaxValue_RoaringBitmap() {
    RoaringBitmap map = new RoaringBitmap();

    map.add(0L, outOfRoaringBitmapRange);
  }

}
