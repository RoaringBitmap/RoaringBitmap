package org.roaringbitmap.longlong;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Comparator;

import org.junit.Assert;
import org.junit.Test;

public class TestTreeRoaringBitmap {
  @Test
  public void testEmpty() {
    RoaringTreeMap map = new RoaringTreeMap();

    Assert.assertFalse(map.iterator().hasNext());

    Assert.assertEquals(0, map.getCardinality());

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
    RoaringTreeMap map = new RoaringTreeMap();

    map.addLong(0);

    {
      LongIterator iterator = map.iterator();
      Assert.assertTrue(iterator.hasNext());
      Assert.assertEquals(0, iterator.next());
      Assert.assertEquals(0, map.select(0));
      Assert.assertFalse(iterator.hasNext());
    }

    Assert.assertEquals(1, map.getCardinality());

    Assert.assertEquals(0, map.rankLong(Long.MIN_VALUE));
    Assert.assertEquals(0, map.rankLong(Integer.MIN_VALUE - 1L));
    Assert.assertEquals(0, map.rankLong(-1));
    Assert.assertEquals(1, map.rankLong(0));
    Assert.assertEquals(1, map.rankLong(1));
    Assert.assertEquals(1, map.rankLong(Integer.MAX_VALUE + 1L));
    Assert.assertEquals(1, map.rankLong(Long.MAX_VALUE));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAddOneSelect2() {
    RoaringTreeMap map = new RoaringTreeMap();

    map.addLong(123);

    Assert.assertEquals(123, map.select(1));
  }

  @Test
  public void testIterator_NextWithoutHasNext_Filled() {
    RoaringTreeMap map = new RoaringTreeMap();

    map.addLong(0);

    Assert.assertTrue(map.iterator().hasNext());
    Assert.assertEquals(0, map.iterator().next());
  }

  @Test(expected = IllegalStateException.class)
  public void testIterator_NextWithoutHasNext_Empty() {
    RoaringTreeMap map = new RoaringTreeMap();

    map.iterator().next();
  }

  @Test
  public void testLongMaxValue() {
    RoaringTreeMap map = new RoaringTreeMap();

    map.addLong(Long.MAX_VALUE);

    {
      LongIterator iterator = map.iterator();
      Assert.assertTrue(iterator.hasNext());
      Assert.assertEquals(Long.MAX_VALUE, iterator.next());
      Assert.assertEquals(Long.MAX_VALUE, map.select(0));
      Assert.assertFalse(iterator.hasNext());
    }

    Assert.assertEquals(1, map.getCardinality());

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
    RoaringTreeMap map = new RoaringTreeMap();

    map.addLong(Long.MIN_VALUE);

    {
      LongIterator iterator = map.iterator();
      Assert.assertTrue(iterator.hasNext());
      Assert.assertEquals(Long.MIN_VALUE, iterator.next());
      Assert.assertEquals(Long.MIN_VALUE, map.select(0));
      Assert.assertFalse(iterator.hasNext());
    }

    Assert.assertEquals(1, map.getCardinality());

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
    RoaringTreeMap map = new RoaringTreeMap();

    map.addLong(Long.MIN_VALUE);
    map.addLong(0);
    map.addLong(1);
    map.addLong(Long.MAX_VALUE);

    {
      LongIterator iterator = map.iterator();
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

    Assert.assertEquals(4, map.getCardinality());

    Assert.assertEquals(1, map.rankLong(Long.MIN_VALUE));
    Assert.assertEquals(1, map.rankLong(Long.MIN_VALUE + 1));
    Assert.assertEquals(1, map.rankLong(-1));
    Assert.assertEquals(2, map.rankLong(0));
    Assert.assertEquals(3, map.rankLong(1));
    Assert.assertEquals(3, map.rankLong(2));
    Assert.assertEquals(3, map.rankLong(Long.MAX_VALUE - 1));
    Assert.assertEquals(4, map.rankLong(Long.MAX_VALUE));
  }

  @Test
  public void testPerfManyDifferentBuckets() {
    RoaringTreeMap map = new RoaringTreeMap();

    long problemSize = 100 * 1000L;
    for (long i = 1; i <= problemSize; i++) {
      map.addLong(i * Integer.MAX_VALUE + 1L);
    }

    long cardinality = map.getCardinality();
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
    RoaringTreeMap map = new RoaringTreeMap(true);
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
    RoaringTreeMap map = new RoaringTreeMap(false);
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
    RoaringTreeMap map = new RoaringTreeMap(true);
    map.addLong(positive);
    map.addLong(negative);
    Assert.assertEquals(1, map.rankLong(negative));
  }

  @Test
  public void testLargeRankLong_unsigned() {
    long positive = 1;
    long negative = -1;
    RoaringTreeMap map = new RoaringTreeMap(false);
    map.addLong(positive);
    map.addLong(negative);
    Assert.assertEquals(2, map.rankLong(negative));
  }

  @Test
  public void testIterationOrder_signed() {
    long positive = 1;
    long negative = -1;
    RoaringTreeMap map = new RoaringTreeMap(true);
    map.addLong(positive);
    map.addLong(negative);
    LongIterator it = map.iterator();
    long first = it.next();
    long last = it.next();
    Assert.assertEquals(negative, first);
    Assert.assertEquals(positive, last);
  }

  @Test
  public void testIterationOrder_unsigned() {
    long positive = 1;
    long negative = -1;
    RoaringTreeMap map = new RoaringTreeMap(false);
    map.addLong(positive);
    map.addLong(negative);
    LongIterator it = map.iterator();
    long first = it.next();
    long last = it.next();
    Assert.assertEquals(positive, first);
    Assert.assertEquals(negative, last);
  }

  @Test
  public void testAddingLowValueAfterHighValue() {
    RoaringTreeMap map = new RoaringTreeMap();
    map.addLong(Long.MAX_VALUE);
    Assert.assertEquals(Long.MAX_VALUE, map.select(0));
    map.addLong(666);
    Assert.assertEquals(666, map.select(0));
    Assert.assertEquals(Long.MAX_VALUE, map.select(1));
  }

  @Test
  public void testSerialization_Empty() throws IOException, ClassNotFoundException {
    final RoaringTreeMap map = new RoaringTreeMap();

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(map);
    }

    final RoaringTreeMap clone;
    try (ObjectInputStream ois =
        new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
      clone = (RoaringTreeMap) ois.readObject();
    }

    // Check the test has not simply copied the ref
    Assert.assertNotSame(map, clone);
    Assert.assertEquals(0, clone.getCardinality());
  }

  @Test
  public void testSerialization_OneValue() throws IOException, ClassNotFoundException {
    final RoaringTreeMap map = new RoaringTreeMap();
    map.addLong(123);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(map);
    }

    final RoaringTreeMap clone;
    try (ObjectInputStream ois =
        new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
      clone = (RoaringTreeMap) ois.readObject();
    }

    // Check the test has not simply copied the ref
    Assert.assertNotSame(map, clone);
    Assert.assertEquals(1, clone.getCardinality());
    Assert.assertEquals(123, clone.select(0));
  }

  @Test
  public void testOr_SameBucket() {
    RoaringTreeMap left = new RoaringTreeMap();
    RoaringTreeMap right = new RoaringTreeMap();

    left.addLong(123);
    right.addLong(234);

    left.or(right);

    Assert.assertEquals(2, left.getCardinality());

    Assert.assertEquals(123, left.select(0));
    Assert.assertEquals(234, left.select(1));
  }

  @Test
  public void testOr_DifferentBucket() {
    RoaringTreeMap left = new RoaringTreeMap();
    RoaringTreeMap right = new RoaringTreeMap();

    left.addLong(123);
    right.addLong(Long.MAX_VALUE / 2);

    left.or(right);

    Assert.assertEquals(2, left.getCardinality());

    Assert.assertEquals(123, left.select(0));
    Assert.assertEquals(Long.MAX_VALUE / 2, left.select(1));
  }

  @Test
  public void testOr_CloneInput() {
    RoaringTreeMap left = new RoaringTreeMap();
    RoaringTreeMap right = new RoaringTreeMap();

    right.addLong(123);

    // We push in left a bucket which does not exist
    left.or(right);

    // Then we mutate left: ensure it does not impact right as it should remain unchanged
    left.addLong(234);

    Assert.assertEquals(2, left.getCardinality());
    Assert.assertEquals(123, left.select(0));
    Assert.assertEquals(234, left.select(1));

    Assert.assertEquals(1, right.getCardinality());
    Assert.assertEquals(123, right.select(0));
  }

  @Test
  public void testToString_signed() {
    RoaringTreeMap map = new RoaringTreeMap(true);

    map.addLong(123);
    map.addLong(Long.MAX_VALUE);
    map.addLong(Long.MAX_VALUE + 1L);

    Assert.assertEquals("{-9223372036854775808,123,9223372036854775807}", map.toString());
  }

  @Test
  public void testToString_unsigned() {
    RoaringTreeMap map = new RoaringTreeMap(false);

    map.addLong(123);
    map.addLong(Long.MAX_VALUE);
    map.addLong(Long.MAX_VALUE + 1L);

    Assert.assertEquals("{123,9223372036854775807,9223372036854775808}", map.toString());
  }
}
