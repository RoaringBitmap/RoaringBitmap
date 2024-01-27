package org.roaringbitmap.longlong;

import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.commons.lang3.SerializationUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.*;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmapSupplier;

import java.io.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;


public class TestRoaring64NavigableMap {

  // Used to compare Roaring64NavigableMap behavior with RoaringBitmap
  private Roaring64NavigableMap newDefaultCtor() {
    return new Roaring64NavigableMap();
  }

  // Testing the nocache behavior should not depends on bitmap being on-heap of buffered
  private Roaring64NavigableMap newNoCache() {
    return new Roaring64NavigableMap(true, false);
  }

  private Roaring64NavigableMap newSignedBuffered() {
    return new Roaring64NavigableMap(true, new MutableRoaringBitmapSupplier());
  }

  private Roaring64NavigableMap newUnsignedHeap() {
    return new Roaring64NavigableMap(false, new RoaringBitmapSupplier());
  }

  protected void checkCardinalities(Roaring64NavigableMap bitmap) {
    NavigableMap<Integer, BitmapDataProvider> highToBitmap = bitmap.getHighToBitmap();
    int lowestHighNotValid = bitmap.getLowestInvalidHigh();

    NavigableMap<Integer, BitmapDataProvider> expectedToBeCorrect =
        highToBitmap.headMap(lowestHighNotValid, false);
    long[] expectedCardinalities = new long[expectedToBeCorrect.size()];

    Iterator<BitmapDataProvider> it = expectedToBeCorrect.values().iterator();
    int index = 0;
    while (it.hasNext()) {
      BitmapDataProvider next = it.next();

      if (index == 0) {
        expectedCardinalities[0] = next.getLongCardinality();
      } else {
        expectedCardinalities[index] = expectedCardinalities[index - 1] + next.getLongCardinality();
      }

      index++;
    }

    assertArrayEquals(expectedCardinalities,
        Arrays.copyOf(bitmap.getSortedCumulatedCardinality(), expectedCardinalities.length));
  }

  public static void checkSerializeBytes(ImmutableLongBitmapDataProvider bitmap) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (DataOutputStream oos = new DataOutputStream(baos)) {
      bitmap.serialize(oos);
    }

    assertEquals(baos.toByteArray().length, bitmap.serializedSizeInBytes());
  }

  @Test
  public void testHelperCtor() {
    // RoaringIntPacking is not supposed to be instantiated. Add test for coverage
    assertNotNull(new RoaringIntPacking());
  }

  @Test
  public void testEmpty() {
    Roaring64NavigableMap map = newDefaultCtor();

    assertFalse(map.getLongIterator().hasNext());

    assertEquals(0, map.getLongCardinality());
    assertTrue(map.isEmpty());
    assertFalse(map.contains(0));

    assertEquals(0, map.rankLong(Long.MIN_VALUE));
    assertEquals(0, map.rankLong(Long.MIN_VALUE + 1));
    assertEquals(0, map.rankLong(-1));
    assertEquals(0, map.rankLong(0));
    assertEquals(0, map.rankLong(1));
    assertEquals(0, map.rankLong(Long.MAX_VALUE - 1));
    assertEquals(0, map.rankLong(Long.MAX_VALUE));
  }

  @Test
  public void testZero() {
    Roaring64NavigableMap map = newSignedBuffered();

    map.addLong(0);

    {
      LongIterator iterator = map.getLongIterator();

      assertTrue(iterator.hasNext());
      assertEquals(0, iterator.next());
      assertEquals(0, map.select(0));
      assertTrue(map.contains(0));

      assertFalse(iterator.hasNext());
    }

    assertEquals(1, map.getLongCardinality());
    assertFalse(map.isEmpty());

    assertEquals(0, map.rankLong(Long.MIN_VALUE));
    assertEquals(0, map.rankLong(Integer.MIN_VALUE - 1L));
    assertEquals(0, map.rankLong(-1));
    assertEquals(1, map.rankLong(0));
    assertEquals(1, map.rankLong(1));
    assertEquals(1, map.rankLong(Integer.MAX_VALUE + 1L));
    assertEquals(1, map.rankLong(Long.MAX_VALUE));
  }


  @Test
  public void testMinusOne_Unsigned() {
    Roaring64NavigableMap map = newUnsignedHeap();

    map.addLong(-1);

    {
      LongIterator iterator = map.getLongIterator();

      assertTrue(iterator.hasNext());
      assertEquals(-1, iterator.next());
      assertEquals(-1, map.select(0));
      assertTrue(map.contains(-1));

      assertFalse(iterator.hasNext());
    }

    assertEquals(1, map.getLongCardinality());
    assertFalse(map.isEmpty());

    assertEquals(0, map.rankLong(Long.MIN_VALUE));
    assertEquals(0, map.rankLong(Integer.MIN_VALUE - 1L));
    assertEquals(0, map.rankLong(0));
    assertEquals(0, map.rankLong(1));
    assertEquals(0, map.rankLong(Integer.MAX_VALUE + 1L));
    assertEquals(0, map.rankLong(Long.MAX_VALUE));
    assertEquals(0, map.rankLong(-2));
    assertEquals(1, map.rankLong(-1));


    assertArrayEquals(new long[] {-1L}, map.toArray());
  }


  @Test
  public void testAddNotAddLong() {
    Roaring64NavigableMap map = newSignedBuffered();

    // Use BitmapProvider.add instead of .addLong
    map.addLong(0);

    {
      LongIterator iterator = map.getLongIterator();

      assertTrue(iterator.hasNext());
      assertEquals(0, iterator.next());
      assertEquals(0, map.select(0));
      assertTrue(map.contains(0));

      assertFalse(iterator.hasNext());
    }

    assertEquals(1, map.getLongCardinality());
    assertFalse(map.isEmpty());

    assertEquals(0, map.rankLong(Long.MIN_VALUE));
    assertEquals(0, map.rankLong(Integer.MIN_VALUE - 1L));
    assertEquals(0, map.rankLong(-1));
    assertEquals(1, map.rankLong(0));
    assertEquals(1, map.rankLong(1));
    assertEquals(1, map.rankLong(Integer.MAX_VALUE + 1L));
    assertEquals(1, map.rankLong(Long.MAX_VALUE));
  }

  @Test
  public void testSimpleIntegers() {
    Roaring64NavigableMap map = newDefaultCtor();

    map.addLong(123);
    map.addLong(234);

    {
      LongIterator iterator = map.getLongIterator();

      assertTrue(iterator.hasNext());
      assertEquals(123, iterator.next());
      assertEquals(123, map.select(0));
      assertTrue(map.contains(123));

      assertTrue(iterator.hasNext());
      assertEquals(234, iterator.next());
      assertEquals(234, map.select(1));
      assertTrue(map.contains(234));

      assertFalse(iterator.hasNext());
    }

    assertFalse(map.contains(345));

    assertEquals(2, map.getLongCardinality());

    assertEquals(0, map.rankLong(0));
    assertEquals(1, map.rankLong(123));
    assertEquals(1, map.rankLong(233));
    assertEquals(2, map.rankLong(234));
    assertEquals(2, map.rankLong(235));
    assertEquals(2, map.rankLong(Integer.MAX_VALUE + 1L));
    assertEquals(2, map.rankLong(Long.MAX_VALUE));

    assertArrayEquals(new long[] {123L, 234L}, map.toArray());
  }



  @Test
  public void testHashCodeEquals() {
    Roaring64NavigableMap left = newDefaultCtor();

    left.addLong(123);
    left.addLong(Long.MAX_VALUE);

    Roaring64NavigableMap right = Roaring64NavigableMap.bitmapOf(123, Long.MAX_VALUE);

    assertEquals(left.hashCode(), right.hashCode());
    assertEquals(left, right);
    assertEquals(right, left);
  }

  @Test
  public void testAddOneSelect2() {
    assertThrows(IllegalArgumentException.class, () -> {
      Roaring64NavigableMap map = newDefaultCtor();

      map.addLong(123);

      map.select(1);
    });
  }


  @Test
  public void testAddInt() {
    Roaring64NavigableMap map = newDefaultCtor();

    map.addInt(-1);

    assertEquals(Util.toUnsignedLong(-1), map.select(0));
  }

  @Test
  public void testIterator_NextWithoutHasNext_Filled() {
    Roaring64NavigableMap map = newDefaultCtor();

    map.addLong(0);

    assertTrue(map.getLongIterator().hasNext());
    assertEquals(0, map.getLongIterator().next());
  }

  @Test
  public void testIterator_NextWithoutHasNext_Empty() {
    assertThrows(IllegalStateException.class, () -> {
      Roaring64NavigableMap map = newDefaultCtor();

      map.getLongIterator().next();
    });
  }

  @Test
  public void testLongMaxValue() {
    Roaring64NavigableMap map = newSignedBuffered();

    map.addLong(Long.MAX_VALUE);

    {
      LongIterator iterator = map.getLongIterator();
      assertTrue(iterator.hasNext());
      assertEquals(Long.MAX_VALUE, iterator.next());
      assertEquals(Long.MAX_VALUE, map.select(0));
      assertFalse(iterator.hasNext());
    }

    assertEquals(1, map.getLongCardinality());

    assertEquals(0, map.rankLong(Long.MIN_VALUE));
    assertEquals(0, map.rankLong(Long.MIN_VALUE + 1));
    assertEquals(0, map.rankLong(-1));
    assertEquals(0, map.rankLong(0));
    assertEquals(0, map.rankLong(1));
    assertEquals(0, map.rankLong(Long.MAX_VALUE - 1));
    assertEquals(1, map.rankLong(Long.MAX_VALUE));

    assertArrayEquals(new long[] {Long.MAX_VALUE}, map.toArray());
  }

  @Test
  public void testLongMinValue() {
    Roaring64NavigableMap map = newSignedBuffered();

    map.addLong(Long.MIN_VALUE);

    {
      LongIterator iterator = map.getLongIterator();
      assertTrue(iterator.hasNext());
      assertEquals(Long.MIN_VALUE, iterator.next());
      assertEquals(Long.MIN_VALUE, map.select(0));
      assertFalse(iterator.hasNext());
    }

    assertEquals(1, map.getLongCardinality());

    assertEquals(1, map.rankLong(Long.MIN_VALUE));
    assertEquals(1, map.rankLong(Long.MIN_VALUE + 1));
    assertEquals(1, map.rankLong(-1));
    assertEquals(1, map.rankLong(0));
    assertEquals(1, map.rankLong(1));
    assertEquals(1, map.rankLong(Long.MAX_VALUE - 1));
    assertEquals(1, map.rankLong(Long.MAX_VALUE));
  }

  @Test
  public void testLongMinValueZeroOneMaxValue() {
    Roaring64NavigableMap map = newSignedBuffered();

    map.addLong(Long.MIN_VALUE);
    map.addLong(0);
    map.addLong(1);
    map.addLong(Long.MAX_VALUE);

    {
      LongIterator iterator = map.getLongIterator();
      assertTrue(iterator.hasNext());
      assertEquals(Long.MIN_VALUE, iterator.next());
      assertEquals(Long.MIN_VALUE, map.select(0));
      assertEquals(0, iterator.next());
      assertEquals(0, map.select(1));
      assertEquals(1, iterator.next());
      assertEquals(1, map.select(2));
      assertEquals(Long.MAX_VALUE, iterator.next());
      assertEquals(Long.MAX_VALUE, map.select(3));
      assertFalse(iterator.hasNext());
    }

    assertEquals(4, map.getLongCardinality());

    assertEquals(1, map.rankLong(Long.MIN_VALUE));
    assertEquals(1, map.rankLong(Long.MIN_VALUE + 1));
    assertEquals(1, map.rankLong(-1));
    assertEquals(2, map.rankLong(0));
    assertEquals(3, map.rankLong(1));
    assertEquals(3, map.rankLong(2));
    assertEquals(3, map.rankLong(Long.MAX_VALUE - 1));
    assertEquals(4, map.rankLong(Long.MAX_VALUE));

    final List<Long> foreach = new ArrayList<>();
    map.forEach(new LongConsumer() {

      @Override
      public void accept(long value) {
        foreach.add(value);
      }
    });
    assertEquals(Arrays.asList(Long.MIN_VALUE, 0L, 1L, Long.MAX_VALUE), foreach);
  }

  @Test
  public void testReverseIterator_SingleBuket() {
    Roaring64NavigableMap map = newDefaultCtor();

    map.addLong(123);
    map.addLong(234);

    {
      LongIterator iterator = map.getReverseLongIterator();
      assertTrue(iterator.hasNext());
      assertEquals(234, iterator.next());
      assertTrue(iterator.hasNext());
      assertEquals(123, iterator.next());
      assertFalse(iterator.hasNext());
    }
  }

  @Test
  public void testReverseIterator_MultipleBuket() {
    Roaring64NavigableMap map = newDefaultCtor();

    map.addLong(123);
    map.addLong(Long.MAX_VALUE);

    {
      LongIterator iterator = map.getReverseLongIterator();
      assertTrue(iterator.hasNext());
      assertEquals(Long.MAX_VALUE, iterator.next());
      assertTrue(iterator.hasNext());
      assertEquals(123, iterator.next());
      assertFalse(iterator.hasNext());
    }
  }

  @Test
  public void testEmptyAfterRemove() {
    Roaring64NavigableMap rbm = new Roaring64NavigableMap();
    Roaring64NavigableMap empty = new Roaring64NavigableMap();
    rbm.addLong(1);
    assertEquals(rbm.getHighToBitmap().size(), 1);
    rbm.removeLong(1);
    assertTrue(rbm.getHighToBitmap().isEmpty());
    assertEquals(rbm, empty);
  }

  @Test
  public void testNotEmptyAfterRemove() {
    Roaring64NavigableMap rbm = new Roaring64NavigableMap();
    rbm.addLong(1L);
    assertEquals(rbm.getHighToBitmap().size(), 1);
    rbm.addLong(3L * Integer.MAX_VALUE);
    assertEquals(rbm.getHighToBitmap().size(), 2);

    // This will remove a highToBitmap entry
    rbm.removeLong(3L * Integer.MAX_VALUE);
    assertEquals(rbm.getHighToBitmap().size(), 1);

    // This shall create again a highToBitmap entry
    rbm.addLong(3L * Integer.MAX_VALUE);
    assertEquals(rbm.getHighToBitmap().size(), 2);
  }

  @Test
  public void testRemove_Signed() {
    Roaring64NavigableMap map = newSignedBuffered();

    // Add a value
    map.addLong(123);
    assertEquals(1L, map.getLongCardinality());

    // Remove it
    map.removeLong(123L);
    assertEquals(0L, map.getLongCardinality());
    assertTrue(map.isEmpty());

    // Add it back
    map.addLong(123);
    assertEquals(1L, map.getLongCardinality());
  }

  @Test
  public void testRemove_Unsigned() {
    Roaring64NavigableMap map = newUnsignedHeap();

    // Add a value
    map.addLong(123);
    assertEquals(1L, map.getLongCardinality());

    // Remove it
    map.removeLong(123L);
    assertEquals(0L, map.getLongCardinality());
    assertTrue(map.isEmpty());

    // Add it back
    map.addLong(123);
    assertEquals(1L, map.getLongCardinality());
  }

  @Test
  public void testRemoveDifferentBuckets() {
    Roaring64NavigableMap map = newDefaultCtor();

    // Add two values
    map.addLong(123);
    map.addLong(Long.MAX_VALUE);
    assertEquals(2L, map.getLongCardinality());

    // Remove biggest
    map.removeLong(Long.MAX_VALUE);
    assertEquals(1L, map.getLongCardinality());

    assertEquals(123L, map.select(0));

    // Add back to different bucket
    map.addLong(Long.MAX_VALUE);
    assertEquals(2L, map.getLongCardinality());

    assertEquals(123L, map.select(0));
    assertEquals(Long.MAX_VALUE, map.select(1));
  }

  @Test
  public void testRemoveDifferentBuckets_RemoveBigAddIntermediate() {
    Roaring64NavigableMap map = newDefaultCtor();

    // Add two values
    map.addLong(123);
    map.addLong(Long.MAX_VALUE);
    assertEquals(2L, map.getLongCardinality());

    // Remove biggest
    map.removeLong(Long.MAX_VALUE);
    assertEquals(1L, map.getLongCardinality());

    assertEquals(123L, map.select(0));

    // Add back to different bucket
    map.addLong(Long.MAX_VALUE / 2L);
    assertEquals(2L, map.getLongCardinality());

    assertEquals(123L, map.select(0));
    assertEquals(Long.MAX_VALUE / 2L, map.select(1));
  }

  @Test
  public void testRemoveDifferentBuckets_RemoveIntermediateAddBug() {
    Roaring64NavigableMap map = newDefaultCtor();

    // Add two values
    map.addLong(123);
    map.addLong(Long.MAX_VALUE / 2L);
    assertEquals(2L, map.getLongCardinality());

    // Remove biggest
    map.removeLong(Long.MAX_VALUE / 2L);
    assertEquals(1L, map.getLongCardinality());

    assertEquals(123L, map.select(0));

    // Add back to different bucket
    map.addLong(Long.MAX_VALUE);
    assertEquals(2L, map.getLongCardinality());

    assertEquals(123L, map.select(0));
    assertEquals(Long.MAX_VALUE, map.select(1));
  }

  @Test
  public void testPerfManyDifferentBuckets_WithCache() {
    Roaring64NavigableMap map = new Roaring64NavigableMap(true, true);

    long problemSize = 1000 * 1000L;
    for (long i = 1; i <= problemSize; i++) {
      map.addLong(i * Integer.MAX_VALUE + 1L);
    }

    long cardinality = map.getLongCardinality();
    assertEquals(problemSize, cardinality);

    long last = map.select(cardinality - 1);
    assertEquals(problemSize * Integer.MAX_VALUE + 1L, last);
    assertEquals(cardinality, map.rankLong(last));
  }

  @Test
  public void testPerfManyDifferentBuckets_NoCache() {
    Roaring64NavigableMap map = newNoCache();

    long problemSize = 100 * 1000L;
    for (long i = 1; i <= problemSize; i++) {
      map.addLong(i * Integer.MAX_VALUE + 1L);
    }

    long cardinality = map.getLongCardinality();
    assertEquals(problemSize, cardinality);

    long last = map.select(cardinality - 1);
    assertEquals(problemSize * Integer.MAX_VALUE + 1L, last);
    assertEquals(cardinality, map.rankLong(last));
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
    assertTrue(natural.compare(-1, 1) < 0);
    assertFalse(unsigned.compare(-1, 1) < 0);

    // Comparator Long.MAX_VALUE and Long.MAX_VALUE + 1 differs
    assertTrue(natural.compare(Integer.MAX_VALUE, Integer.MAX_VALUE + 1) > 0);
    assertFalse(unsigned.compare(Integer.MAX_VALUE, Integer.MAX_VALUE + 1) > 0);

    // 'Integer.MAX_VALUE+1' is lower than 'Integer.MAX_VALUE+2'
    assertTrue(unsigned.compare(Integer.MAX_VALUE + 1, Integer.MAX_VALUE + 2) < 0);
  }

  @Test
  public void testLargeSelectLong_signed() {
    long positive = 1;
    long negative = -1;
    Roaring64NavigableMap map = newSignedBuffered();
    map.addLong(positive);
    map.addLong(negative);
    long first = map.select(0);
    long last = map.select(1);

    // signed: positive is after negative
    assertEquals(negative, first);
    assertEquals(positive, last);
  }

  @Test
  public void testLargeSelectLong_unsigned() {
    long positive = 1;
    long negative = -1;
    Roaring64NavigableMap map = newUnsignedHeap();
    map.addLong(positive);
    map.addLong(negative);
    long first = map.select(0);
    long last = map.select(1);

    // unsigned: negative means bigger than Long.MAX_VALUE
    assertEquals(positive, first);
    assertEquals(negative, last);
  }

  @Test
  public void testLargeRankLong_signed() {
    long positive = 1;
    long negative = -1;
    Roaring64NavigableMap map = newSignedBuffered();
    map.addLong(positive);
    map.addLong(negative);
    assertEquals(1, map.rankLong(negative));
  }

  @Test
  public void testLargeRankLong_unsigned() {
    long positive = 1;
    long negative = -1;
    Roaring64NavigableMap map = newUnsignedHeap();
    map.addLong(positive);
    map.addLong(negative);
    assertEquals(2, map.rankLong(negative));
  }

  @Test
  public void testIterationOrder_signed() {
    long positive = 1;
    long negative = -1;
    Roaring64NavigableMap map = newSignedBuffered();
    map.addLong(positive);
    map.addLong(negative);
    LongIterator it = map.getLongIterator();
    long first = it.next();
    long last = it.next();
    assertEquals(negative, first);
    assertEquals(positive, last);
  }

  @Test
  public void testIterationOrder_unsigned() {
    long positive = 1;
    long negative = -1;
    Roaring64NavigableMap map = newUnsignedHeap();
    map.addLong(positive);
    map.addLong(negative);
    LongIterator it = map.getLongIterator();
    long first = it.next();
    long last = it.next();
    assertEquals(positive, first);
    assertEquals(negative, last);
  }

  @Test
  public void testAddingLowValueAfterHighValue() {
    Roaring64NavigableMap map = newDefaultCtor();
    map.addLong(Long.MAX_VALUE);
    assertEquals(Long.MAX_VALUE, map.select(0));
    map.addLong(666);
    assertEquals(666, map.select(0));
    assertEquals(Long.MAX_VALUE, map.select(1));
  }

  private Roaring64NavigableMap clone(Roaring64NavigableMap map) throws IOException, ClassNotFoundException {
    return SerializationUtils.clone(map);
  }

  @Test
  public void testSerialization_Empty() throws IOException, ClassNotFoundException {
    final Roaring64NavigableMap map = newDefaultCtor();

    final Roaring64NavigableMap clone = clone(map);

    // Check the test has not simply copied the ref
    assertNotSame(map, clone);
    assertEquals(0, clone.getLongCardinality());
  }

  @Test
  public void testSerialization_OneValue() throws IOException, ClassNotFoundException {
    final Roaring64NavigableMap map = newDefaultCtor();
    map.addLong(123);

    final Roaring64NavigableMap clone = clone(map);

    // Check the test has not simply copied the ref
    assertNotSame(map, clone);
    assertEquals(1, clone.getLongCardinality());
    assertEquals(123, clone.select(0));
  }


  @Test
  public void testSerialization_Unsigned() throws IOException, ClassNotFoundException {
    final Roaring64NavigableMap map = newUnsignedHeap();
    map.addLong(123);

    final Roaring64NavigableMap clone = clone(map);

    // Check the test has not simply copied the ref
    assertNotSame(map, clone);
    assertEquals(1, clone.getLongCardinality());
    assertEquals(123, clone.select(0));
  }


  @Test
  public void testSerialization_MultipleBuckets_Signed() throws IOException, ClassNotFoundException {
    final Roaring64NavigableMap map = newSignedBuffered();
    map.addLong(-123);
    map.addLong(123);
    map.addLong(Long.MAX_VALUE);

    final Roaring64NavigableMap clone = clone(map);

    // Check the test has not simply copied the ref
    assertNotSame(map, clone);
    assertEquals(3, clone.getLongCardinality());
    assertEquals(-123, clone.select(0));
    assertEquals(123, clone.select(1));
    assertEquals(Long.MAX_VALUE, clone.select(2));
  }

  @Test
  public void testSerialization_MultipleBuckets_Unsigned() throws IOException, ClassNotFoundException {
    final Roaring64NavigableMap map = newUnsignedHeap();
    map.addLong(-123);
    map.addLong(123);
    map.addLong(Long.MAX_VALUE);

    final Roaring64NavigableMap clone = clone(map);

    // Check the test has not simply copied the ref
    assertNotSame(map, clone);
    assertEquals(3, clone.getLongCardinality());
    assertEquals(123, clone.select(0));
    assertEquals(Long.MAX_VALUE, clone.select(1));
    assertEquals(-123, clone.select(2));
  }

  @Test
  public void testSerializationSizeInBytes_singleBucket() throws IOException, ClassNotFoundException {
    final Roaring64NavigableMap map = newDefaultCtor();
    map.addLong(123);

    checkSerializeBytes(map);
  }

  @Test
  public void testSerializationSizeInBytes_multipleBuckets() throws IOException, ClassNotFoundException {
    final Roaring64NavigableMap map = newDefaultCtor();
    map.addLong(123);
    map.addLong(Long.MAX_VALUE);

    checkSerializeBytes(map);
  }

  @Test
  public void testSupplierIsTransient_defaultIsImmutable() throws IOException, ClassNotFoundException, NoSuchFieldException {
    Roaring64NavigableMap map = new Roaring64NavigableMap(new MutableRoaringBitmapSupplier());
    map.add(0);

    final Roaring64NavigableMap clone = clone(map);

    // Demonstrate we fallback to RoaringBitmapSupplier as default
    Assertions.assertTrue(map.getHighToBitmap().firstEntry().getValue() instanceof MutableRoaringBitmap);
    Assertions.assertTrue(clone.getHighToBitmap().firstEntry().getValue() instanceof RoaringBitmap);
  }

  @Test
  public void testOr_SameBucket() {
    Roaring64NavigableMap left = newDefaultCtor();
    Roaring64NavigableMap right = newDefaultCtor();

    left.addLong(123);
    right.addLong(234);

    left.or(right);

    assertEquals(2, left.getLongCardinality());

    assertEquals(123, left.select(0));
    assertEquals(234, left.select(1));
  }

  @Test
  public void testOr_MultipleBuckets() {
    Roaring64NavigableMap left = newDefaultCtor();
    Roaring64NavigableMap right = newDefaultCtor();

    left.addLong(123);
    left.addLong(Long.MAX_VALUE);
    right.addLong(234);

    left.or(right);

    assertEquals(3, left.getLongCardinality());

    assertEquals(123, left.select(0));
    assertEquals(234, left.select(1));
    assertEquals(Long.MAX_VALUE, left.select(2));
  }

  @Test
  public void testOr_DifferentBucket_NotBuffer() {
    Roaring64NavigableMap left = newSignedBuffered();
    Roaring64NavigableMap right = newSignedBuffered();

    left.addLong(123);
    right.addLong(Long.MAX_VALUE / 2);

    left.or(right);

    assertEquals(2, left.getLongCardinality());

    assertEquals(123, left.select(0));
    assertEquals(Long.MAX_VALUE / 2, left.select(1));
  }


  @Test
  public void testOr_SameBucket_NotBuffer() {
    Roaring64NavigableMap left = new Roaring64NavigableMap(true, new RoaringBitmapSupplier());
    Roaring64NavigableMap right = new Roaring64NavigableMap(true, new RoaringBitmapSupplier());

    left.addLong(123);
    right.addLong(234);

    left.or(right);

    assertEquals(2, left.getLongCardinality());

    assertEquals(123, left.select(0));
    assertEquals(234, left.select(1));
  }

  @Test
  public void testOr_DifferentBucket_Buffer() {
    Roaring64NavigableMap left = newSignedBuffered();
    Roaring64NavigableMap right = newSignedBuffered();

    left.addLong(123);
    right.addLong(Long.MAX_VALUE);

    left.or(right);

    assertEquals(2, left.getLongCardinality());

    assertEquals(123, left.select(0));
    assertEquals(Long.MAX_VALUE, left.select(1));
  }

  @Test
  public void testOr_SameBucket_Buffer() {
    Roaring64NavigableMap left = newSignedBuffered();
    Roaring64NavigableMap right = newSignedBuffered();

    left.addLong(123);
    right.addLong(234);

    left.or(right);

    assertEquals(2, left.getLongCardinality());

    assertEquals(123, left.select(0));
    assertEquals(234, left.select(1));
  }

  @Test
  public void testOr_CloneInput() {
    Roaring64NavigableMap left = newDefaultCtor();
    Roaring64NavigableMap right = newDefaultCtor();

    right.addLong(123);

    // We push in left a bucket which does not exist
    left.or(right);

    // Then we mutate left: ensure it does not impact right as it should remain unchanged
    left.addLong(234);

    assertEquals(2, left.getLongCardinality());
    assertEquals(123, left.select(0));
    assertEquals(234, left.select(1));

    assertEquals(1, right.getLongCardinality());
    assertEquals(123, right.select(0));
  }


  @Test
  public void testXor_SingleBucket() {
    Roaring64NavigableMap left = newDefaultCtor();
    Roaring64NavigableMap right = newDefaultCtor();

    left.addLong(123);
    left.addLong(234);
    right.addLong(234);
    right.addLong(345);

    // We have 1 shared value: 234
    left.xor(right);

    assertEquals(2, left.getLongCardinality());
    assertEquals(123, left.select(0));
    assertEquals(345, left.select(1));
  }

  @Test
  public void testXor_Buffer() {
    Roaring64NavigableMap left = newSignedBuffered();
    Roaring64NavigableMap right = newSignedBuffered();

    left.addLong(123);
    left.addLong(234);
    right.addLong(234);
    right.addLong(345);

    left.xor(right);

    assertEquals(2, left.getLongCardinality());
    assertEquals(123, left.select(0));
    assertEquals(345, left.select(1));
  }

  @Test
  public void testXor_DifferentBucket() {
    Roaring64NavigableMap left = newDefaultCtor();
    Roaring64NavigableMap right = newDefaultCtor();

    left.addLong(123);
    right.addLong(Long.MAX_VALUE);

    // We have 1 shared value: 234
    left.xor(right);

    assertEquals(2, left.getLongCardinality());
    assertEquals(123, left.select(0));
    assertEquals(Long.MAX_VALUE, left.select(1));
  }

  @Test
  public void testXor_MultipleBucket() {
    Roaring64NavigableMap left = newDefaultCtor();
    Roaring64NavigableMap right = newDefaultCtor();

    left.addLong(123);
    left.addLong(Long.MAX_VALUE);
    right.addLong(Long.MAX_VALUE);

    // We have 1 shared value: 234
    left.xor(right);

    assertEquals(1, left.getLongCardinality());
    assertEquals(123, left.select(0));
  }


  @Test
  public void testAnd_SingleBucket() {
    Roaring64NavigableMap left = newDefaultCtor();
    Roaring64NavigableMap right = newDefaultCtor();

    left.addLong(123);
    left.addLong(234);
    right.addLong(234);
    right.addLong(345);

    // We have 1 shared value: 234
    left.and(right);

    assertEquals(1, left.getLongCardinality());
    assertEquals(234, left.select(0));
  }

  @Test
  public void testAnd_Buffer() {
    Roaring64NavigableMap left = newSignedBuffered();
    Roaring64NavigableMap right = newSignedBuffered();

    left.addLong(123);
    right.addLong(123);

    // We have 1 shared value: 234
    left.and(right);

    assertEquals(1, left.getLongCardinality());
    assertEquals(123, left.select(0));
  }

  @Test
  public void testAnd_DifferentBucket() {
    Roaring64NavigableMap left = newDefaultCtor();
    Roaring64NavigableMap right = newDefaultCtor();

    left.addLong(123);
    right.addLong(Long.MAX_VALUE);

    // We have 1 shared value: 234
    left.and(right);

    assertEquals(0, left.getLongCardinality());
  }

  @Test
  public void testAnd_MultipleBucket() {
    Roaring64NavigableMap left = newDefaultCtor();
    Roaring64NavigableMap right = newDefaultCtor();

    left.addLong(123);
    left.addLong(Long.MAX_VALUE);
    right.addLong(Long.MAX_VALUE);

    // We have 1 shared value: 234
    left.and(right);

    assertEquals(1, left.getLongCardinality());
    assertEquals(Long.MAX_VALUE, left.select(0));
  }


  @Test
  public void testAndNot_SingleBucket() {
    Roaring64NavigableMap left = newDefaultCtor();
    Roaring64NavigableMap right = newDefaultCtor();

    left.addLong(123);
    left.addLong(234);
    right.addLong(234);
    right.addLong(345);

    // We have 1 shared value: 234
    left.andNot(right);

    assertEquals(1, left.getLongCardinality());
    assertEquals(123, left.select(0));
  }

  @Test
  public void testAndNot_Buffer() {
    Roaring64NavigableMap left = newSignedBuffered();
    Roaring64NavigableMap right = newSignedBuffered();

    left.addLong(123);
    right.addLong(234);

    // We have 1 shared value: 234
    left.andNot(right);

    assertEquals(1, left.getLongCardinality());
    assertEquals(123, left.select(0));
  }

  @Test
  public void testAndNot_DifferentBucket() {
    Roaring64NavigableMap left = newDefaultCtor();
    Roaring64NavigableMap right = newDefaultCtor();

    left.addLong(123);
    right.addLong(Long.MAX_VALUE);

    // We have 1 shared value: 234
    left.andNot(right);

    assertEquals(1, left.getLongCardinality());
    assertEquals(123, left.select(0));
  }

  @Test
  public void testAndNot_MultipleBucket() {
    Roaring64NavigableMap left = newDefaultCtor();
    Roaring64NavigableMap right = newDefaultCtor();

    left.addLong(123);
    left.addLong(Long.MAX_VALUE);
    right.addLong(Long.MAX_VALUE);

    // We have 1 shared value: 234
    left.andNot(right);

    assertEquals(1, left.getLongCardinality());
    assertEquals(123, left.select(0));
  }

  @Test
  public void testToString_signed() {
    Roaring64NavigableMap map = new Roaring64NavigableMap(true);

    map.addLong(123);
    map.addLong(Long.MAX_VALUE);
    map.addLong(Long.MAX_VALUE + 1L);

    assertEquals("{-9223372036854775808,123,9223372036854775807}", map.toString());
  }

  @Test
  public void testToString_unsigned() {
    Roaring64NavigableMap map = new Roaring64NavigableMap(false);

    map.addLong(123);
    map.addLong(Long.MAX_VALUE);
    map.addLong(Long.MAX_VALUE + 1L);

    assertEquals("{123,9223372036854775807,9223372036854775808}", map.toString());
  }

  @Test
  public void testAddRange_SingleBucket_NotBuffer() {
    Roaring64NavigableMap map = newUnsignedHeap();

    map.addRange(5L, 12L);
    assertEquals(7L, map.getLongCardinality());

    assertEquals(5L, map.select(0));
    assertEquals(11L, map.select(6L));
  }


  @Test
  public void testAddRange_SingleBucket_Buffer() {
    Roaring64NavigableMap map = newSignedBuffered();

    map.addRange(5L, 12L);
    assertEquals(7L, map.getLongCardinality());

    assertEquals(5L, map.select(0));
    assertEquals(11L, map.select(6L));
  }

  // Edge case: the last high is excluded and should not lead to a new bitmap. However, it may be
  // seen only while trying to add for high=1
  @Test
  public void testAddRange_EndExcludingNextBitmapFirstLow() {
    Roaring64NavigableMap map = newDefaultCtor();

    long end = Util.toUnsignedLong(-1) + 1;

    map.addRange(end - 2, end);
    assertEquals(2, map.getLongCardinality());

    assertEquals(end - 2, map.select(0));
    assertEquals(end - 1, map.select(1));
  }


  @Test
  public void testAddRange_MultipleBuckets() {
    Roaring64NavigableMap map = newDefaultCtor();

    int enableTrim = 5;

    long from = RoaringIntPacking.pack(0, -1 - enableTrim);
    long to = from + 2 * enableTrim;
    map.addRange(from, to);
    int nbItems = (int) (to - from);
    assertEquals(nbItems, map.getLongCardinality());

    assertEquals(from, map.select(0));
    assertEquals(to - 1, map.select(nbItems - 1));
  }


  public static final long outOfRoaringBitmapRange = 2L * Integer.MAX_VALUE + 3L;

  // Check this range is not handled by RoaringBitmap
  @Test
  public void testCardinalityAboveIntegerMaxValue_RoaringBitmap() {
    assertThrows(IllegalArgumentException.class, () -> {
      RoaringBitmap map = new RoaringBitmap();

      map.add(0L, outOfRoaringBitmapRange);
    });
  }

  @Test
  public void testCardinalityAboveIntegerMaxValue() {
    Roaring64NavigableMap map = newDefaultCtor();

    long outOfSingleRoaring = outOfRoaringBitmapRange - 3;

    // This should fill entirely one bitmap,and add one in the next bitmap
    map.addRange(0, outOfSingleRoaring);
    assertEquals(outOfSingleRoaring, map.getLongCardinality());

    assertEquals(outOfSingleRoaring, map.getLongCardinality());

    assertEquals(0, map.select(0));
    assertEquals(outOfSingleRoaring - 1, map.select(outOfSingleRoaring - 1));

  }

  @Test
  public void testRoaringBitmap_SelectAboveIntegerMaxValue() {
    RoaringBitmap map = new RoaringBitmap();

    long maxForRoaringBitmap = Util.toUnsignedLong(-1) + 1;
    map.add(0L, maxForRoaringBitmap);

    assertEquals(maxForRoaringBitmap, map.getLongCardinality());
    assertEquals(-1, map.select(-1));
  }

  @Test
  public void testRoaringBitmap_SelectAboveIntegerMaxValuePlusOne() {
    RoaringBitmap map = new RoaringBitmap();

    long maxForRoaringBitmap = Util.toUnsignedLong(-1) + 1;
    map.add(0L, maxForRoaringBitmap);

    assertEquals(maxForRoaringBitmap, map.getLongCardinality());
    assertEquals(-1, map.select(-1));
  }

  @Test
  public void testTrim() {
    Roaring64NavigableMap map = new Roaring64NavigableMap(true);

    // How many contiguous values do we have to set to enable .trim?
    int enableTrim = 100;

    long from = RoaringIntPacking.pack(0, -1 - enableTrim);
    long to = from + 2 * enableTrim;

    // Check we cover different buckets
    assertNotEquals(RoaringIntPacking.high(to), RoaringIntPacking.high(from));

    for (long i = from; i <= to; i++) {
      map.addLong(i);
    }

    map.trim();
  }

  @Test
  public void testAutoboxedIterator() {
    Roaring64NavigableMap map = newUnsignedHeap();

    map.addLong(123);
    map.addLong(234);

    Iterator<Long> it = map.iterator();

    assertTrue(it.hasNext());
    assertEquals(123L, it.next().longValue());
    assertTrue(it.hasNext());
    assertEquals(234, it.next().longValue());
    assertFalse(it.hasNext());
  }

  @Test
  public void testAutoboxedIterator_CanNotRemove() {
    assertThrows(UnsupportedOperationException.class, () -> {
      Roaring64NavigableMap map = newUnsignedHeap();

      map.addLong(123);
      map.addLong(234);

      Iterator<Long> it = map.iterator();

      assertTrue(it.hasNext());

      // Should throw a UnsupportedOperationException
      it.remove();
    });
  }



  @Test
  public void testSelect_NoCache_MultipleBuckets() {
    Roaring64NavigableMap map = newNoCache();

    map.addLong(123);
    map.addLong(Long.MAX_VALUE);

    assertEquals(123L, map.select(0));
    assertEquals(Long.MAX_VALUE, map.select(1));
  }

  @Test
  public void testSelect_Empty() {
    assertThrows(IllegalArgumentException.class, () -> {
      Roaring64NavigableMap map = newUnsignedHeap();

      map.select(0);
    });
  }


  @Test
  public void testSelect_OutOfBounds_MatchCardinality() {
    assertThrows(IllegalArgumentException.class, () -> {
      Roaring64NavigableMap map = newUnsignedHeap();

      map.addLong(123);

      map.select(1);
    });
  }

  @Test
  public void testSelect_OutOfBounds_OtherCardinality() {
    assertThrows(IllegalArgumentException.class, () -> {
      Roaring64NavigableMap map = newUnsignedHeap();

      map.addLong(123);

      map.select(2);
    });
  }


  @Test
  public void testRank_NoCache_MultipleBuckets() {
    Roaring64NavigableMap map = newNoCache();

    map.addLong(123);
    map.addLong(Long.MAX_VALUE);

    assertEquals(0, map.rankLong(0));
    assertEquals(1, map.rankLong(123));
    assertEquals(1, map.rankLong(Long.MAX_VALUE - 1));
    assertEquals(2, map.rankLong(Long.MAX_VALUE));
  }

  @Test
  public void testRank_NoCache_HighNotPresent() {
    Roaring64NavigableMap map = newNoCache();

    map.addLong(123);
    map.addLong(Long.MAX_VALUE);

    assertEquals(1, map.rankLong(Long.MAX_VALUE / 2L));
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
    Roaring64NavigableMap map = newSignedBuffered();

    map.addLong(123);
    map.addLong(234);

    map.runOptimize();
  }

  @Test
  public void testFlipBackward() {
    final Roaring64NavigableMap r = newUnsignedHeap();
    final long value = 1L;
    r.addLong(value);
    assertEquals(1, r.getLongCardinality());

    r.flip(1);
    assertEquals(0, r.getLongCardinality());
  }

  @Test
  public void testFlip_NotBuffer() {
    Roaring64NavigableMap map = newUnsignedHeap();

    map.addLong(0);
    map.flip(0);

    assertFalse(map.contains(0));

    checkCardinalities(map);
  }

  @Test
  public void testFlip_Buffer() {
    Roaring64NavigableMap map = newSignedBuffered();

    map.addLong(0);
    map.flip(0);

    assertFalse(map.contains(0));
  }

  // Ensure the ordering behavior with default constructors is the same between RoaringBitmap and
  // Roaring64NavigableMap. Typically ensures longs are managed as unsigned longs
  @Test
  public void testDefaultBehaviorLikeRoaring() {
    Roaring64NavigableMap longBitmap = newDefaultCtor();
    RoaringBitmap bitmap = new RoaringBitmap();

    longBitmap.addLong(-1);
    bitmap.add(-1);

    longBitmap.addLong(1);
    bitmap.add(1);

    int[] bitmapAsIntArray = bitmap.toArray();

    long[] longBitmapAsArray = longBitmap.toArray();

    // The array seems equivalent, but beware one represents unsigned integers while the others
    // holds unsigned longs: -1 have a different meaning
    assertArrayEquals(bitmapAsIntArray, Ints.toArray(Longs.asList(longBitmapAsArray)));
    assertArrayEquals(Longs.toArray(Ints.asList(bitmapAsIntArray)), longBitmapAsArray);

    long[] bitmapAsLongArray = new long[bitmapAsIntArray.length];
    for (int i = 0; i < bitmapAsIntArray.length; i++) {
      bitmapAsLongArray[i] = Util.toUnsignedLong(bitmapAsIntArray[i]);
    }
  }

  @Disabled(".add have a different meaning between Roaring64NavigableMap and RoaringBitmap")
  @Test
  public void testDefaultBehaviorLikeRoaring_MinusOneAsInt() {
    Roaring64NavigableMap longBitmap = newDefaultCtor();
    RoaringBitmap bitmap = new RoaringBitmap();

    longBitmap.addLong(-1);
    bitmap.add(-1);

    // Ok as -1 === -1
    assertEquals(bitmap.select(0), longBitmap.select(0));

    // But RoaringBitmap.select has to be interpreted as an unsigned integer: not as an unsigned
    // long
    assertEquals(Util.toUnsignedLong(bitmap.select(0)), longBitmap.select(0));
  }

  @Test
  public void testRandomAddRemove() {
    Random r = new Random(1234);

    // We need to max the considered range of longs, else each long would be in a different bucket
    long max = Integer.MAX_VALUE * 20L;

    long targetCardinality = 1000;

    Roaring64NavigableMap map = newSignedBuffered();

    // Add a lot of items
    while (map.getIntCardinality() < targetCardinality) {
      map.addLong(r.nextLong() % max);
    }

    // Remove them by chunks
    int chunks = 10;
    for (int j = 0; j < chunks; j++) {
      long chunksSize = targetCardinality / chunks;
      for (int i = 0; i < chunksSize; i++) {
        map.removeLong(map.select(r.nextInt(map.getIntCardinality())));
      }
      assertEquals(targetCardinality - chunksSize * (j + 1), map.getIntCardinality());
    }
    assertTrue(map.isEmpty());
  }

  @Test
  public void testLongSizeInBytes() {
    Roaring64NavigableMap map = newSignedBuffered();

    // Size when empty
    assertEquals(16, map.getLongSizeInBytes());

    // Add values so that the underlying Map holds multiple entries
    map.add(0);
    map.add(2L * Integer.MAX_VALUE);
    map.addRange(8L * Integer.MAX_VALUE, 8L * Integer.MAX_VALUE + 1024);
    
    assertEquals(3, map.getHighToBitmap().size());

    // Size with multiple entries
    assertEquals(228, map.getLongSizeInBytes());
    
    // Select does allocate some cache
    map.select(16);
    assertEquals(264, map.getLongSizeInBytes());
  }

  @Test
  public void testLazyOr() {
    Roaring64NavigableMap map1 = Roaring64NavigableMap.bitmapOf(1 << 16, 1 << 18, 1 << 19, 1L << 33);
    map1.naivelazyor(Roaring64NavigableMap.bitmapOf(4, 7, 8, 9));
    map1.naivelazyor(Roaring64NavigableMap.bitmapOf(1, 2, 3, 4, 5, 1 << 16, 1 << 17, 1 << 20));
    map1.repairAfterLazy();
    Roaring64NavigableMap map2 = Roaring64NavigableMap.bitmapOf(1, 2, 3, 4, 5, 7, 8, 9, 1 << 16, 1 << 17, 1 << 18, 1 << 19, 1 << 20 , 1L << 33);
    assertEquals(map2, map1);
  }

  // https://github.com/RoaringBitmap/RoaringBitmap/issues/528
  @Test
  public void testAnd_ImplicitRoaringBitmap() {
    // Based on RoaringBitmap
    Roaring64NavigableMap x = new Roaring64NavigableMap();
    x.addRange(123, 124);
    Assertions.assertTrue(x.getHighToBitmap().values().iterator().next() instanceof RoaringBitmap);

    // Based on MutableRoaringBitmap
    Roaring64NavigableMap y = Roaring64NavigableMap.bitmapOf(4L);
    Assertions.assertTrue(y.getHighToBitmap().values().iterator().next() instanceof RoaringBitmap);

    {
      x.and(y);

      BitmapDataProvider singleBitmap = x.getHighToBitmap().values().iterator().next();
      Assertions.assertTrue(singleBitmap instanceof RoaringBitmap);
      Assertions.assertTrue(singleBitmap.isEmpty());
    }
  }

  @Test
  public void testAnd_MutableRoaringBitmap() {
    // Based on RoaringBitmap
    Roaring64NavigableMap x = new Roaring64NavigableMap(new MutableRoaringBitmapSupplier());
    x.addRange(0, 16);

    // Based on MutableRoaringBitmap
    Roaring64NavigableMap y = new Roaring64NavigableMap(new MutableRoaringBitmapSupplier());
    y.addRange(8, 32);

    Assertions.assertEquals(16L, x.getLongCardinality());
    x.and(y);
    Assertions.assertEquals(8L, x.getLongCardinality());
  }

  @Test
  public void testAnd_IncompatibleImplementations() {
    // Based on RoaringBitmap
    Roaring64NavigableMap x = new Roaring64NavigableMap(new RoaringBitmapSupplier());
    x.addRange(0, 16);

    // Based on MutableRoaringBitmap
    Roaring64NavigableMap y = new Roaring64NavigableMap(new MutableRoaringBitmapSupplier());
    y.addRange(8, 32);

    Assertions.assertThrows(UnsupportedOperationException.class, () -> x.and(y));
  }

  @Test
  public void testAndNot_ImplicitRoaringBitmap() {
    // Based on RoaringBitmap
    Roaring64NavigableMap x = new Roaring64NavigableMap();
    x.addRange(8, 16);

    // Based on MutableRoaringBitmap
    Roaring64NavigableMap y = new Roaring64NavigableMap();
    y.addRange(12, 32);

    {
      x.andNot(y);

      BitmapDataProvider singleBitmap = x.getHighToBitmap().values().iterator().next();
      Assertions.assertTrue(singleBitmap instanceof RoaringBitmap);
      Assertions.assertEquals(4L, singleBitmap.getLongCardinality());
      Assertions.assertEquals(8L, singleBitmap.select(0));
      Assertions.assertEquals(11L, singleBitmap.select(3));
    }
  }

  @Test
  public void testOr_ImplicitRoaringBitmap() {
    // Based on RoaringBitmap
    Roaring64NavigableMap x = new Roaring64NavigableMap();
    x.addRange(123, 124);

    // Based on MutableRoaringBitmap
    Roaring64NavigableMap y = Roaring64NavigableMap.bitmapOf(4L);

    {
      x.or(y);

      BitmapDataProvider singleBitmap = x.getHighToBitmap().values().iterator().next();
      Assertions.assertTrue(singleBitmap instanceof RoaringBitmap);
      Assertions.assertEquals(2L, singleBitmap.getLongCardinality());
    }
  }

  @Test
  public void testNaivelazyor_ImplicitRoaringBitmap() {
    // Based on RoaringBitmap
    Roaring64NavigableMap x = new Roaring64NavigableMap();
    x.addRange(123, 124);

    // Based on MutableRoaringBitmap
    Roaring64NavigableMap y = Roaring64NavigableMap.bitmapOf(4L);

    {
      x.naivelazyor(y);
      x.repairAfterLazy();

      BitmapDataProvider singleBitmap = x.getHighToBitmap().values().iterator().next();
      Assertions.assertTrue(singleBitmap instanceof RoaringBitmap);
      Assertions.assertEquals(2L, singleBitmap.getLongCardinality());
    }
  }

  private void checkConsistencyWithResource(String resourceName, Roaring64NavigableMap bitmap) throws IOException {
    byte[] reference = ByteStreams.toByteArray(TestAdversarialInputs.openInputstream(resourceName));

    Assertions.assertEquals(reference.length, bitmap.serializedSizeInBytes());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    bitmap.serialize(new DataOutputStream(baos));
    Assertions.assertArrayEquals(reference, baos.toByteArray());
  }

  @Test
  public void testSerialization_empty() throws IOException, ClassNotFoundException {
    // This example binary comes from https://github.com/RoaringBitmap/CRoaring/tree/master/tests/testdata
    String resourceName = "/testdata/64mapempty.bin";
    InputStream inputStream = TestAdversarialInputs.openInputstream(resourceName);

    Roaring64NavigableMap bitmap = new Roaring64NavigableMap();

    Roaring64NavigableMap.SERIALIZATION_MODE = Roaring64NavigableMap.SERIALIZATION_MODE_PORTABLE;
    bitmap.deserialize(new DataInputStream(inputStream));

    // https://github.com/RoaringBitmap/CRoaring/blob/master/tests/cpp_unit_util.cpp#L20
    Assertions.assertEquals(0, bitmap.getLongCardinality());
    Assertions.assertEquals(0, bitmap.getHighToBitmap().size());

    checkConsistencyWithResource(resourceName, bitmap);
  }

  @Test
  public void testSerialization_64map32bitvals() throws IOException, ClassNotFoundException {
    // This example binary comes from https://github.com/RoaringBitmap/CRoaring/tree/master/tests/testdata
    String resourceName = "/testdata/64map32bitvals.bin";
    InputStream inputStream = TestAdversarialInputs.openInputstream(resourceName);

    Roaring64NavigableMap bitmap = new Roaring64NavigableMap();

    Roaring64NavigableMap.SERIALIZATION_MODE = Roaring64NavigableMap.SERIALIZATION_MODE_PORTABLE;
    bitmap.deserialize(new DataInputStream(inputStream));

    // https://github.com/RoaringBitmap/CRoaring/blob/master/tests/cpp_unit_util.cpp#L27
    Assertions.assertEquals(10, bitmap.getLongCardinality());
    Assertions.assertEquals(1, bitmap.getHighToBitmap().size());
    Assertions.assertEquals(0, bitmap.select(0));
    Assertions.assertEquals(9, bitmap.select(9));

    checkConsistencyWithResource(resourceName, bitmap);
  }

  @Test
  public void testSerialization_spreadvals() throws IOException, ClassNotFoundException {
    // This example binary comes from https://github.com/RoaringBitmap/CRoaring/tree/master/tests/testdata
    String resourceName = "/testdata/64mapspreadvals.bin";
    InputStream inputStream = TestAdversarialInputs.openInputstream(resourceName);

    Roaring64NavigableMap bitmap = new Roaring64NavigableMap();

    Roaring64NavigableMap.SERIALIZATION_MODE = Roaring64NavigableMap.SERIALIZATION_MODE_PORTABLE;
    bitmap.deserialize(new DataInputStream(inputStream));

    // https://github.com/RoaringBitmap/CRoaring/blob/master/tests/cpp_unit_util.cpp#L36
    Assertions.assertEquals(100, bitmap.getLongCardinality());
    Assertions.assertEquals(10, bitmap.getHighToBitmap().size());
    Assertions.assertEquals(0, bitmap.select(0));
    Assertions.assertEquals(9, bitmap.select(9));
    Assertions.assertEquals((9L << 32) + 0L, bitmap.select(90));
    Assertions.assertEquals((9L << 32) + 1L, bitmap.select(91));
    Assertions.assertEquals((9L << 32) + 9L, bitmap.select(99));

    checkConsistencyWithResource(resourceName, bitmap);
  }

  @Test
  public void testSerialization_highvals() throws IOException, ClassNotFoundException {
    // This example binary comes from https://github.com/RoaringBitmap/CRoaring/tree/master/tests/testdata
    String resourceName = "/testdata/64maphighvals.bin";
    InputStream inputStream = TestAdversarialInputs.openInputstream(resourceName);

    Roaring64NavigableMap bitmap = new Roaring64NavigableMap();

    Roaring64NavigableMap.SERIALIZATION_MODE = Roaring64NavigableMap.SERIALIZATION_MODE_PORTABLE;
    bitmap.deserialize(new DataInputStream(inputStream));

    // https://github.com/RoaringBitmap/CRoaring/blob/master/tests/cpp_unit_util.cpp#L46
    long maxInt = Util.toUnsignedLong(-1);
    Assertions.assertEquals(121, bitmap.getLongCardinality());
    Assertions.assertEquals(11, bitmap.getHighToBitmap().size());
    Assertions.assertEquals(((maxInt - 10L) << 32) + (maxInt - 10), bitmap.select(0));
    Assertions.assertEquals(((maxInt - 10L) << 32) + (maxInt - 0), bitmap.select(10));
    Assertions.assertEquals(((maxInt - 0L) << 32) + (maxInt - 10), bitmap.select(110));
    Assertions.assertEquals(((maxInt - 0L) << 32) + (maxInt - 9), bitmap.select(111));
    Assertions.assertEquals(((maxInt - 0L) << 32) + (maxInt - 0), bitmap.select(120));

    checkConsistencyWithResource(resourceName, bitmap);
  }

  @Test
  public void testAddExtremes() {
    // Based on RoaringBitmap
    Roaring64NavigableMap x = newDefaultCtor();
    x.addLong(0L);
    x.addLong(Long.MAX_VALUE);
    x.addLong(-1L);

    Assertions.assertEquals(3L, x.getLongCardinality());
    Assertions.assertArrayEquals(new long[] {0, Long.MAX_VALUE, -1L}, x.toArray());
  }

  @Test
  public void testAddExtremes_signed() {
    Roaring64NavigableMap x = newSignedBuffered();
    x.addLong(0L);
    x.addLong(Long.MAX_VALUE);
    x.addLong(-1L);

    Assertions.assertEquals(3L, x.getLongCardinality());
    Assertions.assertArrayEquals(new long[] {-1L, 0, Long.MAX_VALUE}, x.toArray());
  }

  @Test
  public void testRangeExtremeEnd() {
    Roaring64NavigableMap x = newDefaultCtor();
    x.addRange(-3L, -1L);

    Assertions.assertEquals(2L, x.getLongCardinality());
    Assertions.assertArrayEquals(new long[] {-3L, -2L}, x.toArray());
  }

  @Test
  public void testRangeExtremeEnd_signed() {
    Roaring64NavigableMap x = newSignedBuffered();
    x.addRange(-3L, -1L);

    Assertions.assertEquals(2L, x.getLongCardinality());
    Assertions.assertArrayEquals(new long[] {-3L, -2L}, x.toArray());
  }

  @Test
  public void testRangeAroundIntegerMax() {
    Roaring64NavigableMap x = newDefaultCtor();
    x.addRange(Integer.MAX_VALUE - 1L, Integer.MAX_VALUE + 3L);

    Assertions.assertEquals(4L, x.getLongCardinality());
    Assertions.assertEquals(1L, x.getHighToBitmap().size());
    Assertions.assertArrayEquals(new long[] {Integer.MAX_VALUE - 1L, Integer.MAX_VALUE, Integer.MAX_VALUE + 1L, Integer.MAX_VALUE + 2L}, x.toArray());
  }

  @Test
  public void testRangeAround2TimesIntegerMax() {
    Roaring64NavigableMap x = newDefaultCtor();
    long rangeStart = 2L * Integer.MAX_VALUE;
    x.addRange(rangeStart, rangeStart + 4L);

    Assertions.assertEquals(4L, x.getLongCardinality());
    Assertions.assertEquals(2L, x.getHighToBitmap().size());
    Assertions.assertArrayEquals(new long[] {rangeStart, rangeStart+1L, rangeStart+2L, rangeStart+3L}, x.toArray());
  }

  @Test
  public void testRangeAroundLongMax() {
    Roaring64NavigableMap x = newDefaultCtor();
    x.addRange(Long.MAX_VALUE - 1L, Long.MAX_VALUE + 3L);

    Assertions.assertEquals(4L, x.getLongCardinality());
    Assertions.assertArrayEquals(new long[] {Long.MAX_VALUE - 1L, Long.MAX_VALUE, Long.MIN_VALUE, Long.MIN_VALUE + 1L}, x.toArray());
  }

  @Test()
  public void testRangeAroundLongMax_signed() {
    Roaring64NavigableMap x = newSignedBuffered();

    Assertions.assertThrows(IllegalArgumentException.class, () -> {
        x.addRange(Long.MAX_VALUE - 1L, Long.MAX_VALUE + 3L);
    });
  }

  @Test
  public void testEmptyFirst() {
    assertThrows(NoSuchElementException.class, () -> newDefaultCtor().first());
  }

  @Test
  public void testEmptyLast() {
    assertThrows(NoSuchElementException.class, () -> newDefaultCtor().last());
  }


  @Test
  public void testFirstLast_32b() {
    Roaring64NavigableMap rb = newDefaultCtor();

    rb.add(2);
    rb.add(4);
    rb.add(8);
    assertEquals(2, rb.first());
    assertEquals(8, rb.last());
  }

  @Test
  public void testFirstLast_64b() {
    Roaring64NavigableMap rb = newDefaultCtor();

    rb.add(-128);
    rb.add(-64);
    rb.add(-32);
    assertEquals(-128, rb.first());
    assertEquals(-32, rb.last());
  }

  @Test
  public void testFirstLast_32_64b() {
    Roaring64NavigableMap rb = newDefaultCtor();

    rb.add(2);
    rb.add(4);
    rb.add(8);
    rb.add(-128);
    rb.add(-64);
    rb.add(-32);
    assertEquals(2, rb.first());
    assertEquals(-32, rb.last());
  }
}
