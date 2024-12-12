package org.roaringbitmap.longlong;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.roaringbitmap.Util.toUnsignedLong;
import static org.roaringbitmap.ValidationRangeConsumer.Value.ABSENT;
import static org.roaringbitmap.ValidationRangeConsumer.Value.PRESENT;

import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.ValidationRangeConsumer;
import org.roaringbitmap.art.LeafNode;
import org.roaringbitmap.art.LeafNodeIterator;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.commons.lang3.SerializationUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;

public class TestRoaring64Bitmap {

  private Roaring64Bitmap newDefaultCtor() {
    return new Roaring64Bitmap();
  }

  public static Set<Long> getSourceForAllKindsOfNodeTypes() {
    Random random = new Random(1234);
    Set<Long> source = new HashSet<>();
    int total = 10000;
    for (int i = 0; i < total; i++) {
      while (!source.add(random.nextLong())) {
        // Retry adding a different long which is not in the Set
      }
    }
    Assertions.assertEquals(total, source.size());
    return source;
  }

  @Test
  public void testEquality() {
    Roaring64Bitmap rb1 = new Roaring64Bitmap();
    Roaring64Bitmap rb2 = new Roaring64Bitmap();
    assertEquals(rb1, rb2);
    rb1.addLong(1);
    assertNotEquals(rb1, rb2);
    rb1.removeLong(1);
    assertEquals(rb1, rb2);
  }

  @Test
  public void test() throws Exception {
    Random random = new Random(1234);
    Roaring64Bitmap roaring64Bitmap = new Roaring64Bitmap();
    Set<Long> source = new HashSet<>();
    int total = 1000000;
    for (int i = 0; i < total; i++) {
      long l = random.nextLong();
      roaring64Bitmap.addLong(l);
      source.add(l);
    }
    LongIterator longIterator = roaring64Bitmap.getLongIterator();
    int i = 0;
    while (longIterator.hasNext()) {
      long actual = longIterator.next();
      Assertions.assertTrue(source.contains(actual));
      i++;
    }
    Assertions.assertEquals(total, i);
  }

  @Test
  public void testAllKindOfNodeTypesSerDeser() throws Exception {
    Set<Long> source = getSourceForAllKindsOfNodeTypes();

    Roaring64Bitmap roaring64Bitmap = new Roaring64Bitmap();
    source.forEach(roaring64Bitmap::addLong);

    LongIterator longIterator = roaring64Bitmap.getLongIterator();
    int i = 0;
    while (longIterator.hasNext()) {
      long actual = longIterator.next();
      Assertions.assertTrue(source.contains(actual));
      i++;
    }
    Assertions.assertEquals(source.size(), i);
    // test all kind of nodes's serialization/deserialization
    long sizeL = roaring64Bitmap.serializedSizeInBytes();
    if (sizeL > Integer.MAX_VALUE) {
      return;
    }
    int sizeInt = (int) sizeL;
    long select2 = roaring64Bitmap.select(2);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(sizeInt);
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    roaring64Bitmap.serialize(dataOutputStream);
    ByteArrayInputStream byteArrayInputStream =
        new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
    DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);
    Roaring64Bitmap deserStreamOne = new Roaring64Bitmap();
    deserStreamOne.deserialize(dataInputStream);
    Assertions.assertEquals(select2, deserStreamOne.select(2));
    deserStreamOne = null;
    byteArrayInputStream = null;
    byteArrayOutputStream = null;
    ByteBuffer byteBuffer = ByteBuffer.allocate(sizeInt).order(ByteOrder.LITTLE_ENDIAN);
    roaring64Bitmap.serialize(byteBuffer);
    roaring64Bitmap = null;
    byteBuffer.flip();
    Roaring64Bitmap deserBBOne = new Roaring64Bitmap();
    deserBBOne.deserialize(byteBuffer);
    Assertions.assertEquals(select2, deserBBOne.select(2));
  }

  @Test
  public void testEmpty() {
    Roaring64Bitmap map = newDefaultCtor();

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
    Roaring64Bitmap map = newDefaultCtor();
    map.addLong(0);

    LongIterator iterator = map.getLongIterator();
    assertTrue(iterator.hasNext());
    assertEquals(0, iterator.next());
    assertEquals(0, map.select(0));
    assertTrue(map.contains(0));
    assertFalse(iterator.hasNext());
    assertEquals(1, map.getLongCardinality());
    assertFalse(map.isEmpty());
    assertEquals(1, map.rankLong(Long.MIN_VALUE));
    assertEquals(1, map.rankLong(Integer.MIN_VALUE - 1L));
    assertEquals(1, map.rankLong(-1));
    assertEquals(1, map.rankLong(0));
    assertEquals(1, map.rankLong(1));
    assertEquals(1, map.rankLong(Integer.MAX_VALUE + 1L));
    assertEquals(1, map.rankLong(Long.MAX_VALUE));
  }

  @Test
  public void testMinusOne_Unsigned() {
    Roaring64Bitmap map = newDefaultCtor();
    map.addLong(-1);

    LongIterator iterator = map.getLongIterator();
    assertTrue(iterator.hasNext());
    assertEquals(-1, iterator.next());
    assertEquals(-1, map.select(0));
    assertTrue(map.contains(-1));
    assertFalse(iterator.hasNext());
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
  public void testSimpleIntegers() {
    Roaring64Bitmap map = newDefaultCtor();
    map.addLong(123);
    map.addLong(234);

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
  public void testAddOneSelect2() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          Roaring64Bitmap map = newDefaultCtor();
          map.addLong(123);
          map.select(1);
        });
  }

  @Test
  public void testAddInt() {
    Roaring64Bitmap map = newDefaultCtor();
    map.addInt(-1);
    assertEquals(0xFFFFFFFFL, map.select(0));
  }

  @Test
  public void testIterator_NextWithoutHasNext_Filled() {
    Roaring64Bitmap map = newDefaultCtor();
    map.addLong(0);
    assertTrue(map.getLongIterator().hasNext());
    assertEquals(0, map.getLongIterator().next());
  }

  @Test
  public void testIterator_NextWithoutHasNext_Empty() {
    assertThrows(
        IllegalStateException.class,
        () -> {
          Roaring64Bitmap map = newDefaultCtor();
          map.getLongIterator().next();
        });
  }

  @Test
  public void testLongMaxValue() {
    Roaring64Bitmap map = newDefaultCtor();
    map.addLong(Long.MAX_VALUE);
    LongIterator iterator = map.getLongIterator();
    assertTrue(iterator.hasNext());
    assertEquals(Long.MAX_VALUE, iterator.next());
    assertEquals(Long.MAX_VALUE, map.select(0));
    assertFalse(iterator.hasNext());
    assertEquals(1, map.getLongCardinality());
    assertEquals(1, map.rankLong(Long.MIN_VALUE));
    assertEquals(1, map.rankLong(Long.MIN_VALUE + 1));
    assertEquals(1, map.rankLong(-1));
    assertEquals(0, map.rankLong(0));
    assertEquals(0, map.rankLong(1));
    assertEquals(0, map.rankLong(Long.MAX_VALUE - 1));
    assertEquals(1, map.rankLong(Long.MAX_VALUE));
    assertArrayEquals(new long[] {Long.MAX_VALUE}, map.toArray());
  }

  @Test
  public void testLongMinValue() {
    Roaring64Bitmap map = newDefaultCtor();
    map.addLong(Long.MIN_VALUE);
    LongIterator iterator = map.getLongIterator();
    assertTrue(iterator.hasNext());
    assertEquals(Long.MIN_VALUE, iterator.next());
    assertEquals(Long.MIN_VALUE, map.select(0));
    assertFalse(iterator.hasNext());
    assertEquals(1, map.getLongCardinality());
    assertEquals(1, map.rankLong(Long.MIN_VALUE));
    assertEquals(1, map.rankLong(Long.MIN_VALUE + 1));
    assertEquals(1, map.rankLong(-1));
    assertEquals(0, map.rankLong(0));
    assertEquals(0, map.rankLong(1));
    assertEquals(0, map.rankLong(Long.MAX_VALUE - 1));
    assertEquals(0, map.rankLong(Long.MAX_VALUE));
  }

  @Test
  public void testLongMinValueZeroOneMaxValue() {
    Roaring64Bitmap map = newDefaultCtor();

    map.addLong(Long.MIN_VALUE);
    map.addLong(0);
    map.addLong(1);
    map.addLong(Long.MAX_VALUE);
    LongIterator iterator = map.getLongIterator();
    assertEquals(0, iterator.next());
    assertEquals(0, map.select(0));
    assertEquals(1, iterator.next());
    assertEquals(1, map.select(1));
    assertEquals(Long.MAX_VALUE, iterator.next());
    assertEquals(Long.MAX_VALUE, map.select(2));
    assertEquals(Long.MIN_VALUE, iterator.next());
    assertEquals(Long.MIN_VALUE, map.select(3));
    assertFalse(iterator.hasNext());
    assertEquals(4, map.getLongCardinality());
    assertEquals(4, map.rankLong(Long.MIN_VALUE));
    assertEquals(4, map.rankLong(Long.MIN_VALUE + 1));
    assertEquals(4, map.rankLong(-1));
    assertEquals(1, map.rankLong(0));
    assertEquals(2, map.rankLong(1));
    assertEquals(2, map.rankLong(2));
    assertEquals(2, map.rankLong(Long.MAX_VALUE - 1));
    assertEquals(3, map.rankLong(Long.MAX_VALUE));

    final List<Long> foreach = new ArrayList<>();
    map.forEach(
        new LongConsumer() {

          @Override
          public void accept(long value) {
            foreach.add(value);
          }
        });
    assertEquals(Arrays.asList(0L, 1L, Long.MAX_VALUE, Long.MIN_VALUE), foreach);
  }

  @Test
  public void testReverseIterator_SingleBuket() {
    Roaring64Bitmap map = newDefaultCtor();
    map.addLong(123);
    map.addLong(234);
    LongIterator iterator = map.getReverseLongIterator();
    assertTrue(iterator.hasNext());
    assertEquals(234, iterator.next());
    assertTrue(iterator.hasNext());
    assertEquals(123, iterator.next());
    assertFalse(iterator.hasNext());
  }

  @Test
  public void testReverseIterator_MultipleBuket() {
    Roaring64Bitmap map = newDefaultCtor();

    map.addLong(123);
    map.addLong(Long.MAX_VALUE);
    LongIterator iterator = map.getReverseLongIterator();
    assertTrue(iterator.hasNext());
    assertEquals(Long.MAX_VALUE, iterator.next());
    assertTrue(iterator.hasNext());
    assertEquals(123, iterator.next());
    assertFalse(iterator.hasNext());
  }

  @Test
  public void testRemove() {
    Roaring64Bitmap map = newDefaultCtor();

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
    Roaring64Bitmap map = newDefaultCtor();

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
    Roaring64Bitmap map = newDefaultCtor();

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
    Roaring64Bitmap map = newDefaultCtor();

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
  public void testPerfManyDifferentBuckets() {
    Roaring64Bitmap map = newDefaultCtor();

    long problemSize = 1000 * 1000L;
    for (long i = 1; i <= problemSize; i++) {
      map.addLong(i * Integer.MAX_VALUE + 1L);
    }

    long cardinality = map.getLongCardinality();
    assertEquals(problemSize, cardinality);

    long last = map.select(cardinality - 1);
    assertEquals(cardinality, map.rankLong(last));
  }

  @Test
  public void testLargeSelectLong() {
    long positive = 1;
    long negative = -1;
    Roaring64Bitmap map = newDefaultCtor();
    map.addLong(positive);
    map.addLong(negative);
    long first = map.select(0);
    long last = map.select(1);

    assertEquals(positive, first);
    assertEquals(negative, last);
  }

  @Test
  public void testLargeRankLong() {
    long positive = 1;
    long negative = -1;
    Roaring64Bitmap map = newDefaultCtor();
    map.addLong(positive);
    map.addLong(negative);
    assertEquals(2, map.rankLong(negative));
  }

  @Test
  public void testIterationOrder() {
    long positive = 1;
    long negative = -1;
    Roaring64Bitmap map = newDefaultCtor();
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
    Roaring64Bitmap map = newDefaultCtor();
    map.addLong(Long.MAX_VALUE);
    assertEquals(Long.MAX_VALUE, map.select(0));
    map.addLong(666);
    assertEquals(666, map.select(0));
    assertEquals(Long.MAX_VALUE, map.select(1));
  }

  @Test
  public void testSerializationEmpty() throws IOException, ClassNotFoundException {
    final Roaring64Bitmap map = newDefaultCtor();

    TestRoaring64NavigableMap.checkSerializeBytes(map);

    final Roaring64Bitmap clone = SerializationUtils.clone(map);

    // Check the test has not simply copied the ref
    assertNotSame(map, clone);
    assertEquals(0, clone.getLongCardinality());
  }

  @Test
  public void testSerialization_ToBigEndianBuffer() throws IOException {
    final Roaring64Bitmap map = newDefaultCtor();
    map.addLong(123);
    ByteBuffer buffer =
        ByteBuffer.allocate((int) map.serializedSizeInBytes()).order(ByteOrder.BIG_ENDIAN);
    map.serialize(buffer);
    assertEquals(map.serializedSizeInBytes(), buffer.position());
  }

  @Test
  public void testSerialization_OneValue() throws IOException, ClassNotFoundException {
    final Roaring64Bitmap map = newDefaultCtor();
    map.addLong(123);

    TestRoaring64NavigableMap.checkSerializeBytes(map);

    final Roaring64Bitmap clone = SerializationUtils.clone(map);

    // Check the test has not simply copied the ref
    assertNotSame(map, clone);
    assertEquals(1, clone.getLongCardinality());
    assertEquals(123, clone.select(0));
  }

  @Test
  public void testSerialization() throws IOException, ClassNotFoundException {
    final Roaring64Bitmap map = newDefaultCtor();
    map.addLong(123);

    TestRoaring64NavigableMap.checkSerializeBytes(map);

    final Roaring64Bitmap clone = SerializationUtils.clone(map);

    // Check the test has not simply copied the ref
    assertNotSame(map, clone);
    assertEquals(1, clone.getLongCardinality());
    assertEquals(123, clone.select(0));
  }

  @Test
  public void testSerializationMultipleBuckets() throws IOException, ClassNotFoundException {
    final Roaring64Bitmap map = newDefaultCtor();
    map.addLong(-123);
    map.addLong(123);
    map.addLong(Long.MAX_VALUE);

    TestRoaring64NavigableMap.checkSerializeBytes(map);

    final Roaring64Bitmap clone = SerializationUtils.clone(map);

    // Check the test has not simply copied the ref
    assertNotSame(map, clone);
    assertEquals(3, clone.getLongCardinality());
    assertEquals(123, clone.select(0));
    assertEquals(Long.MAX_VALUE, clone.select(1));
    assertEquals(-123, clone.select(2));
    int sizeInByteInt = map.getSizeInBytes();
    ByteBuffer byteBuffer = ByteBuffer.allocate(sizeInByteInt).order(ByteOrder.LITTLE_ENDIAN);
    map.serialize(byteBuffer);
    byteBuffer.flip();
    Roaring64Bitmap anotherDeserMap = newDefaultCtor();
    anotherDeserMap.deserialize(byteBuffer);
    assertEquals(3, anotherDeserMap.getLongCardinality());
    assertEquals(123, anotherDeserMap.select(0));
    assertEquals(Long.MAX_VALUE, anotherDeserMap.select(1));
    assertEquals(-123, anotherDeserMap.select(2));
  }

  @Test
  public void testOrSameBucket() {
    Roaring64Bitmap left = newDefaultCtor();
    Roaring64Bitmap right = newDefaultCtor();

    left.addLong(123);
    right.addLong(234);

    Roaring64Bitmap orNotInPlace = Roaring64Bitmap.or(left, right);
    left.or(right);

    assertEquals(2, left.getLongCardinality());

    assertEquals(123, left.select(0));
    assertEquals(234, left.select(1));

    assertEquals(2, orNotInPlace.getLongCardinality());

    assertEquals(123, orNotInPlace.select(0));
    assertEquals(234, orNotInPlace.select(1));
  }

  @Test
  public void testOrMultipleBuckets() {
    Roaring64Bitmap left = newDefaultCtor();
    Roaring64Bitmap right = newDefaultCtor();

    left.addLong(123);
    left.addLong(Long.MAX_VALUE);
    right.addLong(234);

    Roaring64Bitmap orNotInPlace = Roaring64Bitmap.or(left, right);
    left.or(right);

    assertEquals(3, left.getLongCardinality());

    assertEquals(123, left.select(0));
    assertEquals(234, left.select(1));
    assertEquals(Long.MAX_VALUE, left.select(2));

    assertEquals(3, orNotInPlace.getLongCardinality());

    assertEquals(123, orNotInPlace.select(0));
    assertEquals(234, orNotInPlace.select(1));
    assertEquals(Long.MAX_VALUE, orNotInPlace.select(2));
  }

  @Test
  public void testOrDifferentBucket() {
    Roaring64Bitmap left = newDefaultCtor();
    Roaring64Bitmap right = newDefaultCtor();

    left.addLong(123);
    right.addLong(Long.MAX_VALUE / 2);

    Roaring64Bitmap orNotInPlace = Roaring64Bitmap.or(left, right);
    left.or(right);

    assertEquals(2, left.getLongCardinality());

    assertEquals(123, left.select(0));
    assertEquals(Long.MAX_VALUE / 2, left.select(1));

    assertEquals(2, orNotInPlace.getLongCardinality());

    assertEquals(123, orNotInPlace.select(0));
    assertEquals(Long.MAX_VALUE / 2, orNotInPlace.select(1));
  }

  @Test
  public void testOrDifferentBucket2() {
    Roaring64Bitmap left = newDefaultCtor();
    Roaring64Bitmap right = newDefaultCtor();

    left.addLong(123);
    right.addLong(Long.MAX_VALUE);

    Roaring64Bitmap orNotInPlace = Roaring64Bitmap.or(left, right);
    left.or(right);

    assertEquals(2, left.getLongCardinality());

    assertEquals(123, left.select(0));
    assertEquals(Long.MAX_VALUE, left.select(1));

    assertEquals(2, orNotInPlace.getLongCardinality());

    assertEquals(123, orNotInPlace.select(0));
    assertEquals(Long.MAX_VALUE, orNotInPlace.select(1));
  }

  @Test
  public void testOrCloneInput() {
    Roaring64Bitmap left = newDefaultCtor();
    Roaring64Bitmap right = newDefaultCtor();

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
  public void testXorBucket() {
    Roaring64Bitmap left = newDefaultCtor();
    Roaring64Bitmap right = newDefaultCtor();

    left.addLong(123);
    left.addLong(234);
    right.addLong(234);
    right.addLong(345);

    // We have 1 shared value: 234
    Roaring64Bitmap xorNotInPlace = Roaring64Bitmap.xor(left, right);
    left.xor(right);

    assertEquals(2, left.getLongCardinality());
    assertEquals(123, left.select(0));
    assertEquals(345, left.select(1));

    assertEquals(2, xorNotInPlace.getLongCardinality());
    assertEquals(123, xorNotInPlace.select(0));
    assertEquals(345, xorNotInPlace.select(1));
  }

  @Test
  public void testXor() {
    Roaring64Bitmap left = newDefaultCtor();
    Roaring64Bitmap right = newDefaultCtor();

    left.addLong(123);
    left.addLong(234);
    right.addLong(234);
    right.addLong(345);

    Roaring64Bitmap xorNotInPlace = Roaring64Bitmap.xor(left, right);
    left.xor(right);

    assertEquals(2, left.getLongCardinality());
    assertEquals(123, left.select(0));
    assertEquals(345, left.select(1));

    assertEquals(2, xorNotInPlace.getLongCardinality());
    assertEquals(123, xorNotInPlace.select(0));
    assertEquals(345, xorNotInPlace.select(1));
  }

  @Test
  public void testXorDifferentBucket() {
    Roaring64Bitmap left = newDefaultCtor();
    Roaring64Bitmap right = newDefaultCtor();

    left.addLong(123);
    right.addLong(Long.MAX_VALUE);

    // We have 1 shared value: 234
    Roaring64Bitmap xorNotInPlace = Roaring64Bitmap.xor(left, right);
    left.xor(right);

    assertEquals(2, left.getLongCardinality());
    assertEquals(123, left.select(0));
    assertEquals(Long.MAX_VALUE, left.select(1));

    assertEquals(2, xorNotInPlace.getLongCardinality());
    assertEquals(123, xorNotInPlace.select(0));
    assertEquals(Long.MAX_VALUE, xorNotInPlace.select(1));
  }

  @Test
  public void testXor_MultipleBucket() {
    Roaring64Bitmap left = newDefaultCtor();
    Roaring64Bitmap right = newDefaultCtor();

    left.addLong(123);
    left.addLong(Long.MAX_VALUE);
    right.addLong(Long.MAX_VALUE);

    // We have 1 shared value: 234
    Roaring64Bitmap xorNotInPlace = Roaring64Bitmap.xor(left, right);
    left.xor(right);

    assertEquals(1, left.getLongCardinality());
    assertEquals(123, left.select(0));

    assertEquals(1, xorNotInPlace.getLongCardinality());
    assertEquals(123, xorNotInPlace.select(0));
  }

  @Test
  public void testAndSingleBucket() {
    Roaring64Bitmap left = newDefaultCtor();
    Roaring64Bitmap right = newDefaultCtor();

    left.addLong(123);
    left.addLong(234);
    right.addLong(234);
    right.addLong(345);

    // We have 1 shared value: 234
    Roaring64Bitmap andNotInPlace = Roaring64Bitmap.and(left, right);
    left.and(right);

    assertEquals(1, left.getLongCardinality());
    assertEquals(234, left.select(0));

    assertEquals(1, andNotInPlace.getLongCardinality());
    assertEquals(234, andNotInPlace.select(0));
  }

  @Test
  public void testAnd() {
    Roaring64Bitmap left = newDefaultCtor();
    Roaring64Bitmap right = newDefaultCtor();

    left.addLong(123);
    right.addLong(123);

    // We have 1 shared value: 234
    Roaring64Bitmap andNotInPlace = Roaring64Bitmap.and(left, right);
    left.and(right);

    assertEquals(1, left.getLongCardinality());
    assertEquals(123, left.select(0));

    assertEquals(1, andNotInPlace.getLongCardinality());
    assertEquals(123, andNotInPlace.select(0));
  }

  @Test
  public void testAndDisjoint() {
    // There are no shared values between these maps.
    final long[] leftData =
        new long[] {1076595327100L, 1074755534972L, 5060192403580L, 5060308664444L};
    final long[] rightData = new long[] {3470563844L};

    Roaring64Bitmap left = Roaring64Bitmap.bitmapOf(leftData);
    Roaring64Bitmap right = Roaring64Bitmap.bitmapOf(rightData);

    Roaring64Bitmap andNotInPlace = Roaring64Bitmap.and(left, right);
    left.and(right);

    Roaring64Bitmap swapLeft = Roaring64Bitmap.bitmapOf(rightData);
    Roaring64Bitmap swapRight = Roaring64Bitmap.bitmapOf(leftData);

    Roaring64Bitmap swapAndNotInPlace = Roaring64Bitmap.and(left, right);
    swapLeft.and(swapRight);

    assertEquals(0, left.getLongCardinality());
    assertEquals(0, swapLeft.getLongCardinality());
    assertThrows(IllegalArgumentException.class, () -> left.select(0));
    assertThrows(IllegalArgumentException.class, () -> swapLeft.select(0));

    assertEquals(0, andNotInPlace.getLongCardinality());
    assertEquals(0, swapAndNotInPlace.getLongCardinality());
    assertThrows(IllegalArgumentException.class, () -> andNotInPlace.select(0));
    assertThrows(IllegalArgumentException.class, () -> swapAndNotInPlace.select(0));
  }

  @Test
  void testToArrayAfterAndOptHasEmptyContainer() {
    Roaring64Bitmap bitmap = new Roaring64Bitmap();
    bitmap.addLong(0);

    Roaring64Bitmap bitmap2 = new Roaring64Bitmap();
    bitmap2.addLong(1);
    // bit and
    Roaring64Bitmap andNotInPlace = Roaring64Bitmap.and(bitmap, bitmap2);
    bitmap.and(bitmap2);
    // to array
    Assertions.assertDoesNotThrow(bitmap::toArray);
    Assertions.assertDoesNotThrow(andNotInPlace::toArray);
  }

  @Test
  public void testAndDifferentBucket() {
    Roaring64Bitmap left = newDefaultCtor();
    Roaring64Bitmap right = newDefaultCtor();

    left.addLong(123);
    right.addLong(Long.MAX_VALUE);

    // We have 1 shared value: 234
    left.and(right);

    assertEquals(0, left.getLongCardinality());
  }

  @Test
  public void testAndMultipleBucket() {
    Roaring64Bitmap left = newDefaultCtor();
    Roaring64Bitmap right = newDefaultCtor();

    left.addLong(123);
    left.addLong(Long.MAX_VALUE);
    right.addLong(Long.MAX_VALUE);

    // We have 1 shared value: 234
    Roaring64Bitmap andNotInPlace = Roaring64Bitmap.and(left, right);
    left.and(right);

    assertEquals(1, left.getLongCardinality());
    assertEquals(Long.MAX_VALUE, left.select(0));

    assertEquals(1, andNotInPlace.getLongCardinality());
    assertEquals(Long.MAX_VALUE, andNotInPlace.select(0));
  }

  @Test
  public void intersecttest() {
    final Roaring64Bitmap rr1 = new Roaring64Bitmap();
    final Roaring64Bitmap rr2 = new Roaring64Bitmap();
    for (int k = 0; k < 40000; ++k) {
      rr1.add(2 * k);
      rr2.add(2 * k + 1);
    }
    assertFalse(Roaring64Bitmap.intersects(rr1, rr2));
    rr1.add(2 * 500 + 1);
    assertTrue(Roaring64Bitmap.intersects(rr1, rr2));
    final Roaring64Bitmap rr3 = new Roaring64Bitmap();
    rr3.add(2 * 501 + 1);
    assertTrue(Roaring64Bitmap.intersects(rr3, rr2));
    assertFalse(Roaring64Bitmap.intersects(rr3, rr1));
    for (int k = 0; k < 40000; ++k) {
      rr1.add(2 * k + 1);
    }
    rr1.runOptimize();
    assertTrue(Roaring64Bitmap.intersects(rr1, rr2));
  }

  @Test
  public void andcounttest() {
    // This is based on andtest
    final Roaring64Bitmap rr = new Roaring64Bitmap();
    for (int k = 0; k < 4000; ++k) {
      rr.add(k);
    }
    rr.add(100000);
    rr.add(110000);
    final Roaring64Bitmap rr2 = new Roaring64Bitmap();
    rr2.add(13);
    final Roaring64Bitmap rrand = Roaring64Bitmap.and(rr, rr2);
    assertEquals(rrand.getLongCardinality(), Roaring64Bitmap.andCardinality(rr, rr2));
    assertEquals(rrand.getLongCardinality(), Roaring64Bitmap.andCardinality(rr2, rr));
    rr.and(rr2);
    assertEquals(rrand.getLongCardinality(), Roaring64Bitmap.andCardinality(rr2, rr));
  }

  @Test
  public void andCounttest3() {
    // This is based on andtest3
    final int[] arrayand = new int[11256];
    int pos = 0;
    final Roaring64Bitmap rr = new Roaring64Bitmap();
    for (int k = 4000; k < 4256; ++k) {
      rr.add(k);
    }
    for (int k = 65536; k < 65536 + 4000; ++k) {
      rr.add(k);
    }
    for (int k = 3 * 65536; k < 3 * 65536 + 1000; ++k) {
      rr.add(k);
    }
    for (int k = 3 * 65536 + 1000; k < 3 * 65536 + 7000; ++k) {
      rr.add(k);
    }
    for (int k = 3 * 65536 + 7000; k < 3 * 65536 + 9000; ++k) {
      rr.add(k);
    }
    for (int k = 4 * 65536; k < 4 * 65536 + 7000; ++k) {
      rr.add(k);
    }
    for (int k = 6 * 65536; k < 6 * 65536 + 10000; ++k) {
      rr.add(k);
    }
    for (int k = 8 * 65536; k < 8 * 65536 + 1000; ++k) {
      rr.add(k);
    }
    for (int k = 9 * 65536; k < 9 * 65536 + 30000; ++k) {
      rr.add(k);
    }
    final Roaring64Bitmap rr2 = new Roaring64Bitmap();
    for (int k = 4000; k < 4256; ++k) {
      rr2.add(k);
      arrayand[pos++] = k;
    }
    for (int k = 65536; k < 65536 + 4000; ++k) {
      rr2.add(k);
      arrayand[pos++] = k;
    }
    for (int k = 3 * 65536 + 1000; k < 3 * 65536 + 7000; ++k) {
      rr2.add(k);
      arrayand[pos++] = k;
    }
    for (int k = 6 * 65536; k < 6 * 65536 + 1000; ++k) {
      rr2.add(k);
      arrayand[pos++] = k;
    }
    for (int k = 7 * 65536; k < 7 * 65536 + 1000; ++k) {
      rr2.add(k);
    }
    for (int k = 10 * 65536; k < 10 * 65536 + 5000; ++k) {
      rr2.add(k);
    }

    final Roaring64Bitmap rrand = Roaring64Bitmap.and(rr, rr2);
    final long rrandCount = Roaring64Bitmap.andCardinality(rr, rr2);

    assertEquals(rrand.getLongCardinality(), rrandCount);
  }

  @Test
  public void testAndNotSingleBucket() {
    Roaring64Bitmap left = newDefaultCtor();
    Roaring64Bitmap right = newDefaultCtor();

    left.addLong(123);
    left.addLong(234);
    right.addLong(234);
    right.addLong(345);

    // We have 1 shared value: 234
    Roaring64Bitmap andNotNotInPlace = Roaring64Bitmap.andNot(left, right);
    left.andNot(right);

    assertEquals(1, left.getLongCardinality());
    assertEquals(123, left.select(0));

    assertEquals(1, andNotNotInPlace.getLongCardinality());
    assertEquals(123, andNotNotInPlace.select(0));
  }

  @Test
  public void testAndNot() {
    Roaring64Bitmap left = newDefaultCtor();
    Roaring64Bitmap right = newDefaultCtor();

    left.addLong(123);
    right.addLong(234);

    // We have 1 shared value: 234
    Roaring64Bitmap andNotNotInPlace = Roaring64Bitmap.andNot(left, right);
    left.andNot(right);

    assertEquals(1, left.getLongCardinality());
    assertEquals(123, left.select(0));

    assertEquals(1, andNotNotInPlace.getLongCardinality());
    assertEquals(123, andNotNotInPlace.select(0));
  }

  @Test
  public void testAndNotDifferentBucket() {
    Roaring64Bitmap left = newDefaultCtor();
    Roaring64Bitmap right = newDefaultCtor();

    left.addLong(123);
    right.addLong(Long.MAX_VALUE);

    // We have 1 shared value: 234
    Roaring64Bitmap andNotNotInPlace = Roaring64Bitmap.andNot(left, right);
    left.andNot(right);

    assertEquals(1, left.getLongCardinality());
    assertEquals(123, left.select(0));

    assertEquals(1, andNotNotInPlace.getLongCardinality());
    assertEquals(123, andNotNotInPlace.select(0));
  }

  @Test
  public void testAndNot_MultipleBucket() {
    Roaring64Bitmap left = newDefaultCtor();
    Roaring64Bitmap right = newDefaultCtor();

    left.addLong(123);
    left.addLong(Long.MAX_VALUE);
    right.addLong(Long.MAX_VALUE);

    // We have 1 shared value: 234
    Roaring64Bitmap andNotNotInPlace = Roaring64Bitmap.andNot(left, right);
    left.andNot(right);

    assertEquals(1, left.getLongCardinality());
    assertEquals(123, left.select(0));

    assertEquals(1, andNotNotInPlace.getLongCardinality());
    assertEquals(123, andNotNotInPlace.select(0));
  }

  @Test
  public void testFlipSameContainer() {
    Roaring64Bitmap map = newDefaultCtor();

    map.addLong(0);
    map.flip(1, 2);

    assertEquals(2, map.getLongCardinality());
    assertEquals(1, map.select(1));
  }

  @Test
  public void testFlipMiddleContainer() {
    Roaring64Bitmap map = newDefaultCtor();

    map.addLong(0);
    map.addLong(0x20001);

    map.flip(0x10001, 0x10002);

    assertEquals(3, map.getLongCardinality());
    assertEquals(0x10001, map.select(1));
  }

  @Test
  public void testFlipNextContainer() {
    Roaring64Bitmap map = newDefaultCtor();

    map.addLong(0);
    map.flip(0x10001, 0x10002);

    assertEquals(2, map.getLongCardinality());
    assertEquals(0x10001, map.select(1));
  }

  @Test
  public void testFlipToEdgeContainer() {
    Roaring64Bitmap map = newDefaultCtor();

    map.addLong(0);
    map.flip(0xFFFF, 0x10000);

    assertEquals(2, map.getLongCardinality());
    assertEquals(0xFFFF, map.select(1));
  }

  @Test
  public void testFlipOverEdgeContainer() {
    Roaring64Bitmap map = newDefaultCtor();

    map.addLong(0);
    map.flip(0xFFFF, 0x10002);

    assertEquals(4, map.getLongCardinality());
    assertEquals(0x10001, map.select(3));
  }

  @Test
  public void testFlipPriorContainer() {
    Roaring64Bitmap map = newDefaultCtor();

    map.addLong(0x10001);
    map.flip(1L, 2L);

    assertEquals(2, map.getLongCardinality());
    assertEquals(1, map.select(0));
    assertEquals(0x10001, map.select(1));
  }

  @Test
  public void testFlipSameNonZeroValuesNoChange() {
    Roaring64Bitmap map = newDefaultCtor();

    map.addLong(0);
    map.flip(1L, 1L);

    assertEquals(1, map.getLongCardinality());
    assertEquals(0, map.select(0));
  }

  @Test
  public void testFlipPositiveStartGreaterThanEndNoChange() {
    Roaring64Bitmap map = newDefaultCtor();

    map.addLong(0);
    map.flip(2L, 1L);

    assertEquals(1, map.getLongCardinality());
    assertEquals(0, map.select(0));
  }

  @Test
  public void testFlipNegStartGreaterThanEndNoChange() {
    Roaring64Bitmap map = newDefaultCtor();

    map.addLong(0);
    map.flip(-1L, -3L);

    assertEquals(1, map.getLongCardinality());
    assertEquals(0, map.select(0));
  }

  @Test
  public void testFlipNegStartGreaterThanPosEndNoChange() {
    Roaring64Bitmap map = newDefaultCtor();

    map.addLong(0);
    map.flip(-1L, 0x7FffFFffFFffFFffL);

    assertEquals(1, map.getLongCardinality());
    assertEquals(0, map.select(0));
  }

  @Test
  public void testFlipRangeCrossingFromPosToNegInHexWorks() {
    Roaring64Bitmap map = newDefaultCtor();

    map.addLong(0);
    map.flip(0x7FffFFffFFffFFffL, 0x8000000000000001L);

    assertEquals(3, map.getLongCardinality());
    assertEquals(0L, map.select(0));
    assertEquals(0x7FffFFffFFffFFffL, map.select(1));
    assertEquals(0x8000000000000000L, map.select(2));
  }

  @Test
  public void testFlipRangeCrossingFromPosToNegInDecWorks() {
    Roaring64Bitmap map = newDefaultCtor();

    map.addLong(0);
    map.flip(9223372036854775807L, -9223372036854775807L);

    assertEquals(3, map.getLongCardinality());
    assertEquals(0L, map.select(0));
    assertEquals(9223372036854775807L, map.select(1));
    assertEquals(-9223372036854775808L, map.select(2));
  }

  @Test
  public void testFlipSmallRangesInNegWorks() {
    Roaring64Bitmap map = newDefaultCtor();

    map.addLong(0);
    map.flip(-4294967297L, -4294967296L);

    assertEquals(2, map.getLongCardinality());
    assertEquals(0L, map.select(0));
    assertEquals(-4294967297L, map.select(1));
  }

  @Test
  public void testFlipEdgeOfLongWorks() {
    Roaring64Bitmap map = newDefaultCtor();

    map.addLong(0);
    map.flip(-2L, 0L);

    assertEquals(3, map.getLongCardinality());
    assertEquals(0L, map.select(0));
    assertEquals(-2L, map.select(1));
  }

  @Test
  public void testToString() {
    Roaring64Bitmap map = newDefaultCtor();

    map.addLong(123);
    map.addLong(Long.MAX_VALUE);
    map.addLong(Long.MAX_VALUE + 1L);

    assertEquals("{123,9223372036854775807,-9223372036854775808}", map.toString());
  }

  @Test
  public void testInvalidIntMask() {
    Roaring64Bitmap map = new Roaring64Bitmap();
    int a = 0xFFFFFFFF; // -1 in two's compliment
    map.addInt(a);
    assertEquals(map.getIntCardinality(), 1);
    long addedInt = map.getLongIterator().next();
    assertEquals(0xFFFFFFFFL, addedInt);
  }

  @Test
  public void testAddInvalidRange() {
    Roaring64Bitmap map = new Roaring64Bitmap();
    // Zero edge-case
    assertThrows(IllegalArgumentException.class, () -> map.addRange(0L, 0L));

    // Same higher parts, different lower parts
    assertThrows(IllegalArgumentException.class, () -> map.addRange(1L, 0L));
    assertThrows(IllegalArgumentException.class, () -> map.addRange(-1, -2));

    // Different higher parts
    assertThrows(IllegalArgumentException.class, () -> map.addRange(Long.MAX_VALUE, 0L));
    assertThrows(
        IllegalArgumentException.class, () -> map.addRange(Long.MIN_VALUE, Long.MAX_VALUE));
  }

  @Test
  public void testAddRangeSingleBucket() {
    Roaring64Bitmap map = newDefaultCtor();

    map.addRange(5L, 12L);
    assertEquals(7L, map.getLongCardinality());

    assertEquals(5L, map.select(0));
    assertEquals(11L, map.select(6L));
  }

  // Edge case: the last high is excluded and should not lead to a new bitmap. However, it may be
  // seen only while trying to add for high=1
  @Test
  public void testAddRangeEndExcludingNextBitmapFirstLow() {
    Roaring64Bitmap map = newDefaultCtor();

    long end = toUnsignedLong(-1) + 1;

    map.addRange(end - 2, end);
    assertEquals(2, map.getLongCardinality());

    assertEquals(end - 2, map.select(0));
    assertEquals(end - 1, map.select(1));
  }

  @Test
  public void testAddRangeMultipleBuckets() {
    Roaring64Bitmap map = newDefaultCtor();

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
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          RoaringBitmap map = new RoaringBitmap();

          map.add(0L, outOfRoaringBitmapRange);
        });
  }

  @Test
  public void testCardinalityAboveIntegerMaxValue() {
    Roaring64Bitmap map = newDefaultCtor();

    long outOfSingleRoaring = outOfRoaringBitmapRange - 3;

    // This should fill entirely one bitmap,and add one in the next bitmap
    map.addRange(0, outOfSingleRoaring);
    assertEquals(outOfSingleRoaring, map.getLongCardinality());

    assertEquals(outOfSingleRoaring, map.getLongCardinality());

    assertEquals(0, map.select(0));
    assertEquals(outOfSingleRoaring - 1, map.select(outOfSingleRoaring - 1));
  }

  @Test
  public void testRoaringBitmapSelectAboveIntegerMaxValue() {
    RoaringBitmap map = new RoaringBitmap();

    long maxForRoaringBitmap = toUnsignedLong(-1) + 1;
    map.add(0L, maxForRoaringBitmap);

    assertEquals(maxForRoaringBitmap, map.getLongCardinality());
    assertEquals(-1, map.select(-1));
  }

  @Test
  public void testTrim() {
    Roaring64Bitmap map = newDefaultCtor();

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
    Roaring64Bitmap map = newDefaultCtor();

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
  public void testAutoboxedIteratorCanNotRemove() {
    assertThrows(
        UnsupportedOperationException.class,
        () -> {
          Roaring64Bitmap map = newDefaultCtor();

          map.addLong(123);
          map.addLong(234);

          Iterator<Long> it = map.iterator();

          assertTrue(it.hasNext());

          // Should throw a UnsupportedOperationException
          it.remove();
        });
  }

  @Test
  public void testSelectMultipleBuckets() {
    Roaring64Bitmap map = newDefaultCtor();

    map.addLong(123);
    map.addLong(Long.MAX_VALUE);

    assertEquals(123L, map.select(0));
    assertEquals(Long.MAX_VALUE, map.select(1));
  }

  @Test
  public void testSelectEmpty() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          Roaring64Bitmap map = newDefaultCtor();

          map.select(0);
        });
  }

  @Test
  public void testSelectOutOfBoundsMatchCardinality() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          Roaring64Bitmap map = newDefaultCtor();
          map.addLong(123);
          map.select(1);
        });
  }

  @Test
  public void testSelectOutOfBoundsOtherCardinality() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          Roaring64Bitmap map = newDefaultCtor();
          map.addLong(123);
          map.select(2);
        });
  }

  @Test
  public void testRankMultipleBuckets() {
    Roaring64Bitmap map = newDefaultCtor();

    map.addLong(123);
    map.addLong(Long.MAX_VALUE);

    assertEquals(0, map.rankLong(0));
    assertEquals(1, map.rankLong(123));
    assertEquals(1, map.rankLong(Long.MAX_VALUE - 1));
    assertEquals(2, map.rankLong(Long.MAX_VALUE));
  }

  @Test
  public void testRankHighNotPresent() {
    Roaring64Bitmap map = newDefaultCtor();
    map.addLong(123);
    map.addLong(Long.MAX_VALUE);

    assertEquals(1, map.rankLong(Long.MAX_VALUE / 2L));
  }

  @Test
  public void testRunOptimize() {
    Roaring64Bitmap map = new Roaring64Bitmap();
    map.addLong(123);
    map.addLong(234);
    map.runOptimize();
  }

  @Test
  public void testFlipBackward() {
    final Roaring64Bitmap r = newDefaultCtor();
    final long value = 1L;
    r.addLong(value);
    assertEquals(1, r.getLongCardinality());
    r.flip(1);
    assertEquals(0, r.getLongCardinality());
  }

  @Test
  public void testFlip() {
    Roaring64Bitmap map = newDefaultCtor();

    map.addLong(0);
    map.flip(0);

    assertFalse(map.contains(0));
    assertTrue(map.getLongCardinality() == 0);
  }

  // Ensure the ordering behavior with default constructors is the same between RoaringBitmap and
  // Roaring64Bitmap. Typically ensures longs are managed as unsigned longs
  @Test
  public void testDefaultBehaviorLikeRoaring() {
    Roaring64Bitmap longBitmap = newDefaultCtor();
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
      bitmapAsLongArray[i] = toUnsignedLong(bitmapAsIntArray[i]);
    }
  }

  @Test
  public void testRandomAddRemove() {
    Random r = new Random(1234);

    // We need to max the considered range of longs, else each long would be in a different bucket
    long max = Integer.MAX_VALUE * 20L;

    long targetCardinality = 1000;

    Roaring64Bitmap map = newDefaultCtor();

    // Add a lot of items
    while (map.getIntCardinality() < targetCardinality) {
      long v = r.nextLong() % max;
      map.addLong(v);
    }
    // Remove them by chunks
    int chunks = 10;
    for (int j = 0; j < chunks; j++) {
      long chunksSize = targetCardinality / chunks;
      for (int i = 0; i < chunksSize; i++) {
        long v = map.select(r.nextInt(map.getIntCardinality()));
        assertTrue(map.contains(v));
        map.removeLong(v);
        assertFalse(map.contains(v));
      }
      assertEquals(targetCardinality - chunksSize * (j + 1), map.getIntCardinality());
    }
    assertTrue(map.isEmpty());
  }

  @Test
  public void testSerializationSizeInBytes() throws IOException, ClassNotFoundException {
    final Roaring64Bitmap map = newDefaultCtor();
    map.addLong(123);
    map.addLong(Long.MAX_VALUE);

    TestRoaring64NavigableMap.checkSerializeBytes(map);
  }

  @Test
  public void testHashCodeEquals() {
    Roaring64Bitmap left = newDefaultCtor();

    left.addLong(123);
    left.addLong(Long.MAX_VALUE);

    Roaring64Bitmap right = Roaring64Bitmap.bitmapOf(123, Long.MAX_VALUE);

    assertEquals(left.hashCode(), right.hashCode());
    assertEquals(left, right);
    assertEquals(right, left);
  }

  @Test
  public void testIssue428() {
    long input = 1353768194141061120L;

    long[] compare =
        new long[] {
          5192650370358181888L,
          5193776270265024512L,
          5194532734264934400L,
          5194544828892839936L,
          5194545653526560768L,
          5194545688960040960L,
          5194545692181266432L,
          5194545705066168320L,
          5194545722246037504L,
          5194545928404467712L,
          5194550326450978816L,
          5194620695195156480L,
          5206161169240293376L
        };

    Roaring64Bitmap inputRB = new Roaring64Bitmap();
    inputRB.add(input);

    Roaring64Bitmap compareRB = new Roaring64Bitmap();
    compareRB.add(compare);
    compareRB.and(inputRB);
    assertEquals(0, compareRB.getIntCardinality());

    compareRB = new Roaring64Bitmap();
    compareRB.add(compare);
    compareRB.or(inputRB);
    assertEquals(14, compareRB.getIntCardinality());

    compareRB = new Roaring64Bitmap();
    compareRB.add(compare);
    compareRB.andNot(inputRB);
    assertEquals(13, compareRB.getIntCardinality());
  }

  @Test
  public void shouldNotThrowNPE() {

    long[] inputs = new long[] {5183829215128059904L};
    long[] crossers =
        new long[] {
          4413527634823086080L,
          4418031234450456576L,
          4421408934170984448L,
          4421690409147695104L,
          4421479302915162112L,
          4421426526357028864L,
          4421413332217495552L,
          4421416630752378880L,
          4421416905630285824L,
          4421417111788716032L,
          4421417128968585216L,
          4421417133263552512L,
          4421417134337294336L
        };

    Roaring64Bitmap refRB = new Roaring64Bitmap();
    refRB.add(inputs);
    Roaring64Bitmap crossRB = new Roaring64Bitmap();
    crossRB.add(crossers);
    crossRB.and(refRB);
    assertEquals(0, crossRB.getIntCardinality());
  }

  @Test
  public void shouldNotThrowAIOOB() {
    long[] inputs = new long[] {5183829215128059904L};
    long[] crossers =
        new long[] {
          4413527634823086080L,
          4418031234450456576L,
          4421408934170984448L,
          4421127459194273792L,
          4420916352961740800L,
          4420863576403607552L,
          4420850382264074240L,
          4420847083729190912L,
          4420847358607097856L,
          4420847564765528064L,
          4420847616305135616L,
          4420847620600102912L,
          4420847623821328384L
        };
    Roaring64Bitmap referenceRB = new Roaring64Bitmap();
    referenceRB.add(inputs);
    Roaring64Bitmap crossRB = new Roaring64Bitmap();
    crossRB.add(crossers);
    crossRB.and(referenceRB);
    assertEquals(0, crossRB.getIntCardinality());
  }

  @Test
  public void shouldNotThrowIAE() {

    long[] inputs = new long[] {5183829215128059904L};
    long[] crossers = new long[] {4421416447812311717L, 4420658333523655893L, 4420658332008999025L};

    Roaring64Bitmap referenceRB = new Roaring64Bitmap();
    referenceRB.add(inputs);
    Roaring64Bitmap crossRB = new Roaring64Bitmap();
    crossRB.add(crossers);
    crossRB.and(referenceRB);
    assertEquals(0, crossRB.getIntCardinality());
  }

  @Test
  public void testSkips() {
    final Random source = new Random(0xcb000a2b9b5bdfb6l);
    final long[] data = takeSortedAndDistinct(source, 45000);
    Roaring64Bitmap bitmap = Roaring64Bitmap.bitmapOf(data);
    PeekableLongIterator pii = bitmap.getLongIterator();
    for (int i = 0; i < data.length; ++i) {
      pii.advanceIfNeeded(data[i]);
      assertEquals(data[i], pii.peekNext());
    }
    pii = bitmap.getLongIterator();
    for (int i = 0; i < data.length; ++i) {
      pii.advanceIfNeeded(data[i]);
      assertEquals(data[i], pii.next());
    }
    pii = bitmap.getLongIterator();
    for (int i = 1; i < data.length; ++i) {
      pii.advanceIfNeeded(data[i - 1]);
      assertEquals(data[i - 1], pii.next());
      assertEquals(data[i], pii.peekNext());
    }
    bitmap.getLongIterator().advanceIfNeeded(-1); // should not crash
    bitmap.getLongIteratorFrom(-1); // should not crash
  }

  @Test
  public void testSkipsDense() {
    Roaring64Bitmap bitmap = new Roaring64Bitmap();
    int n = 100000;
    for (long i = 0; i < n; ++i) {
      bitmap.add(2 * i + Integer.MAX_VALUE);
    }

    // use advance
    for (long i = 0; i < n; ++i) {
      PeekableLongIterator pii = bitmap.getLongIterator();
      long expected = 2 * i + Integer.MAX_VALUE;
      pii.advanceIfNeeded(expected);
      assertEquals(expected, pii.peekNext());
      assertEquals(expected, pii.next());
    }

    // use iterator from
    for (long i = 0; i < n; ++i) {
      long expected = 2 * i + Integer.MAX_VALUE;
      PeekableLongIterator pii = bitmap.getLongIteratorFrom(expected);
      assertEquals(expected, pii.peekNext());
      assertEquals(expected, pii.next());
    }
  }

  @Test
  public void testSkipsMultipleHighPoints() {
    Roaring64Bitmap bitmap = new Roaring64Bitmap();

    int n = 100000;
    int numHighPoints = 10;
    for (long h = 0; h < numHighPoints; ++h) {
      long base = h << 16;
      for (long i = 0; i < n; ++i) {
        bitmap.add(2 * i + base);
      }
    }
    for (long h = 0; h < numHighPoints; ++h) {
      long base = h << 16;

      // use advance
      for (long i = 0; i < n; ++i) {
        PeekableLongIterator pii = bitmap.getLongIterator();
        long expected = 2 * i + base;
        pii.advanceIfNeeded(expected);
        assertEquals(expected, pii.peekNext());
        assertEquals(expected, pii.next());
      }

      // use iterator from
      for (long i = 0; i < n; ++i) {
        long expected = 2 * i + base;
        PeekableLongIterator pii = bitmap.getLongIteratorFrom(expected);
        assertEquals(expected, pii.peekNext());
        assertEquals(expected, pii.next());
      }
    }
  }

  @Test
  public void testSkipsRun() {
    Roaring64Bitmap bitmap = new Roaring64Bitmap();
    bitmap.addRange(4L, 100000L);
    bitmap.runOptimize();
    // use advance
    for (int i = 4; i < 100000; ++i) {
      PeekableLongIterator pii = bitmap.getLongIterator();
      pii.advanceIfNeeded(i);
      assertEquals(i, pii.peekNext());
      assertEquals(i, pii.next());
    }
    // use iterator from
    for (int i = 4; i < 100000; ++i) {
      PeekableLongIterator pii = bitmap.getLongIteratorFrom(i);
      assertEquals(i, pii.peekNext());
      assertEquals(i, pii.next());
    }
  }

  @Test
  public void testEmptySkips() {
    Roaring64Bitmap bitmap = new Roaring64Bitmap();
    PeekableLongIterator it = bitmap.getLongIterator();
    it.advanceIfNeeded(0);

    bitmap.getLongIteratorFrom(0);
  }

  @Test
  public void testSkipsReverse() {
    final Random source = new Random(0xcb000a2b9b5bdfb6l);
    final long[] data = takeSortedAndDistinct(source, 45000);
    Roaring64Bitmap bitmap = Roaring64Bitmap.bitmapOf(data);
    PeekableLongIterator pii = bitmap.getReverseLongIterator();
    for (int i = data.length - 1; i >= 0; --i) {
      pii.advanceIfNeeded(data[i]);
      assertEquals(data[i], pii.peekNext());
    }
    pii = bitmap.getReverseLongIterator();
    for (int i = data.length - 1; i >= 0; --i) {
      pii.advanceIfNeeded(data[i]);
      assertEquals(data[i], pii.next());
    }
    pii = bitmap.getReverseLongIterator();
    for (int i = data.length - 2; i >= 0; --i) {
      pii.advanceIfNeeded(data[i + 1]);
      pii.next();
      assertEquals(data[i], pii.peekNext());
    }
    bitmap.getReverseLongIterator().advanceIfNeeded(-1); // should not crash
    bitmap.getReverseLongIteratorFrom(-1); // should not crash
  }

  @Test
  public void testSkipsDenseReverse() {
    Roaring64Bitmap bitmap = new Roaring64Bitmap();
    int n = 100000;
    for (long i = 0; i < n; ++i) {
      bitmap.add(2 * i + Integer.MAX_VALUE);
    }
    // use advance
    for (long i = n - 1; i >= 0; --i) {
      long expected = 2 * i + Integer.MAX_VALUE;
      PeekableLongIterator pii = bitmap.getReverseLongIterator();
      pii.advanceIfNeeded(expected);
      assertEquals(expected, pii.peekNext());
      assertEquals(expected, pii.next());
    }

    // use iterator from
    for (long i = n - 1; i >= 0; --i) {
      long expected = 2 * i + Integer.MAX_VALUE;
      PeekableLongIterator pii = bitmap.getReverseLongIteratorFrom(expected);
      assertEquals(expected, pii.peekNext());
      assertEquals(expected, pii.next());
    }
  }

  @Test
  public void testSkipsMultipleHighPointsReverse() {
    Roaring64Bitmap bitmap = new Roaring64Bitmap();

    int n = 100000;
    int numHighPoints = 10;
    for (long h = 0; h < numHighPoints; ++h) {
      long base = h << 16;
      for (long i = 0; i < n; ++i) {
        bitmap.add(2 * i + base);
      }
    }
    for (long h = 0; h < numHighPoints; ++h) {
      long base = h << 16;

      // use advance
      for (long i = n - 1; i >= 0; --i) {
        PeekableLongIterator pii = bitmap.getReverseLongIterator();
        long expected = 2 * i + base;
        pii.advanceIfNeeded(expected);
        assertEquals(expected, pii.peekNext());
        assertEquals(expected, pii.next());
      }

      // use iterator from
      for (long i = n - 1; i >= 0; --i) {
        long expected = 2 * i + base;
        PeekableLongIterator pii = bitmap.getReverseLongIteratorFrom(expected);
        assertEquals(expected, pii.peekNext());
        assertEquals(expected, pii.next());
      }
    }
  }

  @Test
  public void testSkipsRunReverse() {
    Roaring64Bitmap bitmap = new Roaring64Bitmap();
    bitmap.addRange(4L, 100000L);
    bitmap.runOptimize();

    // use advance
    for (int i = 99999; i >= 4; --i) {
      PeekableLongIterator pii = bitmap.getReverseLongIterator();
      pii.advanceIfNeeded(i);
      assertEquals(i, pii.peekNext());
      assertEquals(i, pii.next());
    }

    // use iterator from
    for (int i = 99999; i >= 4; --i) {
      PeekableLongIterator pii = bitmap.getReverseLongIteratorFrom(i);
      assertEquals(i, pii.peekNext());
      assertEquals(i, pii.next());
    }
  }

  @Test
  public void testEmptySkipsReverse() {
    Roaring64Bitmap bitmap = new Roaring64Bitmap();
    PeekableLongIterator it = bitmap.getReverseLongIterator();
    it.advanceIfNeeded(0);

    bitmap.getReverseLongIteratorFrom(0);
  }

  @Test
  public void testSkipIntoGaps() {
    Roaring64Bitmap bitset = new Roaring64Bitmap();
    long b1 = 2000000000L;
    long b1s = 18500L;
    long b1e = b1 + b1s;
    long p2 = b1 + (b1s / 2);
    long pgap = p2 + b1s;
    long b2 = 4000000000L;
    long b2s = 100L;
    long b2e = b2 + b2s;

    bitset.addRange(b1, b1e);
    bitset.addRange(b2, b2e);

    PeekableLongIterator bitIt = bitset.getLongIterator();

    assertEquals(b1, bitIt.peekNext());
    assertEquals(b1, bitIt.next());

    assertTrue(bitset.contains(p2));
    bitIt.advanceIfNeeded(p2);
    assertEquals(p2, bitIt.peekNext());
    assertEquals(p2, bitIt.next());

    // advancing to a value not in either range should go to the first value of second range
    assertFalse(bitset.contains(pgap));
    bitIt.advanceIfNeeded(pgap);

    assertTrue(bitset.contains(b2));
    assertTrue(bitset.contains(b2e - 1L));
    assertEquals(b2, bitIt.peekNext());

    assertTrue(bitset.contains(b2));
    bitIt.advanceIfNeeded(b2);
    assertEquals(b2, bitIt.peekNext());
    assertEquals(b2, bitIt.next());
  }

  @Test
  public void testSkipIntoFarAwayGaps() {
    Roaring64Bitmap bitset = new Roaring64Bitmap();
    // long runLength = 18500L;
    long runLength = 4 << 20; // ~ 4mio
    long b1 = 2000000000L;
    long b1e = b1 + runLength;
    long p2 = b1 + (runLength / 2);
    long b2 = 4000000000L;
    long b2e = b2 + runLength;
    long p3 = b2 + (runLength / 2);
    long pgapSameContainer = p3 + runLength;
    long pgapNextContainer = p3 + 5 * runLength;
    long b3 = 6000000000L;
    long b3e = b3 + runLength;

    bitset.addRange(b1, b1e);
    bitset.addRange(b2, b2e);
    bitset.addRange(b3, b3e);

    PeekableLongIterator bitIt = bitset.getLongIterator();

    assertEquals(b1, bitIt.peekNext());
    assertEquals(b1, bitIt.next());

    assertTrue(bitset.contains(p2));
    bitIt.advanceIfNeeded(p2);
    assertEquals(p2, bitIt.peekNext());
    assertEquals(p2, bitIt.next());

    // advancing to a value not in any range but beyond second range
    // should go to the first value of third range
    assertFalse(bitset.contains(pgapSameContainer));
    bitIt.advanceIfNeeded(pgapSameContainer);

    assertTrue(bitset.contains(b3));
    assertTrue(bitset.contains(b3e - 1L));
    assertEquals(b3, bitIt.peekNext());

    assertTrue(bitset.contains(b3));
    bitIt.advanceIfNeeded(b3);
    assertEquals(b3, bitIt.peekNext());
    assertEquals(b3, bitIt.next());

    // reset
    bitIt = bitset.getLongIterator();
    bitIt.advanceIfNeeded(p2);

    // advancing to a value not in any range but beyond second range
    // should go to the first value of third range
    assertFalse(bitset.contains(pgapNextContainer));
    bitIt.advanceIfNeeded(pgapNextContainer);

    assertEquals(b3, bitIt.peekNext());

    bitIt.advanceIfNeeded(b3);
    assertEquals(b3, bitIt.peekNext());
    assertEquals(b3, bitIt.next());
  }

  @Test
  public void testSkipIntoGapsReverse() {
    Roaring64Bitmap bitset = new Roaring64Bitmap();
    long b1 = 2000000000L;
    long b1s = 18500L;
    long b1e = b1 + b1s;
    long b2 = 4000000000L;
    long b2s = 100L;
    long b2e = b2 + b2s;
    long p2 = b2 + (b2s / 2);
    long pgap = p2 - b1s;

    bitset.addRange(b1, b1e);
    bitset.addRange(b2, b2e);

    PeekableLongIterator bitIt = bitset.getReverseLongIterator();

    assertEquals(b2e - 1L, bitIt.peekNext());
    assertEquals(b2e - 1L, bitIt.next());

    assertTrue(bitset.contains(p2));
    bitIt.advanceIfNeeded(p2);
    assertEquals(p2, bitIt.peekNext());
    assertEquals(p2, bitIt.next());

    // advancing to a value not in either range should go to the first value of second range
    assertFalse(bitset.contains(pgap));
    bitIt.advanceIfNeeded(pgap);

    assertTrue(bitset.contains(b1));
    assertTrue(bitset.contains(b1e - 1L));
    assertEquals(b1e - 1L, bitIt.peekNext());

    assertTrue(bitset.contains(b2));
    bitIt.advanceIfNeeded(b1e);
    assertEquals(b1e - 1L, bitIt.peekNext());
    assertEquals(b1e - 1L, bitIt.next());
  }

  @Test
  public void testSkipIntoFarAwayGapsReverse() {
    Roaring64Bitmap bitset = new Roaring64Bitmap();
    // long runLength = 18500L;
    long runLength = 4 << 20; // ~ 4mio
    long b1 = 2000000000L;
    long b1e = b1 + runLength;
    long b2 = 4000000000L;
    long b2e = b2 + runLength;
    long p3 = b2 + (runLength / 2);
    long pgapSameContainer = p3 - runLength;
    long pgapNextContainer = p3 - 5 * runLength;
    long b3 = 6000000000L;
    long b3e = b3 + runLength;

    bitset.addRange(b1, b1e);
    bitset.addRange(b2, b2e);
    bitset.addRange(b3, b3e);

    PeekableLongIterator bitIt = bitset.getReverseLongIterator();

    assertEquals(b3e - 1L, bitIt.peekNext());
    assertEquals(b3e - 1L, bitIt.next());

    assertTrue(bitset.contains(p3));
    bitIt.advanceIfNeeded(p3);
    assertEquals(p3, bitIt.peekNext());
    assertEquals(p3, bitIt.next());

    // advancing to a value not in any range but beyond second range
    // should go to the first value of third range
    assertFalse(bitset.contains(pgapSameContainer));
    bitIt.advanceIfNeeded(pgapSameContainer);

    assertTrue(bitset.contains(b1));
    assertTrue(bitset.contains(b1e - 1L));
    assertEquals(b1e - 1L, bitIt.peekNext());

    assertTrue(bitset.contains(b1));
    bitIt.advanceIfNeeded(b1e);
    assertEquals(b1e - 1L, bitIt.peekNext());
    assertEquals(b1e - 1L, bitIt.next());

    // reset
    bitIt = bitset.getReverseLongIterator();
    bitIt.advanceIfNeeded(p3);

    // advancing to a value not in any range but beyond second range
    // should go to the first value of third range
    assertFalse(bitset.contains(pgapNextContainer));
    bitIt.advanceIfNeeded(pgapNextContainer);

    assertEquals(b1e - 1L, bitIt.peekNext());

    bitIt.advanceIfNeeded(b1e);
    assertEquals(b1e - 1L, bitIt.peekNext());
    assertEquals(b1e - 1L, bitIt.next());
  }

  @Test
  public void testLongTreatedAsUnsignedOnAdvance() {
    Roaring64Bitmap bitset = new Roaring64Bitmap();
    bitset.addRange(Long.MAX_VALUE, Long.MIN_VALUE + 3);

    PeekableLongIterator bitIt = bitset.getLongIterator();

    bitIt.advanceIfNeeded(Long.MAX_VALUE);
    assertEquals(Long.MAX_VALUE, bitIt.peekNext());

    bitIt.advanceIfNeeded(Long.MIN_VALUE + 1);
    assertEquals(Long.MIN_VALUE + 1, bitIt.peekNext());
  }

  @Test
  public void testLongTreatedAsUnsignedOnAdvanceReverse() {
    Roaring64Bitmap bitset = new Roaring64Bitmap();
    bitset.addRange(Long.MAX_VALUE, Long.MIN_VALUE + 3);

    PeekableLongIterator bitIt = bitset.getReverseLongIterator();

    bitIt.advanceIfNeeded(Long.MIN_VALUE + 1);
    assertEquals(Long.MIN_VALUE + 1, bitIt.peekNext());

    bitIt.advanceIfNeeded(Long.MAX_VALUE);
    assertEquals(Long.MAX_VALUE, bitIt.peekNext());
  }

  private static long[] takeSortedAndDistinct(Random source, int count) {
    LinkedHashSet<Long> longs = new LinkedHashSet<>(count);
    for (int size = 0; size < count; size++) {
      long next;
      do {
        next = Math.abs(source.nextLong());
      } while (!longs.add(next));
    }
    long[] unboxed = Longs.toArray(longs);
    Arrays.sort(unboxed);
    return unboxed;
  }

  @Test
  public void leafNodeIteratorPeeking() {
    final Random source = new Random(0xcb000a2b9b5bdfb6l);
    final long[] data = takeSortedAndDistinct(source, 45000);
    Roaring64Bitmap bitmap = Roaring64Bitmap.bitmapOf(data);
    bitmap.runOptimize();

    LeafNodeIterator lni = bitmap.getLeafNodeIterator();
    lni.peekNext();
    while (lni.hasNext()) {
      LeafNode peeked = lni.peekNext();
      LeafNode next = lni.next();
      assertEquals(peeked, next);
    }
    assertThrows(NoSuchElementException.class, () -> lni.peekNext());
  }

  @Test
  public void testForAllInRangeContinuous() {
    Roaring64Bitmap bitmap = new Roaring64Bitmap();
    bitmap.addRange(100L, 10000L);

    ValidationRangeConsumer consumer = ValidationRangeConsumer.validateContinuous(9900, PRESENT);
    bitmap.forAllInRange(100, 9900, consumer);
    assertEquals(9900, consumer.getNumberOfValuesConsumed());

    ValidationRangeConsumer consumer2 = ValidationRangeConsumer.validateContinuous(1000, ABSENT);
    bitmap.forAllInRange(10001, 1000, consumer2);
    assertEquals(1000, consumer2.getNumberOfValuesConsumed());

    ValidationRangeConsumer consumer3 =
        ValidationRangeConsumer.validate(
            new ValidationRangeConsumer.Value[] {ABSENT, ABSENT, PRESENT, PRESENT, PRESENT});
    bitmap.forAllInRange(98, 5, consumer3);
    assertEquals(5, consumer3.getNumberOfValuesConsumed());

    ValidationRangeConsumer consumer4 =
        ValidationRangeConsumer.validate(
            new ValidationRangeConsumer.Value[] {PRESENT, PRESENT, ABSENT, ABSENT, ABSENT});
    bitmap.forAllInRange(9998, 5, consumer4);
    assertEquals(5, consumer4.getNumberOfValuesConsumed());
  }

  @Test
  public void testForAllInRangeDense() {
    Roaring64Bitmap bitmap = new Roaring64Bitmap();
    ValidationRangeConsumer.Value[] expected = new ValidationRangeConsumer.Value[100000];
    Arrays.fill(expected, ABSENT);
    for (int k = 0; k < 100000; k += 3) {
      bitmap.add(k);
      expected[k] = PRESENT;
    }

    ValidationRangeConsumer consumer = ValidationRangeConsumer.validate(expected);
    bitmap.forAllInRange(0, 100000, consumer);
    assertEquals(100000, consumer.getNumberOfValuesConsumed());

    ValidationRangeConsumer.Value[] expectedSubRange = Arrays.copyOfRange(expected, 2500, 6000);
    ValidationRangeConsumer consumer2 = ValidationRangeConsumer.validate(expectedSubRange);
    bitmap.forAllInRange(2500, 3500, consumer2);
    assertEquals(3500, consumer2.getNumberOfValuesConsumed());

    ValidationRangeConsumer consumer3 =
        ValidationRangeConsumer.validate(
            new ValidationRangeConsumer.Value[] {
              expected[99997], expected[99998], expected[99999], ABSENT, ABSENT, ABSENT
            });
    bitmap.forAllInRange(99997, 6, consumer3);
    assertEquals(6, consumer3.getNumberOfValuesConsumed());
  }

  @Test
  public void testForAllInRangeSparse() {
    Roaring64Bitmap bitmap = new Roaring64Bitmap();
    ValidationRangeConsumer.Value[] expected = new ValidationRangeConsumer.Value[100000];
    Arrays.fill(expected, ABSENT);
    for (int k = 0; k < 100000; k += 3000) {
      bitmap.add(k);
      expected[k] = PRESENT;
    }

    ValidationRangeConsumer consumer = ValidationRangeConsumer.validate(expected);
    bitmap.forAllInRange(0, 100000, consumer);
    assertEquals(100000, consumer.getNumberOfValuesConsumed());

    ValidationRangeConsumer.Value[] expectedSubRange = Arrays.copyOfRange(expected, 2500, 6001);
    ValidationRangeConsumer consumer2 = ValidationRangeConsumer.validate(expectedSubRange);
    bitmap.forAllInRange(2500, 3500, consumer2);
    assertEquals(3500, consumer2.getNumberOfValuesConsumed());

    ValidationRangeConsumer consumer3 = ValidationRangeConsumer.ofSize(1000);
    bitmap.forAllInRange(2500, 1000, consumer3);
    consumer3.assertAllAbsentExcept(new int[] {3000 - 2500});
    assertEquals(1000, consumer3.getNumberOfValuesConsumed());
  }

  @Test
  public void testIssue537() {
    Roaring64Bitmap a = Roaring64Bitmap.bitmapOf(275846320L);
    Roaring64Bitmap b = Roaring64Bitmap.bitmapOf(275846320L);
    Roaring64Bitmap c =
        Roaring64Bitmap.bitmapOf(
            275845652L,
            275845746L,
            275846148L,
            275847372L,
            275847380L,
            275847388L,
            275847459L,
            275847528L,
            275847586L,
            275847588L,
            275847600L,
            275847607L,
            275847610L,
            275847613L,
            275847631L,
            275847664L,
            275847672L,
            275847677L,
            275847680L,
            275847742L,
            275847808L,
            275847811L,
            275847824L,
            275847830L,
            275847856L,
            275847861L,
            275847863L,
            275847872L,
            275847896L,
            275847923L,
            275847924L,
            275847975L,
            275847990L,
            275847995L,
            275848003L,
            275848080L,
            275848081L,
            275848084L,
            275848095L,
            275848100L,
            275848120L,
            275848129L,
            275848134L,
            275848163L,
            275848174L,
            275848206L,
            275848218L,
            275848231L,
            275848272L,
            275848281L,
            275848308L,
            275848344L,
            275848376L,
            275848382L,
            275848395L,
            275848400L,
            275848411L,
            275848426L,
            275848445L,
            275848449L,
            275848451L,
            275848454L,
            275848469L);
    c.and(b);
    assertFalse(c.contains(275846320L));
    c.and(a);
    assertFalse(c.contains(275846320L));
  }

  @Test
  public void testIssue558() {
    Roaring64Bitmap rb = new Roaring64Bitmap();
    Random random = new Random(1234);
    for (int i = 0; i < 1000000; i++) {
      rb.addLong(random.nextLong());
      rb.removeLong(random.nextLong());
    }
  }

  @Test
  public void testIssue577Case1() {
    Roaring64Bitmap bitmap = new Roaring64Bitmap();
    bitmap.add(
        45011744312L,
        45008074636L,
        41842920068L,
        41829418930L,
        40860008694L,
        40232297287L,
        40182908832L,
        40171852270L,
        39933922233L,
        39794107638L);
    long maxLong = bitmap.getReverseLongIterator().peekNext();
    assertEquals(maxLong, 45011744312L);

    bitmap.forEachInRange(
        46000000000L, 1000000000, value -> fail("No values in this range, but got: " + value));
  }

  @Test
  public void testIssue577Case2() {
    Roaring64Bitmap bitmap = new Roaring64Bitmap();
    bitmap.add(30385375409L, 30399869293L, 34362979339L, 35541844320L, 36637965094L);

    bitmap.forEachInRange(33000000000L, 1000000000, value -> assertEquals(34362979339L, value));
  }

  @Test
  public void testIssue577Case3() {
    Roaring64Bitmap bitmap = new Roaring64Bitmap();
    bitmap.add(14510802367L, 26338197481L, 32716744974L, 32725817880L, 35679129730L);

    final long[] expected = new long[] {32716744974L, 32725817880L};

    bitmap.forEachInRange(
        32000000000L,
        1000000000,
        new LongConsumer() {

          int offset = 0;

          @Override
          public void accept(long value) {
            assertEquals(expected[offset], value);
            offset++;
          }
        });
  }

  @Test
  public void testWithYourself() {
    Roaring64Bitmap b1 = Roaring64Bitmap.bitmapOf(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    b1.runOptimize();
    b1.or(b1);
    assertTrue(b1.equals(Roaring64Bitmap.bitmapOf(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));
    b1.xor(b1);
    assertTrue(b1.isEmpty());
    b1 = Roaring64Bitmap.bitmapOf(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    b1.and(b1);
    assertTrue(b1.equals(Roaring64Bitmap.bitmapOf(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));
    b1.andNot(b1);
    assertTrue(b1.isEmpty());
  }

  @Test
  public void testIssue580() {
    Roaring64Bitmap rb =
        Roaring64Bitmap.bitmapOf(
            3242766498713841665L,
            3492544636360507394L,
            3418218112527884289L,
            3220956490660966402L,
            3495344165583036418L,
            3495023214002368514L,
            3485108231289675778L);
    LongIterator it = rb.getLongIterator();
    int count = 0;
    while (it.hasNext()) {
      it.next();
      count++;
    }
    assertEquals(count, 7);
  }

  @Test
  public void testAddExtremes() {
    Roaring64Bitmap x = new Roaring64Bitmap();
    x.addLong(0L);
    x.addLong(Long.MAX_VALUE);
    x.addLong(-1L);

    Assertions.assertEquals(3L, x.getLongCardinality());
    Assertions.assertArrayEquals(x.toArray(), new long[] {0, Long.MAX_VALUE, -1L});
  }

  @Test
  public void testRangeAroundLongMax() {
    Roaring64Bitmap x = new Roaring64Bitmap();
    x.addRange(Long.MAX_VALUE - 1L, Long.MAX_VALUE + 3L);

    Assertions.assertEquals(4L, x.getLongCardinality());
    Assertions.assertArrayEquals(
        x.toArray(),
        new long[] {Long.MAX_VALUE - 1L, Long.MAX_VALUE, Long.MIN_VALUE, Long.MIN_VALUE + 1L});
  }

  @Test
  public void testRangeExtremeEnd() {
    Roaring64Bitmap x = newDefaultCtor();
    x.addRange(-3L, -1L);

    Assertions.assertEquals(2L, x.getLongCardinality());
    Assertions.assertArrayEquals(new long[] {-3L, -2L}, x.toArray());
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
    Roaring64Bitmap rb = newDefaultCtor();

    rb.add(2);
    rb.add(4);
    rb.add(8);
    assertEquals(2, rb.first());
    assertEquals(8, rb.last());
  }

  @Test
  public void testFirstLast_64b() {
    Roaring64Bitmap rb = newDefaultCtor();

    rb.add(-128);
    rb.add(-64);
    rb.add(-32);
    assertEquals(-128, rb.first());
    assertEquals(-32, rb.last());
  }

  @Test
  public void testFirstLast_32_64b() {
    Roaring64Bitmap rb = newDefaultCtor();

    rb.add(2);
    rb.add(4);
    rb.add(8);
    rb.add(-128);
    rb.add(-64);
    rb.add(-32);
    assertEquals(2, rb.first());
    assertEquals(-32, rb.last());
  }

  @Test
  public void testFirstLast_AllKindsOfNodeTypes() {
    Roaring64Bitmap rb = newDefaultCtor();
    Set<Long> source = getSourceForAllKindsOfNodeTypes();
    source.forEach(rb::addLong);

    assertEquals(source.stream().min((l, r) -> Long.compareUnsigned(l, r)).get(), rb.first());
    assertEquals(source.stream().max((l, r) -> Long.compareUnsigned(l, r)).get(), rb.last());
  }

  @Test
  public void testIssue619() {
    long[] CLEANER_VALUES = {140664568792144l};
    long[] ADDRESS_SPACE_VALUES = {140662937752432l};
    Roaring64Bitmap addressSpace = new Roaring64Bitmap();
    Roaring64Bitmap cleaner = new Roaring64Bitmap();
    int iteration = 0;
    cleaner.add(CLEANER_VALUES);
    while (true) {
      addressSpace.add(ADDRESS_SPACE_VALUES);
      addressSpace.add(CLEANER_VALUES);
      if (iteration == 33) {
        // This test case can safely break here.
        break;
      }
      addressSpace.andNot(cleaner);
      iteration++;
    }
    assertEquals(2, addressSpace.getIntCardinality());
  }

  @Test
  public void testEmptyRoaring64BitmapClonesWithoutException() {
    assertEquals(new Roaring64Bitmap(), new Roaring64Bitmap().clone());
  }
}
