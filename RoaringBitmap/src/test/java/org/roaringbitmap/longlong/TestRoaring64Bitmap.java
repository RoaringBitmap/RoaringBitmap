package org.roaringbitmap.longlong;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.roaringbitmap.Util.toUnsignedLong;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.Util;

public class TestRoaring64Bitmap {

  private Roaring64Bitmap newDefaultCtor() {
    return new Roaring64Bitmap();
  }

  @Test
  public void test() throws Exception {
    Random random = new Random();
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
    Random random = new Random();
    Roaring64Bitmap roaring64Bitmap = new Roaring64Bitmap();
    Set<Long> source = new HashSet<>();
    int total = 10000;
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
    //test all kind of nodes's serialization/deserialization
    long sizeL = roaring64Bitmap.serializedSizeInBytes();
    if (sizeL > Integer.MAX_VALUE) {
      return;
    }
    int sizeInt = (int) sizeL;
    long select2 = roaring64Bitmap.select(2);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(sizeInt);
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    roaring64Bitmap.serialize(dataOutputStream);
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(
        byteArrayOutputStream.toByteArray());
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
    assertArrayEquals(new long[]{-1L}, map.toArray());
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
    assertArrayEquals(new long[]{123L, 234L}, map.toArray());
  }

  @Test
  public void testAddOneSelect2() {
    assertThrows(IllegalArgumentException.class, () -> {
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
    assertThrows(IllegalStateException.class, () -> {
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
    assertArrayEquals(new long[]{Long.MAX_VALUE}, map.toArray());
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
    map.forEach(new LongConsumer() {

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

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(map);
    }

    final Roaring64Bitmap clone;
    try (ObjectInputStream ois =
        new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
      clone = (Roaring64Bitmap) ois.readObject();
    }

    // Check the test has not simply copied the ref
    assertNotSame(map, clone);
    assertEquals(0, clone.getLongCardinality());
  }

  @Test
  public void testSerialization_ToBigEndianBuffer() throws IOException, ClassNotFoundException {
    final Roaring64Bitmap map = newDefaultCtor();
    map.addLong(123);
    ByteBuffer buffer = ByteBuffer.allocate((int) map.serializedSizeInBytes())
        .order(ByteOrder.BIG_ENDIAN);
    map.serialize(buffer);
    assertEquals(map.serializedSizeInBytes(), buffer.position());
  }

  @Test
  public void testSerialization_OneValue() throws IOException, ClassNotFoundException {
    final Roaring64Bitmap map = newDefaultCtor();
    map.addLong(123);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(map);
    }

    final Roaring64Bitmap clone;
    try (ObjectInputStream ois =
        new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
      clone = (Roaring64Bitmap) ois.readObject();
    }

    // Check the test has not simply copied the ref
    assertNotSame(map, clone);
    assertEquals(1, clone.getLongCardinality());
    assertEquals(123, clone.select(0));
  }


  @Test
  public void testSerialization() throws IOException, ClassNotFoundException {
    final Roaring64Bitmap map = newDefaultCtor();
    map.addLong(123);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(map);
    }

    final Roaring64Bitmap clone;
    try (ObjectInputStream ois =
        new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
      clone = (Roaring64Bitmap) ois.readObject();
    }

    // Check the test has not simply copied the ref
    assertNotSame(map, clone);
    assertEquals(1, clone.getLongCardinality());
    assertEquals(123, clone.select(0));
  }


  @Test
  public void testSerializationMultipleBuckets()
      throws IOException, ClassNotFoundException {
    final Roaring64Bitmap map = newDefaultCtor();
    map.addLong(-123);
    map.addLong(123);
    map.addLong(Long.MAX_VALUE);
    long sizeInByteL = map.serializedSizeInBytes();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(map);
    }

    final Roaring64Bitmap clone;
    try (ObjectInputStream ois =
        new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
      clone = (Roaring64Bitmap) ois.readObject();
    }

    // Check the test has not simply copied the ref
    assertNotSame(map, clone);
    assertEquals(3, clone.getLongCardinality());
    assertEquals(123, clone.select(0));
    assertEquals(Long.MAX_VALUE, clone.select(1));
    assertEquals(-123, clone.select(2));
    int sizeInByteInt = (int) sizeInByteL;
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

    left.or(right);

    assertEquals(2, left.getLongCardinality());

    assertEquals(123, left.select(0));
    assertEquals(234, left.select(1));
  }

  @Test
  public void testOrMultipleBuckets() {
    Roaring64Bitmap left = newDefaultCtor();
    Roaring64Bitmap right = newDefaultCtor();

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
  public void testOrDifferentBucket() {
    Roaring64Bitmap left = newDefaultCtor();
    Roaring64Bitmap right = newDefaultCtor();

    left.addLong(123);
    right.addLong(Long.MAX_VALUE / 2);

    left.or(right);

    assertEquals(2, left.getLongCardinality());

    assertEquals(123, left.select(0));
    assertEquals(Long.MAX_VALUE / 2, left.select(1));
  }


  @Test
  public void testOrDifferentBucket2() {
    Roaring64Bitmap left = newDefaultCtor();
    Roaring64Bitmap right = newDefaultCtor();

    left.addLong(123);
    right.addLong(Long.MAX_VALUE);

    left.or(right);

    assertEquals(2, left.getLongCardinality());

    assertEquals(123, left.select(0));
    assertEquals(Long.MAX_VALUE, left.select(1));
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
    left.xor(right);

    assertEquals(2, left.getLongCardinality());
    assertEquals(123, left.select(0));
    assertEquals(345, left.select(1));
  }

  @Test
  public void testXor() {
    Roaring64Bitmap left = newDefaultCtor();
    Roaring64Bitmap right = newDefaultCtor();

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
  public void testXorDifferentBucket() {
    Roaring64Bitmap left = newDefaultCtor();
    Roaring64Bitmap right = newDefaultCtor();

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
    Roaring64Bitmap left = newDefaultCtor();
    Roaring64Bitmap right = newDefaultCtor();

    left.addLong(123);
    left.addLong(Long.MAX_VALUE);
    right.addLong(Long.MAX_VALUE);

    // We have 1 shared value: 234
    left.xor(right);

    assertEquals(1, left.getLongCardinality());
    assertEquals(123, left.select(0));
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
    left.and(right);

    assertEquals(1, left.getLongCardinality());
    assertEquals(234, left.select(0));
  }

  @Test
  public void testAnd() {
    Roaring64Bitmap left = newDefaultCtor();
    Roaring64Bitmap right = newDefaultCtor();

    left.addLong(123);
    right.addLong(123);

    // We have 1 shared value: 234
    left.and(right);

    assertEquals(1, left.getLongCardinality());
    assertEquals(123, left.select(0));
  }

  @Test
  void testToArrayAfterAndOptHasEmptyContainer() {
    Roaring64Bitmap bitmap = new Roaring64Bitmap();
    bitmap.addLong(0);

    Roaring64Bitmap bitmap2 = new Roaring64Bitmap();
    bitmap2.addLong(1);
    //bit and
    bitmap.and(bitmap2);
    //to array
    Assertions.assertDoesNotThrow(bitmap::toArray);
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
    left.and(right);

    assertEquals(1, left.getLongCardinality());
    assertEquals(Long.MAX_VALUE, left.select(0));
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
    left.andNot(right);

    assertEquals(1, left.getLongCardinality());
    assertEquals(123, left.select(0));
  }

  @Test
  public void testAndNot() {
    Roaring64Bitmap left = newDefaultCtor();
    Roaring64Bitmap right = newDefaultCtor();

    left.addLong(123);
    right.addLong(234);

    // We have 1 shared value: 234
    left.andNot(right);

    assertEquals(1, left.getLongCardinality());
    assertEquals(123, left.select(0));
  }

  @Test
  public void testAndNotDifferentBucket() {
    Roaring64Bitmap left = newDefaultCtor();
    Roaring64Bitmap right = newDefaultCtor();

    left.addLong(123);
    right.addLong(Long.MAX_VALUE);

    // We have 1 shared value: 234
    left.andNot(right);

    assertEquals(1, left.getLongCardinality());
    assertEquals(123, left.select(0));
  }

  @Test
  public void testAndNot_MultipleBucket() {
    Roaring64Bitmap left = newDefaultCtor();
    Roaring64Bitmap right = newDefaultCtor();

    left.addLong(123);
    left.addLong(Long.MAX_VALUE);
    right.addLong(Long.MAX_VALUE);

    // We have 1 shared value: 234
    left.andNot(right);

    assertEquals(1, left.getLongCardinality());
    assertEquals(123, left.select(0));
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
    int a = 0xFFFFFFFF;  // -1 in two's compliment
    map.addInt(a);
    assertEquals(map.getIntCardinality(), 1);
    long addedInt = map.getLongIterator().next();
    assertEquals(0xFFFFFFFFL, addedInt);
  }

  @Test
  public void testAddRangeSingleBucket() {
    Roaring64Bitmap map = newDefaultCtor();

    map.add(5L, 12L);
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

    map.add(end - 2, end);
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
    map.add(from, to);
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
    Roaring64Bitmap map = newDefaultCtor();

    long outOfSingleRoaring = outOfRoaringBitmapRange - 3;

    // This should fill entirely one bitmap,and add one in the next bitmap
    map.add(0, outOfSingleRoaring);
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
    assertThrows(UnsupportedOperationException.class, () -> {
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
    assertThrows(IllegalArgumentException.class, () -> {
      Roaring64Bitmap map = newDefaultCtor();

      map.select(0);
    });
  }


  @Test
  public void testSelectOutOfBoundsMatchCardinality() {
    assertThrows(IllegalArgumentException.class, () -> {
      Roaring64Bitmap map = newDefaultCtor();
      map.addLong(123);
      map.select(1);
    });
  }

  @Test
  public void testSelectOutOfBoundsOtherCardinality() {
    assertThrows(IllegalArgumentException.class, () -> {
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
    Random r = new Random();

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
        map.removeLong(v);
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

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (DataOutputStream oos = new DataOutputStream(baos)) {
      map.serialize(oos);
    }
    assertEquals(baos.toByteArray().length, map.serializedSizeInBytes());
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

    long[] compare = new long[]{
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

    long[] inputs = new long[]{5183829215128059904L};
    long[] crossers = new long[]{4413527634823086080L, 4418031234450456576L, 4421408934170984448L,
        4421690409147695104L, 4421479302915162112L, 4421426526357028864L, 4421413332217495552L,
        4421416630752378880L, 4421416905630285824L, 4421417111788716032L, 4421417128968585216L,
        4421417133263552512L, 4421417134337294336L};

    Roaring64Bitmap refRB = new Roaring64Bitmap();
    refRB.add(inputs);
    Roaring64Bitmap crossRB = new Roaring64Bitmap();
    crossRB.add(crossers);
    crossRB.and(refRB);
    assertEquals(0, crossRB.getIntCardinality());
  }

  @Test
  public void shouldNotThrowAIOOB() {
    long[] inputs = new long[]{5183829215128059904L};
    long[] crossers = new long[]{4413527634823086080L, 4418031234450456576L, 4421408934170984448L,
        4421127459194273792L, 4420916352961740800L, 4420863576403607552L, 4420850382264074240L,
        4420847083729190912L, 4420847358607097856L, 4420847564765528064L, 4420847616305135616L,
        4420847620600102912L, 4420847623821328384L};
    Roaring64Bitmap referenceRB = new Roaring64Bitmap();
    referenceRB.add(inputs);
    Roaring64Bitmap crossRB = new Roaring64Bitmap();
    crossRB.add(crossers);
    crossRB.and(referenceRB);
    assertEquals(0, crossRB.getIntCardinality());
  }

  @Test
  public void shouldNotThrowIAE() {

    long[] inputs = new long[]{5183829215128059904L};
    long[] crossers = new long[]{4421416447812311717L, 4420658333523655893L, 4420658332008999025L};

    Roaring64Bitmap referenceRB = new Roaring64Bitmap();
    referenceRB.add(inputs);
    Roaring64Bitmap crossRB = new Roaring64Bitmap();
    crossRB.add(crossers);
    crossRB.and(referenceRB);
    assertEquals(0, crossRB.getIntCardinality());
  }
}
