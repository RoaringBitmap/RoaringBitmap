package org.roaringbitmap.bsi;

import org.roaringbitmap.bsi.longlong.Roaring64BitmapSliceIndex;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public class R64BSITest {
  private Map<Long, Long> testDataSet = new HashMap<>();

  private Roaring64BitmapSliceIndex bsi;

  @BeforeEach
  public void setup() {
    LongStream.range(1, 100).forEach(x -> testDataSet.put(x, x));
    bsi = new Roaring64BitmapSliceIndex(1, 99);
    testDataSet.forEach(
        (k, v) -> {
          bsi.setValue(k, v);
        });
  }

  @Test
  public void testSetAndGet() {
    LongStream.range(1, 100)
        .forEach(
            x -> {
              Pair<Long, Boolean> pair = bsi.getValue(x);
              Assertions.assertTrue(pair.getRight());
              Assertions.assertEquals((long) pair.getKey(), x);
            });

    IntStream.range(1, 100)
        .forEach(
            x -> {
              Pair<Long, Boolean> pair = bsi.getValue(x);
              Assertions.assertTrue(pair.getRight());
              Assertions.assertEquals((long) pair.getKey(), x);
            });
  }

  @Test
  public void testMerge() {
    Roaring64BitmapSliceIndex bsiA = new Roaring64BitmapSliceIndex();
    IntStream.range(1, 100).forEach(x -> bsiA.setValue(x, x));
    Roaring64BitmapSliceIndex bsiB = new Roaring64BitmapSliceIndex();
    IntStream.range(100, 199).forEach(x -> bsiB.setValue(x, x));
    Assertions.assertEquals(bsiA.getExistenceBitmap().getLongCardinality(), 99);
    Assertions.assertEquals(bsiB.getExistenceBitmap().getLongCardinality(), 99);
    bsiA.merge(bsiB);
    IntStream.range(1, 199)
        .forEach(
            x -> {
              Pair<Long, Boolean> bsiValue = bsiA.getValue(x);
              Assertions.assertTrue(bsiValue.getRight());
              Assertions.assertEquals((long) bsiValue.getKey(), x);
            });
  }

  @Test
  public void testClone() {
    Roaring64BitmapSliceIndex bsi = new Roaring64BitmapSliceIndex(1, 99);
    List<Pair<Long, Long>> collect =
        testDataSet.entrySet().stream()
            .map(x -> Pair.newPair(x.getKey(), x.getValue()))
            .collect(Collectors.toList());

    bsi.setValues(collect);

    Assertions.assertEquals(bsi.getExistenceBitmap().getLongCardinality(), 99);
    final Roaring64BitmapSliceIndex clone = bsi.clone();

    IntStream.range(1, 100)
        .forEach(
            x -> {
              Pair<Long, Boolean> bsiValue = clone.getValue(x);
              Assertions.assertTrue(bsiValue.getRight());
              Assertions.assertEquals((long) bsiValue.getKey(), x);
            });
  }

  @Test
  public void testAdd() {
    Roaring64BitmapSliceIndex bsiA = new Roaring64BitmapSliceIndex();
    LongStream.range(1, 100).forEach(x -> bsiA.setValue(x, x));
    Roaring64BitmapSliceIndex bsiB = new Roaring64BitmapSliceIndex();
    LongStream.range(1, 120).forEach(x -> bsiB.setValue(x, x));

    bsiA.add(bsiB);

    LongStream.range(1, 120)
        .forEach(
            x -> {
              Pair<Long, Boolean> bsiValue = bsiA.getValue(x);
              Assertions.assertTrue(bsiValue.getRight());
              if (x < 100) {
                Assertions.assertEquals((long) bsiValue.getKey(), x * 2);
              } else {
                Assertions.assertEquals((long) bsiValue.getKey(), x);
              }
            });
  }

  @Test
  public void testAddAndEvaluate() {
    Roaring64BitmapSliceIndex bsiA = new Roaring64BitmapSliceIndex();
    IntStream.range(1, 100).forEach(x -> bsiA.setValue(x, x));
    Roaring64BitmapSliceIndex bsiB = new Roaring64BitmapSliceIndex();
    IntStream.range(1, 120).forEach(x -> bsiB.setValue(120 - x, x));

    bsiA.add(bsiB);

    Roaring64Bitmap result = bsiA.compare(BitmapSliceIndex.Operation.EQ, 120, 0, null);
    Assertions.assertEquals(99, result.getLongCardinality());
    Assertions.assertArrayEquals(result.toArray(), LongStream.range(1, 100).toArray());

    result = bsiA.compare(BitmapSliceIndex.Operation.RANGE, 1, 20, null);
    Assertions.assertEquals(20, result.getLongCardinality());
    Assertions.assertArrayEquals(result.toArray(), LongStream.range(100, 120).toArray());
  }

  @Test
  public void TestIO4Stream() throws IOException {
    Roaring64BitmapSliceIndex bsi = new Roaring64BitmapSliceIndex(1, 99);
    LongStream.range(1, 100).forEach(x -> bsi.setValue(x, x));
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream bdo = new DataOutputStream(bos);
    bsi.serialize(bdo);
    byte[] data = bos.toByteArray();

    Roaring64BitmapSliceIndex newBsi = new Roaring64BitmapSliceIndex();

    ByteArrayInputStream bis = new ByteArrayInputStream(data);
    DataInputStream bdi = new DataInputStream(bis);
    newBsi.deserialize(bdi);

    Assertions.assertEquals(newBsi.getExistenceBitmap().getLongCardinality(), 99);

    LongStream.range(1, 100)
        .forEach(
            x -> {
              Pair<Long, Boolean> bsiValue = newBsi.getValue(x);
              Assertions.assertTrue(bsiValue.getRight());
              Assertions.assertEquals((long) bsiValue.getKey(), x);
            });
  }

  @Test
  public void testIO4Buffer() throws IOException {
    Roaring64BitmapSliceIndex bsi = new Roaring64BitmapSliceIndex(1, 99);
    LongStream.range(1, 100).forEach(x -> bsi.setValue(x, x));
    ByteBuffer buffer = ByteBuffer.allocate(bsi.serializedSizeInBytes());
    bsi.serialize(buffer);

    byte[] data = buffer.array();
    Roaring64BitmapSliceIndex newBsi = new Roaring64BitmapSliceIndex();
    newBsi.deserialize(ByteBuffer.wrap(data));
    Assertions.assertEquals(newBsi.getExistenceBitmap().getLongCardinality(), 99);

    LongStream.range(1, 100)
        .forEach(
            x -> {
              Pair<Long, Boolean> bsiValue = newBsi.getValue(x);
              Assertions.assertTrue(bsiValue.getRight());
              Assertions.assertEquals((long) bsiValue.getKey(), x);
            });
  }

  @Test
  public void testIOFromExternal() {
    Roaring64BitmapSliceIndex bsi = new Roaring64BitmapSliceIndex(1, 99);
    LongStream.range(1, 100).forEach(x -> bsi.setValue(x, x));

    LongStream.range(1, 100)
        .forEach(
            x -> {
              Pair<Long, Boolean> bsiValue = bsi.getValue(x);
              Assertions.assertTrue(bsiValue.getRight());
              Assertions.assertEquals((long) bsiValue.getKey(), x);
            });
  }

  @Test
  public void testEQ() {
    Roaring64BitmapSliceIndex bsi = new Roaring64BitmapSliceIndex(1, 99);
    LongStream.range(1, 100)
        .forEach(
            x -> {
              if (x <= 50) {
                bsi.setValue(x, 1);
              } else {
                bsi.setValue(x, x);
              }
            });

    Roaring64Bitmap bitmap = bsi.compare(BitmapSliceIndex.Operation.EQ, 1, 0, null);
    Assertions.assertEquals(50L, bitmap.getLongCardinality());
  }

  @Test
  public void testNotEQ() {
    bsi = new Roaring64BitmapSliceIndex();
    bsi.setValue(1, 99);
    bsi.setValue(2, 1);
    bsi.setValue(3, 50);

    Roaring64Bitmap result = bsi.compare(BitmapSliceIndex.Operation.NEQ, 99, 0, null);
    Assertions.assertEquals(2, result.getLongCardinality());
    Assertions.assertArrayEquals(new long[] {2, 3}, result.toArray());

    result = bsi.compare(BitmapSliceIndex.Operation.NEQ, 100, 0, null);
    Assertions.assertEquals(3, result.getLongCardinality());
    Assertions.assertArrayEquals(new long[] {1, 2, 3}, result.toArray());

    bsi = new Roaring64BitmapSliceIndex();
    bsi.setValue(1, 99);
    bsi.setValue(2, 99);
    bsi.setValue(3, 99);

    result = bsi.compare(BitmapSliceIndex.Operation.NEQ, 99, 0, null);
    Assertions.assertTrue(result.isEmpty());

    result = bsi.compare(BitmapSliceIndex.Operation.NEQ, 1, 0, null);
    Assertions.assertEquals(3, result.getLongCardinality());
    Assertions.assertArrayEquals(new long[] {1, 2, 3}, result.toArray());
  }

  // parallel operation test

  @Test
  public void testGT() {
    Roaring64Bitmap result = bsi.compare(BitmapSliceIndex.Operation.GT, 50, 0, null);
    Assertions.assertEquals(49, result.getLongCardinality());
    Assertions.assertArrayEquals(LongStream.range(51, 100).toArray(), result.toArray());

    result = bsi.compare(BitmapSliceIndex.Operation.GT, 0, 0, null);
    Assertions.assertEquals(99, result.getLongCardinality());
    Assertions.assertArrayEquals(LongStream.range(1, 100).toArray(), result.toArray());

    result = bsi.compare(BitmapSliceIndex.Operation.GT, 99, 0, null);
    Assertions.assertTrue(result.isEmpty());
  }

  @Test
  public void testGE() {
    Roaring64Bitmap result = bsi.compare(BitmapSliceIndex.Operation.GE, 50, 0, null);
    Assertions.assertEquals(50, result.getLongCardinality());
    Assertions.assertArrayEquals(LongStream.range(50, 100).toArray(), result.toArray());

    result = bsi.compare(BitmapSliceIndex.Operation.GE, 1, 0, null);
    Assertions.assertEquals(99, result.getLongCardinality());
    Assertions.assertArrayEquals(LongStream.range(1, 100).toArray(), result.toArray());

    result = bsi.compare(BitmapSliceIndex.Operation.GE, 100, 0, null);
    Assertions.assertTrue(result.isEmpty());
  }

  @Test
  public void testLT() {
    Roaring64Bitmap result = bsi.compare(BitmapSliceIndex.Operation.LT, 50, 0, null);
    Assertions.assertEquals(49, result.getLongCardinality());
    Assertions.assertArrayEquals(LongStream.range(1, 50).toArray(), result.toArray());

    result = bsi.compare(BitmapSliceIndex.Operation.LT, Integer.MAX_VALUE, 0, null);
    Assertions.assertEquals(99, result.getLongCardinality());
    Assertions.assertArrayEquals(LongStream.range(1, 100).toArray(), result.toArray());

    result = bsi.compare(BitmapSliceIndex.Operation.LT, 1, 0, null);
    Assertions.assertTrue(result.isEmpty());
  }

  @Test
  public void testLE() {
    Roaring64Bitmap result = bsi.compare(BitmapSliceIndex.Operation.LE, 50, 0, null);
    Assertions.assertEquals(50, result.getLongCardinality());
    Assertions.assertArrayEquals(LongStream.range(1, 51).toArray(), result.toArray());

    result = bsi.compare(BitmapSliceIndex.Operation.LE, Integer.MAX_VALUE, 0, null);
    Assertions.assertEquals(99, result.getLongCardinality());
    Assertions.assertArrayEquals(LongStream.range(1, 100).toArray(), result.toArray());

    result = bsi.compare(BitmapSliceIndex.Operation.LE, 0, 0, null);
    Assertions.assertTrue(result.isEmpty());
  }

  @Test
  public void testRANGE() {
    Roaring64Bitmap result = bsi.compare(BitmapSliceIndex.Operation.RANGE, 10, 20, null);
    Assertions.assertEquals(11, result.getLongCardinality());
    Assertions.assertArrayEquals(LongStream.range(10, 21).toArray(), result.toArray());

    result = bsi.compare(BitmapSliceIndex.Operation.RANGE, 1, 200, null);
    Assertions.assertEquals(99, result.getLongCardinality());
    Assertions.assertArrayEquals(LongStream.range(1, 100).toArray(), result.toArray());

    result = bsi.compare(BitmapSliceIndex.Operation.RANGE, 1000, 2000, null);
    Assertions.assertTrue(result.isEmpty());
  }

  @Test
  public void testSum() {
    Roaring64BitmapSliceIndex bsi = new Roaring64BitmapSliceIndex(1, 99);
    LongStream.range(1, 100).forEach(x -> bsi.setValue(x, x));

    Roaring64Bitmap foundSet = Roaring64Bitmap.bitmapOf(LongStream.range(1, 51).toArray());

    Pair<Long, Long> sumPair = bsi.sum(foundSet);

    System.out.println("sum:" + sumPair.toString());

    long sum = LongStream.range(1, 51).sum();
    long count = LongStream.range(1, 51).count();

    Assertions.assertTrue(sumPair.getLeft().intValue() == sum && sumPair.getRight() == count);
  }

  @Test
  public void testValueZero() {
    bsi = new Roaring64BitmapSliceIndex();
    bsi.setValue(0, 0);
    bsi.setValue(1, 0);
    bsi.setValue(2, 1);

    Roaring64Bitmap result = bsi.compare(BitmapSliceIndex.Operation.EQ, 0, 0, null);
    Assertions.assertEquals(2, result.getLongCardinality());
    Assertions.assertArrayEquals(new long[] {0, 1}, result.toArray());

    result = bsi.compare(BitmapSliceIndex.Operation.EQ, 1, 0, null);
    Assertions.assertEquals(1, result.getLongCardinality());
    Assertions.assertArrayEquals(new long[] {2}, result.toArray());
  }

  @Test
  public void testTopK() {
    bsi = new Roaring64BitmapSliceIndex();
    bsi.setValue(1, 3);
    bsi.setValue(2, 6);
    bsi.setValue(3, 4);
    bsi.setValue(4, 10);
    bsi.setValue(5, 7);
    Roaring64Bitmap re = bsi.topK(bsi.getExistenceBitmap(), 2);
    Assertions.assertEquals(re, Roaring64Bitmap.bitmapOf(4, 5));
  }

  @Test
  public void testTranspose() {
    bsi = new Roaring64BitmapSliceIndex();
    bsi.setValue(1, 2);
    bsi.setValue(2, 4);
    bsi.setValue(3, 4);
    bsi.setValue(4, 8);
    bsi.setValue(5, 8);
    Roaring64Bitmap re = bsi.transpose(null);
    Assertions.assertEquals(re, Roaring64Bitmap.bitmapOf(2, 4, 8));
  }

  @Test
  public void testTransposeWithCount() {
    bsi = new Roaring64BitmapSliceIndex();
    bsi.setValue(1, 2);
    bsi.setValue(2, 4);
    bsi.setValue(3, 4);
    bsi.setValue(4, 8);
    bsi.setValue(5, 8);
    Roaring64BitmapSliceIndex re = bsi.transposeWithCount(null);
    Assertions.assertEquals(re.getExistenceBitmap(), Roaring64Bitmap.bitmapOf(2, 4, 8));
    Assertions.assertEquals(re.getValue(2).getKey(), 1);
    Assertions.assertEquals(re.getValue(4).getKey(), 2);
    Assertions.assertEquals(re.getValue(8).getKey(), 2);
  }

  @Test
  public void testIssue743() throws IOException {
    Roaring64BitmapSliceIndex bsi = new Roaring64BitmapSliceIndex();
    bsi.setValue(100L, 3L);
    bsi.setValue(1L, 392L);
    System.out.println(bsi.getValue(100L));
    System.out.println(bsi.getValue(1L));

    ByteBuffer buffer = ByteBuffer.allocate(bsi.serializedSizeInBytes());
    bsi.serialize(buffer);

    Roaring64BitmapSliceIndex de_bsi = new Roaring64BitmapSliceIndex();
    de_bsi.deserialize(ByteBuffer.wrap(buffer.array()));
    Assertions.assertEquals(de_bsi.getValue(100L), bsi.getValue(100L));
    Assertions.assertEquals(de_bsi.getValue(1L), bsi.getValue(1L));
  }

  @Test
  public void testIssue753() throws IOException {
    bsi = new Roaring64BitmapSliceIndex();
    LongStream.range(1, 100).forEach(x -> bsi.setValue(x, x));
    Assertions.assertEquals(
        bsi.compare(BitmapSliceIndex.Operation.RANGE, -4, 56, null).getLongCardinality(), 56);
    Assertions.assertEquals(
        bsi.compare(BitmapSliceIndex.Operation.RANGE, -4, 129, null).getLongCardinality(), 99);
    Assertions.assertEquals(
        bsi.compare(BitmapSliceIndex.Operation.RANGE, -4, 200, null).getLongCardinality(), 99);
    Assertions.assertEquals(
        bsi.compare(BitmapSliceIndex.Operation.RANGE, -4, 20000, null).getLongCardinality(), 99);
    Assertions.assertEquals(
        bsi.compare(BitmapSliceIndex.Operation.RANGE, -4, -129, null).getLongCardinality(), 0);
    Assertions.assertEquals(
        bsi.compare(BitmapSliceIndex.Operation.RANGE, -4, -2, null).getLongCardinality(), 0);

    Assertions.assertEquals(
        bsi.compare(BitmapSliceIndex.Operation.RANGE, 4, 56, null).getLongCardinality(), 53);
    Assertions.assertEquals(
        bsi.compare(BitmapSliceIndex.Operation.RANGE, 4, 129, null).getLongCardinality(), 96);
    Assertions.assertEquals(
        bsi.compare(BitmapSliceIndex.Operation.RANGE, 4, 200, null).getLongCardinality(), 96);
    Assertions.assertEquals(
        bsi.compare(BitmapSliceIndex.Operation.RANGE, 4, 20000, null).getLongCardinality(), 96);
    Assertions.assertEquals(
        bsi.compare(BitmapSliceIndex.Operation.RANGE, 4, -129, null).getLongCardinality(), 0);
    Assertions.assertEquals(
        bsi.compare(BitmapSliceIndex.Operation.RANGE, 4, 2, null).getLongCardinality(), 0);

    Assertions.assertEquals(
        bsi.compare(BitmapSliceIndex.Operation.RANGE, -129, -14, null).getLongCardinality(), 0);
    Assertions.assertEquals(
        bsi.compare(BitmapSliceIndex.Operation.RANGE, 129, 2000, null).getLongCardinality(), 0);
  }

  @Test
  public void testIssue755() throws IOException {
    Roaring64BitmapSliceIndex bsi = new Roaring64BitmapSliceIndex();
    bsi.setValue(100L, 3L);
    bsi.setValue(1L, (long) Integer.MAX_VALUE * 2 + 23456);
    bsi.setValue(2L, (long) Integer.MAX_VALUE + 23456);
    Assertions.assertEquals(bsi.getValue(100L).getKey(), 3L); // {{3,true}}
    Assertions.assertEquals(
        bsi.getValue(1L).getKey(), (long) Integer.MAX_VALUE * 2 + 23456); // {23455,true}
    Assertions.assertEquals(
        bsi.getValue(2L).getKey(), (long) Integer.MAX_VALUE + 23456); // {-2147460193,true}
  }
}
