package org.roaringbitmap.bsi;

import org.junit.jupiter.api.*;
import org.roaringbitmap.RoaringBitmap;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * RBBsiTest
 * created by haihuang@alibaba-inc.com on 2021/6/6
 */
public class RBBsiTest {
    private Map<Integer, Integer> testDataSet = new HashMap<>();

    private RoaringBitmapSliceIndex bsi;

    @BeforeEach
    public void setup() {
        IntStream.range(1, 100).forEach(x -> testDataSet.put(x, x));
        bsi = new RoaringBitmapSliceIndex(1, 99);
        testDataSet.forEach((k, v) -> {
            bsi.setValue(k, v);
        });
    }

    @Test
    public void testSetAndGet() {
        IntStream.range(1, 100).forEach(x -> {
            Pair<Integer, Boolean> pair = bsi.getValue(x);
            Assertions.assertTrue(pair.getRight());
            Assertions.assertTrue(pair.getKey() == x);
        });

        IntStream.range(1, 100).forEach(x -> {
            Pair<Integer, Boolean> pair = bsi.getValue(x);
            Assertions.assertTrue(pair.getRight());
            Assertions.assertTrue(pair.getKey() == x);
        });
    }

    @Test
    public void testMerge() {
        RoaringBitmapSliceIndex bsiA = new RoaringBitmapSliceIndex();
        IntStream.range(1, 100).forEach(x -> bsiA.setValue(x, x));
        RoaringBitmapSliceIndex bsiB = new RoaringBitmapSliceIndex();
        IntStream.range(100, 199).forEach(x -> bsiB.setValue(x, x));
        Assertions.assertEquals(bsiA.getExistenceBitmap().getLongCardinality(), 99);
        Assertions.assertEquals(bsiB.getExistenceBitmap().getLongCardinality(), 99);
        bsiA.merge(bsiB);
        IntStream.range(1, 199).forEach(x -> {
            Pair<Integer, Boolean> bsiValue = bsiA.getValue(x);
            Assertions.assertTrue(bsiValue.getRight());
            Assertions.assertEquals((int) bsiValue.getKey(), x);
        });
    }


    @Test
    public void testClone() {
        RoaringBitmapSliceIndex bsi = new RoaringBitmapSliceIndex(1, 99);
        List<Pair<Integer, Integer>> collect = testDataSet.entrySet()
                .stream().map(x -> Pair.newPair(x.getKey(), x.getValue())).collect(Collectors.toList());

        bsi.setValues(collect);

        Assertions.assertEquals(bsi.getExistenceBitmap().getLongCardinality(), 99);
        final RoaringBitmapSliceIndex clone = bsi.clone();

        IntStream.range(1, 100).forEach(x -> {
            Pair<Integer, Boolean> bsiValue = clone.getValue(x);
            Assertions.assertTrue(bsiValue.getRight());
            Assertions.assertEquals((int) bsiValue.getKey(), x);
        });
    }


    @Test
    public void testAdd() {
        RoaringBitmapSliceIndex bsiA = new RoaringBitmapSliceIndex();
        IntStream.range(1, 100).forEach(x -> bsiA.setValue(x, x));
        RoaringBitmapSliceIndex bsiB = new RoaringBitmapSliceIndex();
        IntStream.range(1, 120).forEach(x -> bsiB.setValue(x, x));

        bsiA.add(bsiB);

        IntStream.range(1, 120).forEach(x -> {
            Pair<Integer, Boolean> bsiValue = bsiA.getValue(x);
            Assertions.assertTrue(bsiValue.getRight());
            if (x < 100) {
                Assertions.assertEquals((int) bsiValue.getKey(), x * 2);
            } else {
                Assertions.assertEquals((int) bsiValue.getKey(), x);
            }

        });
    }

    @Test
    public void testAddAndEvaluate() {
        RoaringBitmapSliceIndex bsiA = new RoaringBitmapSliceIndex();
        IntStream.range(1, 100).forEach(x -> bsiA.setValue(x, x));
        RoaringBitmapSliceIndex bsiB = new RoaringBitmapSliceIndex();
        IntStream.range(1, 120).forEach(x -> bsiB.setValue(120 - x, x));

        bsiA.add(bsiB);

        RoaringBitmap result = bsiA.compare(BitmapSliceIndex.Operation.EQ, 120, 0, null);
        Assertions.assertTrue(result.getLongCardinality() == 99);
        Assertions.assertArrayEquals(result.toArray(), IntStream.range(1, 100).toArray());

        result = bsiA.compare(BitmapSliceIndex.Operation.RANGE, 1, 20, null);
        Assertions.assertTrue(result.getLongCardinality() == 20);
        Assertions.assertArrayEquals(result.toArray(), IntStream.range(100, 120).toArray());
    }


    @Test
    public void TestIO4Stream() throws IOException {
        RoaringBitmapSliceIndex bsi = new RoaringBitmapSliceIndex(1, 99);
        IntStream.range(1, 100).forEach(x -> bsi.setValue(x, x));
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream bdo = new DataOutputStream(bos);
        bsi.serialize(bdo);
        byte[] data = bos.toByteArray();

        RoaringBitmapSliceIndex newBsi = new RoaringBitmapSliceIndex();

        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        DataInputStream bdi = new DataInputStream(bis);
        newBsi.deserialize(bdi);

        Assertions.assertEquals(newBsi.getExistenceBitmap().getLongCardinality(), 99);

        IntStream.range(1, 100).forEach(x -> {
            Pair<Integer, Boolean> bsiValue = newBsi.getValue(x);
            Assertions.assertTrue(bsiValue.getRight());
            Assertions.assertEquals((int) bsiValue.getKey(), x);
        });
    }

    @Test
    public void testIO4Buffer() throws IOException {
        RoaringBitmapSliceIndex bsi = new RoaringBitmapSliceIndex(1, 99);
        IntStream.range(1, 100).forEach(x -> bsi.setValue(x, x));
        ByteBuffer buffer = ByteBuffer.allocate(bsi.serializedSizeInBytes());
        bsi.serialize(buffer);

        byte[] data = buffer.array();
        RoaringBitmapSliceIndex newBsi = new RoaringBitmapSliceIndex();
        newBsi.deserialize(ByteBuffer.wrap(data));
        Assertions.assertEquals(newBsi.getExistenceBitmap().getLongCardinality(), 99);

        IntStream.range(1, 100).forEach(x -> {
            Pair<Integer, Boolean> bsiValue = newBsi.getValue(x);
            Assertions.assertTrue(bsiValue.getRight());
            Assertions.assertEquals((int) bsiValue.getKey(), x);
        });
    }


    @Test
    public void testIOFromExternal() {
        RoaringBitmapSliceIndex bsi = new RoaringBitmapSliceIndex(1, 99);
        IntStream.range(1, 100).forEach(x -> bsi.setValue(x, x));

        IntStream.range(1, 100).forEach(x -> {
            Pair<Integer, Boolean> bsiValue = bsi.getValue(x);
            Assertions.assertTrue(bsiValue.getRight());
            Assertions.assertEquals((int) bsiValue.getKey(), x);
        });
    }


    @Test
    public void testEQ() {
        RoaringBitmapSliceIndex bsi = new RoaringBitmapSliceIndex(1, 99);
        IntStream.range(1, 100).forEach(x -> {
            if (x <= 50) {
                bsi.setValue(x, 1);
            } else {
                bsi.setValue(x, x);
            }

        });

        RoaringBitmap bitmap = bsi.compare(BitmapSliceIndex.Operation.EQ, 1, 0, null);
        Assertions.assertTrue(bitmap.getLongCardinality() == 50L);

    }

    @Test
    public void testNotEQ() {
        bsi = new RoaringBitmapSliceIndex();
        bsi.setValue(1, 99);
        bsi.setValue(2, 1);
        bsi.setValue(3, 50);

        RoaringBitmap result = bsi.compare(BitmapSliceIndex.Operation.NEQ, 99, 0, null);
        Assertions.assertTrue(result.getLongCardinality() == 2);
        Assertions.assertArrayEquals(new int[]{2, 3}, result.toArray());

        result = bsi.compare(BitmapSliceIndex.Operation.NEQ, 100, 0, null);
        Assertions.assertTrue(result.getLongCardinality() == 3);
        Assertions.assertArrayEquals(new int[]{1, 2, 3}, result.toArray());

        bsi = new RoaringBitmapSliceIndex();
        bsi.setValue(1, 99);
        bsi.setValue(2, 99);
        bsi.setValue(3, 99);

        result = bsi.compare(BitmapSliceIndex.Operation.NEQ, 99, 0, null);
        Assertions.assertTrue(result.isEmpty());

        result = bsi.compare(BitmapSliceIndex.Operation.NEQ, 1, 0, null);
        Assertions.assertTrue(result.getLongCardinality() == 3);
        Assertions.assertArrayEquals(new int[]{1, 2, 3}, result.toArray());
    }


    // parallel operation test

    @Test
    public void testGT() {
        RoaringBitmap result = bsi.compare(BitmapSliceIndex.Operation.GT, 50, 0, null);
        Assertions.assertTrue(result.getLongCardinality() == 49);
        Assertions.assertArrayEquals(IntStream.range(51, 100).toArray(), result.toArray());

        result = bsi.compare(BitmapSliceIndex.Operation.GT, 0, 0, null);
        Assertions.assertTrue(result.getLongCardinality() == 99);
        Assertions.assertArrayEquals(IntStream.range(1, 100).toArray(), result.toArray());

        result = bsi.compare(BitmapSliceIndex.Operation.GT, 99, 0, null);
        Assertions.assertTrue(result.isEmpty());
    }


    @Test
    public void testGE() {
        RoaringBitmap result = bsi.compare(BitmapSliceIndex.Operation.GE, 50, 0, null);
        Assertions.assertTrue(result.getLongCardinality() == 50);
        Assertions.assertArrayEquals(IntStream.range(50, 100).toArray(), result.toArray());

        result = bsi.compare(BitmapSliceIndex.Operation.GE, 1, 0, null);
        Assertions.assertTrue(result.getLongCardinality() == 99);
        Assertions.assertArrayEquals(IntStream.range(1, 100).toArray(), result.toArray());

        result = bsi.compare(BitmapSliceIndex.Operation.GE, 100, 0, null);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testLT() {
        RoaringBitmap result = bsi.compare(BitmapSliceIndex.Operation.LT, 50, 0, null);
        Assertions.assertTrue(result.getLongCardinality() == 49);
        Assertions.assertArrayEquals(IntStream.range(1, 50).toArray(), result.toArray());

        result = bsi.compare(BitmapSliceIndex.Operation.LT, Integer.MAX_VALUE, 0, null);
        Assertions.assertTrue(result.getLongCardinality() == 99);
        Assertions.assertArrayEquals(IntStream.range(1, 100).toArray(), result.toArray());

        result = bsi.compare(BitmapSliceIndex.Operation.LT, 1, 0, null);
        Assertions.assertTrue(result.isEmpty());
    }


    @Test
    public void testLE() {
        RoaringBitmap result = bsi.compare(BitmapSliceIndex.Operation.LE, 50, 0, null);
        Assertions.assertTrue(result.getLongCardinality() == 50);
        Assertions.assertArrayEquals(IntStream.range(1, 51).toArray(), result.toArray());

        result = bsi.compare(BitmapSliceIndex.Operation.LE, Integer.MAX_VALUE, 0, null);
        Assertions.assertTrue(result.getLongCardinality() == 99);
        Assertions.assertArrayEquals(IntStream.range(1, 100).toArray(), result.toArray());

        result = bsi.compare(BitmapSliceIndex.Operation.LE, 0, 0, null);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testRANGE() {
        RoaringBitmap result = bsi.compare(BitmapSliceIndex.Operation.RANGE, 10, 20, null);
        Assertions.assertTrue(result.getLongCardinality() == 11);
        Assertions.assertArrayEquals(IntStream.range(10, 21).toArray(), result.toArray());

        result = bsi.compare(BitmapSliceIndex.Operation.RANGE, 1, 200, null);
        Assertions.assertTrue(result.getLongCardinality() == 99);
        Assertions.assertArrayEquals(IntStream.range(1, 100).toArray(), result.toArray());

        result = bsi.compare(BitmapSliceIndex.Operation.RANGE, 1000, 2000, null);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testSum() {
        RoaringBitmapSliceIndex bsi = new RoaringBitmapSliceIndex(1, 99);
        IntStream.range(1, 100).forEach(x -> bsi.setValue(x, x));

        RoaringBitmap foundSet = RoaringBitmap.bitmapOf(IntStream.range(1, 51).toArray());

        Pair<Long, Long> sumPair = bsi.sum(foundSet);

        System.out.println("sum:" + sumPair.toString());

        int sum = IntStream.range(1, 51).sum();
        long count = IntStream.range(1, 51).count();

        Assertions.assertTrue(sumPair.getLeft().intValue() == sum && sumPair.getRight() == count);
    }

    @Test
    public void testValueZero() {
        bsi = new RoaringBitmapSliceIndex();
        bsi.setValue(0, 0);
        bsi.setValue(1, 0);
        bsi.setValue(2, 1);

        RoaringBitmap result = bsi.compare(BitmapSliceIndex.Operation.EQ, 0, 0, null);
        Assertions.assertTrue(result.getLongCardinality() == 2);
        Assertions.assertArrayEquals(new int[]{0, 1}, result.toArray());

        result = bsi.compare(BitmapSliceIndex.Operation.EQ, 1, 0, null);
        Assertions.assertTrue(result.getLongCardinality() == 1);
        Assertions.assertArrayEquals(new int[]{2}, result.toArray());
    }
}

