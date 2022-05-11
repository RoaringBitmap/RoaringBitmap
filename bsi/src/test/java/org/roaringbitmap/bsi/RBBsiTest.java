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

        bsi.setValues(collect, 99, 1);

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


    // parallel operation test

    @Test
    public void testGT() {
        RoaringBitmap result = bsi.compare(BitmapSliceIndex.Operation.GT, 50, 0, null);
        Assertions.assertTrue(result.getLongCardinality() == 49);
        Assertions.assertArrayEquals(IntStream.range(51, 100).toArray(), result.toArray());
    }


    @Test
    public void testGE() {
        RoaringBitmap result = bsi.compare(BitmapSliceIndex.Operation.GE, 50, 0, null);
        Assertions.assertTrue(result.getLongCardinality() == 50);
        Assertions.assertArrayEquals(IntStream.range(50, 100).toArray(), result.toArray());
    }

    @Test
    public void testLT() {
        RoaringBitmap result = bsi.compare(BitmapSliceIndex.Operation.LT, 50, 0, null);
        Assertions.assertTrue(result.getLongCardinality() == 49);
        Assertions.assertArrayEquals(IntStream.range(1, 50).toArray(), result.toArray());
    }


    @Test
    public void testLE() {
        RoaringBitmap result = bsi.compare(BitmapSliceIndex.Operation.LE, 50, 0, null);
        Assertions.assertTrue(result.getLongCardinality() == 50);
        Assertions.assertArrayEquals(IntStream.range(1, 51).toArray(), result.toArray());
    }

    @Test
    public void testRANGE() {
        RoaringBitmap result = bsi.compare(BitmapSliceIndex.Operation.RANGE, 10, 20, null);
        Assertions.assertTrue(result.getLongCardinality() == 11);
        Assertions.assertArrayEquals(IntStream.range(10, 21).toArray(), result.toArray());
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


}

