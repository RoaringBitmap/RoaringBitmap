 package org.roaringbitmap.bsi;


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.roaringbitmap.bsi.buffer.ImmutableBitSliceIndex;
import org.roaringbitmap.bsi.buffer.MutableBitSliceIndex;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * TestBase
 *
 */
public class BufferBSITest {

    private Map<Integer, Integer> testDataSet = new HashMap<>();

    private MutableBitSliceIndex mBsi;

    private ImmutableBitSliceIndex imBsi;


    @BeforeEach
    public void setup() {
        IntStream.range(1, 100).forEach(x -> testDataSet.put(x, x));
        mBsi = new MutableBitSliceIndex(1, 99);
        testDataSet.forEach((k, v) -> {
            mBsi.setValue(k, v);
        });
        imBsi = mBsi.toImmutableBitSliceIndex();
    }

    @Test
    public void testSetAndGet() {
        IntStream.range(1, 100).forEach(x -> {
            Pair<Integer, Boolean> pair = mBsi.getValue(x);
            Assertions.assertTrue(pair.getRight());
            Assertions.assertTrue(pair.getKey() == x);
        });

        IntStream.range(1, 100).forEach(x -> {
            Pair<Integer, Boolean> pair = imBsi.getValue(x);
            Assertions.assertTrue(pair.getRight());
            Assertions.assertTrue(pair.getKey() == x);
        });
    }

    @Test
    public void testMerge() {
        MutableBitSliceIndex bsiA = new MutableBitSliceIndex();
        IntStream.range(1, 100).forEach(x -> bsiA.setValue(x, x));
        MutableBitSliceIndex bsiB = new MutableBitSliceIndex();
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
        MutableBitSliceIndex bsi = new MutableBitSliceIndex(1, 99);
        List<Pair<Integer, Integer>> collect = testDataSet.entrySet()
                .stream().map(x -> Pair.newPair(x.getKey(), x.getValue())).collect(Collectors.toList());

        bsi.setValues(collect);

        Assertions.assertEquals(bsi.getExistenceBitmap().getLongCardinality(), 99);
        final MutableBitSliceIndex clone = bsi.clone();

        IntStream.range(1, 100).forEach(x -> {
            Pair<Integer, Boolean> bsiValue = clone.getValue(x);
            Assertions.assertTrue(bsiValue.getRight());
            Assertions.assertEquals((int) bsiValue.getKey(), x);
        });
    }


    @Test
    public void testAdd() {
        MutableBitSliceIndex bsiA = new MutableBitSliceIndex();
        IntStream.range(1, 100).forEach(x -> bsiA.setValue(x, x));
        MutableBitSliceIndex bsiB = new MutableBitSliceIndex();
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
        MutableBitSliceIndex bsiA = new MutableBitSliceIndex();
        IntStream.range(1, 100).forEach(x -> bsiA.setValue(x, x));
        MutableBitSliceIndex bsiB = new MutableBitSliceIndex();
        IntStream.range(1, 120).forEach(x -> bsiB.setValue(120 - x, x));

        bsiA.add(bsiB);

        ImmutableRoaringBitmap result = bsiA.compare(BitmapSliceIndex.Operation.EQ, 120, 0, null);
        Assertions.assertTrue(result.getLongCardinality() == 99);
        Assertions.assertArrayEquals(result.toArray(), IntStream.range(1, 100).toArray());

        result = bsiA.compare(BitmapSliceIndex.Operation.RANGE, 1, 20, null);
        Assertions.assertTrue(result.getLongCardinality() == 20);
        Assertions.assertArrayEquals(result.toArray(), IntStream.range(100, 120).toArray());
    }


    @Test
    public void TestIO4Stream() throws IOException {
        MutableBitSliceIndex bsi = new MutableBitSliceIndex(1, 99);
        IntStream.range(1, 100).forEach(x -> bsi.setValue(x, x));
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream bdo = new DataOutputStream(bos);
        bsi.serialize(bdo);
        byte[] data = bos.toByteArray();

        MutableBitSliceIndex newBsi = new MutableBitSliceIndex();

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
        MutableBitSliceIndex bsi = new MutableBitSliceIndex(1, 99);
        IntStream.range(1, 100).forEach(x -> bsi.setValue(x, x));
        ByteBuffer buffer = ByteBuffer.allocate(bsi.serializedSizeInBytes());
        bsi.serialize(buffer);

        byte[] data = buffer.array();
        MutableBitSliceIndex newBsi = new MutableBitSliceIndex();
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
        MutableBitSliceIndex bsi = new MutableBitSliceIndex(1, 99);
        IntStream.range(1, 100).forEach(x -> bsi.setValue(x, x));


        ImmutableBitSliceIndex iBsi = bsi.toImmutableBitSliceIndex();

        IntStream.range(1, 100).forEach(x -> {
            Pair<Integer, Boolean> bsiValue = iBsi.getValue(x);
            Assertions.assertTrue(bsiValue.getRight());
            Assertions.assertEquals((int) bsiValue.getKey(), x);
        });
    }

    // non parallel operation test
    @Test
    public void testSum() {
        MutableBitSliceIndex bsi = new MutableBitSliceIndex(1, 99);
        IntStream.range(1, 100).forEach(x -> bsi.setValue(x, x));


        ImmutableBitSliceIndex iBsi = bsi.toImmutableBitSliceIndex();

        MutableRoaringBitmap foundSet = MutableRoaringBitmap.bitmapOf(IntStream.range(1, 51).toArray());

        Pair<Long, Long> sumPair = iBsi.sum(foundSet);

        System.out.println("sum:" + sumPair.toString());

        int sum = IntStream.range(1, 51).sum();
        long count = IntStream.range(1, 51).count();

        Assertions.assertTrue(sumPair.getLeft().intValue() == sum && sumPair.getRight() == count);

    }

    @Test
    public void testEQ() {
        MutableBitSliceIndex bsi = new MutableBitSliceIndex(1, 99);
        IntStream.range(1, 100).forEach(x -> {
            if (x <= 50) {
                bsi.setValue(x, 1);
            } else {
                bsi.setValue(x, x);
            }

        });

        ImmutableRoaringBitmap bitmap = bsi.toImmutableBitSliceIndex().rangeEQ(null, 1);
        Assertions.assertTrue(bitmap.getLongCardinality() == 50L);
        ImmutableRoaringBitmap bitmap129 = bsi.toImmutableBitSliceIndex().rangeEQ(null, 129);
        Assertions.assertTrue(bitmap129.getLongCardinality() == 0L);

        ImmutableRoaringBitmap bitmap99 = bsi.toImmutableBitSliceIndex().rangeEQ(null, 99);
        Assertions.assertTrue(bitmap99.getLongCardinality() == 1L);
        Assertions.assertTrue(bitmap99.contains(99));
    }

    @Test
    public void testNotEQ() {
        MutableBitSliceIndex bsi = new MutableBitSliceIndex();
        bsi.setValue(1, 99);
        bsi.setValue(2, 1);
        bsi.setValue(3, 50);

        ImmutableRoaringBitmap result = bsi.compare(BitmapSliceIndex.Operation.NEQ, 99, 0, null);
        Assertions.assertTrue(result.getLongCardinality() == 2);
        Assertions.assertArrayEquals(new int[]{2, 3}, result.toArray());

        result = bsi.compare(BitmapSliceIndex.Operation.NEQ, 100, 0, null);
        Assertions.assertTrue(result.getLongCardinality() == 3);
        Assertions.assertArrayEquals(new int[]{1, 2, 3}, result.toArray());

        bsi = new MutableBitSliceIndex();
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
    public void testGT()  {
        ImmutableRoaringBitmap result = imBsi.compare(BitmapSliceIndex.Operation.GT, 50, 0, null);
        Assertions.assertTrue(result.getLongCardinality() == 49);
        Assertions.assertArrayEquals(IntStream.range(51, 100).toArray(), result.toArray());

        result = imBsi.compare(BitmapSliceIndex.Operation.GT, 0, 0, null);
        Assertions.assertTrue(result.getLongCardinality() == 99);
        Assertions.assertArrayEquals(IntStream.range(1, 100).toArray(), result.toArray());

        result = imBsi.compare(BitmapSliceIndex.Operation.GT, 99, 0, null);
        Assertions.assertTrue(result.isEmpty());
    }


    @Test
    public void testGE()  {
        ImmutableRoaringBitmap result = imBsi.compare(BitmapSliceIndex.Operation.GE, 50, 0, null);
        Assertions.assertTrue(result.getLongCardinality() == 50);
        Assertions.assertArrayEquals(IntStream.range(50, 100).toArray(), result.toArray());

        result = imBsi.compare(BitmapSliceIndex.Operation.GE, 1, 0, null);
        Assertions.assertTrue(result.getLongCardinality() == 99);
        Assertions.assertArrayEquals(IntStream.range(1, 100).toArray(), result.toArray());

        result = imBsi.compare(BitmapSliceIndex.Operation.GE, 100, 0, null);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testLT()  {
        ImmutableRoaringBitmap result = imBsi.compare(BitmapSliceIndex.Operation.LT, 50, 0, null);
        Assertions.assertTrue(result.getLongCardinality() == 49);
        Assertions.assertArrayEquals(IntStream.range(1, 50).toArray(), result.toArray());

        result = imBsi.compare(BitmapSliceIndex.Operation.LT, Integer.MAX_VALUE, 0, null);
        Assertions.assertTrue(result.getLongCardinality() == 99);
        Assertions.assertArrayEquals(IntStream.range(1, 100).toArray(), result.toArray());

        result = imBsi.compare(BitmapSliceIndex.Operation.LT, 1, 0, null);
        Assertions.assertTrue(result.isEmpty());
    }


    @Test
    public void testLE()  {
        ImmutableRoaringBitmap result = imBsi.compare(BitmapSliceIndex.Operation.LE, 50, 0, null);
        Assertions.assertTrue(result.getLongCardinality() == 50);
        Assertions.assertArrayEquals(IntStream.range(1, 51).toArray(), result.toArray());

        result = imBsi.compare(BitmapSliceIndex.Operation.LE, Integer.MAX_VALUE, 0, null);
        Assertions.assertTrue(result.getLongCardinality() == 99);
        Assertions.assertArrayEquals(IntStream.range(1, 100).toArray(), result.toArray());

        result = imBsi.compare(BitmapSliceIndex.Operation.LE, 0, 0, null);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testRANGE()  {
        ImmutableRoaringBitmap result = imBsi.compare(BitmapSliceIndex.Operation.RANGE, 10, 20, null);
        Assertions.assertTrue(result.getLongCardinality() == 11);
        Assertions.assertArrayEquals(IntStream.range(10, 21).toArray(), result.toArray());

        result = imBsi.compare(BitmapSliceIndex.Operation.RANGE, 1, 200, null);
        Assertions.assertTrue(result.getLongCardinality() == 99);
        Assertions.assertArrayEquals(IntStream.range(1, 100).toArray(), result.toArray());

        result = imBsi.compare(BitmapSliceIndex.Operation.RANGE, 1000, 2000, null);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testValueZero() {
        MutableBitSliceIndex bsi = new MutableBitSliceIndex();
        bsi.setValue(0, 0);
        bsi.setValue(1, 0);
        bsi.setValue(2, 1);

        ImmutableRoaringBitmap result = bsi.compare(BitmapSliceIndex.Operation.EQ, 0, 0, null);
        Assertions.assertTrue(result.getLongCardinality() == 2);
        Assertions.assertArrayEquals(new int[]{0, 1}, result.toArray());

        result = bsi.compare(BitmapSliceIndex.Operation.EQ, 1, 0, null);
        Assertions.assertTrue(result.getLongCardinality() == 1);
        Assertions.assertArrayEquals(new int[]{2}, result.toArray());
    }

    @Test
    public void testIN() throws ExecutionException, InterruptedException {

        Set<Integer> values = new HashSet<>();
        int[] valArr = new int[20];
        for (int i = 0; i < 20; i++) {
            values.add(i + 1);
            valArr[i] = i + 1;
        }


        ExecutorService pool = Executors.newFixedThreadPool(2);
        ImmutableRoaringBitmap result = imBsi.parallelIn(2,
                null, values, pool);
        Assertions.assertTrue(result.getLongCardinality() == values.size());
        Assertions.assertArrayEquals(valArr, result.toArray());
        pool.shutdownNow();
    }


    @Test
    public void testTransposeWithCount() throws ExecutionException, InterruptedException {
        MutableBitSliceIndex bsi = new MutableBitSliceIndex(1, 99);
        IntStream.range(1, 100).forEach(x -> {
            if (x <= 30) {
                bsi.setValue(x, 1);
            } else if (x <= 60) {
                bsi.setValue(x, 2);
            } else {
                bsi.setValue(x, 3);
            }

        });

        ExecutorService pool = Executors.newFixedThreadPool(2);
        ImmutableBitSliceIndex iBsi = bsi.toImmutableBitSliceIndex();

        final MutableBitSliceIndex result = iBsi.parallelTransposeWithCount(null, 2, pool);

        List<Pair<Integer, Integer>> pairs = result.toPairList();
        pairs.forEach(System.out::println);


        Assertions.assertEquals(30, (int) result.getValue(1).getKey());
        Assertions.assertEquals(30, (int) result.getValue(2).getKey());
        Assertions.assertEquals(39, (int) result.getValue(3).getKey());

    }


    @Test
    public void testTopK() {
        MutableBitSliceIndex bsi = new MutableBitSliceIndex(1, 100001);
        IntStream.range(1, 100001).forEach(x -> bsi.setValue(x, x));
        long start = System.currentTimeMillis();
        MutableRoaringBitmap top = bsi.topK(null, 20);
        long end = System.currentTimeMillis();
        System.out.println(top.toString() + " \ntime cost:" + (end - start));
    }


}

