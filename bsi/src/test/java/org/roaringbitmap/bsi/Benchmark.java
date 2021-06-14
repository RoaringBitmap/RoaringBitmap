package org.roaringbitmap.bsi;


import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.bsi.buffer.ImmutableBitSliceIndex;
import org.roaringbitmap.bsi.buffer.MutableBitSliceIndex;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

/**
 * Benchmark
 *
 */
public class Benchmark {

    private Map<Integer, Integer> testDataSet = new HashMap<>();

    private MutableBitSliceIndex mBsi;

    private ImmutableBitSliceIndex imBsi;


    @BeforeEach
    public void setup() {
        IntStream.range(1, 1000000).forEach(x -> testDataSet.put(x, x));
        mBsi = new MutableBitSliceIndex(1, 1000000-1);
        testDataSet.forEach((k, v) -> {
            mBsi.setValue(k, v);
        });
        imBsi = mBsi.toImmutableBitSliceIndex();

    }

    @Test
    public void testCompare(){
        int startOrValue = 1000;
        for (BitmapSliceIndex.Operation op : BitmapSliceIndex.Operation.values()) {
            int end = 0;
            if(op.equals(BitmapSliceIndex.Operation.RANGE)){
                end = 10000;
            }
            long s = System.currentTimeMillis();
            ImmutableRoaringBitmap rb= imBsi.compare(op,startOrValue,end,null);
            System.out.println("card:" + rb.getLongCardinality()
                    + " operation:" + op.name() + " time cost:" +(System.currentTimeMillis()-s));
        }
    }

    @Test
    public void testTopK(){
        int[] kArr = new int[]{10,100,1000,10000,100000};

        for (int k:kArr){
            long s = System.currentTimeMillis();
            MutableRoaringBitmap b = imBsi.topK(null, k);
            System.out.println("topK k=" + k + " card:" + b.getCardinality()+ " time cost:" +(System.currentTimeMillis()-s));
        }

    }

    @Test
    public void testSum(){
        long s = System.currentTimeMillis();
        Pair<Long, Long> pair = imBsi.sum(imBsi.getExistenceBitmap());
        System.out.println("sum:" + pair.getRight() + " time cost:" + (System.currentTimeMillis()-s) );
    }

    @Test
    public void testToPairList(){
        long s = System.currentTimeMillis();
        List<Pair<Integer, Integer>> pairList = imBsi.toPairList();
        System.out.println("testToPairList pair size:" + pairList.size() + " time cost:" + (System.currentTimeMillis()-s) );
    }

    @Test
    public void testTranspose() throws ExecutionException, InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        long s = System.currentTimeMillis();
        MutableBitSliceIndex bsiT = imBsi.parallelTransposeWithCount(null,2,pool);
        System.out.println("transpose card:" + bsiT.getLongCardinality() + " time cost:" + (System.currentTimeMillis()-s) );
        pool.shutdownNow();
    }

    @Test
    public void benchmark(){
        int[] cards = new int[]{10_000_000,20_000_000,30_000_000,40_000_000,50_000_000};

        for (int card : cards) {
            MutableBitSliceIndex bsi = new MutableBitSliceIndex();
            IntStream.range(1,card+1).forEach(x->bsi.setValue(x,x));
            int mSize = bsi.serializedSizeInBytes();
            bsi.runOptimize();
            MutableRoaringBitmap foundSet = new MutableRoaringBitmap();
            foundSet.flip(1,10_000_000L);
            long s1 =System.currentTimeMillis();
            bsi.compare(BitmapSliceIndex.Operation.EQ,1000,0,null);
            long s2 =System.currentTimeMillis();
            bsi.compare(BitmapSliceIndex.Operation.EQ,1000,0,foundSet);
            long s3 =System.currentTimeMillis();
            bsi.compare(BitmapSliceIndex.Operation.LE,100000,0,null);
            long s4 =System.currentTimeMillis();
            bsi.compare(BitmapSliceIndex.Operation.LT,100000,0,null);
            String msg = card + ":" + mSize/(1024*1024) + ":" + (s2-s1) +":" + (s3-s2) +":" +(s4-s3);
            System.out.println(msg);
        }

    }





}

