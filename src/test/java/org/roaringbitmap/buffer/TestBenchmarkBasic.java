package org.roaringbitmap.buffer;

import org.junit.Test;

public class TestBenchmarkBasic {
    
    @Test
    public void createBitmapOrdered() {
        long besttime = Long.MAX_VALUE;
        for (int j = 0; j < 100; ++j) {
            MutableRoaringBitmap r = new MutableRoaringBitmap();
            long bef = System.nanoTime();
            for (int k = 0; k < 65536; k++) {
                r.add(k * 32);
            }
            long aft = System.nanoTime();
            if(besttime > aft - bef) besttime = aft-bef;
        }
        System.out.println("(buffer) Ordered creation time " + besttime);
    }
    
    @Test
    public void createBitmapUnordered() {
        long besttime = Long.MAX_VALUE;
        for (int j = 0; j < 100; ++j) {
            MutableRoaringBitmap r = new MutableRoaringBitmap();
            long bef = System.nanoTime();
            for (int k = 65536-1; k >=0 ; k--) {
                r.add(k * 32);
            }
            long aft = System.nanoTime();
            if(besttime > aft - bef) besttime = aft-bef;
        }
        System.out.println("(buffer) Reverse creation time " + besttime);

    }


}
