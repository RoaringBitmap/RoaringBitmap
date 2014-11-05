package org.roaringbitmap.buffer;

import static org.junit.Assert.assertTrue;
import junit.framework.Assert;

import org.junit.Test;

public class TestCompressionRates {
    
    @Test
    public void SimpleCompressionRateTest() {
        int N = 500000;
        for (int gap = 1; gap < 1024; gap *= 2) {
            MutableRoaringBitmap mrb = new MutableRoaringBitmap();
            for(int k = 0; k < N * gap ; k+=gap) {
                mrb.add(k);
            }
            int maxval = gap;
            if(maxval > 16 ) maxval = 16;
            assertTrue(mrb.serializedSizeInBytes()*8.0/N < (maxval+1));
        }
    }

}
