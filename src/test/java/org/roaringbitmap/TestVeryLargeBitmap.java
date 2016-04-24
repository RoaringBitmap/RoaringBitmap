package org.roaringbitmap;

import org.junit.Assert;
import org.junit.Test;

public class TestVeryLargeBitmap {
  
  //@Test // this should run fine given enough memory
  public void stupidlyLarge() {
    RoaringBitmap rb = new RoaringBitmap();
    rb.add(0, 1L<<32);
    for(long k = 1L; k<(1L<<32); k *=2 ) {
      Assert.assertEquals(rb.rankLong((int) k), k+1);      
    }
    for(long k = (1L<<32)-10; k<(1L<<32); k++ ) {
      Assert.assertEquals(rb.rankLong((int) k), k+1);      
    }
    Assert.assertEquals(rb.getLongCardinality(), 1L<<32);
    // next line should not make things crash
    System.out.println(rb.toString());
  }

}
