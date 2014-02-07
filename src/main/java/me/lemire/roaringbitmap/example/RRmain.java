package me.lemire.roaringbitmap.example;

import java.util.Iterator;
import java.util.Map.Entry;

import me.lemire.roaringbitmap.Container;
import me.lemire.roaringbitmap.RoaringBitmap;
import me.lemire.roaringbitmap.Util;


public class RRmain {
	/**
	 * This is a test example to see how the internals behave.
	 * @param args
	 */
	public static void main(String[] args) {
        RoaringBitmap rr = new RoaringBitmap();
        for(int k = 4000; k<4256;++k) rr.add(k); //Seq
        for(int k = 65535; k<65535+4000;++k) rr.add(k); //bitmap
        for(int k = 4*65535; k<4*65535+4000;++k) rr.add(k); //4 ds seq et 3996 bitmap
        for(int k = 6*65535; k<6*65535+1000;++k) rr.add(k); //6 ds seq et 994 ds bitmap 
        
        RoaringBitmap rr2 = new RoaringBitmap();
        for(int k = 4000; k<4256;++k) rr2.add(k);
        for(int k = 65535; k<65535+4000;++k) rr2.add(k);
        for(int k = 6*65535; k<6*65535+1000;++k) rr2.add(k);
        
        RoaringBitmap rror = RoaringBitmap.or(rr, rr2);
        
        
	}

}
