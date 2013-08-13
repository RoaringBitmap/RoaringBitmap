package me.lemire.roaringbitmap.example;

import java.util.Iterator;
import java.util.Map.Entry;

import me.lemire.roaringbitmap.Container;
import me.lemire.roaringbitmap.RoaringBitmap;


public class RRmain {
	/**
	 * This is a test example to see how the internals behave.
	 * @param args
	 */
	public static void main(String[] args) {
        RoaringBitmap rr = new RoaringBitmap();
        for(int k = 4000; k<4256;++k) rr.set(k); //Seq
        for(int k = 65535; k<65535+4000;++k) rr.set(k); //bitmap
        for(int k = 4*65535; k<4*65535+4000;++k) rr.set(k); //4 ds seq et 3996 bitmap
        for(int k = 6*65535; k<6*65535+1000;++k) rr.set(k); //6 ds seq et 994 ds bitmap 
        
        RoaringBitmap rr2 = new RoaringBitmap();
        for(int k = 4000; k<4256;++k) rr2.set(k);
        for(int k = 65535; k<65535+4000;++k) rr2.set(k);
        for(int k = 6*65535; k<6*65535+1000;++k) rr2.set(k);
        
        RoaringBitmap rror = RoaringBitmap.or(rr, rr2);
        
        final Iterator<Entry<Short, Container>> p1 = rr.highlowcontainer.entrySet().iterator();
        final Iterator<Entry<Short, Container>> p2 = rr2.highlowcontainer.entrySet().iterator();
        final Iterator<Entry<Short, Container>> p5 = rror.highlowcontainer.entrySet().iterator();
        Entry<Short, Container> s1;
        
        System.out.println("\n rr new version: ");
        while (p1.hasNext()) 
        { s1=p1.next(); System.out.print("  "+s1.getKey().shortValue()/*+" "+s1.getValue().getCardinality()*/);}
        
        System.out.println("\n rr2 : ");
        while (p2.hasNext()) 
        { s1=p2.next(); System.out.print("  "+s1.getKey().shortValue()); }
        
        System.out.println("\n rrxor : ");
        while (p5.hasNext()) 
        { s1=p5.next(); System.out.print("  "+s1.getKey().shortValue()); }
                
        display(rror);
        
	}

	public static void display(RoaringBitmap x) {
		final Iterator<Entry<Short, Container>> p1 = x.highlowcontainer.entrySet().iterator();
		Entry<Short, Container> s1;

		while (p1.hasNext()) {
			s1 = p1.next();
			System.out.println("\n" + s1.getKey().shortValue());
			Container c = s1.getValue();
			System.out.println(c.toString());
		}
	}

}
