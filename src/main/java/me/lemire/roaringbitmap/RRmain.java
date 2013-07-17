package me.lemire.roaringbitmap;

import java.util.Iterator;
import java.util.Map.Entry;


public class RRmain {
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
        RoaringBitmap rr = new RoaringBitmap();
        /*rr.add(13); rr.add(50); rr.add(70); rr.add(60000); rr.add(60500); rr.add(70500); rr.add(300200); 
        rr.add(407001); rr.add(419534);rr.add(472537);//rr.add(550606); rr.add(616142);*/
        for(int k = 4000; k<4256;++k) rr.add(k); //Seq
        for(int k = 65535; k<65535+4000;++k) rr.add(k); //bitmap
        for(int k = 4*65535; k<4*65535+4000;++k) rr.add(k); //4 ds seq et 3996 bitmap
        for(int k = 6*65535; k<6*65535+1000;++k) rr.add(k); //6 ds seq et 994 ds bitmap 
        
        RoaringBitmap rr2 = new RoaringBitmap();
        /*rr2.add(44); rr2.add(70); rr2.add(10050); rr2.add(50550); rr2.add(60000);
        rr2.add(60500);  rr2.add(300200); rr2.add(407001); rr2.add(472537); rr2.add(616142);*/
        for(int k = 4000; k<4256;++k) rr2.add(k);
        for(int k = 65535; k<65535+4000;++k) rr2.add(k);
        for(int k = 6*65535; k<6*65535+1000;++k) rr2.add(k);
        
        RoaringBitmap rrxor = RoaringBitmap.xor(rr, rr2);
        
        final Iterator<Entry<Short, Container>> p1 = rr.c.entrySet().iterator();
        final Iterator<Entry<Short, Container>> p2 = rr2.c.entrySet().iterator();
        final Iterator<Entry<Short, Container>> p5 = rrxor.c.entrySet().iterator();
        Entry<Short, Container> s1;
        
        System.out.println("\n rr : ");
        while (p1.hasNext()) 
        { s1=p1.next(); System.out.print("  "+s1.getKey().shortValue()/*+" "+s1.getValue().getCardinality()*/);}
        
        System.out.println("\n rr2 : ");
        while (p2.hasNext()) 
        { s1=p2.next(); System.out.print("  "+s1.getKey().shortValue()); }
        
        /*System.out.println("\n rror : ");
        while (p3.hasNext()) 
        { s1=p3.next(); System.out.print("  "+s1.getKey().shortValue()); }
        
        System.out.println("\n rrand : ");
        while (p4.hasNext()) 
        { s1=p4.next(); System.out.print("  "+s1.getKey().shortValue()); }*/
        
        System.out.println("\n rrxor : ");
        while (p5.hasNext()) 
        { s1=p5.next(); System.out.print("  "+s1.getKey().shortValue()); }
                
        RoaringBitmap.afficher(rrxor);
        
        /*int x = 472537;
        System.out.println("\nhigh bits = "+Util.highbits(x)+" low bits = "+(short) Util.lowbits(x));
        System.out.println("rr : ");
        for(int i : rr) System.out.print(i+" ");
        System.out.println("\n rr2 : ");
        for(int i : rr2) System.out.print(i+" ");
        /*System.out.println("\n rror : ");
        for(int i : rror) System.out.print(i+" ");
        System.out.println("\n rrand : ");
        for(int i : rrand) System.out.print(i+" ");*/
	}
}
