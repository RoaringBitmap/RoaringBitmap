
import java.util.Arrays;
import java.util.Vector;
import junit.framework.Assert;
import me.lemire.roaringbitmap.ArrayContainer;
import me.lemire.roaringbitmap.BitmapContainer;
import me.lemire.roaringbitmap.RoaringBitmap;
import org.junit.Test;

@SuppressWarnings("static-method")
public class RoaringBitmapTest {
	@Test
	public void arraytest() {
		ArrayContainer rr = new ArrayContainer();
		rr.add((short) 110);
		rr.add((short) 114);
		rr.add((short) 115);
		short[] array = new short[3];
		int pos = 0;
		for(short i : rr)
			array[pos++] = i;
		Assert.assertEquals(array[0], (short) 110);
		Assert.assertEquals(array[1], (short) 114);		
		Assert.assertEquals(array[2], (short) 115);
	}
	
	@Test
	public void bitmaptest() {
		BitmapContainer rr = new BitmapContainer();
		rr.add((short) 110);
		rr.add((short) 114);
		rr.add((short) 115);
		short[] array = new short[3];
		int pos = 0;
		for(short i : rr)
			array[pos++] = i;
		Assert.assertEquals(array[0], (short) 110);
		Assert.assertEquals(array[1], (short) 114);		
		Assert.assertEquals(array[2], (short) 115);
	}
	
	@Test
	public void basictest() {
		RoaringBitmap rr = new RoaringBitmap();
		int[] a = new int[4002];
		int pos = 0;
		for(int k = 0; k<4000; ++k) {
			rr.add(k);
			a[pos++] = k;
		}
		rr.add(100000);
		a[pos++] = 100000;
		rr.add(110000);
		a[pos++] = 110000;
		int[] array = new int[4002];
		pos = 0;
		for(int i : rr) {
			array[pos++] = i;
		}
		Assert.assertTrue(Arrays.equals(array, a));
	}
	
	@Test
	public void andtest() {
		RoaringBitmap rr = new RoaringBitmap();
		for(int k = 0; k<4000;++k) {
			rr.add(k);
		}
		rr.add(100000);
		rr.add(110000);
		RoaringBitmap rr2 = new RoaringBitmap();
		rr2.add(13);
		RoaringBitmap rrand = RoaringBitmap.and(rr, rr2);
		
		int[] array = new int[1];
		int pos = 0;
		for(int i : rrand) {
			array[pos++] = i;
		}
		Assert.assertEquals(array[0],13);
	}
	
	@Test
	public void andtest2() {
		RoaringBitmap rr = new RoaringBitmap();
		for(int k = 0; k<4000;++k) {
			rr.add(k);
		}
		rr.add(100000);
		rr.add(110000);
		RoaringBitmap rr2 = new RoaringBitmap();
		rr2.add(13);
		RoaringBitmap rrand = RoaringBitmap.and(rr, rr2);
		
		int[] array = new int[1];
		int pos = 0;
		for(int i : rrand) {
			array[pos++] = i;
		}
		Assert.assertEquals(array[0],13);
	}
	
	@Test
	/** Tester la nom répétition de valeurs communes entre les deux ensembles */
	public void ortest() {
		RoaringBitmap rr = new RoaringBitmap();
		for(int k = 0; k<4000;++k) {
			rr.add(k);
		}
		rr.add(100000);
		rr.add(110000);
		RoaringBitmap rr2 = new RoaringBitmap();
		for(int k = 0; k<4000;++k) {
			rr2.add(k);
		}		
		
		/*RoaringBitmap rr = new RoaringBitmap();
        rr.add(44); rr.add(50); rr.add(60000); rr.add(60500); rr.add(100000); rr.add(110000);
        RoaringBitmap rr2 = new RoaringBitmap();
        //rr2.add(44); rr2.add(70); rr2.add(10050); rr2.add(50550); rr2.add(60000); rr2.add(60500);
        rr2.add(50); rr2.add(60500); rr2.add(100000);*/ 
		
        RoaringBitmap rror = RoaringBitmap.or(rr, rr2);       			
		
		int[] array = new int[4002]; int[] arrayrr = new int[4002];
		int pos = 0;
		for(int i : rror) {	array[pos++] = i;	}
		pos = 0;
		for(int i : rr) {	arrayrr[pos++] = i;	}
		
		Assert.assertTrue(Arrays.equals(array, arrayrr));		
	}
	
	@Test
	/** Tester l'existence dans le résultat de toutes les valeurs distinctes des deux ensembles */
	public void ortest2() {
		int[] arrayrr = new int[4000+4000+2]; int pos=0;
		RoaringBitmap rr = new RoaringBitmap();
		for(int k = 0; k<4000;++k) {
			rr.add(k);
			arrayrr[pos++] = k;
		}
		rr.add(100000); 
		rr.add(110000);
		RoaringBitmap rr2 = new RoaringBitmap();
		for(int k = 4000; k<8000;++k) {
			rr2.add(k); arrayrr[pos++] = k;
		}		
		
		arrayrr[pos++] = 100000;
		arrayrr[pos++] = 110000;
		
        RoaringBitmap rror = RoaringBitmap.or(rr, rr2);       			
		pos=0;
		int[] arrayor = new int[4000+4000+2];		
		for(int i : rror) {	arrayor[pos++] = i;	}		
		
		Assert.assertTrue(Arrays.equals(arrayor, arrayrr));		
	}
	
	@Test	
	public void andtest3() {
		int[] arrayand = new int[5256]; 
		int[] arrayres = new int[5256];
		int pos=0;
		RoaringBitmap rr = new RoaringBitmap();
		for(int k = 4000; k<4256;++k) rr.add(k); //Seq
        for(int k = 65536; k<65536+4000;++k) rr.add(k); //bitmap
        for(int k = 4*65535; k<4*65535+4000;++k) rr.add(k); //4 ds seq et 3996 bitmap
        for(int k = 6*65535; k<6*65535+1000;++k) rr.add(k); //6 ds seq et 994 ds bitmap
        
		RoaringBitmap rr2 = new RoaringBitmap();
		for(int k = 4000; k<4256;++k) {rr2.add(k); arrayand[pos++] = k;}
        for(int k = 65536; k<65536+4000;++k) {rr2.add(k); arrayand[pos++] = k;}
        for(int k = 6*65535; k<6*65535+1000;++k) {rr2.add(k); arrayand[pos++] = k;}        
		
        RoaringBitmap rrand = RoaringBitmap.and(rr, rr2);       			
		pos=0;				
		for(int i : rrand)  arrayres[pos++] = i;		
		
		Assert.assertTrue(Arrays.equals(arrayand, arrayres));
		
	}
	
	@Test 
	public void ortest3() {
		Vector<Integer> V1 = new Vector<Integer>(); Vector<Integer> V2 = new Vector<Integer>();
		
		RoaringBitmap rr = new RoaringBitmap();
		for(int k = 4000; k<4256;++k) {rr.add(k); V1.add(new Integer(k)); }//Seq
        for(int k = 65536; k<65536+4000;++k) {rr.add(k); V1.add(new Integer(k)); } //bitmap
        for(int k = 4*65535; k<4*65535+4000;++k) {rr.add(k); V1.add(new Integer(k)); } //4 ds seq et 3996 bitmap
        for(int k = 6*65535; k<6*65535+1000;++k) {rr.add(k); V1.add(new Integer(k)); } //6 ds seq et 994 ds bitmap
        
		RoaringBitmap rr2 = new RoaringBitmap();
		for(int k = 4000; k<4256;++k) rr2.add(k); 
        for(int k = 65536; k<65536+4000;++k) rr2.add(k);
        for(int k = 7*65535; k<7*65535+2000;++k) {rr2.add(k); V1.add(new Integer(k)); }        
		
        RoaringBitmap rror = RoaringBitmap.or(rr, rr2);       			
		boolean valide = true;
		
		//Si tous les elements de rror sont dans V1 et que tous les elements de V1 sont dans rror(V2)
		//alors V1 == rror
		
		for(int i : rror)  {if(!V1.contains(new Integer(i))) valide=false; V2.add(new Integer(i));}				
		for(int i=0; i<V1.size(); i++) 
			if(!V2.contains(V1.elementAt(i))) valide=false;
		
		Assert.assertEquals(valide, true);		
	}
	
	@Test 
	public void xortest1() {
		Vector<Integer> V1 = new Vector<Integer>(); Vector<Integer> V2 = new Vector<Integer>();
		
		RoaringBitmap rr = new RoaringBitmap();
		for(int k = 4000; k<4256;++k) {rr.add(k); }//Seq
        for(int k = 65536; k<65536+4000;++k) {rr.add(k); } //bitmap
        for(int k = 4*65535; k<4*65535+4000;++k) {rr.add(k); V1.add(new Integer(k)); } //4 in seq et 3996 in bitmap
        for(int k = 6*65535; k<6*65535+1000;++k) {rr.add(k); V1.add(new Integer(k)); } //6 in seq et 994 in bitmap
        
		RoaringBitmap rr2 = new RoaringBitmap();
		for(int k = 4000; k<4256;++k) rr2.add(k); 
        for(int k = 65536; k<65536+4000;++k) rr2.add(k);
        for(int k = 7*65535; k<7*65535+2000;++k) {rr2.add(k); V1.add(new Integer(k)); }        
		
        RoaringBitmap rrxor = RoaringBitmap.xor(rr, rr2);       			
		boolean valide = true;
		
		//Si tous les elements de rror sont dans V1 et que tous les elements de V1 sont dans rror(V2)
		//alors V1 == rror
		
		for(int i : rrxor)  {if(!V1.contains(new Integer(i))) valide=false; V2.add(new Integer(i));}				
		for(int i=0; i<V1.size() && valide; i++) 
			if(!V2.contains(V1.elementAt(i))) valide=false;
		
		Assert.assertEquals(valide, true);		
	}
}
