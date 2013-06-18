
import java.util.Arrays;

import junit.framework.Assert;
import me.lemire.roaringbitmap.ArrayContainer;
import me.lemire.roaringbitmap.BitmapContainer;
import me.lemire.roaringbitmap.RoaringBitmap;

import org.junit.Test;
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
		for(int k = 0; k<4000;++k) {
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
}
