import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Vector;
import junit.framework.Assert;
import me.lemire.roaringbitmap.ArrayContainer;
import me.lemire.roaringbitmap.BitmapContainer;
import me.lemire.roaringbitmap.RoaringBitmap;
import org.junit.Test;

@SuppressWarnings("static-method")
public class RoaringBitmapTest {

        @Test
        public void ArrayContainerCardinalityTest() {
                ArrayContainer ac = new ArrayContainer();
                for(short k = 0; k < 100; ++k) {
                        ac.add(k);
                        Assert.assertEquals(
                                ac.getCardinality(), k+1);
                }
                for(short k = 0; k < 100; ++k) {
                        ac.add(k);
                        Assert.assertEquals(
                                ac.getCardinality(), 100);
                }
        }

        @Test
        public void BitmapContainerCardinalityTest() {
                BitmapContainer ac = new BitmapContainer();
                for(short k = 0; k < 100; ++k) {
                        ac.add(k);
                        Assert.assertEquals(
                                ac.getCardinality(), k+1);
                }
                for(short k = 0; k < 100; ++k) {
                        ac.add(k);
                        Assert.assertEquals(
                                ac.getCardinality(), 100);
                }
        }

        @Test
        public void simplecardinalityTest() {
                final int N =  512;
                final int gap = 70;
                
                RoaringBitmap rb = new RoaringBitmap();
                for (int k = 0; k < N; k++) {
                        rb.add(k * gap);
                        Assert.assertEquals(
                                rb.getCardinality(), k + 1);
                }
                Assert.assertEquals(
                        rb.getCardinality(), N);
                for (int k = 0; k < N; k++) {
                        rb.add(k * gap);
                        Assert.assertEquals(
                                rb.getCardinality(), N);
                }
          
        }

        @Test
        public void cardinalityTest() {
                //System.out.println("Testing cardinality computations (can take a few minutes)");
                final int N = 1024;
                for (int gap = 7; gap < 100000; gap *= 10) {
                        for (int offset = 2; offset <= 1024; offset *= 2) {
                                //System.out.println("testing cardinality with gap = "+gap+" and offset = "+offset);
                                RoaringBitmap rb = new RoaringBitmap();
                                for (int k = 0; k < N; k++) {
                                        rb.add(k * gap);
                                        Assert.assertEquals(
                                                rb.getCardinality(), k + 1);
                                }
                                Assert.assertEquals(
                                        rb.getCardinality(), N);
                                for (int k = 0; k < N; k++) {
                                        rb.add(k * gap);
                                        Assert.assertEquals(
                                                rb.getCardinality(), N);
                                }
                                RoaringBitmap rb2 = new RoaringBitmap();
                                for (int k = 0; k < N; k++) {
                                        rb2.add(k * gap * offset);
                                        Assert.assertEquals(
                                                rb2.getCardinality(), k + 1);
                                }
                                Assert.assertEquals(
                                        rb2.getCardinality(), N);
                                for (int k = 0; k < N; k++) {
                                        rb2.add(k * gap * offset);
                                        Assert.assertEquals(
                                                rb2.getCardinality(), N);
                                }
                                Assert.assertEquals(RoaringBitmap.and(rb, rb2)
                                        .getCardinality(), N / offset);
                                Assert.assertEquals(RoaringBitmap.or(rb, rb2)
                                        .getCardinality(), 2 * N - N / offset);
                                Assert.assertEquals(RoaringBitmap.xor(rb, rb2)
                                        .getCardinality(), 2 * N - 2 * N
                                        / offset);
                        }
                }
        }
        
    
        
	@Test
	public void arraytest() {
		ArrayContainer rr = new ArrayContainer();
		rr.add((short) 110);
		rr.add((short) 114);
		rr.add((short) 115);
		short[] array = new short[3];
		int pos = 0;
		for (short i : rr)
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
		for (short i : rr)
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
		for (int k = 0; k < 4000; ++k) {
			rr.add(k);
			a[pos++] = k;
		}
		rr.add(100000);
		a[pos++] = 100000;
		rr.add(110000);
		a[pos++] = 110000;
		int[] array = new int[4002];
		pos = 0;
		for (int i : rr) {
			array[pos++] = i;
		}
		Assert.assertTrue(Arrays.equals(array, a));
	}

	@Test
	public void andtest() {
		RoaringBitmap rr = new RoaringBitmap();
		for (int k = 0; k < 4000; ++k) {
			rr.add(k);
		}
		rr.add(100000);
		rr.add(110000);
		RoaringBitmap rr2 = new RoaringBitmap();
		rr2.add(13);
		RoaringBitmap rrand = RoaringBitmap.and(rr, rr2);
		
		int[] array = new int[1];
		int pos = 0;
		for (int i : rrand) {
			array[pos++] = i;
		}
		Assert.assertEquals(array[0], 13);
	}

	@Test
	public void andtest2() {
		RoaringBitmap rr = new RoaringBitmap();
		for (int k = 0; k < 4000; ++k) {
			rr.add(k);
		}
		rr.add(100000);
		rr.add(110000);
		RoaringBitmap rr2 = new RoaringBitmap();
		rr2.add(13);
		RoaringBitmap rrand = RoaringBitmap.and(rr, rr2);

		int[] array = new int[1];
		int pos = 0;
		for (int i : rrand) {
			array[pos++] = i;
		}
		Assert.assertEquals(array[0], 13);
	}

	@Test
	public void ortest() {
		RoaringBitmap rr = new RoaringBitmap();
		for (int k = 0; k < 4000; ++k) {
			rr.add(k);
		}
		rr.add(100000);
		rr.add(110000);
		RoaringBitmap rr2 = new RoaringBitmap();
		for (int k = 0; k < 4000; ++k) {
			rr2.add(k);
		}

		RoaringBitmap rror = RoaringBitmap.or(rr, rr2);

		int[] array = new int[4002];
		int[] arrayrr = new int[4002];
		int pos = 0;
		for (int i : rror) {
			array[pos++] = i;
		}
		pos = 0;
		for (int i : rr) {
			arrayrr[pos++] = i;
		}

		Assert.assertTrue(Arrays.equals(array, arrayrr));
	}

	@Test
	public void ortest2() {
		int[] arrayrr = new int[4000 + 4000 + 2];
		int pos = 0;
		RoaringBitmap rr = new RoaringBitmap();
		for (int k = 0; k < 4000; ++k) {
			rr.add(k);
			arrayrr[pos++] = k;
		}
		rr.add(100000);
		rr.add(110000);
		RoaringBitmap rr2 = new RoaringBitmap();
		for (int k = 4000; k < 8000; ++k) {
			rr2.add(k);
			arrayrr[pos++] = k;
		}

		arrayrr[pos++] = 100000;
		arrayrr[pos++] = 110000;

		RoaringBitmap rror = RoaringBitmap.or(rr, rr2);
		pos = 0;
		int[] arrayor = new int[4000 + 4000 + 2];
		for (int i : rror) {
			arrayor[pos++] = i;
		}

		Assert.assertTrue(Arrays.equals(arrayor, arrayrr));
	}

	@Test
	public void andtest3() {
		int[] arrayand = new int[5256];
		int[] arrayres = new int[5256];
		int pos = 0;
		RoaringBitmap rr = new RoaringBitmap();
		for (int k = 4000; k < 4256; ++k)
			rr.add(k); // Seq
		for (int k = 65536; k < 65536 + 4000; ++k)
			rr.add(k); // bitmap
		for (int k = 4 * 65535; k < 4 * 65535 + 4000; ++k)
			rr.add(k); // 4 ds seq et 3996 bitmap
		for (int k = 6 * 65535; k < 6 * 65535 + 1000; ++k)
			rr.add(k); // 6 ds seq et 994 ds bitmap

		RoaringBitmap rr2 = new RoaringBitmap();
		for (int k = 4000; k < 4256; ++k) {
			rr2.add(k);
			arrayand[pos++] = k;
		}
		for (int k = 65536; k < 65536 + 4000; ++k) {
			rr2.add(k);
			arrayand[pos++] = k;
		}
		for (int k = 6 * 65535; k < 6 * 65535 + 1000; ++k) {
			rr2.add(k);
			arrayand[pos++] = k;
		}

		RoaringBitmap rrand = RoaringBitmap.and(rr, rr2);
		pos = 0;
		for (int i : rrand)
			arrayres[pos++] = i;

		Assert.assertTrue(Arrays.equals(arrayand, arrayres));

	}

	@Test
	public void ortest3() {
		HashSet<Integer> V1 = new HashSet<Integer>();
		HashSet<Integer> V2 = new HashSet<Integer>();

		RoaringBitmap rr = new RoaringBitmap();
		RoaringBitmap rr2 = new RoaringBitmap();
		//For the first 65536: rr2 has a bitmap container, and rr has an array container. 
		//We will check the union between a BitmapCintainer and an arrayContainer  
		for (int k = 0; k < 4000; ++k){
			rr2.add(k);
			V1.add(new Integer(k));
		}
		for (int k = 3500; k < 4500; ++k) {
			rr.add(k);
			V1.add(new Integer(k));
		}
		for (int k = 4000; k < 65000; ++k){
			rr2.add(k);
			V1.add(new Integer(k));
		}
		
		//In the second node of each roaring bitmap, we have two bitmap containers. 
		//So, we will check the union between two BitmapContainers
		for (int k = 65536; k < 65536 + 10000; ++k) {
			rr.add(k);
			V1.add(new Integer(k));
		}
		
		for (int k = 65536; k < 65536 + 14000; ++k) {
			rr2.add(k);
			V1.add(new Integer(k));
		}
		
		//In the 3rd node of each Roaring Bitmap, we have an ArrayContainer, so, we will try the union between two 
		//ArrayContainers. 
		for (int k = 4 * 65535; k < 4 * 65535 + 1000; ++k) {
			rr.add(k);
			V1.add(new Integer(k));			
		} 
		
		for (int k = 4 * 65535; k < 4 * 65535 + 800; ++k) {
			rr2.add(k);
			V1.add(new Integer(k));			
		} 

		//For the rest, we will check if the union will take them in the result
		for (int k = 6 * 65535; k < 6 * 65535 + 1000; ++k) {
			rr.add(k);
			V1.add(new Integer(k));
		} 
				
		for (int k = 7 * 65535; k < 7 * 65535 + 2000; ++k) {
			rr2.add(k);
			V1.add(new Integer(k));
		}

		RoaringBitmap rror = RoaringBitmap.or(rr, rr2);
		boolean valide = true;

		// Si tous les elements de rror sont dans V1 et que tous les elements de
		// V1 sont dans rror(V2)
		// alors V1 == rror

		Object[] tab = V1.toArray();
		Vector<Integer> vector = new Vector<Integer>();
		for(int i=0; i<tab.length; i++)
			vector.add((Integer) tab[i]);		
		
		for (int i : rror) {
			if (!vector.contains(new Integer(i))) {
				System.out.println(" "+i);
				valide = false;
			}
			V2.add(new Integer(i));
		}
		for (int i = 0; i < V1.size(); i++)
			if (!V2.contains(vector.elementAt(i))){
				valide = false;
				System.out.println(" "+vector.elementAt(i));
			}
		
		System.out.println(rr.toString());
		System.out.println(rr2.toString());
		
		Assert.assertEquals(valide, true);
	}

	@Test
	public void xortest1() {
		Vector<Integer> V1 = new Vector<Integer>();
		Vector<Integer> V2 = new Vector<Integer>();

		RoaringBitmap rr = new RoaringBitmap();
		for (int k = 4000; k < 4256; ++k) {
			rr.add(k);
		}// Seq
		for (int k = 65536; k < 65536 + 4000; ++k) {
			rr.add(k);
		} // bitmap
		for (int k = 4 * 65535; k < 4 * 65535 + 4000; ++k) {
			rr.add(k);
			V1.add(new Integer(k));
		} // 4 in seq et 3996 in bitmap
		for (int k = 6 * 65535; k < 6 * 65535 + 1000; ++k) {
			rr.add(k);
			V1.add(new Integer(k));
		} // 6 in seq et 994 in bitmap

		RoaringBitmap rr2 = new RoaringBitmap();
		for (int k = 4000; k < 4256; ++k)
			rr2.add(k);
		for (int k = 65536; k < 65536 + 4000; ++k)
			rr2.add(k);
		for (int k = 7 * 65535; k < 7 * 65535 + 2000; ++k) {
			rr2.add(k);
			V1.add(new Integer(k));
		}

		RoaringBitmap rrxor = RoaringBitmap.xor(rr, rr2);
		boolean valide = true;

		// Si tous les elements de rror sont dans V1 et que tous les elements de
		// V1 sont dans rror(V2)
		// alors V1 == rror

		for (int i : rrxor) {
			if (!V1.contains(new Integer(i)))
				valide = false;
			V2.add(new Integer(i));
		}
		for (int i = 0; i < V1.size() && valide; i++)
			if (!V2.contains(V1.elementAt(i)))
				valide = false;

		Assert.assertEquals(valide, true);
	}	
}
