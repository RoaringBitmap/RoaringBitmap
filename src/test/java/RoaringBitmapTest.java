
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Vector;
import junit.framework.Assert;
import me.lemire.roaringbitmap.ArrayContainer;
import me.lemire.roaringbitmap.BitmapContainer;
import me.lemire.roaringbitmap.ContainerFactory;
import me.lemire.roaringbitmap.RoaringBitmap;
import org.junit.Test;

@SuppressWarnings({ "static-method", "deprecation" })
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
                rb.validate(); 
          
        }

        @Test
        public void cardinalityTest() {
                //System.out.println("Testing cardinality computations (can take a few minutes)");
                final int N = 1024;
                for (int gap = 7; gap < 100000; gap *= 10) {
                       // System.out.println("testing cardinality with gap = "+gap);
                        for (int offset = 2; offset <= 1024; offset *= 2) {
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
                                rb.validate();
                                rb2.validate();
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
		rr.validate();
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
		rr.validate();
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
		rr.validate();
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
		rr.validate();
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
		rr.validate();
	}

	@Test
	public void andtest3() {
		int[] arrayand = new int[20000];
		int[] arrayres = new int[20000];
		int pos = 0;
		RoaringBitmap rr = new RoaringBitmap();
		for (int k = 4000; k < 4256; ++k)
			rr.add(k); 
		for (int k = 65536; k < 65536 + 4000; ++k)
			rr.add(k); 
		for (int k = 3 * 65536; k < 3 * 65536 + 1000; ++k)
			rr.add(k);
		for (int k = 3 * 65536 + 1000; k < 3 * 65536 + 7000; ++k)
			rr.add(k);
		for (int k = 3 * 65536 + 7000; k < 3 * 65536 + 9000; ++k)
			rr.add(k);
		for (int k = 4 * 65535; k < 4 * 65535 + 7000; ++k)
			rr.add(k); 
		for (int k = 6 * 65535; k < 6 * 65535 + 10000; ++k)
			rr.add(k); 
		for (int k = 8 * 65535; k < 8 * 65535 + 1000; ++k)
			rr.add(k);
		for (int k = 9 * 65535; k < 9 * 65535 + 30000; ++k)
			rr.add(k);

		RoaringBitmap rr2 = new RoaringBitmap();
		for (int k = 4000; k < 4256; ++k) {
			rr2.add(k);
			arrayand[pos++] = k;
		}
		for (int k = 65536; k < 65536 + 4000; ++k) {
			rr2.add(k);
			arrayand[pos++] = k;
		}
		for (int k = 3 * 65536 + 1000; k < 3 * 65536 + 7000; ++k) {
			rr2.add(k);
			arrayand[pos++] = k;
		}			
		for (int k = 6 * 65535; k < 6 * 65535 + 1000; ++k) {
			rr2.add(k);
			arrayand[pos++] = k;
		}
		for (int k = 7 * 65535; k < 7 * 65535 + 1000; ++k) {
			rr2.add(k);
		}
		for (int k = 10 * 65535; k < 10 * 65535 + 5000; ++k) {
			rr2.add(k);
		}

		RoaringBitmap rrand = RoaringBitmap.and(rr, rr2);
		pos = 0;
		for (int i : rrand)
			arrayres[pos++] = i;

		Assert.assertTrue(Arrays.equals(arrayand, arrayres));
		
		rr.validate();
	}

	@Test
	public void ortest3() {
	    //System.out.println("ortest3 (can take some time)");
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
				//System.out.println(" "+i);
				valide = false;
			}
			V2.add(new Integer(i));
		}
		for (int i = 0; i < V1.size(); i++)
			if (!V2.contains(vector.elementAt(i))){
				valide = false;
				//System.out.println(" "+vector.elementAt(i));
			}
		
		
		Assert.assertEquals(valide, true);
		rr.validate();
	}

	@Test
	public void xortest1() {
		HashSet<Integer> V1 = new HashSet<Integer>();
		HashSet<Integer> V2 = new HashSet<Integer>();

		RoaringBitmap rr = new RoaringBitmap();
		RoaringBitmap rr2 = new RoaringBitmap();
		//For the first 65536: rr2 has a bitmap container, and rr has an array container. 
		//We will check the union between a BitmapCintainer and an arrayContainer  
		for (int k = 0; k < 4000; ++k){
			rr2.add(k);
			if(k<3500) V1.add(new Integer(k));
		}
		for (int k = 3500; k < 4500; ++k) {
			rr.add(k);			
		}
		for (int k = 4000; k < 65000; ++k){
			rr2.add(k);
		if(k>=4500) V1.add(new Integer(k));
		}
				
		//In the second node of each roaring bitmap, we have two bitmap containers. 
		//So, we will check the union between two BitmapContainers
		for (int k = 65536; k < 65536 + 30000; ++k) {
			rr.add(k);			
		}
				
		for (int k = 65536; k < 65536 + 50000; ++k) {
			rr2.add(k);
			if(k>=65536+30000) V1.add(new Integer(k));
		}
				
		//In the 3rd node of each Roaring Bitmap, we have an ArrayContainer. So, we will try the union between two 
		//ArrayContainers. 
		for (int k = 4 * 65535; k < 4 * 65535 + 1000; ++k) {
			rr.add(k);
			if(k>=4*65535+800) V1.add(new Integer(k));			
		} 
				
		for (int k = 4 * 65535; k < 4 * 65535 + 800; ++k) {
			rr2.add(k);		
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

		RoaringBitmap rrxor = RoaringBitmap.xor(rr, rr2);
		boolean valide = true;

		// Si tous les elements de rror sont dans V1 et que tous les elements de
		// V1 sont dans rror(V2)
		// alors V1 == rror
		Object[] tab = V1.toArray();
		Vector<Integer> vector = new Vector<Integer>();
		for(int i=0; i<tab.length; i++)
			vector.add((Integer) tab[i]);		
				
		for (int i : rrxor) {
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
				
				
		Assert.assertEquals(valide, true);
		rr.validate();
	}
	
	@Test
	public void inPlaceANDtest() {
		ArrayList<Integer> arrayand = new ArrayList<Integer>();
		ArrayList<Integer> arrayres = new ArrayList<Integer>();
		RoaringBitmap rr = new RoaringBitmap();
		for (int k = 4000; k < 4256; ++k)
			rr.add(k); 
		for (int k = 65536; k < 65536 + 4000; ++k)
			rr.add(k); 
		for (int k = 3 * 65536; k < 3 * 65536 + 9000; ++k)
			rr.add(k);
		for (int k = 4 * 65535; k < 4 * 65535 + 7000; ++k)
			rr.add(k); 
		for (int k = 6 * 65535; k < 6 * 65535 + 10000; ++k)
			rr.add(k); 
		for (int k = 8 * 65535; k < 8 * 65535 + 1000; ++k)
			rr.add(k);
		for (int k = 9 * 65535; k < 9 * 65535 + 30000; ++k)
			rr.add(k);

		RoaringBitmap rr2 = new RoaringBitmap();
		for (int k = 4000; k < 4256; ++k) {
			rr2.add(k);
			arrayand.add(k);
		}
		for (int k = 65536; k < 65536 + 4000; ++k) {
			rr2.add(k);
			arrayand.add(k);
		}
		for (int k = 3 * 65536 + 2000; k < 3 * 65536 + 6000; ++k) {
			rr2.add(k);
			arrayand.add(k);
		}			
		for (int k = 6 * 65535; k < 6 * 65535 + 1000; ++k) {
			rr2.add(k);
			arrayand.add(k);
		}
		for (int k = 7 * 65535; k < 7 * 65535 + 1000; ++k) {
			rr2.add(k);
		}
		for (int k = 10 * 65535; k < 10 * 65535 + 5000; ++k) {
			rr2.add(k);
		}
		rr.inPlaceAND(rr2);
		rr.validate();
		RoaringBitmap rrand = rr;
		boolean valide = true; 
		for (int i : rrand) {
			if(!arrayand.contains(i)){
				System.out.println("inPlaceAND and : "+i);
				valide = false;
			}
			arrayres.add(i);
		}
		
		for(int i=0; i<arrayand.size(); i++){
			if(!arrayres.contains(arrayand.get(i))) { 
				System.out.println("inPLaceAND res : "+arrayand.get(i));
				valide = false;
			}
		}
		
		Assert.assertTrue(valide);
		//Assert.assertTrue(Arrays.equals(arrayand.toArray(taband), arrayres.toArray(tabres)));		
		rr.validate();
	}	
	
	@Test
	public void inPlaceORtest() {
	        //System.out.println("ortest3 (can take some time)");
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
		for (int k = 6 * 65535; k < 6 * 65535 + 6000; ++k) {
			rr.add(k);
			V1.add(new Integer(k));
		} 
				
		for (int k = 7 * 65535; k < 7 * 65535 + 2000; ++k) {
			rr2.add(k);
			V1.add(new Integer(k));
		}
		rr.inPlaceOR(rr2);
		rr.validate();
		RoaringBitmap rror = rr;
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
				//System.out.println(" "+i);
				valide = false;
			}
			V2.add(new Integer(i));
		}
		for (int i = 0; i < V1.size(); i++)
			if (!V2.contains(vector.elementAt(i))){
				valide = false;
				//System.out.println(" "+vector.elementAt(i));
			}
		
		
		Assert.assertEquals(valide, true);
		rr.validate();
	}

	@Test
	public void inPlaceXORtest() {
		HashSet<Integer> V1 = new HashSet<Integer>();
		HashSet<Integer> V2 = new HashSet<Integer>();

		RoaringBitmap rr = new RoaringBitmap();
		RoaringBitmap rr2 = new RoaringBitmap();
		//For the first 65536: rr2 has a bitmap container, and rr has an array container. 
		//We will check the union between a BitmapCintainer and an arrayContainer  
		for (int k = 0; k < 4000; ++k){
			rr2.add(k);
			if(k<3500) V1.add(new Integer(k));
		}
		for (int k = 3500; k < 4500; ++k) {
			rr.add(k);			
		}
		for (int k = 4000; k < 65000; ++k){
			rr2.add(k);
		if(k>=4500) V1.add(new Integer(k));
		}
				
		//In the second node of each roaring bitmap, we have two bitmap containers. 
		//So, we will check the union between two BitmapContainers
		for (int k = 65536; k < 65536 + 30000; ++k) {
			rr.add(k);			
		}
				
		for (int k = 65536; k < 65536 + 50000; ++k) {
			rr2.add(k);
			if(k>=65536+30000) V1.add(new Integer(k));
		}
				
		//In the 3rd node of each Roaring Bitmap, we have an ArrayContainer. So, we will try the union between two 
		//ArrayContainers. 
		for (int k = 4 * 65536; k < 4 * 65536 + 1000; ++k) {
			rr.add(k);
			if(k>=4*65536+800) V1.add(new Integer(k));			
		} 
				
		for (int k = 4 * 65536; k < 4 * 65536 + 800; ++k) {
			rr2.add(k);		
		} 

		//For the rest, we will check if the union will take them in the result
		for (int k = 6 * 65536; k < 6 * 65536 + 1000; ++k) {
			rr.add(k);
			V1.add(new Integer(k));
		} 
					
		for (int k = 7 * 65536; k < 7 * 65536 + 2000; ++k) {
			rr2.add(k);
			V1.add(new Integer(k));
		}

		rr.inPlaceXOR(rr2);
		rr.validate();
		RoaringBitmap rrxor = rr;
		boolean valide = true;

		//if V1 contains all rror(V2) elements, and rrxor(V2) contains all V1 elements
		//than V1 == rror		
		Object[] tab = V1.toArray();
		Vector<Integer> vector = new Vector<Integer>();
		for(int i=0; i<tab.length; i++)
			vector.add((Integer) tab[i]);		
				
		for (int i : rrxor) {
			if (!vector.contains(new Integer(i))) {
				System.out.println("rrxor "+i);
				valide = false;
			}
			V2.add(new Integer(i));
		}
		for (int i = 0; i < V1.size(); i++)
			if (!V2.contains(vector.elementAt(i))){
				valide = false;
				System.out.println(" "+vector.elementAt(i));
			}				
				
		Assert.assertEquals(valide, true);
		rr.validate();
	}

	@Test
	public void ContainerFactory() {
		BitmapContainer bc1, bc2, bc3;
		ArrayContainer ac1, ac2, ac3;
		
		bc1 = new BitmapContainer();
		bc2 = new BitmapContainer();
		bc3 = new BitmapContainer();
		ac1 = new ArrayContainer();
		ac2 = new ArrayContainer();
		ac3 = new ArrayContainer();
		
		for(short i=0; i<5000; i++)
			bc1.add((short)(i*70));
		for(short i=0; i<5000; i++)
			bc2.add((short)(i*70));
		for(short i=0; i<5000; i++)
			bc3.add((short)(i*70));
		
		for(short i=0; i<4000; i++)
			ac1.add((short)(i*50));
		for(short i=0; i<4000; i++)
			ac2.add((short)(i*50));
		for(short i=0; i<4000; i++)
			ac3.add((short)(i*50));
		
		BitmapContainer rbc; 
		
		rbc = ContainerFactory.transformToBitmapContainer(ac1.clone());
		Assert.assertTrue(validate(rbc, ac1));
		rbc = ContainerFactory.transformToBitmapContainer(ac2.clone());
		Assert.assertTrue(validate(rbc, ac2));
		rbc = ContainerFactory.transformToBitmapContainer(ac3.clone());
		Assert.assertTrue(validate(rbc, ac3));
	}
	
	boolean validate(BitmapContainer bc, ArrayContainer ac) {
		//Checking the cardinalities of each container
		
		if(bc.getCardinality() != ac.getCardinality()) {
		        System.out.println("cardinality differs");
		        return false;
		}
		// Checking that the two containers contain the same values
		int counter = 0;
		
			int i = bc.nextSetBit(0);
			while (i >= 0) {
				++counter;
				if(!ac.contains((short)i)){
				        System.out.println("content differs");
				        System.out.println(bc);
				        System.out.println(ac);
				        return false;
				}
				i = bc.nextSetBit(i + 1);
			}
		
		//checking the cardinality of the BitmapContainer
		if(counter!=bc.getCardinality()) return false;
		return true;
	}
}
