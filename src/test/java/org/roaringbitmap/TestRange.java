package org.roaringbitmap;



import static org.junit.Assert.*;

import org.junit.Test;


public class TestRange {
	

	
	@Test
	public void testFlipRanges()  {
		int N = 256;
		for(int end = 1; end < N; ++end ) {
			for(int start = 0; start< end; ++start) {
				RoaringBitmap bs1 = new RoaringBitmap();
				for(int k = start; k < end; ++k) {
					bs1.flip(k);
				}
				RoaringBitmap bs2 = new RoaringBitmap();
				bs2.flip(start, end);
				assertEquals(bs2.getCardinality(), end-start);
				assertEquals(bs1, bs2);
			}
		}
	}

	@Test
	public void testSetRanges() {
		int N = 256;
		for(int end = 1; end < N; ++end ) {
			for(int start = 0; start< end; ++start) {
				RoaringBitmap bs1 = new RoaringBitmap();
				for(int k = start; k < end; ++k) {
					bs1.add(k);
				}
				RoaringBitmap bs2 = new RoaringBitmap();
				bs2.add(start, end);
				assertEquals(bs1, bs2);
			}
		}
	}
	

	@Test
	public void testClearRanges()  {
		int N = 16;
		for(int end = 1; end < N; ++end ) {
			for(int start = 0; start< end; ++start) {
				RoaringBitmap bs1 = new RoaringBitmap();
				bs1.add(0, N);
				for(int k = start; k < end; ++k) {
					bs1.remove(k);
				}
				RoaringBitmap bs2 = new RoaringBitmap();
				bs2.add(0, N);
				System.out.println("start "+start);
				System.out.println("end "+end);

				bs2.remove(start, end);
				System.out.println("bs2="+bs2);
				assertEquals(bs1, bs2);
			}
		}
	}

}
