package org.roaringbitmap.buffer;

import org.junit.Test;

import junit.framework.Assert;

public class TestRange {

	
	@Test
	public void testFlipRanges()  {
		int N = 256;
		for(int end = 1; end < N; ++end ) {
			for(int start = 0; start< end; ++start) {
				MutableRoaringBitmap bs1 = new MutableRoaringBitmap();
				for(int k = start; k < end; ++k) {
					bs1.flip(k);
				}
				MutableRoaringBitmap bs2 = new MutableRoaringBitmap();
				bs2.flip(start, end);
				Assert.assertEquals(bs2.getCardinality(), end-start);
				Assert.assertEquals(bs1, bs2);
			}
		}
	}

	@Test
	public void testSetRanges() {
		int N = 256;
		for(int end = 1; end < N; ++end ) {
			for(int start = 0; start< end; ++start) {
				MutableRoaringBitmap bs1 = new MutableRoaringBitmap();
				for(int k = start; k < end; ++k) {
					bs1.add(k);
				}
				MutableRoaringBitmap bs2 = new MutableRoaringBitmap();
				bs2.add(start, end);
				Assert.assertEquals(bs1, bs2);
			}
		}
	}
	

	@Test
	public void testClearRanges()  {
		int N = 16;
		for(int end = 1; end < N; ++end ) {
			for(int start = 0; start< end; ++start) {
				MutableRoaringBitmap bs1 = new MutableRoaringBitmap();
				bs1.add(0, N);
				for(int k = start; k < end; ++k) {
					bs1.remove(k);
				}
				MutableRoaringBitmap bs2 = new MutableRoaringBitmap();
				bs2.add(0, N);
				System.out.println("start "+start);
				System.out.println("end "+end);

				bs2.remove(start, end);
				System.out.println("bs2="+bs2);
				Assert.assertEquals(bs1, bs2);
			}
		}
	}

}
