package org.roaringbitmap;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * This test verifies behaviour of {@link FastRankRoaringBitmap}.
 *
 * @author Jan Novotný (novotny@fg.cz)
 */
class FastRankRoaringBitmapTest {

	@Test
	void issue498_cache_isResettedEvenIfRankIsCalledFirst() {
		FastRankRoaringBitmap rankRoaringBitmap = new FastRankRoaringBitmap();
		assertEquals(0, rankRoaringBitmap.rank(3));
		rankRoaringBitmap.add(3);
		rankRoaringBitmap.add(5);
		assertEquals(3, rankRoaringBitmap.select(0));
		assertEquals(5, rankRoaringBitmap.select(1));
	}

}