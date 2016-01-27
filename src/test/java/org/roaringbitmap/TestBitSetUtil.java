package org.roaringbitmap;

import static org.junit.Assert.*;

import java.util.BitSet;
import java.util.Random;

import org.junit.Test;

public class TestBitSetUtil {
	@Test
	public void testEmptyBitSet() {
		final BitSet bitset = new BitSet();
		final RoaringBitmap bitmap = BitSetUtil.bitmapOf(bitset);
		assertEqualBitsets(bitset, bitmap);
	}

	@Test
	public void testFullBitSet() {
		final BitSet bitset = new BitSet();
		bitset.set(0, BitSetUtil.CHUNK_SIZE * 10);
		final RoaringBitmap bitmap = BitSetUtil.bitmapOf(bitset);
		assertEqualBitsets(bitset, bitmap);
	}

	@Test
	public void testRandomBitmap() {
		final Random random = new Random();
		for (int i = 0; i < 10; i++) {
			final BitSet bitset = randomBitset(random, random.nextInt(BitSetUtil.CHUNK_SIZE * 10));
			final RoaringBitmap bitmap = BitSetUtil.bitmapOf(bitset);
			assertEqualBitsets(bitset, bitmap);
		}
	}

	private void assertEqualBitsets(final BitSet bitset, final RoaringBitmap bitmap) {
		assertEquals(bitset.cardinality(), bitmap.getCardinality());
		final IntIterator it = bitmap.getIntIterator();
		while (it.hasNext()) {
			assertTrue(bitset.get(it.next()));
		}
	}

	private static BitSet randomBitset(final Random random, final int length) {
		final BitSet bitset = new BitSet();
		for (int i = 0; i < length; i++) {
			bitset.set(i, random.nextBoolean());
		}
		return bitset;
	}
}
