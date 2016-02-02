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
		final int nbits = BitSetUtil.BLOCK_LENGTH * Long.SIZE * 50;
		bitset.set(0, nbits);
		final RoaringBitmap bitmap = BitSetUtil.bitmapOf(bitset);
		assertEqualBitsets(bitset, bitmap);
	}
	
	@Test
	public void testFlipFlapBetweenRandomFullAndEmptyBitSet() {		
		final Random random = new Random();	
		final int nbitsPerBlock = BitSetUtil.BLOCK_LENGTH * Long.SIZE;
		final int blocks = 50;
		final BitSet bitset = new BitSet(nbitsPerBlock*blocks);
		
		// i want a mix of empty blocks, randomly filled blocks and full blocks
		for (int block = 0; block < blocks * nbitsPerBlock; block+=nbitsPerBlock) {
			int type = random.nextInt(3);
			switch(type) {
				case 0:
					// a block with random set bits
					appendRandomBitset(random, block, bitset, nbitsPerBlock);
					break;
				case 1:
					// a full block
					bitset.set(block, block+nbitsPerBlock);
					break;
				default:
					// and an empty block; 
					break;
			}
		}
		final RoaringBitmap bitmap = BitSetUtil.bitmapOf(bitset);
		assertEqualBitsets(bitset, bitmap);
	}

	@Test
	public void testRandomBitmap() {
		final Random random = new Random();
		final int runs = 500;
		final int maxNbits = 500000;
		for (int i = 0; i < runs; i++) {
			final BitSet bitset = randomBitset(random, random.nextInt(maxNbits));
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

	private static BitSet appendRandomBitset(final Random random, final int offset, final BitSet bitset, final int nbits) {
		for (int i = 0; i < nbits; i++) {
			bitset.set(offset+i, random.nextBoolean());
		}
		return bitset;
	}
	private static BitSet randomBitset(final Random random, final int length) {
		final BitSet bitset = new BitSet();
		return appendRandomBitset(random, 0, bitset, length); 
	}

}
