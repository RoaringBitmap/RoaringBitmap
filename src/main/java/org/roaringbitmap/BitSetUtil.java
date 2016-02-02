package org.roaringbitmap;

import java.util.Arrays;
import java.util.BitSet;

public class BitSetUtil {
	// a block consists has a maximum of 1024 words, each representing 64 bits, thus representing at maximum 65536 bits
	static final int BLOCK_LENGTH = 1024; 

	/**
	 * Generate RoaringBitmap out of the bitSet
	 * 
	 * @param bitSet
	 * @return roaring bitmap
	 */
	static RoaringBitmap bitmapOf(final BitSet bitSet) {
		final int cardinality = bitSet.cardinality();
		if (cardinality == 0) {
			return new RoaringBitmap();
		} else if (cardinality <= ArrayContainer.DEFAULT_MAX_SIZE) {
			// if it is a tiny bitSet, we can directly build a arrayContainer, without an extra bitSet#toLongArray
			final RoaringBitmap bitmap = new RoaringBitmap();
			bitmap.highLowContainer.append((short)0, arrayContainerOf(cardinality, bitSet));
			return bitmap;
		} else {
			return bitmapOf(cardinality, bitSet.toLongArray());
		}
	}

	/**
	 * Generate RoaringBitmap out of a long[], each long using little-endian representation of its bits 
	 * 
	 * @see BitSet#toLongArray() for an equivalent
	 * @param bitset
	 * @return roaring bitmap
	 */
    static RoaringBitmap bitmapOf(final long[] words) {
		final int overallCardinality = cardinality(0, words.length, words);
		if (overallCardinality == 0) {
			return new RoaringBitmap();
		} else {
			return bitmapOf(overallCardinality, words);
		}
    }
    
	private static RoaringBitmap bitmapOf(final int cardinality, final long[] words) {
		// split long[] into blocks. 
		// each block becomes a single container, if any bit is set
		final RoaringBitmap ans = new RoaringBitmap();
		int containerIndex = 0;
		int currentCardinality = 0;
		
		// for each block
		// (unless we already have reached overall cardinality, e.g. when long[] is way longer and has no bits set, then we can skip everything else)
		for (int from = 0; from < words.length && currentCardinality < cardinality; from += BLOCK_LENGTH) {
			final int to = Math.min(from + BLOCK_LENGTH, words.length);
			final int blockCardinality = cardinality(from, to, words);
			if (blockCardinality > 0) { 
				ans.highLowContainer.insertNewKeyValueAt(containerIndex++, Util.highbits(from * Long.SIZE), BitSetUtil.containerOf(from, to, blockCardinality, words));
				currentCardinality += blockCardinality;
			}
		}
		return ans;
	}

	private static Container containerOf(final int from, final int to, final int blockCardinality, final long[] words) {
		// find the best container available
		if (blockCardinality <= ArrayContainer.DEFAULT_MAX_SIZE) {
			// containers with DEFAULT_MAX_SIZE or less integers should be ArrayContainers
			return arrayContainerOf(from, to, blockCardinality, words);
		} else {
			// otherwise use bitmap container, which is kinda the same as Bitset
			// ideas for the future: 
			// - if blockCardinality is rather high, then runlength encoding via RunContainer might be the better choice) 
			// - (maybe there is also another way of recognizing high amount of repetition? i would rather scan the words once in the beginning and save memory at the end?)	
			return new BitmapContainer(Arrays.copyOfRange(words, from, to), blockCardinality);
		}
	}

	private static ArrayContainer arrayContainerOf(final int from, final int to, final int cardinality, final long[] words) {
		// precondition: cardinality is max 4096
		final short[] content = new short[cardinality];
		int index = 0;
						
		// for each word, unless we already have reached cardinality
		long word = 0;
		for (int i = from, socket = 0; i < to && index < cardinality; i++, socket += Long.SIZE) { 
			if (words[i] == 0) continue;
			
			// for each bit, unless updated word has become 0 (no more bits left) or we already have reached cardinality
			word = words[i];
			for (int bitIndex = 0; word != 0 && bitIndex < Long.SIZE && index < cardinality; word >>>= 1, bitIndex++) {
				// TODO: does it make sense to start with (0+trailing zeros) or not worth the effort? 
				// (what could be the fastest way to iterate through the long?)
				if ((word & 1l) != 0) {
					content[index++] = (short)(socket + bitIndex);
				}
			}
		}
		return new ArrayContainer(content);
	}

	private static ArrayContainer arrayContainerOf(final int cardinality, final BitSet bs) {
		final short[] content = new short[cardinality];
		int index = 0;
	    for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i+1)) {
	             // operate on index i here
	             if (i == Integer.MAX_VALUE) {
	                 break; // or (i+1) would overflow
	             }
	             content[index++] = (short)i;
	    }
	    return new ArrayContainer(content);
	};
	

	private static int cardinality(final int from, final int to, final long[] words) {
		int sum = 0;
		for (int i = from; i < to; i++) {
			sum += Long.bitCount(words[i]);
		}
		return sum;
	}
}
