package org.roaringbitmap;

import java.util.BitSet;

public class BitSetUtil {
	static final int CHUNK_SIZE = 1 << 16;

	/**
	 * Generate RoaringBitmap out of the given bitSet
	 * 
	 * @param bitset
	 * @return bitmap
	 */
	static RoaringBitmap bitmapOf(final BitSet bitset) {
		// dont make method public, so other parties wont use it.
		// if daniel likes it, he will put it to RoaringBitmap :>
		final int bitsetLength = bitset.length();
		if (bitsetLength == 0) {
			return new RoaringBitmap();
		}

		final RoaringBitmap ans = new RoaringBitmap();

		// divide bitset in 1<<16 chunks
		final int chunkSize = 1 << 16;
		for (int offset = 0; offset < bitsetLength; offset += chunkSize) {
			// avoid empty container creation, if there is no bit in range anyways
			if (BitSetUtil.any(offset, chunkSize, bitset)) {
				final Container container = BitSetUtil.containerOf(offset, chunkSize, bitset);

				// it is always the first container, it never replaces or expands an existing one
				// => index is always negative, no extra check needed
				final short hb = Util.highbits(offset);
				final int i = ans.highLowContainer.getIndex(hb);
				ans.highLowContainer.insertNewKeyValueAt(-i - 1, hb, container);
			}
		}
		return ans;
	}

	private static Container containerOf(final int offset, final int length, final BitSet bitSet) {
		// TODO: find a good way to decide between arraycontainer/bitmapcontainer and runcontainer, without using lots of allocations
		final int cardinality = cardinality(offset, length, bitSet);
		if (cardinality <= ArrayContainer.DEFAULT_MAX_SIZE) {
			// containers with DEFAULT_MAX_SZE or less integers should be ArrayContainers
			return arrayContainerOf(offset, length, cardinality, bitSet);
		}

		final int runs = BitSetUtil.nbrRuns(offset, length, bitSet);
		if (runs == 0) {
			return new RunContainer();
		}
		return runContainerOf(offset, length, runs, bitSet);
	}

	private static ArrayContainer arrayContainerOf(final int offset, final int length, final int cardinality, final BitSet bs) {
		// content is sorted, as BitSet is already sorted  
		final short[] content = new short[cardinality];
		int j = 0;
		for (int i = bs.nextSetBit(offset), offsetLength = offset + length; i >= 0 && i < offsetLength; i = bs.nextSetBit(i + 1)) {
			content[j++] = (short) (i - offset);
		}
		return new ArrayContainer(content);
	}

	private static RunContainer runContainerOf(final int offset, final int length, final int nbrRuns, final BitSet bs) {
		if (nbrRuns == 0) {
			return new RunContainer(new short[0], 0);
		}

		short[] valueslength = new short[2 * nbrRuns];

		int index = -1;
		int runLength = 0;
		boolean finish = false;
		int runs = 0;
		for (int i = bs.nextSetBit(offset), offsetLength = offset + length; i >= 0 && i < offsetLength; i = bs.nextSetBit(i + 1)) {
			// operate on index i here
			if (i == Integer.MAX_VALUE) {
				if (index != -1) {
					valueslength[runs * 2] = (short) (index - offset);
					valueslength[runs * 2 + 1] = (short) runLength;
					runs++;
				}
				finish = true;
				break; // or (i+1) would overflow
			}

			if (index == -1) {
				index = i;
				runLength = 0;
			} else {
				if (index + runLength + 1 == i) {
					runLength++;
				} else {
					valueslength[runs * 2] = (short) (index - offset);
					valueslength[runs * 2 + 1] = (short) runLength;
					runs++;

					index = i;
					runLength = 0;
				}
			}
		}

		if (index != -1 && !finish) {
			valueslength[runs * 2] = (short) (index - offset);
			valueslength[runs * 2 + 1] = (short) runLength;
			runs++;
		}

		return new RunContainer(valueslength, nbrRuns);
	}

	/**
	 * How many runs of consecutive values are necessary to represents the set
	 * bits in the range of the bitSet
	 * 
	 * @param offset
	 * @param length
	 * @param bitSet
	 * @return nbrruns
	 */
	private static int nbrRuns(final int offset, final int length, final BitSet bitSet) {
		int index = -1;
		int runLength = 0;
		boolean finish = false;
		int runs = 0;
		for (int i = bitSet.nextSetBit(offset), offsetLength = offset + length; i >= 0 && i < offsetLength; i = bitSet.nextSetBit(i + 1)) {
			// operate on index i here
			if (i == Integer.MAX_VALUE) {
				if (index != -1) {
					runs++;
				}
				finish = true;
				break; // or (i+1) would overflow
			}

			if (index == -1) {
				index = i;
				runLength = 0;
			} else {
				if (index + runLength + 1 == i) {
					runLength++;
				} else {
					runs++;
					index = i;
					runLength = 0;
				}
			}
		}

		if (index != -1 && !finish) {
			runs++;
		}
		return runs;
	}

	/**
	 * Returns true, if any bit is set in range of the bitSet
	 * 
	 * @param offset
	 * @param length
	 * @param bitSet
	 * @return true, if any bit is set in range of the bitSet
	 */
	private static boolean any(final int offset, final int length, final BitSet bitSet) {
		final int index = bitSet.nextSetBit(offset);
		return index >= 0 && index < offset + length;
	}

	/**
	 * Counts set bits in the range of the bitSet
	 * 
	 * @param offset
	 * @param length
	 * @param bitSet
	 * @return counted set bits in the range of the bitSet
	 */
	private static int cardinality(final int offset, final int length, final BitSet bitSet) {
		int cardinality = 0;
		for (int i = bitSet.nextSetBit(offset), offsetLength = offset + length; i >= 0 && i < offsetLength; i = bitSet.nextSetBit(i + 1)) {
			if (i == Integer.MAX_VALUE) {
				break; // or (i+1) would overflow
			}
			cardinality++;
		}
		return cardinality;
	}
}
