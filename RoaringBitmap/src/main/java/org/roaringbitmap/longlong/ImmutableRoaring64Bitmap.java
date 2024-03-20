/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */
package org.roaringbitmap.longlong;

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

import org.roaringbitmap.BitmapDataProvider;
import org.roaringbitmap.ImmutableBitmapDataProvider;
import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

public class ImmutableRoaring64Bitmap implements ImmutableLongBitmapDataProvider {

	public ImmutableRoaring64Bitmap(RandomAccessFile raf) throws IOException {

		try (FileChannel channel = raf.getChannel();
				InputStream is = Channels.newInputStream(channel);
				DataInputStream dis = new DataInputStream(is)) {
			boolean signedLongs = dis.readBoolean();

			int nbHighs = dis.readInt();
			if (signedLongs) {
				highToBitmap = new TreeMap<>();
			} else {
				highToBitmap = new TreeMap<>(RoaringIntPacking.unsignedComparator());
			}

			for (int i = 0; i < nbHighs; i++) {
				int high = dis.readInt();
				long startposition = raf.getFilePointer();

				MutableRoaringBitmap rb = new MutableRoaringBitmap();
				rb.deserialize(dis);
				int size = rb.serializedSizeInBytes();
				assert size > 0;
				long endPosition = raf.getFilePointer();
				assert size == endPosition - startposition;

				highToBitmap.put(high, new ImmutableRoaringBitmap(openByteBuffer(channel, startposition, size)));
			}

		}
		resetPerfHelpers();
	}

	/**
	 * @throws IllegalArgumentException if the
	 */
	private MappedByteBuffer openByteBuffer(FileChannel channel, long startPosition, int size)
			throws IOException, IllegalArgumentException {
		return channel.map(MapMode.READ_ONLY, startPosition, size);
	}

	// Not final to enable initialization in Externalizable.readObject
	private NavigableMap<Integer, ImmutableBitmapDataProvider> highToBitmap;

	// If true, we handle longs a plain java longs: -1 if right before 0
	// If false, we handle longs as unsigned longs: 0 has no predecessor and
	// Long.MAX_VALUE + 1L is
	// expressed as a
	// negative long
	private boolean signedLongs = false;

	// By default, we cache cardinalities
	private transient boolean doCacheCardinalities = true;

	// Prevent recomputing all cardinalities when requesting consecutive ranks
	private transient int firstHighNotValid = highestHigh() + 1;

	// This boolean needs firstHighNotValid == Integer.MAX_VALUE to be allowed to be
	// true
	// If false, it means nearly all cumulated cardinalities are valid, except
	// high=Integer.MAX_VALUE
	// If true, it means all cumulated cardinalities are valid, even
	// high=Integer.MAX_VALUE
	private transient boolean allValid = false;

	// TODO: I would prefer not managing arrays myself
	private transient long[] sortedCumulatedCardinality = new long[0];
	private transient int[] sortedHighs = new int[0];

	// We guess consecutive .addLong will be on proximate longs: we remember the
	// bitmap attached to
	// this bucket in order
	// to skip the indirection
	private transient Map.Entry<Integer, BitmapDataProvider> latestAddedHigh = null;

	private void resetPerfHelpers() {
		firstHighNotValid = RoaringIntPacking.highestHigh(signedLongs) + 1;
		allValid = false;

		sortedCumulatedCardinality = new long[0];
		sortedHighs = new int[0];

		latestAddedHigh = null;
	}

	// Package-friendly: for the sake of unit-testing
	// @VisibleForTesting
	NavigableMap<Integer, ImmutableBitmapDataProvider> getHighToBitmap() {
		return highToBitmap;
	}

	// Package-friendly: for the sake of unit-testing
	// @VisibleForTesting
	int getLowestInvalidHigh() {
		return firstHighNotValid;
	}

	// Package-friendly: for the sake of unit-testing
	// @VisibleForTesting
	long[] getSortedCumulatedCardinality() {
		return sortedCumulatedCardinality;
	}

	private int compare(int x, int y) {
		if (signedLongs) {
			return Integer.compare(x, y);
		} else {
			return RoaringIntPacking.compareUnsigned(x, y);
		}
	}

	/**
	 * Returns the number of distinct integers added to the bitmap (e.g., number of
	 * bits set).
	 *
	 * @return the cardinality
	 */
	@Override
	public long getLongCardinality() {
		if (doCacheCardinalities) {
			if (highToBitmap.isEmpty()) {
				return 0L;
			}
			int indexOk = ensureCumulatives(highestHigh());

			// ensureCumulatives may have removed empty bitmaps
			if (highToBitmap.isEmpty()) {
				return 0L;
			}

			return sortedCumulatedCardinality[indexOk - 1];
		} else {
			long cardinality = 0L;
			for (ImmutableBitmapDataProvider bitmap : highToBitmap.values()) {
				cardinality += bitmap.getLongCardinality();
			}
			return cardinality;
		}
	}

	/**
	 * 
	 * @return the cardinality as an int
	 * 
	 * @throws UnsupportedOperationException if the cardinality does not fit in an
	 *                                       int
	 */
	public int getIntCardinality() throws UnsupportedOperationException {
		long cardinality = getLongCardinality();

		if (cardinality > Integer.MAX_VALUE) {
			// TODO: we should handle cardinality fitting in an unsigned int
			throw new UnsupportedOperationException(
					"Cannot call .getIntCardinality as the cardinality is bigger than Integer.MAX_VALUE");
		}

		return (int) cardinality;
	}

	/**
	 * Return the jth value stored in this bitmap.
	 *
	 * @param j index of the value
	 *
	 * @return the value
	 * @throws IllegalArgumentException if j is out of the bounds of the bitmap
	 *                                  cardinality
	 */
	@Override
	public long select(final long j) throws IllegalArgumentException {
		if (!doCacheCardinalities) {
			return selectNoCache(j);
		}

		// Ensure all cumulatives as we we have straightforward way to know in advance
		// the high of the
		// j-th value
		int indexOk = ensureCumulatives(highestHigh());

		if (highToBitmap.isEmpty()) {
			return throwSelectInvalidIndex(j);
		}

		// Use normal binarySearch as cardinality does not depends on considering longs
		// signed or
		// unsigned
		// We need sortedCumulatedCardinality not to contain duplicated, else
		// binarySearch may return
		// any of the duplicates: we need to ensure it holds no high associated to an
		// empty bitmap
		int position = Arrays.binarySearch(sortedCumulatedCardinality, 0, indexOk, j);

		if (position >= 0) {
			if (position == indexOk - 1) {
				// .select has been called on this.getCardinality
				return throwSelectInvalidIndex(j);
			}

			// There is a bucket leading to this cardinality: the j-th element is the first
			// element of
			// next bucket
			int high = sortedHighs[position + 1];
			ImmutableBitmapDataProvider nextBitmap = highToBitmap.get(high);
			return RoaringIntPacking.pack(high, nextBitmap.select(0));
		} else {
			// There is no bucket with this cardinality
			int insertionPoint = -position - 1;

			final long previousBucketCardinality;
			if (insertionPoint == 0) {
				previousBucketCardinality = 0L;
			} else if (insertionPoint >= indexOk) {
				return throwSelectInvalidIndex(j);
			} else {
				previousBucketCardinality = sortedCumulatedCardinality[insertionPoint - 1];
			}

			// We get a 'select' query for a single bitmap: should fit in an int
			final int givenBitmapSelect = (int) (j - previousBucketCardinality);

			int high = sortedHighs[insertionPoint];
			ImmutableBitmapDataProvider lowBitmap = highToBitmap.get(high);
			int low = lowBitmap.select(givenBitmapSelect);

			return RoaringIntPacking.pack(high, low);
		}
	}

	// For benchmarks: compute without using cardinalities cache
	// https://github.com/RoaringBitmap/CRoaring/blob/master/cpp/roaring64map.hh
	private long selectNoCache(long j) {
		long left = j;

		for (Entry<Integer, ImmutableBitmapDataProvider> entry : highToBitmap.entrySet()) {
			long lowCardinality = entry.getValue().getCardinality();

			if (left >= lowCardinality) {
				left -= lowCardinality;
			} else {
				// It is legit for left to be negative
				int leftAsUnsignedInt = (int) left;
				return RoaringIntPacking.pack(entry.getKey(), entry.getValue().select(leftAsUnsignedInt));
			}
		}

		return throwSelectInvalidIndex(j);
	}

	private long throwSelectInvalidIndex(long j) {
		// see org.roaringbitmap.buffer.ImmutableRoaringBitmap.select(int)
		throw new IllegalArgumentException("select " + j + " when the cardinality is " + this.getLongCardinality());
	}

	/**
	 * For better performance, consider the Use the {@link #forEach forEach} method.
	 *
	 * @return a custom iterator over set bits, the bits are traversed in ascending
	 *         sorted order
	 */
	public Iterator<Long> iterator() {
		final LongIterator it = getLongIterator();

		return new Iterator<Long>() {

			@Override
			public boolean hasNext() {
				return it.hasNext();
			}

			@Override
			public Long next() {
				return it.next();
			}

			@Override
			public void remove() {
				// TODO?
				throw new UnsupportedOperationException();
			}
		};
	}

	@Override
	public void forEach(final LongConsumer lc) {
		for (final Entry<Integer, ImmutableBitmapDataProvider> highEntry : highToBitmap.entrySet()) {
			highEntry.getValue().forEach(new IntConsumer() {

				@Override
				public void accept(int low) {
					lc.accept(RoaringIntPacking.pack(highEntry.getKey(), low));
				}
			});
		}
	}

	@Override
	public long rankLong(long id) {
		int high = RoaringIntPacking.high(id);
		int low = RoaringIntPacking.low(id);

		if (!doCacheCardinalities) {
			return rankLongNoCache(high, low);
		}

		int indexOk = ensureCumulatives(high);

		int highPosition = binarySearch(sortedHighs, 0, indexOk, high);

		if (highPosition >= 0) {
			// There is a bucket holding this item

			final long previousBucketCardinality;
			if (highPosition == 0) {
				previousBucketCardinality = 0;
			} else {
				previousBucketCardinality = sortedCumulatedCardinality[highPosition - 1];
			}

			ImmutableBitmapDataProvider lowBitmap = highToBitmap.get(sortedHighs[highPosition]);

			// Rank is previous cardinality plus rank in current bitmap
			return previousBucketCardinality + lowBitmap.rankLong(low);
		} else {
			// There is no bucket holding this item: insertionPoint is previous bitmap
			int insertionPoint = -highPosition - 1;

			if (insertionPoint == 0) {
				// this key is before all inserted keys
				return 0;
			} else {
				// The rank is the cardinality of this previous bitmap
				return sortedCumulatedCardinality[insertionPoint - 1];
			}
		}
	}

	// https://github.com/RoaringBitmap/CRoaring/blob/master/cpp/roaring64map.hh
	private long rankLongNoCache(int high, int low) {
		long result = 0L;

		ImmutableBitmapDataProvider lastBitmap = highToBitmap.get(high);
		if (lastBitmap == null) {
			// There is no value with same high: the rank is a sum of cardinalities
			for (Entry<Integer, ImmutableBitmapDataProvider> bitmap : highToBitmap.entrySet()) {
				if (bitmap.getKey().intValue() > high) {
					break;
				} else {
					result += bitmap.getValue().getLongCardinality();
				}
			}
		} else {
			for (ImmutableBitmapDataProvider bitmap : highToBitmap.values()) {
				if (bitmap == lastBitmap) {
					result += bitmap.rankLong(low);
					break;
				} else {
					result += bitmap.getLongCardinality();
				}
			}
		}

		return result;
	}

	/**
	 * 
	 * @param high for which high bucket should we compute the cardinality
	 * @return the highest validatedIndex
	 */
	protected int ensureCumulatives(int high) {
		if (allValid) {
			// the whole array is valid (up-to its actual length, not its capacity)
			return highToBitmap.size();
		} else if (compare(high, firstHighNotValid) < 0) {
			// The high is strictly below the first not valid: it is valid

			// sortedHighs may have only a subset of valid values on the right. However,
			// these invalid
			// values have been set to maxValue, and we are here as high < firstHighNotValid
			// ==> high <
			// maxHigh()
			int position = binarySearch(sortedHighs, high);

			if (position >= 0) {
				// This high has a bitmap: +1 as this index will be used as right (excluded)
				// bound in a
				// binary-search
				return position + 1;
			} else {
				// This high has no bitmap: it could be between 2 highs with bitmaps
				int insertionPosition = -position - 1;
				return insertionPosition;
			}
		} else {

			// For each deprecated buckets
			SortedMap<Integer, ImmutableBitmapDataProvider> tailMap = highToBitmap.tailMap(firstHighNotValid, true);

			// TODO .size on tailMap make an iterator: arg
			int indexOk = highToBitmap.size() - tailMap.size();

			// TODO: It should be possible to compute indexOk based on sortedHighs array
			// assert indexOk == binarySearch(sortedHighs, firstHighNotValid);

			Iterator<Entry<Integer, ImmutableBitmapDataProvider>> it = tailMap.entrySet().iterator();
			while (it.hasNext()) {
				Entry<Integer, ImmutableBitmapDataProvider> e = it.next();
				int currentHigh = e.getKey();

				if (compare(currentHigh, high) > 0) {
					// No need to compute more than needed
					break;
				} else if (e.getValue().isEmpty()) {
					// highToBitmap cannot be modified as we iterate over it
					if (latestAddedHigh != null && latestAddedHigh.getKey().intValue() == currentHigh) {
						// Dismiss the cached bitmap as it is removed from the NavigableMap
						latestAddedHigh = null;
					}
					it.remove();
				} else {
					ensureOne(e, currentHigh, indexOk);

					// We have added one valid cardinality
					indexOk++;
				}

			}

			if (highToBitmap.isEmpty() || indexOk == highToBitmap.size()) {
				// We have compute all cardinalities
				allValid = true;
			}

			return indexOk;
		}
	}

	private int binarySearch(int[] array, int key) {
		if (signedLongs) {
			return Arrays.binarySearch(array, key);
		} else {
			return unsignedBinarySearch(array, 0, array.length, key, RoaringIntPacking.unsignedComparator());
		}
	}

	private int binarySearch(int[] array, int from, int to, int key) {
		if (signedLongs) {
			return Arrays.binarySearch(array, from, to, key);
		} else {
			return unsignedBinarySearch(array, from, to, key, RoaringIntPacking.unsignedComparator());
		}
	}

	// From Arrays.binarySearch (Comparator). Check with
	// org.roaringbitmap.Util.unsignedBinarySearch
	private static int unsignedBinarySearch(int[] a, int fromIndex, int toIndex, int key,
			Comparator<? super Integer> c) {
		int low = fromIndex;
		int high = toIndex - 1;

		while (low <= high) {
			int mid = (low + high) >>> 1;
			int midVal = a[mid];
			int cmp = c.compare(midVal, key);
			if (cmp < 0) {
				low = mid + 1;
			} else if (cmp > 0) {
				high = mid - 1;
			} else {
				return mid; // key found
			}
		}
		return -(low + 1); // key not found.
	}

	private void ensureOne(Map.Entry<Integer, ImmutableBitmapDataProvider> e, int currentHigh, int indexOk) {
		// sortedHighs are valid only up to some index
		assert indexOk <= sortedHighs.length : indexOk + " is bigger than " + sortedHighs.length;

		final int index;
		if (indexOk == 0) {
			if (sortedHighs.length == 0) {
				index = -1;
				// } else if (sortedHighs[0] == currentHigh) {
				// index = 0;
			} else {
				index = -1;
			}
		} else if (indexOk < sortedHighs.length) {
			index = -indexOk - 1;
		} else {
			index = -sortedHighs.length - 1;
		}
		assert index == binarySearch(sortedHighs, 0, indexOk, currentHigh) : "Computed " + index
				+ " differs from dummy binary-search index: " + binarySearch(sortedHighs, 0, indexOk, currentHigh);

		if (index >= 0) {
			// This would mean calling .ensureOne is useless: should never got here at the
			// first time
			throw new IllegalStateException("Unexpectedly found " + currentHigh + " in " + Arrays.toString(sortedHighs)
					+ " strictly before index" + indexOk);
		} else {
			int insertionPosition = -index - 1;

			// This is a new key
			if (insertionPosition >= sortedHighs.length) {
				int previousSize = sortedHighs.length;

				// TODO softer growing factor
				int newSize = Math.min(Integer.MAX_VALUE, sortedHighs.length * 2 + 1);

				// Insertion at the end
				sortedHighs = Arrays.copyOf(sortedHighs, newSize);
				sortedCumulatedCardinality = Arrays.copyOf(sortedCumulatedCardinality, newSize);

				// Not actually needed. But simplify the reading of array content
				Arrays.fill(sortedHighs, previousSize, sortedHighs.length, highestHigh());
				Arrays.fill(sortedCumulatedCardinality, previousSize, sortedHighs.length, Long.MAX_VALUE);
			}
			sortedHighs[insertionPosition] = currentHigh;

			final long previousCardinality;
			if (insertionPosition >= 1) {
				previousCardinality = sortedCumulatedCardinality[insertionPosition - 1];
			} else {
				previousCardinality = 0;
			}

			sortedCumulatedCardinality[insertionPosition] = previousCardinality + e.getValue().getLongCardinality();

			if (currentHigh == highestHigh()) {
				// We are already on the highest high. Do not set allValid as it is set anyway
				// out of the
				// loop
				firstHighNotValid = currentHigh;
			} else {
				// The first not valid is the next high
				// TODO: The entry comes from a NavigableMap: it may be quite cheap to know the
				// next high
				firstHighNotValid = currentHigh + 1;
			}
		}
	}

	private int highestHigh() {
		return RoaringIntPacking.highestHigh(signedLongs);
	}

	/**
	 * A string describing the bitmap.
	 *
	 * @return the string
	 */
	@Override
	public String toString() {
		final StringBuilder answer = new StringBuilder();
		final LongIterator i = this.getLongIterator();
		answer.append("{");
		if (i.hasNext()) {
			if (signedLongs) {
				answer.append(i.next());
			} else {
				answer.append(RoaringIntPacking.toUnsignedString(i.next()));
			}
		}
		while (i.hasNext()) {
			answer.append(",");
			// to avoid using too much memory, we limit the size
			if (answer.length() > 0x80000) {
				answer.append("...");
				break;
			}
			if (signedLongs) {
				answer.append(i.next());
			} else {
				answer.append(RoaringIntPacking.toUnsignedString(i.next()));
			}

		}
		answer.append("}");
		return answer.toString();
	}

	/**
	 *
	 * For better performance, consider the Use the {@link #forEach forEach} method.
	 *
	 * @return a custom iterator over set bits, the bits are traversed in ascending
	 *         sorted order
	 */
	@Override
	public LongIterator getLongIterator() {
		final Iterator<Map.Entry<Integer, ImmutableBitmapDataProvider>> it = highToBitmap.entrySet().iterator();

		return toIterator(it, false);
	}

	protected LongIterator toIterator(final Iterator<Map.Entry<Integer, ImmutableBitmapDataProvider>> it,
			final boolean reversed) {
		return new LongIterator() {

			protected int currentKey;
			protected IntIterator currentIt;

			@Override
			public boolean hasNext() {
				if (currentIt == null) {
					// Were initially empty
					if (!moveToNextEntry(it)) {
						return false;
					}
				}

				while (true) {
					if (currentIt.hasNext()) {
						return true;
					} else {
						if (!moveToNextEntry(it)) {
							return false;
						}
					}
				}
			}

			/**
			 *
			 * @param it the underlying iterator which has to be moved to next long
			 * @return true if we MAY have more entries. false if there is definitely
			 *         nothing more
			 */
			private boolean moveToNextEntry(Iterator<Map.Entry<Integer, ImmutableBitmapDataProvider>> it) {
				if (it.hasNext()) {
					Map.Entry<Integer, ImmutableBitmapDataProvider> next = it.next();
					currentKey = next.getKey();
					if (reversed) {
						currentIt = next.getValue().getReverseIntIterator();
					} else {
						currentIt = next.getValue().getIntIterator();
					}

					// We may have more long
					return true;
				} else {
					// We know there is nothing more
					return false;
				}
			}

			@Override
			public long next() {
				if (hasNext()) {
					return RoaringIntPacking.pack(currentKey, currentIt.next());
				} else {
					throw new IllegalStateException("empty");
				}
			}

			@Override
			public LongIterator clone() {
				throw new UnsupportedOperationException("TODO");
			}
		};
	}

	@Override
	public boolean contains(long x) {
		int high = RoaringIntPacking.high(x);
		ImmutableBitmapDataProvider lowBitmap = highToBitmap.get(high);
		if (lowBitmap == null) {
			return false;
		}

		int low = RoaringIntPacking.low(x);
		return lowBitmap.contains(low);
	}

	@Override
	public int getSizeInBytes() {
		return (int) getLongSizeInBytes();
	}

	@Override
	public long getLongSizeInBytes() {
		long size = 8;

		// Size of containers
		size += highToBitmap.values().stream().mapToLong(p -> p.getLongSizeInBytes()).sum();

		// Size of Map data-structure: we consider each TreeMap entry costs 40 bytes
		// http://java-performance.info/memory-consumption-of-java-data-types-2/
		size += 8L + 40L * highToBitmap.size();

		// Size of (boxed) Integers used as keys
		size += 16L * highToBitmap.size();

		// The cache impacts the size in heap
		size += 8L * sortedCumulatedCardinality.length;
		size += 4L * sortedHighs.length;

		return size;
	}

	@Override
	public boolean isEmpty() {
		return getLongCardinality() == 0L;
	}

	@Override
	public ImmutableLongBitmapDataProvider limit(long x) {
		throw new UnsupportedOperationException("TODO");
	}

	/**
	 * Serialize this bitmap.
	 *
	 * Unlike RoaringBitmap, there is no specification for now: it may change from
	 * onve java version to another, and from one RoaringBitmap version to another.
	 *
	 * The current bitmap is not modified.
	 *
	 * @param out the DataOutput stream
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Override
	public void serialize(DataOutput out) throws IOException {
		// TODO: Should we transport the performance tweak 'doCacheCardinalities'?
		out.writeBoolean(signedLongs);

		out.writeInt(highToBitmap.size());

		for (Entry<Integer, ImmutableBitmapDataProvider> entry : highToBitmap.entrySet()) {
			out.writeInt(entry.getKey().intValue());
			entry.getValue().serialize(out);
		}
	}

	@Override
	public long serializedSizeInBytes() {
		long nbBytes = 0L;

		// .writeBoolean for signedLongs boolean
		nbBytes += 1;

		// .writeInt for number of different high values
		nbBytes += 4;

		for (Entry<Integer, ImmutableBitmapDataProvider> entry : highToBitmap.entrySet()) {
			// .writeInt for high
			nbBytes += 4;

			// The low bitmap size in bytes
			nbBytes += entry.getValue().serializedSizeInBytes();
		}

		return nbBytes;
	}

	/**
	 * reset to an empty bitmap; result occupies as much space a newly created
	 * bitmap.
	 */
	public void clear() {
		this.highToBitmap.clear();
		resetPerfHelpers();
	}

	/**
	 * Return the set values as an array, if the cardinality is smaller than
	 * 2147483648. The long values are in sorted order.
	 *
	 * @return array representing the set values.
	 */
	@Override
	public long[] toArray() {
		long cardinality = this.getLongCardinality();
		if (cardinality > Integer.MAX_VALUE) {
			throw new IllegalStateException("The cardinality does not fit in an array");
		}

		final long[] array = new long[(int) cardinality];

		int pos = 0;
		LongIterator it = getLongIterator();

		while (it.hasNext()) {
			array[pos++] = it.next();
		}
		return array;
	}

	@Override
	public LongIterator getReverseLongIterator() {
		return toIterator(highToBitmap.descendingMap().entrySet().iterator(), true);
	}

	@Override
	public int hashCode() {
		return highToBitmap.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		ImmutableRoaring64Bitmap other = (ImmutableRoaring64Bitmap) obj;
		return Objects.equals(highToBitmap, other.highToBitmap);
	}
}
