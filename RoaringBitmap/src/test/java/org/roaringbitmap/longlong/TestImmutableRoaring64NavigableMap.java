package org.roaringbitmap.longlong;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Random;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.roaringbitmap.ImmutableBitmapDataProvider;
import org.roaringbitmap.RoaringBitmapSupplier;
import org.roaringbitmap.Util;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmapSupplier;

public class TestImmutableRoaring64NavigableMap {
	@TempDir
	File tempDir;

	// Used to compare Roaring64NavigableMap behavior with RoaringBitmap
	private Roaring64NavigableMap newDefaultCtor() {
		return new Roaring64NavigableMap();
	}

	// Testing the nocache behavior should not depends on bitmap being on-heap of
	// buffered
	private Roaring64NavigableMap newNoCache() {
		return new Roaring64NavigableMap(true, false);
	}

	private Roaring64NavigableMap newSignedBuffered() {
		return new Roaring64NavigableMap(true, new MutableRoaringBitmapSupplier());
	}

	private Roaring64NavigableMap newUnsignedHeap() {
		return new Roaring64NavigableMap(false, new RoaringBitmapSupplier());
	}

	protected void checkCardinalities(ImmutableRoaring64Bitmap bitmap) {
		NavigableMap<Integer, ImmutableBitmapDataProvider> highToBitmap = bitmap.getHighToBitmap();
		int lowestHighNotValid = bitmap.getLowestInvalidHigh();

		NavigableMap<Integer, ImmutableBitmapDataProvider> expectedToBeCorrect = highToBitmap.headMap(lowestHighNotValid, false);
		long[] expectedCardinalities = new long[expectedToBeCorrect.size()];

		Iterator<ImmutableBitmapDataProvider> it = expectedToBeCorrect.values().iterator();
		int index = 0;
		while (it.hasNext()) {
			ImmutableBitmapDataProvider next = it.next();

			if (index == 0) {
				expectedCardinalities[0] = next.getLongCardinality();
			} else {
				expectedCardinalities[index] = expectedCardinalities[index - 1] + next.getLongCardinality();
			}

			index++;
		}

		assertArrayEquals(expectedCardinalities,
				Arrays.copyOf(bitmap.getSortedCumulatedCardinality(), expectedCardinalities.length));
	}

	@Test
	public void testEmpty() throws IOException {
		Roaring64NavigableMap map = newDefaultCtor();
		ImmutableRoaring64Bitmap imap = saveAndReopen(map);
		assertFalse(imap.getLongIterator().hasNext());

		assertEquals(0, imap.getLongCardinality());
		assertTrue(imap.isEmpty());
		assertFalse(imap.contains(0));

		assertEquals(0, imap.rankLong(Long.MIN_VALUE));
		assertEquals(0, imap.rankLong(Long.MIN_VALUE + 1));
		assertEquals(0, imap.rankLong(-1));
		assertEquals(0, imap.rankLong(0));
		assertEquals(0, imap.rankLong(1));
		assertEquals(0, imap.rankLong(Long.MAX_VALUE - 1));
		assertEquals(0, imap.rankLong(Long.MAX_VALUE));
	}

	private ImmutableRoaring64Bitmap saveAndReopen(Roaring64NavigableMap mutable) throws IOException {
		File file = new File(tempDir, "roaring64navigablemap");
		try (FileOutputStream fos = new FileOutputStream(file); DataOutputStream dos = new DataOutputStream(fos)) {
			mutable.serialize(dos);
		}
		try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
			return new ImmutableRoaring64Bitmap(raf);
		}
	}

	@Test
	public void testZero() throws IOException {
		Roaring64NavigableMap map = newSignedBuffered();

		map.addLong(0);

		ImmutableRoaring64Bitmap imap = saveAndReopen(map);
		{
			LongIterator iterator = imap.getLongIterator();

			assertTrue(iterator.hasNext());
			assertEquals(0, iterator.next());
			assertEquals(0, imap.select(0));
			assertTrue(imap.contains(0));

			assertFalse(iterator.hasNext());
		}

		assertEquals(1, imap.getLongCardinality());
		assertFalse(imap.isEmpty());

		assertEquals(0, imap.rankLong(Long.MIN_VALUE));
		assertEquals(0, imap.rankLong(Integer.MIN_VALUE - 1L));
		assertEquals(0, imap.rankLong(-1));
		assertEquals(1, imap.rankLong(0));
		assertEquals(1, imap.rankLong(1));
		assertEquals(1, imap.rankLong(Integer.MAX_VALUE + 1L));
		assertEquals(1, imap.rankLong(Long.MAX_VALUE));
	}

	@Test
	public void testMinusOne_Unsigned() throws IOException {
		Roaring64NavigableMap map = newUnsignedHeap();

		map.addLong(-1);
		ImmutableRoaring64Bitmap imap = saveAndReopen(map);
		{
			LongIterator iterator = imap.getLongIterator();

			assertTrue(iterator.hasNext());
			assertEquals(-1, iterator.next());
			assertEquals(-1, imap.select(0));
			assertTrue(imap.contains(-1));

			assertFalse(iterator.hasNext());
		}

		assertEquals(1, imap.getLongCardinality());
		assertFalse(imap.isEmpty());

		assertEquals(0, imap.rankLong(Long.MIN_VALUE));
		assertEquals(0, imap.rankLong(Integer.MIN_VALUE - 1L));
		assertEquals(0, imap.rankLong(0));
		assertEquals(0, imap.rankLong(1));
		assertEquals(0, imap.rankLong(Integer.MAX_VALUE + 1L));
		assertEquals(0, imap.rankLong(Long.MAX_VALUE));
		assertEquals(0, imap.rankLong(-2));
		assertEquals(1, imap.rankLong(-1));

		assertArrayEquals(new long[] { -1L }, imap.toArray());
	}

	@Test
	public void testAddNotAddLong() throws IOException {
		Roaring64NavigableMap map = newSignedBuffered();

		// Use BitmapProvider.add instead of .addLong
		map.addLong(0);
		ImmutableRoaring64Bitmap imap = saveAndReopen(map);
		{
			LongIterator iterator = imap.getLongIterator();

			assertTrue(iterator.hasNext());
			assertEquals(0, iterator.next());
			assertEquals(0, imap.select(0));
			assertTrue(imap.contains(0));

			assertFalse(iterator.hasNext());
		}

		assertEquals(1, imap.getLongCardinality());
		assertFalse(imap.isEmpty());

		assertEquals(0, imap.rankLong(Long.MIN_VALUE));
		assertEquals(0, imap.rankLong(Integer.MIN_VALUE - 1L));
		assertEquals(0, imap.rankLong(-1));
		assertEquals(1, imap.rankLong(0));
		assertEquals(1, imap.rankLong(1));
		assertEquals(1, imap.rankLong(Integer.MAX_VALUE + 1L));
		assertEquals(1, imap.rankLong(Long.MAX_VALUE));
	}

	@Test
	public void testSimpleIntegers() throws IOException {
		ImmutableRoaring64Bitmap imap;
		{
			Roaring64NavigableMap map = newDefaultCtor();

			map.addLong(123);
			map.addLong(234);
			imap = saveAndReopen(map);
		}
		{
			LongIterator iterator = imap.getLongIterator();

			assertTrue(iterator.hasNext());
			assertEquals(123, iterator.next());
			assertEquals(123, imap.select(0));
			assertTrue(imap.contains(123));

			assertTrue(iterator.hasNext());
			assertEquals(234, iterator.next());
			assertEquals(234, imap.select(1));
			assertTrue(imap.contains(234));

			assertFalse(iterator.hasNext());
		}

		assertFalse(imap.contains(345));

		assertEquals(2, imap.getLongCardinality());

		assertEquals(0, imap.rankLong(0));
		assertEquals(1, imap.rankLong(123));
		assertEquals(1, imap.rankLong(233));
		assertEquals(2, imap.rankLong(234));
		assertEquals(2, imap.rankLong(235));
		assertEquals(2, imap.rankLong(Integer.MAX_VALUE + 1L));
		assertEquals(2, imap.rankLong(Long.MAX_VALUE));

		assertArrayEquals(new long[] { 123L, 234L }, imap.toArray());
	}

	@Test
	public void testHashCodeEquals() throws IOException {
		ImmutableRoaring64Bitmap leftmap;
		{
			Roaring64NavigableMap left = newDefaultCtor();

			left.addLong(123);
			left.addLong(Long.MAX_VALUE);
			leftmap = saveAndReopen(left);
		}

		ImmutableRoaring64Bitmap rightmap;
		{
			Roaring64NavigableMap right = Roaring64NavigableMap.bitmapOf(123, Long.MAX_VALUE);
			rightmap = saveAndReopen(right);
		}
		assertEquals(leftmap.hashCode(), rightmap.hashCode());
		assertEquals(leftmap, rightmap);
		assertEquals(rightmap, leftmap);
	}

	@Test
	public void testAddOneSelect2() {
		assertThrows(IllegalArgumentException.class, () -> {
			Roaring64NavigableMap map = newDefaultCtor();

			map.addLong(123);
			ImmutableRoaring64Bitmap imap = saveAndReopen(map);
			imap.select(1);
		});
	}

	@Test
	public void testAddInt() throws IOException {
		Roaring64NavigableMap map = newDefaultCtor();

		map.addInt(-1);
		ImmutableRoaring64Bitmap imap = saveAndReopen(map);
		assertEquals(Util.toUnsignedLong(-1), imap.select(0));
	}

	@Test
	public void testIterator_NextWithoutHasNext_Filled() throws IOException {
		Roaring64NavigableMap map = newDefaultCtor();

		map.addLong(0);
		ImmutableRoaring64Bitmap imap = saveAndReopen(map);
		assertTrue(imap.getLongIterator().hasNext());
		assertEquals(0, imap.getLongIterator().next());
	}

	@Test
	public void testIterator_NextWithoutHasNext_Empty() {
		assertThrows(IllegalStateException.class, () -> {
			Roaring64NavigableMap map = newDefaultCtor();
			ImmutableRoaring64Bitmap imap = saveAndReopen(map);

			imap.getLongIterator().next();

		});
	}

	@Test
	public void testLongMaxValue() throws IOException {
		Roaring64NavigableMap map = newSignedBuffered();

		map.addLong(Long.MAX_VALUE);

		ImmutableRoaring64Bitmap imap = saveAndReopen(map);
		{
			LongIterator iterator = imap.getLongIterator();
			assertTrue(iterator.hasNext());
			assertEquals(Long.MAX_VALUE, iterator.next());
			assertEquals(Long.MAX_VALUE, imap.select(0));
			assertFalse(iterator.hasNext());
		}

		assertEquals(1, imap.getLongCardinality());

		assertEquals(0, imap.rankLong(Long.MIN_VALUE));
		assertEquals(0, imap.rankLong(Long.MIN_VALUE + 1));
		assertEquals(0, imap.rankLong(-1));
		assertEquals(0, imap.rankLong(0));
		assertEquals(0, imap.rankLong(1));
		assertEquals(0, imap.rankLong(Long.MAX_VALUE - 1));
		assertEquals(1, imap.rankLong(Long.MAX_VALUE));

		assertArrayEquals(new long[] { Long.MAX_VALUE }, imap.toArray());
	}

	@Test
	public void testLongMinValue() throws IOException {
		Roaring64NavigableMap map = newSignedBuffered();

		map.addLong(Long.MIN_VALUE);
		ImmutableRoaring64Bitmap imap = saveAndReopen(map);
		{
			LongIterator iterator = imap.getLongIterator();
			assertTrue(iterator.hasNext());
			assertEquals(Long.MIN_VALUE, iterator.next());
			assertEquals(Long.MIN_VALUE, imap.select(0));
			assertFalse(iterator.hasNext());
		}

		assertEquals(1, imap.getLongCardinality());

		assertEquals(1, imap.rankLong(Long.MIN_VALUE));
		assertEquals(1, imap.rankLong(Long.MIN_VALUE + 1));
		assertEquals(1, imap.rankLong(-1));
		assertEquals(1, imap.rankLong(0));
		assertEquals(1, imap.rankLong(1));
		assertEquals(1, imap.rankLong(Long.MAX_VALUE - 1));
		assertEquals(1, imap.rankLong(Long.MAX_VALUE));
	}

	@Test
	public void testLongMinValueZeroOneMaxValue() throws IOException {
		ImmutableRoaring64Bitmap imap;
		{
			Roaring64NavigableMap map = newSignedBuffered();

			map.addLong(Long.MIN_VALUE);
			map.addLong(0);
			map.addLong(1);
			map.addLong(Long.MAX_VALUE);
			imap = saveAndReopen(map);
		}
		{
			LongIterator iterator = imap.getLongIterator();
			assertTrue(iterator.hasNext());
			assertEquals(Long.MIN_VALUE, iterator.next());
			assertEquals(Long.MIN_VALUE, imap.select(0));
			assertEquals(0, iterator.next());
			assertEquals(0, imap.select(1));
			assertEquals(1, iterator.next());
			assertEquals(1, imap.select(2));
			assertEquals(Long.MAX_VALUE, iterator.next());
			assertEquals(Long.MAX_VALUE, imap.select(3));
			assertFalse(iterator.hasNext());
		}

		assertEquals(4, imap.getLongCardinality());

		assertEquals(1, imap.rankLong(Long.MIN_VALUE));
		assertEquals(1, imap.rankLong(Long.MIN_VALUE + 1));
		assertEquals(1, imap.rankLong(-1));
		assertEquals(2, imap.rankLong(0));
		assertEquals(3, imap.rankLong(1));
		assertEquals(3, imap.rankLong(2));
		assertEquals(3, imap.rankLong(Long.MAX_VALUE - 1));
		assertEquals(4, imap.rankLong(Long.MAX_VALUE));

		final List<Long> foreach = new ArrayList<>();
		imap.forEach(new LongConsumer() {

			@Override
			public void accept(long value) {
				foreach.add(value);
			}
		});
		assertEquals(Arrays.asList(Long.MIN_VALUE, 0L, 1L, Long.MAX_VALUE), foreach);
	}

	@Test
	public void testReverseIterator_SingleBuket() throws IOException {
		ImmutableRoaring64Bitmap imap;
		{
			Roaring64NavigableMap map = newDefaultCtor();

			map.addLong(123);
			map.addLong(234);
			imap = saveAndReopen(map);
		}

		{
			LongIterator iterator = imap.getReverseLongIterator();
			assertTrue(iterator.hasNext());
			assertEquals(234, iterator.next());
			assertTrue(iterator.hasNext());
			assertEquals(123, iterator.next());
			assertFalse(iterator.hasNext());
		}
	}

	@Test
	public void testReverseIterator_MultipleBuket() throws IOException {
		ImmutableRoaring64Bitmap imap;
		{
			Roaring64NavigableMap map = newDefaultCtor();

			map.addLong(123);
			map.addLong(Long.MAX_VALUE);
			imap = saveAndReopen(map);
		}
		{
			LongIterator iterator = imap.getReverseLongIterator();
			assertTrue(iterator.hasNext());
			assertEquals(Long.MAX_VALUE, iterator.next());
			assertTrue(iterator.hasNext());
			assertEquals(123, iterator.next());
			assertFalse(iterator.hasNext());
		}
	}

	@Test
	public void testEmptyAfterRemove() throws IOException {
		Roaring64NavigableMap rbm = new Roaring64NavigableMap();
//		Roaring64NavigableMap empty = new Roaring64NavigableMap();
		rbm.addLong(1);
		ImmutableRoaring64Bitmap imap = saveAndReopen(rbm);
		assertEquals(rbm.getHighToBitmap().size(), 1);
		assertFalse(rbm.getHighToBitmap().isEmpty());
		assertFalse(imap.getHighToBitmap().isEmpty());
		rbm.removeLong(1);
		imap = saveAndReopen(rbm);
		assertTrue(rbm.getHighToBitmap().isEmpty());
		assertTrue(imap.getHighToBitmap().isEmpty());
		assertEquals(rbm, imap);
	}

	@Test
	public void testNotEmptyAfterRemove() throws IOException {
		Roaring64NavigableMap rbm = new Roaring64NavigableMap();
		rbm.addLong(1L);
		ImmutableRoaring64Bitmap imap = saveAndReopen(rbm);
		assertEquals(rbm.getHighToBitmap().size(), 1);
		assertEquals(imap.getHighToBitmap().size(), 1);
		rbm.addLong(3L * Integer.MAX_VALUE);
		imap = saveAndReopen(rbm);
		assertEquals(rbm.getHighToBitmap().size(), 2);
		assertEquals(imap.getHighToBitmap().size(), 2);

		// This will remove a highToBitmap entry
		rbm.removeLong(3L * Integer.MAX_VALUE);
		imap = saveAndReopen(rbm);
		assertEquals(rbm.getHighToBitmap().size(), 2);
		assertEquals(imap.getHighToBitmap().size(), 1);

		// This shall create again a highToBitmap entry
		rbm.addLong(3L * Integer.MAX_VALUE);
		imap = saveAndReopen(rbm);
		assertEquals(rbm.getHighToBitmap().size(), 2);
		assertEquals(imap.getHighToBitmap().size(), 2);
	}

	@Test
	public void testRemove_Signed() throws IOException {
		Roaring64NavigableMap map = newSignedBuffered();

		// Add a value
		map.addLong(123);
		ImmutableRoaring64Bitmap imap = saveAndReopen(map);
		assertEquals(1L, imap.getLongCardinality());

		// Remove it
		map.removeLong(123L);
		imap = saveAndReopen(map);
		assertEquals(0L, imap.getLongCardinality());
		assertTrue(imap.isEmpty());

		// Add it back
		map.addLong(123);
		imap = saveAndReopen(map);
		assertEquals(1L, imap.getLongCardinality());
	}

	@Test
	public void testRemove_Unsigned() throws IOException {

		Roaring64NavigableMap map = newUnsignedHeap();
		// Add a value
		map.addLong(123);
		ImmutableRoaring64Bitmap imap = saveAndReopen(map);
		assertEquals(1L, imap.getLongCardinality());

		// Remove it
		map.removeLong(123L);
		imap = saveAndReopen(map);
		assertEquals(0L, imap.getLongCardinality());
		assertTrue(imap.isEmpty());

		// Add it back
		map.addLong(123);
		imap = saveAndReopen(map);
		assertEquals(1L, imap.getLongCardinality());
	}

	@Test
	public void testRemoveDifferentBuckets() throws IOException {
		Roaring64NavigableMap map = newDefaultCtor();

		// Add two values
		map.addLong(123);
		map.addLong(Long.MAX_VALUE);
		ImmutableRoaring64Bitmap imap = saveAndReopen(map);
		assertEquals(2L, imap.getLongCardinality());

		// Remove biggest
		map.removeLong(Long.MAX_VALUE);
		imap = saveAndReopen(map);
		assertEquals(1L, imap.getLongCardinality());

		assertEquals(123L, map.select(0));

		// Add back to different bucket
		map.addLong(Long.MAX_VALUE);
		imap = saveAndReopen(map);
		assertEquals(2L, imap.getLongCardinality());

		assertEquals(123L, imap.select(0));
		assertEquals(Long.MAX_VALUE, imap.select(1));
	}

	@Test
	public void testRemoveDifferentBuckets_RemoveBigAddIntermediate() throws IOException {
		Roaring64NavigableMap map = newDefaultCtor();

		// Add two values
		map.addLong(123);
		map.addLong(Long.MAX_VALUE);
		ImmutableRoaring64Bitmap imap = saveAndReopen(map);
		assertEquals(2L, imap.getLongCardinality());

		// Remove biggest
		map.removeLong(Long.MAX_VALUE);
		imap = saveAndReopen(map);
		assertEquals(1L, imap.getLongCardinality());

		assertEquals(123L, imap.select(0));

		// Add back to different bucket
		map.addLong(Long.MAX_VALUE / 2L);
		imap = saveAndReopen(map);
		assertEquals(2L, imap.getLongCardinality());

		assertEquals(123L, imap.select(0));
		assertEquals(Long.MAX_VALUE / 2L, imap.select(1));
	}

	@Test
	public void testRemoveDifferentBuckets_RemoveIntermediateAddBug() throws IOException {
		Roaring64NavigableMap map = newDefaultCtor();

		// Add two values
		map.addLong(123);
		map.addLong(Long.MAX_VALUE / 2L);
		ImmutableRoaring64Bitmap imap = saveAndReopen(map);
		assertEquals(2L, imap.getLongCardinality());

		// Remove biggest
		map.removeLong(Long.MAX_VALUE / 2L);
		imap = saveAndReopen(map);
		assertEquals(1L, imap.getLongCardinality());

		assertEquals(123L, imap.select(0));

		// Add back to different bucket
		map.addLong(Long.MAX_VALUE);
		imap = saveAndReopen(map);
		assertEquals(2L, imap.getLongCardinality());

		assertEquals(123L, imap.select(0));
		assertEquals(Long.MAX_VALUE, imap.select(1));
	}

	@Test
	public void testPerfManyDifferentBuckets_WithCache() throws IOException {
		Roaring64NavigableMap map = new Roaring64NavigableMap(true, true);

		long problemSize = 1000 * 1000L;
		for (long i = 1; i <= problemSize; i++) {
			map.addLong(i * Integer.MAX_VALUE + 1L);
		}
		ImmutableRoaring64Bitmap imap = saveAndReopen(map);
		long cardinality = imap.getLongCardinality();
		assertEquals(problemSize, cardinality);

		long last = imap.select(cardinality - 1);
		assertEquals(problemSize * Integer.MAX_VALUE + 1L, last);
		assertEquals(cardinality, imap.rankLong(last));
	}

	@Test
	public void testPerfManyDifferentBuckets_NoCache() throws IOException {
		Roaring64NavigableMap map = newNoCache();

		long problemSize = 100 * 1000L;
		for (long i = 1; i <= problemSize; i++) {
			map.addLong(i * Integer.MAX_VALUE + 1L);
		}
		ImmutableRoaring64Bitmap imap = saveAndReopen(map);
		long cardinality = imap.getLongCardinality();
		assertEquals(problemSize, cardinality);

		long last = imap.select(cardinality - 1);
		assertEquals(problemSize * Integer.MAX_VALUE + 1L, last);
		assertEquals(cardinality, imap.rankLong(last));
	}

	@Test
	public void testComparator() {
		Comparator<Integer> natural = new Comparator<Integer>() {

			@Override
			public int compare(Integer o1, Integer o2) {
				return Integer.compare(o1, o2);
			}
		};
		Comparator<Integer> unsigned = RoaringIntPacking.unsignedComparator();

		// Comparator a negative and a positive differs from natural comparison
		assertTrue(natural.compare(-1, 1) < 0);
		assertFalse(unsigned.compare(-1, 1) < 0);

		// Comparator Long.MAX_VALUE and Long.MAX_VALUE + 1 differs
		assertTrue(natural.compare(Integer.MAX_VALUE, Integer.MAX_VALUE + 1) > 0);
		assertFalse(unsigned.compare(Integer.MAX_VALUE, Integer.MAX_VALUE + 1) > 0);

		// 'Integer.MAX_VALUE+1' is lower than 'Integer.MAX_VALUE+2'
		assertTrue(unsigned.compare(Integer.MAX_VALUE + 1, Integer.MAX_VALUE + 2) < 0);
	}

	@Test
	public void testLargeSelectLong_signed() throws IOException {
		long positive = 1;
		long negative = -1;
		Roaring64NavigableMap map = newSignedBuffered();
		map.addLong(positive);
		map.addLong(negative);
		ImmutableRoaring64Bitmap imap = saveAndReopen(map);
		long first = imap.select(0);
		long last = imap.select(1);

		// signed: positive is after negative
		assertEquals(negative, first);
		assertEquals(positive, last);
	}

	@Test
	public void testLargeSelectLong_unsigned() throws IOException {
		long positive = 1;
		long negative = -1;
		Roaring64NavigableMap map = newUnsignedHeap();
		map.addLong(positive);
		map.addLong(negative);
		ImmutableRoaring64Bitmap imap = saveAndReopen(map);
		long first = imap.select(0);
		long last = imap.select(1);

		// unsigned: negative means bigger than Long.MAX_VALUE
		assertEquals(positive, first);
		assertEquals(negative, last);
	}

	@Test
	public void testLargeRankLong_signed() throws IOException {
		long positive = 1;
		long negative = -1;
		Roaring64NavigableMap map = newSignedBuffered();
		map.addLong(positive);
		map.addLong(negative);
		ImmutableRoaring64Bitmap imap = saveAndReopen(map);
		assertEquals(1, imap.rankLong(negative));
	}

	@Test
	public void testLargeRankLong_unsigned() throws IOException {
		long positive = 1;
		long negative = -1;
		Roaring64NavigableMap map = newUnsignedHeap();
		map.addLong(positive);
		map.addLong(negative);
		ImmutableRoaring64Bitmap imap = saveAndReopen(map);
		assertEquals(2, imap.rankLong(negative));
	}

	@Test
	public void testIterationOrder_signed() throws IOException {
		long positive = 1;
		long negative = -1;
		Roaring64NavigableMap map = newSignedBuffered();
		map.addLong(positive);
		map.addLong(negative);
		ImmutableRoaring64Bitmap imap = saveAndReopen(map);
		LongIterator it = imap.getLongIterator();
		long first = it.next();
		long last = it.next();
		assertEquals(negative, first);
		assertEquals(positive, last);
	}

	@Test
	public void testIterationOrder_unsigned() throws IOException {
		long positive = 1;
		long negative = -1;
		Roaring64NavigableMap map = newUnsignedHeap();
		map.addLong(positive);
		map.addLong(negative);
		ImmutableRoaring64Bitmap imap = saveAndReopen(map);
		LongIterator it = imap.getLongIterator();
		long first = it.next();
		long last = it.next();
		assertEquals(positive, first);
		assertEquals(negative, last);
	}

	@Test
	public void testAddingLowValueAfterHighValue() throws IOException {
		Roaring64NavigableMap map = newDefaultCtor();
		map.addLong(Long.MAX_VALUE);
		ImmutableRoaring64Bitmap imap = saveAndReopen(map);
		assertEquals(Long.MAX_VALUE, imap.select(0));
		map.addLong(666);
		imap = saveAndReopen(map);
		assertEquals(666, imap.select(0));
		assertEquals(Long.MAX_VALUE, imap.select(1));
	}

	@Test
	public void testOr_SameBucket() throws IOException {
		Roaring64NavigableMap left = newDefaultCtor();
		Roaring64NavigableMap right = newDefaultCtor();

		left.addLong(123);
		right.addLong(234);

		left.or(right);
		ImmutableRoaring64Bitmap ileft = saveAndReopen(left);
		assertEquals(2, ileft.getLongCardinality());

		assertEquals(123, ileft.select(0));
		assertEquals(234, ileft.select(1));
	}

	@Test
	public void testOr_MultipleBuckets() throws IOException {
		Roaring64NavigableMap left = newDefaultCtor();
		Roaring64NavigableMap right = newDefaultCtor();

		left.addLong(123);
		left.addLong(Long.MAX_VALUE);
		right.addLong(234);

		left.or(right);

		ImmutableRoaring64Bitmap ileft = saveAndReopen(left);
		assertEquals(3, ileft.getLongCardinality());

		assertEquals(123, ileft.select(0));
		assertEquals(234, ileft.select(1));
		assertEquals(Long.MAX_VALUE, ileft.select(2));
	}

	@Test
	public void testOr_DifferentBucket_NotBuffer() throws IOException {
		Roaring64NavigableMap left = newSignedBuffered();
		Roaring64NavigableMap right = newSignedBuffered();

		left.addLong(123);
		right.addLong(Long.MAX_VALUE / 2);

		left.or(right);

		ImmutableRoaring64Bitmap ileft = saveAndReopen(left);
		assertEquals(2, ileft.getLongCardinality());

		assertEquals(123, ileft.select(0));
		assertEquals(Long.MAX_VALUE / 2, ileft.select(1));
	}

	@Test
	public void testOr_SameBucket_NotBuffer() throws IOException {
		Roaring64NavigableMap left = new Roaring64NavigableMap(true, new RoaringBitmapSupplier());
		Roaring64NavigableMap right = new Roaring64NavigableMap(true, new RoaringBitmapSupplier());

		left.addLong(123);
		right.addLong(234);

		left.or(right);
		ImmutableRoaring64Bitmap ileft = saveAndReopen(left);
		assertEquals(2, ileft.getLongCardinality());

		assertEquals(123, ileft.select(0));
		assertEquals(234, ileft.select(1));
	}

	@Test
	public void testOr_DifferentBucket_Buffer() throws IOException {
		Roaring64NavigableMap left = newSignedBuffered();
		Roaring64NavigableMap right = newSignedBuffered();

		left.addLong(123);
		right.addLong(Long.MAX_VALUE);

		left.or(right);
		ImmutableRoaring64Bitmap ileft = saveAndReopen(left);
		assertEquals(2, ileft.getLongCardinality());

		assertEquals(123, ileft.select(0));
		assertEquals(Long.MAX_VALUE, ileft.select(1));
	}

	@Test
	public void testOr_SameBucket_Buffer() throws IOException {
		Roaring64NavigableMap left = newSignedBuffered();
		Roaring64NavigableMap right = newSignedBuffered();

		left.addLong(123);
		right.addLong(234);

		left.or(right);
		ImmutableRoaring64Bitmap ileft = saveAndReopen(left);
		assertEquals(2, ileft.getLongCardinality());

		assertEquals(123, ileft.select(0));
		assertEquals(234, ileft.select(1));
	}

	@Test
	public void testOr_CloneInput() throws IOException {
		Roaring64NavigableMap left = newDefaultCtor();
		Roaring64NavigableMap right = newDefaultCtor();

		right.addLong(123);

		// We push in left a bucket which does not exist
		left.or(right);

		// Then we mutate left: ensure it does not impact right as it should remain
		// unchanged
		left.addLong(234);
		ImmutableRoaring64Bitmap iright = saveAndReopen(right);
		ImmutableRoaring64Bitmap ileft = saveAndReopen(left);
		assertEquals(2, ileft.getLongCardinality());
		assertEquals(123, ileft.select(0));
		assertEquals(234, ileft.select(1));

		assertEquals(1, iright.getLongCardinality());
		assertEquals(123, iright.select(0));
	}

	@Test
	public void testXor_SingleBucket() throws IOException {
		Roaring64NavigableMap left = newDefaultCtor();
		Roaring64NavigableMap right = newDefaultCtor();

		left.addLong(123);
		left.addLong(234);
		right.addLong(234);
		right.addLong(345);

		// We have 1 shared value: 234
		left.xor(right);

		ImmutableRoaring64Bitmap ileft = saveAndReopen(left);
		assertEquals(2, ileft.getLongCardinality());
		assertEquals(123, ileft.select(0));
		assertEquals(345, ileft.select(1));
	}

	@Test
	public void testXor_Buffer() throws IOException {
		Roaring64NavigableMap left = newSignedBuffered();
		Roaring64NavigableMap right = newSignedBuffered();

		left.addLong(123);
		left.addLong(234);
		right.addLong(234);
		right.addLong(345);

		left.xor(right);

		ImmutableRoaring64Bitmap ileft = saveAndReopen(left);
		assertEquals(2, ileft.getLongCardinality());
		assertEquals(123, ileft.select(0));
		assertEquals(345, ileft.select(1));
	}

	@Test
	public void testXor_DifferentBucket() throws IOException {
		Roaring64NavigableMap left = newDefaultCtor();
		Roaring64NavigableMap right = newDefaultCtor();

		left.addLong(123);
		right.addLong(Long.MAX_VALUE);

		// We have 1 shared value: 234
		left.xor(right);

		ImmutableRoaring64Bitmap ileft = saveAndReopen(left);
		assertEquals(2, ileft.getLongCardinality());
		assertEquals(123, ileft.select(0));
		assertEquals(Long.MAX_VALUE, ileft.select(1));
	}

	@Test
	public void testXor_MultipleBucket() throws IOException {
		Roaring64NavigableMap left = newDefaultCtor();
		Roaring64NavigableMap right = newDefaultCtor();

		left.addLong(123);
		left.addLong(Long.MAX_VALUE);
		right.addLong(Long.MAX_VALUE);

		// We have 1 shared value: 234
		left.xor(right);

		ImmutableRoaring64Bitmap ileft = saveAndReopen(left);
		assertEquals(1, ileft.getLongCardinality());
		assertEquals(123, ileft.select(0));
	}

	@Test
	public void testAnd_SingleBucket() throws IOException {
		Roaring64NavigableMap left = newDefaultCtor();
		Roaring64NavigableMap right = newDefaultCtor();

		left.addLong(123);
		left.addLong(234);
		right.addLong(234);
		right.addLong(345);

		// We have 1 shared value: 234
		left.and(right);

		ImmutableRoaring64Bitmap ileft = saveAndReopen(left);
		assertEquals(1, ileft.getLongCardinality());
		assertEquals(234, ileft.select(0));
	}

	@Test
	public void testAnd_Buffer() throws IOException {
		Roaring64NavigableMap left = newSignedBuffered();
		Roaring64NavigableMap right = newSignedBuffered();

		left.addLong(123);
		right.addLong(123);

		// We have 1 shared value: 234
		left.and(right);

		ImmutableRoaring64Bitmap ileft = saveAndReopen(left);
		assertEquals(1, ileft.getLongCardinality());
		assertEquals(123, ileft.select(0));
	}

	@Test
	public void testAnd_DifferentBucket() throws IOException {
		Roaring64NavigableMap left = newDefaultCtor();
		Roaring64NavigableMap right = newDefaultCtor();

		left.addLong(123);
		right.addLong(Long.MAX_VALUE);

		// We have 1 shared value: 234
		left.and(right);

		ImmutableRoaring64Bitmap ileft = saveAndReopen(left);
		assertEquals(0, ileft.getLongCardinality());
	}

	@Test
	public void testAnd_MultipleBucket() throws IOException {
		Roaring64NavigableMap left = newDefaultCtor();
		Roaring64NavigableMap right = newDefaultCtor();

		left.addLong(123);
		left.addLong(Long.MAX_VALUE);
		right.addLong(Long.MAX_VALUE);

		// We have 1 shared value: 234
		left.and(right);

		ImmutableRoaring64Bitmap ileft = saveAndReopen(left);
		assertEquals(1, ileft.getLongCardinality());
		assertEquals(Long.MAX_VALUE, ileft.select(0));
	}

	@Test
	public void testAndNot_SingleBucket() throws IOException {
		Roaring64NavigableMap left = newDefaultCtor();
		Roaring64NavigableMap right = newDefaultCtor();

		left.addLong(123);
		left.addLong(234);
		right.addLong(234);
		right.addLong(345);

		// We have 1 shared value: 234
		left.andNot(right);

		ImmutableRoaring64Bitmap ileft = saveAndReopen(left);
		assertEquals(1, ileft.getLongCardinality());
		assertEquals(123, ileft.select(0));
	}

	@Test
	public void testAndNot_Buffer() throws IOException {
		Roaring64NavigableMap left = newSignedBuffered();
		Roaring64NavigableMap right = newSignedBuffered();

		left.addLong(123);
		right.addLong(234);

		// We have 1 shared value: 234
		left.andNot(right);

		ImmutableRoaring64Bitmap ileft = saveAndReopen(left);
		assertEquals(1, ileft.getLongCardinality());
		assertEquals(123, ileft.select(0));
	}

	@Test
	public void testAndNot_DifferentBucket() throws IOException {
		Roaring64NavigableMap left = newDefaultCtor();
		Roaring64NavigableMap right = newDefaultCtor();

		left.addLong(123);
		right.addLong(Long.MAX_VALUE);

		// We have 1 shared value: 234
		left.andNot(right);

		ImmutableRoaring64Bitmap ileft = saveAndReopen(left);
		assertEquals(1, ileft.getLongCardinality());
		assertEquals(123, ileft.select(0));
	}

	@Test
	public void testAndNot_MultipleBucket() throws IOException {
		Roaring64NavigableMap left = newDefaultCtor();
		Roaring64NavigableMap right = newDefaultCtor();

		left.addLong(123);
		left.addLong(Long.MAX_VALUE);
		right.addLong(Long.MAX_VALUE);

		// We have 1 shared value: 234
		left.andNot(right);

		ImmutableRoaring64Bitmap ileft = saveAndReopen(left);
		assertEquals(1, ileft.getLongCardinality());
		assertEquals(123, ileft.select(0));
	}

	@Test
	public void testToString_signed() throws IOException {
		Roaring64NavigableMap map = new Roaring64NavigableMap(true);

		map.addLong(123);
		map.addLong(Long.MAX_VALUE);
		map.addLong(Long.MAX_VALUE + 1L);
		ImmutableRoaring64Bitmap imap = saveAndReopen(map);

		assertEquals("{-9223372036854775808,123,9223372036854775807}", imap.toString());
	}

	@Test
	public void testToString_unsigned() throws IOException {
		Roaring64NavigableMap map = new Roaring64NavigableMap(false);

		map.addLong(123);
		map.addLong(Long.MAX_VALUE);
		map.addLong(Long.MAX_VALUE + 1L);

		ImmutableRoaring64Bitmap imap = saveAndReopen(map);

		assertEquals("{123,9223372036854775807,9223372036854775808}", imap.toString());
	}

	@Test
	public void testAddRange_SingleBucket_NotBuffer() throws IOException {
		Roaring64NavigableMap map = newUnsignedHeap();

		map.add(5L, 12L);
		ImmutableRoaring64Bitmap imap = saveAndReopen(map);

		assertEquals(7L, imap.getLongCardinality());

		assertEquals(5L, imap.select(0));
		assertEquals(11L, imap.select(6L));
	}

	@Test
	public void testAddRange_SingleBucket_Buffer() throws IOException {
		Roaring64NavigableMap map = newSignedBuffered();

		map.add(5L, 12L);
		ImmutableRoaring64Bitmap imap = saveAndReopen(map);

		assertEquals(7L, imap.getLongCardinality());

		assertEquals(5L, imap.select(0));
		assertEquals(11L, imap.select(6L));
	}

	// Edge case: the last high is excluded and should not lead to a new bitmap.
	// However, it may be
	// seen only while trying to add for high=1
	@Test
	public void testAddRange_EndExcludingNextBitmapFirstLow() throws IOException {
		Roaring64NavigableMap map = newDefaultCtor();

		long end = Util.toUnsignedLong(-1) + 1;

		map.add(end - 2, end);
		ImmutableRoaring64Bitmap imap = saveAndReopen(map);
		assertEquals(2, imap.getLongCardinality());

		assertEquals(end - 2, imap.select(0));
		assertEquals(end - 1, imap.select(1));
	}

	@Test
	public void testAddRange_MultipleBuckets() throws IOException {
		Roaring64NavigableMap map = newDefaultCtor();

		int enableTrim = 5;

		long from = RoaringIntPacking.pack(0, -1 - enableTrim);
		long to = from + 2 * enableTrim;
		map.add(from, to);
		int nbItems = (int) (to - from);
		ImmutableRoaring64Bitmap imap = saveAndReopen(map);
		assertEquals(nbItems, imap.getLongCardinality());

		assertEquals(from, imap.select(0));
		assertEquals(to - 1, imap.select(nbItems - 1));
	}

	@Test
	public void testTrim() {
		Roaring64NavigableMap map = new Roaring64NavigableMap(true);

		// How many contiguous values do we have to set to enable .trim?
		int enableTrim = 100;

		long from = RoaringIntPacking.pack(0, -1 - enableTrim);
		long to = from + 2 * enableTrim;

		// Check we cover different buckets
		assertNotEquals(RoaringIntPacking.high(to), RoaringIntPacking.high(from));

		for (long i = from; i <= to; i++) {
			map.addLong(i);
		}

		map.trim();
	}

	@Test
	public void testAutoboxedIterator() throws IOException {
		Roaring64NavigableMap map = newUnsignedHeap();

		map.addLong(123);
		map.addLong(234);

		ImmutableRoaring64Bitmap imap = saveAndReopen(map);
		Iterator<Long> it = imap.iterator();

		assertTrue(it.hasNext());
		assertEquals(123L, it.next().longValue());
		assertTrue(it.hasNext());
		assertEquals(234, it.next().longValue());
		assertFalse(it.hasNext());
	}

	@Test
	public void testAutoboxedIterator_CanNotRemove() {
		assertThrows(UnsupportedOperationException.class, () -> {
			Roaring64NavigableMap map = newUnsignedHeap();

			map.addLong(123);
			map.addLong(234);
			ImmutableRoaring64Bitmap imap = saveAndReopen(map);
			Iterator<Long> it = imap.iterator();

			assertTrue(it.hasNext());

			// Should throw a UnsupportedOperationException
			it.remove();
		});
	}

	@Test
	public void testSelect_NoCache_MultipleBuckets() throws IOException {
		Roaring64NavigableMap map = newNoCache();

		map.addLong(123);
		map.addLong(Long.MAX_VALUE);

		ImmutableRoaring64Bitmap imap = saveAndReopen(map);
		assertEquals(123L, imap.select(0));
		assertEquals(Long.MAX_VALUE, imap.select(1));
	}

	@Test
	public void testSelect_Empty() {
		assertThrows(IllegalArgumentException.class, () -> {
			Roaring64NavigableMap map = newUnsignedHeap();
			ImmutableRoaring64Bitmap imap = saveAndReopen(map);
			imap.select(0);
		});
	}

	@Test
	public void testSelect_OutOfBounds_MatchCardinality() {
		assertThrows(IllegalArgumentException.class, () -> {
			Roaring64NavigableMap map = newUnsignedHeap();

			map.addLong(123);
			ImmutableRoaring64Bitmap imap = saveAndReopen(map);
			imap.select(1);
		});
	}

	@Test
	public void testSelect_OutOfBounds_OtherCardinality() {
		assertThrows(IllegalArgumentException.class, () -> {
			Roaring64NavigableMap map = newUnsignedHeap();

			map.addLong(123);
			ImmutableRoaring64Bitmap imap = saveAndReopen(map);
			imap.select(2);
		});
	}

	@Test
	public void testRank_NoCache_MultipleBuckets() throws IOException {
		Roaring64NavigableMap map = newNoCache();

		map.addLong(123);
		map.addLong(Long.MAX_VALUE);
		ImmutableRoaring64Bitmap imap = saveAndReopen(map);
		assertEquals(0, imap.rankLong(0));
		assertEquals(1, imap.rankLong(123));
		assertEquals(1, imap.rankLong(Long.MAX_VALUE - 1));
		assertEquals(2, imap.rankLong(Long.MAX_VALUE));
	}

	@Test
	public void testRank_NoCache_HighNotPresent() throws IOException {
		Roaring64NavigableMap map = newNoCache();

		map.addLong(123);
		map.addLong(Long.MAX_VALUE);
		ImmutableRoaring64Bitmap imap = saveAndReopen(map);
		assertEquals(1, imap.rankLong(Long.MAX_VALUE / 2L));
	}

	@Test
	public void testFlipBackward() throws IOException {
		final Roaring64NavigableMap r = newUnsignedHeap();
		final long value = 1L;
		r.addLong(value);
		ImmutableRoaring64Bitmap imap = saveAndReopen(r);
		assertEquals(1, imap.getLongCardinality());

		r.flip(1);
		imap = saveAndReopen(r);
		assertEquals(0, imap.getLongCardinality());
	}

	@Test
	public void testFlip_NotBuffer() throws IOException {
		Roaring64NavigableMap map = newUnsignedHeap();

		map.addLong(0);
		map.flip(0);
		ImmutableRoaring64Bitmap imap = saveAndReopen(map);
		assertFalse(imap.contains(0));

		checkCardinalities(imap);
	}

	@Test
	public void testFlip_Buffer() throws IOException {
		Roaring64NavigableMap map = newSignedBuffered();

		map.addLong(0);
		map.flip(0);
		ImmutableRoaring64Bitmap imap = saveAndReopen(map);
		assertFalse(imap.contains(0));
	}

	@Test
	public void testRandomAddRemove() throws IOException {
		Random r = new Random(1234);

		// We need to max the considered range of longs, else each long would be in a
		// different bucket
		long max = Integer.MAX_VALUE * 20L;

		long targetCardinality = 1000;

		Roaring64NavigableMap map = newSignedBuffered();

		// Add a lot of items
		while (map.getIntCardinality() < targetCardinality) {
			map.addLong(r.nextLong() % max);
		}

		// Remove them by chunks
		int chunks = 10;
		for (int j = 0; j < chunks; j++) {
			long chunksSize = targetCardinality / chunks;
			for (int i = 0; i < chunksSize; i++) {
				map.removeLong(map.select(r.nextInt(map.getIntCardinality())));
			}
			ImmutableRoaring64Bitmap imap = saveAndReopen(map);
			assertEquals(targetCardinality - chunksSize * (j + 1), imap.getIntCardinality());
		}
		ImmutableRoaring64Bitmap imap = saveAndReopen(map);
		assertTrue(imap.isEmpty());
	}

	@Test
	public void testLongSizeInBytes() throws IOException {
		Roaring64NavigableMap map = newSignedBuffered();

		// Size when empty
		assertEquals(16, map.getLongSizeInBytes());

		// Add values so that the underlying Map holds multiple entries
		map.add(0);
		map.add(2L * Integer.MAX_VALUE);
		map.add(8L * Integer.MAX_VALUE, 8L * Integer.MAX_VALUE + 1024);
		ImmutableRoaring64Bitmap imap = saveAndReopen(map);
		assertEquals(3, imap.getHighToBitmap().size());

		// Size with multiple entries
		assertEquals(228, imap.getLongSizeInBytes());

		// Select does allocate some cache
		imap.select(16);
		assertEquals(264, imap.getLongSizeInBytes());
	}

	// https://github.com/RoaringBitmap/RoaringBitmap/issues/528
	@Test
	public void testAnd_ImplicitRoaringBitmap() throws IOException {
		// Based on RoaringBitmap
		Roaring64NavigableMap x = new Roaring64NavigableMap();
		x.add(123, 124);
		ImmutableRoaring64Bitmap ix = saveAndReopen(x);
		Assertions.assertTrue(ix.getHighToBitmap().values().iterator().next() instanceof ImmutableRoaringBitmap);

		// Based on MutableRoaringBitmap
		Roaring64NavigableMap y = Roaring64NavigableMap.bitmapOf(4L);
		ImmutableRoaring64Bitmap iy = saveAndReopen(y);
		Assertions.assertTrue(iy.getHighToBitmap().values().iterator().next() instanceof ImmutableRoaringBitmap);
	}

	@Test
	public void testAnd_MutableRoaringBitmap() throws IOException {
		// Based on RoaringBitmap
		Roaring64NavigableMap x = new Roaring64NavigableMap(new MutableRoaringBitmapSupplier());
		x.add(0, 16);

		// Based on MutableRoaringBitmap
		Roaring64NavigableMap y = new Roaring64NavigableMap(new MutableRoaringBitmapSupplier());
		y.add(8, 32);

		ImmutableRoaring64Bitmap ix = saveAndReopen(x);
		Assertions.assertEquals(16L, ix.getLongCardinality());
		x.and(y);
		ix = saveAndReopen(x);
		Assertions.assertEquals(8L, ix.getLongCardinality());
	}

	@Test
	public void testAndNot_ImplicitRoaringBitmap() throws IOException {
		// Based on RoaringBitmap
		Roaring64NavigableMap x = new Roaring64NavigableMap();
		x.add(8, 16);

		// Based on MutableRoaringBitmap
		Roaring64NavigableMap y = new Roaring64NavigableMap();
		y.add(12, 32);

		{
			x.andNot(y);
			ImmutableRoaring64Bitmap ix = saveAndReopen(x);
			ImmutableBitmapDataProvider singleBitmap = ix.getHighToBitmap().values().iterator().next();
			Assertions.assertTrue(singleBitmap instanceof ImmutableRoaringBitmap);
			Assertions.assertEquals(4L, singleBitmap.getLongCardinality());
			Assertions.assertEquals(8L, singleBitmap.select(0));
			Assertions.assertEquals(11L, singleBitmap.select(3));
		}
	}

	@Test
	public void testOr_ImplicitRoaringBitmap() throws IOException {
		// Based on RoaringBitmap
		Roaring64NavigableMap x = new Roaring64NavigableMap();
		x.add(123, 124);

		// Based on MutableRoaringBitmap
		Roaring64NavigableMap y = Roaring64NavigableMap.bitmapOf(4L);

		{
			x.or(y);
			ImmutableRoaring64Bitmap ix = saveAndReopen(x);
			ImmutableBitmapDataProvider singleBitmap = ix.getHighToBitmap().values().iterator().next();
			Assertions.assertTrue(singleBitmap instanceof ImmutableRoaringBitmap);
			Assertions.assertEquals(2L, singleBitmap.getLongCardinality());
		}
	}

	@Test
	public void testNaivelazyor_ImplicitRoaringBitmap() throws IOException {
		// Based on RoaringBitmap
		Roaring64NavigableMap x = new Roaring64NavigableMap();
		x.add(123, 124);

		// Based on MutableRoaringBitmap
		Roaring64NavigableMap y = Roaring64NavigableMap.bitmapOf(4L);

		{
			x.naivelazyor(y);
			x.repairAfterLazy();
			ImmutableRoaring64Bitmap ix = saveAndReopen(x);
			ImmutableBitmapDataProvider singleBitmap = ix.getHighToBitmap().values().iterator().next();
			Assertions.assertTrue(singleBitmap instanceof ImmutableRoaringBitmap);
			Assertions.assertEquals(2L, singleBitmap.getLongCardinality());
		}
	}

}
