package org.roaringbitmap.longlong;

import org.junit.Assert;
import org.junit.Test;

public class TestTreeRoaringBitmap {
	@Test
	public void testEmpty() {
		RoaringTreeMap map = new RoaringTreeMap();

		Assert.assertFalse(map.iterator().hasNext());

		Assert.assertEquals(0, map.getCardinality());

		Assert.assertEquals(0, map.rankLong(Long.MIN_VALUE));
		Assert.assertEquals(0, map.rankLong(Long.MIN_VALUE + 1));
		Assert.assertEquals(0, map.rankLong(-1));
		Assert.assertEquals(0, map.rankLong(0));
		Assert.assertEquals(0, map.rankLong(1));
		Assert.assertEquals(0, map.rankLong(Long.MAX_VALUE - 1));
		Assert.assertEquals(0, map.rankLong(Long.MAX_VALUE));
	}

	@Test
	public void testZero() {
		RoaringTreeMap map = new RoaringTreeMap();

		map.addLong(0);

		{
			LongIterator iterator = map.iterator();
			Assert.assertTrue(iterator.hasNext());
			Assert.assertEquals(0, iterator.next());
			Assert.assertEquals(0, map.select(0));
			Assert.assertFalse(iterator.hasNext());
		}

		Assert.assertEquals(1, map.getCardinality());

		Assert.assertEquals(0, map.rankLong(Long.MIN_VALUE));
		Assert.assertEquals(0, map.rankLong(Integer.MIN_VALUE - 1L));
		Assert.assertEquals(0, map.rankLong(-1));
		Assert.assertEquals(1, map.rankLong(0));
		Assert.assertEquals(1, map.rankLong(1));
		Assert.assertEquals(1, map.rankLong(Integer.MAX_VALUE + 1L));
		Assert.assertEquals(1, map.rankLong(Long.MAX_VALUE));
	}

	@Test
	public void testIterator_NextWithoutHasNext_Filled() {
		RoaringTreeMap map = new RoaringTreeMap();

		map.addLong(0);

		Assert.assertTrue(map.iterator().hasNext());
		Assert.assertEquals(0, map.iterator().next());
	}

	@Test(expected = IllegalStateException.class)
	public void testIterator_NextWithoutHasNext_Empty() {
		RoaringTreeMap map = new RoaringTreeMap();

		map.iterator().next();
	}

	@Test
	public void testLongMaxValue() {
		RoaringTreeMap map = new RoaringTreeMap();

		map.addLong(Long.MAX_VALUE);

		{
			LongIterator iterator = map.iterator();
			Assert.assertTrue(iterator.hasNext());
			Assert.assertEquals(Long.MAX_VALUE, iterator.next());
			Assert.assertEquals(Long.MAX_VALUE, map.select(0));
			Assert.assertFalse(iterator.hasNext());
		}

		Assert.assertEquals(1, map.getCardinality());

		Assert.assertEquals(0, map.rankLong(Long.MIN_VALUE));
		Assert.assertEquals(0, map.rankLong(Long.MIN_VALUE + 1));
		Assert.assertEquals(0, map.rankLong(-1));
		Assert.assertEquals(0, map.rankLong(0));
		Assert.assertEquals(0, map.rankLong(1));
		Assert.assertEquals(0, map.rankLong(Long.MAX_VALUE - 1));
		Assert.assertEquals(1, map.rankLong(Long.MAX_VALUE));
	}

	@Test
	public void testLongMinValue() {
		RoaringTreeMap map = new RoaringTreeMap();

		map.addLong(Long.MIN_VALUE);

		{
			LongIterator iterator = map.iterator();
			Assert.assertTrue(iterator.hasNext());
			Assert.assertEquals(Long.MIN_VALUE, iterator.next());
			Assert.assertEquals(Long.MIN_VALUE, map.select(0));
			Assert.assertFalse(iterator.hasNext());
		}

		Assert.assertEquals(1, map.getCardinality());

		Assert.assertEquals(1, map.rankLong(Long.MIN_VALUE));
		Assert.assertEquals(1, map.rankLong(Long.MIN_VALUE + 1));
		Assert.assertEquals(1, map.rankLong(-1));
		Assert.assertEquals(1, map.rankLong(0));
		Assert.assertEquals(1, map.rankLong(1));
		Assert.assertEquals(1, map.rankLong(Long.MAX_VALUE - 1));
		Assert.assertEquals(1, map.rankLong(Long.MAX_VALUE));
	}

	@Test
	public void testLongMinValueZeroOneMaxValue() {
		RoaringTreeMap map = new RoaringTreeMap();

		map.addLong(Long.MIN_VALUE);
		map.addLong(0);
		map.addLong(1);
		map.addLong(Long.MAX_VALUE);

		{
			LongIterator iterator = map.iterator();
			Assert.assertTrue(iterator.hasNext());
			Assert.assertEquals(Long.MIN_VALUE, iterator.next());
			Assert.assertEquals(Long.MIN_VALUE, map.select(0));
			Assert.assertEquals(0, iterator.next());
			Assert.assertEquals(0, map.select(1));
			Assert.assertEquals(1, iterator.next());
			Assert.assertEquals(1, map.select(2));
			Assert.assertEquals(Long.MAX_VALUE, iterator.next());
			Assert.assertEquals(Long.MAX_VALUE, map.select(3));
			Assert.assertFalse(iterator.hasNext());
		}

		Assert.assertEquals(4, map.getCardinality());

		Assert.assertEquals(1, map.rankLong(Long.MIN_VALUE));
		Assert.assertEquals(1, map.rankLong(Long.MIN_VALUE + 1));
		Assert.assertEquals(1, map.rankLong(-1));
		Assert.assertEquals(2, map.rankLong(0));
		Assert.assertEquals(3, map.rankLong(1));
		Assert.assertEquals(3, map.rankLong(2));
		Assert.assertEquals(3, map.rankLong(Long.MAX_VALUE - 1));
		Assert.assertEquals(4, map.rankLong(Long.MAX_VALUE));
	}

	@Test
	public void testPerfManyDifferentBuckets() {
		RoaringTreeMap map = new RoaringTreeMap();

		long problemSize = 100 * 1000L;
		for (long i = 1; i <= problemSize; i++) {
			map.addLong(i * Integer.MAX_VALUE + 1L);
		}

		long cardinality = map.getCardinality();
		Assert.assertEquals(problemSize, cardinality);

		long last = map.select(cardinality - 1);
		Assert.assertEquals(problemSize * Integer.MAX_VALUE + 1L, last);
		Assert.assertEquals(cardinality, map.rankLong(last));
	}
}
