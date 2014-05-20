package org.roaringbitmap.buffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;
import java.util.Arrays;


public final class ImmutableRoaringArray implements PointableArray {

	protected static final short SERIAL_COOKIE = MappeableRoaringArray.SERIAL_COOKIE;

	protected static final int INITIAL_CAPACITY = 4;

	protected short[] keys;
	protected int[] containeroffsets;
	protected short[] cardinalities;
	ByteBuffer buffer;

	private int getCardinality(int k) {
		return BufferUtil.toIntUnsigned(cardinalities[k]) + 1;
	}

	/**
	 * Create an array based on a previously serialized ByteBuffer.
	 * 
	 * @param bb
	 *            The source ByteBuffer
	 */
	protected ImmutableRoaringArray(ByteBuffer bb) {
		buffer = bb.duplicate();
		buffer.order(ByteOrder.LITTLE_ENDIAN);
		if (buffer.getInt() != SERIAL_COOKIE)
			throw new RuntimeException("I failed to find the right cookie.");
		int size = bb.getInt();
		buffer.mark();
		this.keys = new short[size];
		this.containeroffsets = new int[size + 1];
		for (int k = 0; k < size; ++k) {
			keys[k] = bb.getShort();
			cardinalities[k] = bb.getShort();
			boolean isBitmap = getCardinality(k) > MappeableArrayContainer.DEFAULT_MAX_SIZE;
			if (isBitmap)
				this.containeroffsets[k + 1] = this.containeroffsets[k]
						+ MappeableBitmapContainer.MAX_CAPACITY / 64 * 8;
			else
				this.containeroffsets[k + 1] = this.containeroffsets[k]
						+ getCardinality(k) * 2;
		}
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof ImmutableRoaringArray) {
			final ImmutableRoaringArray srb = (ImmutableRoaringArray) o;
			if (!Arrays.equals(keys,srb.keys))
				return false;
			if (!Arrays.equals(cardinalities,srb.cardinalities))
				return false;

			for (int i = 0; i < srb.keys.length; ++i) {
				if(!this.getContainerAtIndex(i).equals(srb.getContainerAtIndex(i)))
					return false;
			}
			return true;
		}

		if (o instanceof MappeableRoaringArray) {
			final MappeableRoaringArray srb = (MappeableRoaringArray) o;
			MappeableContainerPointer cp1 = srb.getContainerPointer();
			MappeableContainerPointer cp2 = srb.getContainerPointer();
			while(cp1.hasContainer()) {
				if(! cp2.hasContainer()) return false;
				if(cp1.key()!= cp2.key()) return false;
				if(cp1.getCardinality() != cp2.getCardinality()) return false;
				if(!cp1.getContainer().equals(cp2.getContainer())) return false;
			}
			if(cp2.hasContainer()) return false;
			return true;		
		}
		return false;
	}

	@Override
	public int hashCode() {
		MappeableContainerPointer cp = this.getContainerPointer();
		int hashvalue = 0;
		while(cp.hasContainer()) {
			int th = cp.key() * 0xF0F0F0 + cp.getContainer().hashCode();
			hashvalue = 31 * hashvalue + th;
		}
		return hashvalue;
	}


	// involves a binary search
	public MappeableContainer getContainer(short x) {

		final int i = unsignedBinarySearch(keys, 0, keys.length, x);
		if (i < 0)
			return null;
		return getContainerAtIndex(i);
	}

	protected static int unsignedBinarySearch(short[] array, int begin,
			int end, short k) {
		int low = begin;
		int high = end - 1;
		int ikey = BufferUtil.toIntUnsigned(k);

		while (low <= high) {
			final int middleIndex = (low + high) >>> 1;
			final int middleValue = BufferUtil
					.toIntUnsigned(array[middleIndex]);

			if (middleValue < ikey)
				low = middleIndex + 1;
			else if (middleValue > ikey)
				high = middleIndex - 1;
			else
				return middleIndex;
		}
		return -(low + 1);
	}
	
	public ImmutableRoaringArray clone()  {
		ImmutableRoaringArray sa;
		try {
			sa = (ImmutableRoaringArray) super.clone();
		} catch (CloneNotSupportedException e) {
			return null;// should never happen
		}
		return sa;
	}

	public MappeableContainer getContainerAtIndex(int i) {

		boolean isBitmap = getCardinality(i) > MappeableArrayContainer.DEFAULT_MAX_SIZE;
		ByteBuffer bb = buffer.duplicate();
		bb.position(this.containeroffsets[i]);
		if (isBitmap) {
			final LongBuffer bitmapArray = bb.asLongBuffer().slice();
			bitmapArray.limit(MappeableBitmapContainer.MAX_CAPACITY / 64);
			return new MappeableBitmapContainer(bitmapArray, getCardinality(i));
		} else {
			final ShortBuffer shortArray = bb.asShortBuffer().slice();
			shortArray.limit(getCardinality(i) * 2);
			return new MappeableArrayContainer(shortArray, getCardinality(i));
		}

	}

	// involves a binary search
	protected int getIndex(short x) {
		return  unsignedBinarySearch(keys, 0, keys.length, x);
	}

	protected short getKeyAtIndex(int i) {
		return this.keys[i];
	}

	protected int size() {
		return this.keys.length;
	}

	public MappeableContainerPointer getContainerPointer() {
		return new MappeableContainerPointer() {
			int k = 0;

			@Override
			public MappeableContainer getContainer() {
				if (k >= ImmutableRoaringArray.this.keys.length)
					return null;
				return ImmutableRoaringArray.this.getContainerAtIndex(k);
			}

			@Override
			public void advance() {
				++k;

			}

			@Override
			public short key() {
				return ImmutableRoaringArray.this.keys[k];

			}


			@Override
			public int compareTo(MappeableContainerPointer o) {
				if (key() != o.key())
					return BufferUtil.toIntUnsigned(key()) - BufferUtil.toIntUnsigned(o.key());
				return o.getCardinality()
						- getCardinality();
			}

			@Override
			public int getCardinality() {
				return ImmutableRoaringArray.this.getCardinality(k);
			}
			
			@Override
			public boolean hasContainer() {
				return k < ImmutableRoaringArray.this.keys.length;
			}


		};

	}

}
