package me.lemire.roaringbitmap;

import java.util.Arrays;
import java.util.Iterator;

public class ArrayContainer implements Container {
	short[] content = new short[2048];// we don't want more than 1024
	int cardinality = 0;

	public ArrayContainer(BitmapContainer bitmapContainer) {
		this.cardinality = bitmapContainer.cardinality;
		for (short i = bitmapContainer.nextSetBit((short) 0); i >= 0; i = bitmapContainer
				.nextSetBit((short) (i + 1))) {
			content[cardinality++] = i;
		}
	}

	public ArrayContainer() {
	}

	public boolean contains(short x) {
		return Arrays.binarySearch(content, 0, cardinality, x) >= 0;
	}

	/**
	 * running time is in O(n) time if insert is not in order.
	 * 
	 */
	public Container add(short x) {
		int loc = Arrays.binarySearch(content, 0, cardinality, x);
		if (loc < 0) {
			if (cardinality == content.length) {
				BitmapContainer a = new BitmapContainer(this);
				a.add(x);
				return a;
			}
			// insertion
			System.arraycopy(content, -loc - 1, content, -loc, cardinality
					+ loc + 1);
			content[-loc - 1] = x;
			++cardinality;
		}
		return this;
	}

	public Container remove(short x) {
		int loc = Arrays.binarySearch(content, 0, cardinality, x);
		if (loc >= 0) {
			// insertion
			System.arraycopy(content, loc + 1, content, loc, cardinality - loc
					- 1);
			++cardinality;
		}
		return this;

	}

	public int getCardinality() {
		return cardinality;
	}

	public Iterator<Short> iterator() {
		return new Iterator<Short>() {
			short pos = 0;

			public boolean hasNext() {
				return pos < ArrayContainer.this.cardinality;
			}

			public Short next() {
				return ArrayContainer.this.content[pos++];
			}

			public void remove() {
				ArrayContainer.this.remove(pos);
			}

		};
	}

	public  ArrayContainer and( ArrayContainer value2) {
		ArrayContainer value1 = this;
		ArrayContainer answer = new ArrayContainer();
		answer.cardinality = Util.localintersect2by2(value1.content, value1.getCardinality(), value2.content, value2.getCardinality(),answer.content );
		return answer;
	}

}
