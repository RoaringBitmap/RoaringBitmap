package me.lemire.roaringbitmap;

import java.util.Arrays;
import java.util.Iterator;

public class ArrayContainer implements Container {
	public short[] content = new short[1024];// we don't want more than 1024 2048
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

	@Override
	public boolean contains(short x) {
		return Arrays.binarySearch(content, 0, cardinality, x) >= 0;
	}

	/**
	 * running time is in O(n) time if insert is not in order.
	 * 
	 */
	@Override
	public Container add(short x) {
		int loc = Arrays.binarySearch(content, 0, cardinality, x);
		if (loc < 0) {
			if (cardinality == content.length) {
				BitmapContainer a = new BitmapContainer(this);
				a.add(x);
				return a;
			}
			// insertion : shift the elements > x by one position to the right 
			//and put x in its appropriate place
			System.arraycopy(content, -loc - 1, content, -loc, cardinality
					+ loc + 1);
			content[-loc - 1] = x;
			++cardinality;
		}
		return this;
	}

	@Override
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

	@Override
	public int getCardinality() {
		return cardinality;
	}

	@Override
	public Iterator<Short> iterator() {
		return new Iterator<Short>() {
			short pos = 0;

			@Override
			public boolean hasNext() {
				return pos < ArrayContainer.this.cardinality;
			}

			@Override
			public Short next() {
				return new Short(ArrayContainer.this.content[pos++]);
			}

			@Override
			public void remove() {
				ArrayContainer.this.remove(pos);
			}
		};
	}

	public ArrayContainer and(ArrayContainer value2) {
		ArrayContainer value1 = this;
		ArrayContainer answer = new ArrayContainer();
		answer.cardinality = Util.localintersect2by2(value1.content,
				value1.getCardinality(), value2.content,
				value2.getCardinality(), answer.content); // diminuer nbr params
		return answer;
	}

	public Container or(ArrayContainer value2) {
		ArrayContainer value1 = this;
		ArrayContainer answer = new ArrayContainer();
		answer.cardinality = Util.union2by2(value1.content,
				value1.getCardinality(), value2.content,
				value2.getCardinality(), answer.content); // diminuer nbr params
		if (answer.cardinality >= 1024)
			return new BitmapContainer(answer);
		return answer;
	}

	public Container xor(ArrayContainer value2) {
		ArrayContainer value1 = this;
		ArrayContainer answer = new ArrayContainer();
		answer.cardinality = Util.ExclusiveUnion2by2(value1.content,
				value1.getCardinality(), value2.content,
				value2.getCardinality(), answer.content); // diminuer nbr params
		//if (answer.cardinality == 0)
		//	return null;// Daniel: why on Earth???
		if (answer.cardinality >= 1024)
			return new BitmapContainer(answer);
		return answer;
	}

	@Override
	public String toString() {
		if (this.cardinality == 0)
			return "{}";
		StringBuffer sb = new StringBuffer();
		sb.append("{");
		for (int i = 0; i < this.cardinality - 1; i++) {
			sb.append(this.content[i]);
			sb.append(",");
		}
		sb.append(this.content[this.cardinality - 1]);
		sb.append("}");
		return sb.toString();
	}

	@Override
	public int getSizeInBits() {
		// TODO Auto-generated method stub
		return content.length*16;
	}

}
