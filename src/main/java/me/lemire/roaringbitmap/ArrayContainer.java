package me.lemire.roaringbitmap;

import java.util.Arrays;
import java.util.Iterator;

public class ArrayContainer implements Container, Cloneable {
	public short[] content;
	int cardinality = 0;
		
	public ArrayContainer(BitmapContainer bitmapContainer) {
		content = new short[bitmapContainer.cardinality];
		this.cardinality = bitmapContainer.cardinality;
		int pos = 0;
		for (int i = bitmapContainer.nextSetBit( 0); i >= 0; i = bitmapContainer
				.nextSetBit(i + 1)) {
			content[pos++] = (short)i;
		}
		if(pos != this.cardinality) throw new RuntimeException("bug");
		Arrays.sort(content);
	}
	
	public ArrayContainer(int capacity) {
		content = new short[capacity];
	}
	
	public ArrayContainer() {
		content = new short[2048];// we don't want more than 1024
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
		final int desiredcapacity = Math.min(value1.getCardinality(),  value2.getCardinality());
		ArrayContainer answer = new ArrayContainer(desiredcapacity);
		answer.cardinality = Util.localintersect2by2(value1.content,
				value1.getCardinality(), value2.content,
				value2.getCardinality(), answer.content); // diminuer nbr params
		return answer;
	}

	public Container or(ArrayContainer value2) {
		ArrayContainer value1 = this;
		final int desiredcapacity = Math.min(value1.getCardinality() + value2.getCardinality(),65536);
		ArrayContainer answer = new ArrayContainer(desiredcapacity);
		answer.cardinality = Util.union2by2(value1.content,
				value1.getCardinality(), value2.content,
				value2.getCardinality(), answer.content); // diminuer nbr params
		if (answer.cardinality >= 1024)
			return new BitmapContainer(answer);
		return answer;
	}
	
	@Override
	public void validate() {
		if(this.cardinality == 0) return;
		short val1 = this.content[0];
		for(int k = 1; k< this.cardinality; ++k) {
			if(val1>this.content[k]) throw new RuntimeException("bug : content's not sorted");
			val1 = this.content[k];
		}
	}

	public Container xor(ArrayContainer value2) {
		ArrayContainer value1 = this;
		final int desiredcapacity = Math.min(value1.getCardinality() + value2.getCardinality(),65536);
		ArrayContainer answer = new ArrayContainer(desiredcapacity);
		answer.cardinality = Util.ExclusiveUnion2by2(value1.content,
				value1.getCardinality(), value2.content,
				value2.getCardinality(), answer.content); 
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
		return this.cardinality*16;
	}
	
	@Override
	public ArrayContainer clone() {
		try {
			ArrayContainer x = (ArrayContainer) super.clone();
			x.cardinality = this.cardinality;
			x.content = Arrays.copyOf(content,content.length);
			return x;
		} catch (CloneNotSupportedException e) {
			throw new java.lang.RuntimeException();
		}
	}

}
