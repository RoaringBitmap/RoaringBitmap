package me.lemire.roaringbitmap;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;

public class ArrayContainer implements Container, Cloneable, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public short[] content;
	int cardinality = 0;
	public final static int  DEFAULTMAXSIZE = 4096;	
	
	public void loadData(BitmapContainer bitmapContainer) {
	        if(content.length < bitmapContainer.cardinality)
	                content = new short[bitmapContainer.cardinality];
                this.cardinality = bitmapContainer.cardinality;
                int pos = 0;
                for (int i = bitmapContainer.nextSetBit( 0); i >= 0; i = bitmapContainer
                                .nextSetBit(i + 1)) {
                        content[pos++] = (short)i;
                }
                if(pos != this.cardinality) throw new RuntimeException("bug "+pos+" "+this.cardinality);
	}
		
	
	public ArrayContainer(int capacity) {
		content = new short[capacity];
	}
	
	public ArrayContainer() {
		content = new short[DEFAULTMAXSIZE];// we don't want more than DEFAULTMAXSIZE
	}
	
	@Override
        public boolean contains(short x) {
		return Util.unsigned_binarySearch(content, 0, cardinality, x) >= 0;
	}

	/**
	 * running time is in O(n) time if insert is not in order.
	 * 
	 */
	@Override
	public Container add(short x) {
		
	        if(( cardinality == 0 )  || (Util.toIntUnsigned(x) > Util.toIntUnsigned(content[cardinality-1]))) {
	                if (cardinality == content.length) {
                                BitmapContainer a = ContainerFactory.transformToBitmapContainer(this);
                                a.add(x);
                                return a;
                        }
	                content[cardinality++] = x;
	                return this;
	        }
		int loc = Util.unsigned_binarySearch(content, 0, cardinality, x);
		if (loc < 0) {
			// Transform the ArrayContainer to a BitmapContainer when cardinality = DEFAULTMAXSIZE 
			if (cardinality == content.length) {
				BitmapContainer a = ContainerFactory.transformToBitmapContainer(this);
				a.add(x);
				return a;
			}
			// insertion : shift the elements > x by one position to the right 
			// and put x in it's appropriate place
			System.arraycopy(content, -loc - 1, content, -loc, cardinality
					+ loc + 1);
			content[-loc - 1] = x;
			++cardinality;
		}
		return this;
	}

	@Override
	public Container remove(short x) {
		int loc = Util.unsigned_binarySearch(content, 0, cardinality, x);
		if (loc >= 0) {
			// insertion
			System.arraycopy(content, loc + 1, content, loc, cardinality - loc
					- 1);
			--cardinality;
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
		answer.cardinality = Util.unsigned_intersect2by2(value1.content,
				value1.getCardinality(), value2.content,
				value2.getCardinality(), answer.content);
		return answer;
	}

	public Container or(ArrayContainer value2) {
		ArrayContainer value1 = this;
		int tailleAC = value1.getCardinality()+value2.getCardinality();
		final int desiredcapacity = tailleAC > 65535 ? 65535 : tailleAC;
		//final int desiredcapacity = Math.min(value1.getCardinality() + value2.getCardinality(),65536);
		ArrayContainer answer = new ArrayContainer(desiredcapacity);
		answer.cardinality = Util.unsigned_union2by2(value1.content,
				value1.getCardinality(), value2.content,
				value2.getCardinality(), answer.content);
		if (answer.cardinality > DEFAULTMAXSIZE)
			return ContainerFactory.transformToBitmapContainer(answer);
		return answer;
	}
	
	@Override
	public void validate() {
		if(this.cardinality == 0) return;
		short val1 = this.content[0];
		HashSet<Short> hs = new HashSet<Short>();
		hs.add(val1);
		for(int k = 1; k< this.cardinality; ++k) {
			if(Util.toIntUnsigned(val1)>Util.toIntUnsigned(this.content[k])) 
				throw new RuntimeException("bug : content's not sorted");
			val1 = this.content[k];
			hs.add(val1);
		}
		if(hs.size()!=this.cardinality)		
			throw new RuntimeException("bug : ArrayContainer with repeated values");	
	}

	public Container xor(ArrayContainer value2) {
		ArrayContainer value1 = this;
		final int desiredcapacity = Math.min(value1.getCardinality() + value2.getCardinality(),65536);
		ArrayContainer answer = new ArrayContainer(desiredcapacity);
		answer.cardinality = Util.unsigned_exclusiveunion2by2(value1.content,
				value1.getCardinality(), value2.content,
				value2.getCardinality(), answer.content); 
		if (answer.cardinality > DEFAULTMAXSIZE)
			return ContainerFactory.transformToBitmapContainer(answer);
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

        @Override
        public ShortIterator getShortIterator() {
                return new ShortIterator() {
                        int pos = 0;

                        @Override
                        public boolean hasNext() {
                                return pos < ArrayContainer.this.cardinality;
                        }

                        @Override
                        public short next() {
                        	 return ArrayContainer.this.content[pos++];
                        }
                };
        }

        @Override
        public void clear() {
                cardinality = 0;                
        }
}
