package me.lemire.roaringbitmap;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

public class BitmapContainer implements Container, Cloneable, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 2L;
	long[] bitmap = new long[(1 << 16) / 64]; //a max of 65535 integers
	int cardinality;

	
	public BitmapContainer() {
		this.cardinality = 0;
	}
	
	public void loadData(ArrayContainer arrayContainer) {
                this.cardinality = arrayContainer.cardinality;
                
                for(int k = 0; k < arrayContainer.cardinality; ++k) {
                        final short x = arrayContainer.content[k];
                        bitmap[Util.toIntUnsigned(x)/64] |= (1l << (x % 64));
                }                
	}

	
	@Override
        public boolean contains(short i) {
	        final int x = Util.toIntUnsigned(i);
		return (bitmap[x/64] & (1l << (x % 64))) != 0;		
	}

	@Override
	public Container add(short i) {
		final int x = Util.toIntUnsigned(i);
		final long previous = bitmap[x/64];
		if(previous != (bitmap[x/64] |= (1l << (x % 64))) )
		        ++cardinality;
		return this;
	}

	@Override
	public Container remove(short x) {
		if (contains(x)) {
			--cardinality;
			bitmap[x / 64] &= ~(1l << (x % 64));
			if (cardinality < ArrayContainer.DEFAULTMAXSIZE) {
				return ContainerFactory.transformToArrayContainer(this);
			}
		}
		return this;
	}

	public int nextSetBit(int i) {
		int x = i / 64;
		if(x>=bitmap.length) return -1;
		long w = bitmap[x];
		w >>>= (i % 64);
		if (w != 0) {
			return i + Long.numberOfTrailingZeros(w);
		}
		++x;
		for (; x < bitmap.length; ++x) {
			if (bitmap[x] != 0) {
				return x * 64 + Long.numberOfTrailingZeros(bitmap[x]);
			}
		}
		return -1;
	}

	public short nextUnsetBit(int i) {
		int x = i / 64;
		long w = ~bitmap[x];
		w >>>= (i % 64);
		if (w != 0) {
			return (short) (i + Long.numberOfTrailingZeros(w));
		}
		++x;
		for (; x < bitmap.length; ++x) {
			if (bitmap[x] != ~0) {
				return (short) (x * 64 + Long.numberOfTrailingZeros(~bitmap[x]));
			}
		}
		return -1;
	}

	@Override
        public void clear() {
		this.cardinality = 0;
		Arrays.fill(bitmap, 0);
	}

	@Override
	public Iterator<Short> iterator() {
		return new Iterator<Short>() {
			int i = BitmapContainer.this.nextSetBit(0);

			@Override
			public boolean hasNext() {
				return i >= 0;
			}

			@Override
			public Short next() {
				short j = (short) i;
				i = BitmapContainer.this.nextSetBit(i + 1);
				return new Short(j);
			}

			@Override
			public void remove() {
				BitmapContainer.this.remove((short) i);
			}

		};
	}

	@Override
	public int getCardinality() {
		return cardinality;
	}

	public Container and(BitmapContainer value2) {
		
		BitmapContainer answer = new BitmapContainer();
		for (int k = 0; k < answer.bitmap.length; ++k) 
		{
			answer.bitmap[k] = this.bitmap[k] & value2.bitmap[k];
			if(answer.bitmap[k]!=0)
				answer.cardinality += Long.bitCount(answer.bitmap[k]);
		}
		if (answer.cardinality <= ArrayContainer.DEFAULTMAXSIZE)
			return ContainerFactory.transformToArrayContainer(answer);
		return answer;
	}

	public ArrayContainer and(ArrayContainer value2) 
	{		
		ArrayContainer answer = new ArrayContainer();
		for (int k = 0; k < value2.getCardinality(); ++k)
			if (this.contains(value2.content[k]))
				answer.content[answer.cardinality++] = value2.content[k];
		return answer;
	}

	public BitmapContainer or(ArrayContainer value2) 
	{		
		BitmapContainer answer = new BitmapContainer();
		for (int k = 0; k < value2.cardinality; ++k)	{				
			int i = Util.toIntUnsigned(value2.content[k])/64;
			long previous = answer.bitmap[i];
			if(previous==0) {
				answer.bitmap[i] = this.bitmap[i] | (1l << (value2.content[k] % 64));
				answer.cardinality += Long.bitCount(answer.bitmap[i]);
			}
			else {				
				answer.bitmap[i] = answer.bitmap[i] | (1l << (value2.content[k] % 64));
    	   		long newv = answer.bitmap[i];
    	   		if(previous<0 && newv>=0) answer.cardinality--;
    	   		else if((previous>0 && newv<0)||(previous<newv)) answer.cardinality++;    	   		
			}			 			
		}
		return answer;
	}

	public Container or(BitmapContainer value2) {
		
		BitmapContainer answer = new BitmapContainer();
		for (int k = 0; k < answer.bitmap.length; ++k) 
		{
			answer.bitmap[k] = this.bitmap[k] | value2.bitmap[k];
			if(answer.bitmap[k]!=0)
	        	answer.cardinality += Long.bitCount(answer.bitmap[k]);
		}		
		return answer;
	}

	@Override
	public int getSizeInBits() {
		//the standard size is DEFAULTMAXSIZE chunks * 64bits each=65536 bits, 
		//each 1 bit represents an integer from 0 to 65535
		return 65536; 
	}

	public Container xor(ArrayContainer value2) 
	{
		BitmapContainer answer = new BitmapContainer();
		for (int k = 0; k < value2.getCardinality(); ++k) {
		        final int index = Util.toIntUnsigned(value2.content[k])/64;
		        long previous = answer.bitmap[index];
		       if(previous == 0) { 
		    	   answer.bitmap[index] = this.bitmap[index] ^ (1l << (value2.content[k] % 64));
		    	   answer.cardinality += Long.bitCount(answer.bitmap[index]);
		       }
		       else {
		    	   		answer.bitmap[index] ^=  (1l << (value2.content[k] % 64));
		    	   		long newv = answer.bitmap[index];
		    	   		if(previous<0 && newv>=0) answer.cardinality--;
		    	   		else if((previous>=0 && newv<0) || (previous<newv)) answer.cardinality++;
		    	   		else if(previous>newv) answer.cardinality--;
		       		}
		}		
		if (answer.cardinality <= ArrayContainer.DEFAULTMAXSIZE)
			return ContainerFactory.transformToArrayContainer(answer);
		return answer;
	}

	public Container xor(BitmapContainer value2) {
		
		BitmapContainer answer = new BitmapContainer();
		for (int k = 0; k < answer.bitmap.length; ++k) {
			answer.bitmap[k] = this.bitmap[k] ^ value2.bitmap[k];
			if(answer.bitmap[k]!=0)
				answer.cardinality += Long.bitCount(answer.bitmap[k]);
		}
		if (answer.cardinality <= ArrayContainer.DEFAULTMAXSIZE)
			return ContainerFactory.transformToArrayContainer(answer);
		return answer;
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		int counter = 0;
		sb.append("{");
		int i = this.nextSetBit(0);
		while (i >= 0) {
			sb.append(i);
			++counter;
			i = this.nextSetBit(i + 1);
			if (i >= 0)
				sb.append(",");
		} 
		sb.append("}");
		System.out.println("cardinality = "+cardinality+" "+counter);
		return sb.toString();
	}

	private int expensiveComputeCardinality() {
		int counter = 0;
		for(long x : this.bitmap)
			counter += Long.bitCount(x);
		
		return counter;
	}	
	
	@Override
	public void validate() {
		int counter = 0;
		{
			int i = this.nextSetBit(0);
			while (i >= 0) {
				++counter;
				i = this.nextSetBit(i + 1);
			}
		}
		int card = this.expensiveComputeCardinality();
		if(card != counter) throw	new RuntimeException("problem"); 
		if(card != this.cardinality)
		throw new RuntimeException("bug : "+card+" "+this.cardinality);
	}
	 
	@Override
	public BitmapContainer clone() {
		try {
			BitmapContainer x = (BitmapContainer) super.clone();
			x.cardinality = this.cardinality;
			x.bitmap = Arrays.copyOf(bitmap,bitmap.length);
			return x;
		} catch (CloneNotSupportedException e) {
			throw new java.lang.RuntimeException();
		}
	}

        @Override
        public ShortIterator getShortIterator() {
                return new ShortIterator() {
                        int i = BitmapContainer.this.nextSetBit(0);

                        @Override
                        public boolean hasNext() {
                                return i >= 0;
                        }

                        @Override
                        public short next() {
                                short j = (short) i;
                                i = BitmapContainer.this.nextSetBit(i + 1);
                                return j;
                        }

                };

        }
}
