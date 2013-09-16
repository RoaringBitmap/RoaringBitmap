package me.lemire.roaringbitmap;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

public final class BitmapContainer implements Container, Cloneable, Serializable {

	private static final long serialVersionUID = 2L;
	long[] bitmap = new long[(1 << 16) / 64]; //a max of 65535 integers
	int cardinality;

	
	public BitmapContainer() {
		this.cardinality = 0;
	}
	
	public void loadData(final ArrayContainer arrayContainer) {
                this.cardinality = arrayContainer.cardinality;
                
                for(int k = 0; k < arrayContainer.cardinality; ++k) {
                        final short x = arrayContainer.content[k];
                        bitmap[Util.toIntUnsigned(x)/64] |= (1l << x);
                }                
	}

	
	@Override
        public boolean contains(final short i) {
	        final int x = Util.toIntUnsigned(i);
		return (bitmap[x/64] & (1l << x )) != 0;		
	}

	@Override
	public Container add(final short i) {
		final int x = Util.toIntUnsigned(i);
		final long previous = bitmap[x/64];
		if(previous != (bitmap[x/64] |= (1l << x)) )
		        ++cardinality;
		return this;
	}

	@Override
	public Container remove(final short x) {
		if (contains(x)) {
	                --cardinality;
	                bitmap[x / 64] &= ~(1l << x );
			if (cardinality < ArrayContainer.DEFAULTMAXSIZE) {
				return ContainerFactory.transformToArrayContainer(this);
			}
		}
		return this;
	}

	public int nextSetBit(final int i) {
	        int x = i / 64;
		if(x>=bitmap.length) return -1;
		long w = bitmap[x];
		w >>>= i;
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

	public short nextUnsetBit(final int i) {
		int x = i / 64;
		long w = ~bitmap[x];
		w >>>= i;
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

	public Container and(final BitmapContainer value2) {
	        final BitmapContainer answer = ContainerFactory.getBitmapContainer();
		for (int k = 0; k < answer.bitmap.length; ++k) 
		{
			answer.bitmap[k] = this.bitmap[k] & value2.bitmap[k];
			if(answer.bitmap[k]!=0)// this might happen often enough, but performance effect should be checked
				answer.cardinality += Long.bitCount(answer.bitmap[k]);
		}
		if (answer.cardinality <= ArrayContainer.DEFAULTMAXSIZE)
			return ContainerFactory.transformToArrayContainer(answer);
		return answer;
	}

	public ArrayContainer and(final ArrayContainer value2) 
	{		
	        final ArrayContainer answer = ContainerFactory.getArrayContainer();
		if(answer.content.length<value2.content.length)
		        answer.content = new short[value2.content.length];
		for (int k = 0; k < value2.getCardinality(); ++k)
			if (this.contains(value2.content[k]))
				answer.content[answer.cardinality++] = value2.content[k];
		return answer;
	}

	public BitmapContainer or(final ArrayContainer value2) 
	{		
	        final BitmapContainer answer = ContainerFactory.getCopyOfBitmapContainer(this);
		for (int k = 0; k < value2.cardinality; ++k)	{				
		        final int i = Util.toIntUnsigned(value2.content[k]) >>> 6;
			answer.cardinality += ((~answer.bitmap[i]) & (1l << value2.content[k])) >>> value2.content[k] ;// in Java, shifts are always "modulo"
			answer.bitmap[i] = answer.bitmap[i]
                                        | (1l << value2.content[k]);
		}
		return answer;
	}

	public Container or(final BitmapContainer value2) {
	        final BitmapContainer answer = ContainerFactory.getBitmapContainer();
		for (int k = 0; k < answer.bitmap.length; ++k) 
		{
			answer.bitmap[k] = this.bitmap[k] | value2.bitmap[k];
			//if(answer.bitmap[k]!=0) //DL: I am not sure that the branching helps here. Would need to benchmark this.
	        	answer.cardinality += Long.bitCount(answer.bitmap[k]);
		}		
		return answer;
	}	

	@Override
	public int getSizeInBits() {
		//the standard size is DEFAULTMAXSIZE chunks * 64bits each=65536 bits, 
		//each 1 bit represents an integer from 0 to 65535
		return 65536 + 32; 
	}
	@Override
        public int getSizeInBytes() {
                return this.bitmap.length * 8 + 4;
        }
        
	public Container xor(final ArrayContainer value2) 
	{
	        final BitmapContainer answer = ContainerFactory.getCopyOfBitmapContainer(this);

		for (int k = 0; k < value2.getCardinality(); ++k) {
		        final int index = Util.toIntUnsigned(value2.content[k]) >>> 6;
		        answer.cardinality +=  1- 2*((answer.bitmap[index] & (1l << value2.content[k] )) >>> value2.content[k]);
                        answer.bitmap[index] = answer.bitmap[index]
                                        ^ (1l << value2.content[k] );

		}	
		if (answer.cardinality <= ArrayContainer.DEFAULTMAXSIZE)
			return ContainerFactory.transformToArrayContainer(answer);
		return answer;
	}
	
	public Container xor(BitmapContainer value2) {		
        final BitmapContainer answer = ContainerFactory.getBitmapContainer();
	for (int k = 0; k < answer.bitmap.length; ++k) {
		answer.bitmap[k] = this.bitmap[k] ^ value2.bitmap[k];
		//if(answer.bitmap[k]!=0) // probably not wise performance-wise
		answer.cardinality += Long.bitCount(answer.bitmap[k]);
	}
	if (answer.cardinality <= ArrayContainer.DEFAULTMAXSIZE)
		return ContainerFactory.transformToArrayContainer(answer);
	return answer;
	}
	
	public Container inPlaceAND(final BitmapContainer B2) {
		this.cardinality = 0;
		for(int k=0; k<this.bitmap.length; k++) {
			this.bitmap[k] &= B2.bitmap[k];
			//this.cardinality += Long.bitCount(this.bitmap[k]);
		}
		this.cardinality = this.expensiveComputeCardinality();
		return this;
	}
	
	
	public Container inPlaceOR(final BitmapContainer B2) {
		this.cardinality = 0;
		for(int k=0; k<this.bitmap.length; k++) {
			this.bitmap[k] |= B2.bitmap[k];
			//this.cardinality += Long.bitCount(this.bitmap[k]);
		}
		this.cardinality = this.expensiveComputeCardinality();
		return this;
	}
	
	public BitmapContainer inPlaceOR(final ArrayContainer value2) 
	{		
	     for (int k = 0; k < value2.cardinality; ++k)	{				
		    final int i = Util.toIntUnsigned(value2.content[k]) >>> 6;
			this.cardinality += ((~this.bitmap[i]) & (1l << value2.content[k])) >>> value2.content[k] ;// in Java, shifts are always "modulo"
			this.bitmap[i] |= (1l << value2.content[k]);
		}
	     
	     //DL: Please don't call "expensiveComputeCardinality" here unless you have benchmarked
	     // it and you know it is faster. If there is
	     // a bug above, please write a unit test for it.
	    //this.cardinality = this.expensiveComputeCardinality();
		return this;
	}
	
	public Container inPlaceXOR(BitmapContainer B2) {
	this.cardinality = 0;
	for (int k = 0; k < this.bitmap.length; ++k) {
		this.bitmap[k] ^= B2.bitmap[k];
		//if(answer.bitmap[k]!=0) // probably not wise performance-wise
		//this.cardinality += Long.bitCount(this.bitmap[k]);
	}
	this.cardinality = this.expensiveComputeCardinality();
	if (this.cardinality <= ArrayContainer.DEFAULTMAXSIZE)
		return ContainerFactory.transformToArrayContainer(this);
	return this;
	}
	
	public Container inPlaceXOR(final ArrayContainer value2) 
	{
                for (int k = 0; k < value2.getCardinality(); ++k) {
                        final int index = Util.toIntUnsigned(value2.content[k]) >>> 6;
                        this.cardinality += 1 - 2 * ((this.bitmap[index] & (1l << value2.content[k])) >>> value2.content[k]);
                        this.bitmap[index] ^=  (1l << value2.content[k]);
                }
                // DL: Please don't call "expensiveComputeCardinality" unless your benchmarks
                // should that it is faster. Debug code above, use unit tests to report problem.
                // this.cardinality = this.expensiveComputeCardinality();
                if (this.cardinality <= ArrayContainer.DEFAULTMAXSIZE)
                        return ContainerFactory.transformToArrayContainer(this);
                return this;
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
		throw new RuntimeException("bug : true cardinality is "+card+", precomputed cardinality is"+this.cardinality);
	}
	 
	@Override
	public BitmapContainer clone() {
		try {
		        final BitmapContainer x = (BitmapContainer) super.clone();
			x.cardinality = this.cardinality;
			x.bitmap = Arrays.copyOf(this.bitmap, this.bitmap.length);
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
