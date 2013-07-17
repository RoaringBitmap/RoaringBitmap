package me.lemire.roaringbitmap;

import java.util.Arrays;
import java.util.Iterator;

public class BitmapContainer implements Container {
	long[] bitmap = new long[(1 << 16) / 64]; //65535 entiers peuvent y être stockés
	int cardinality;
	
	public BitmapContainer() {
		this.cardinality = 0;
	}
	
	public BitmapContainer(ArrayContainer arrayContainer) {
		this.cardinality = arrayContainer.cardinality;
		for (short x : arrayContainer.content)
			bitmap[x / 64] |= (1l << (x % 64));
	}

	public boolean contains(short i) {
		return (bitmap[i / 64] & (1l << (i % 64))) != 0;
	}

	public Container add(short i) {
		if (!contains(i)) {
			bitmap[i / 64] |= (1l << (i % 64));
			++cardinality;
		}
		return this;
	}

	public Container remove(short x) {
		if (contains(x)) {
			--cardinality;
			bitmap[x / 64] &= ~(1l << (x % 64));
			if (cardinality <= 1024)
				return new ArrayContainer(this);
		}
		return this;
	}

	// for(int i=bs.nextSetBit(0); i>=0; i=bs.nextSetBit(i+1)) { // operate on
	// index i here }
	public short nextSetBit(int i) {
		int x = i / 64;
		long w = bitmap[x];
		w >>>= (i % 64);
		if (w != 0) {
			return (short) (i + Long.numberOfTrailingZeros(w));
		}
		++x;
		for (; x < bitmap.length; ++x) {
			if (bitmap[x] != 0) {
				return (short) (x * 64 + Long.numberOfTrailingZeros(bitmap[x]));
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

	public void clear() {
		cardinality = 0;
		Arrays.fill(bitmap, 0);
	}

	public Iterator<Short> iterator() {
		return new Iterator<Short>() {
			int i = BitmapContainer.this.nextSetBit(0);

			public boolean hasNext() {
				return i >= 0;
			}

			public Short next() {
				short j = (short) i;
				i = BitmapContainer.this.nextSetBit(i + 1);
				return j;
			}

			public void remove() {
				BitmapContainer.this.remove((short) i);
			}

		};
	}

	public int getCardinality() {
		return cardinality;
	}
	
	public  Container and( BitmapContainer value2) {
		BitmapContainer value1 = this;
		BitmapContainer answer = new BitmapContainer();
		for(int k = 0; k<answer.bitmap.length;++k) //optimiser à max(last set bit of value1, value2)
			{ answer.bitmap[k]=value1.bitmap[k]&value2.bitmap[k];		
			  answer.cardinality+=Long.bitCount(answer.bitmap[k]);
			}
		if(cardinality < 1024)
			return new ArrayContainer(answer);
		return answer;
	}	

	public  ArrayContainer and(ArrayContainer value2) //intersect de 2 seq d'entiers
	{
		BitmapContainer value1 = this;
		ArrayContainer answer = new ArrayContainer();
		for(int k=0;k<value2.getCardinality();++k)
			if(value1.contains(value2.content[k]))
			answer.content[answer.cardinality++]=value2.content[k];
		return answer;
	}
	
	public  BitmapContainer or(ArrayContainer value2) //intersect de 2 seq d'entiers
	{
		BitmapContainer value1 = this;
		BitmapContainer answer = new BitmapContainer();
		for(int k=0; k<value2.getCardinality(); ++k)
			if(!value1.contains(value2.content[k])) // si la val de la seq !exist on l'ajoute ds le bitmap
				answer.bitmap[value2.content[k] / 64] |= (1l << (value2.content[k] % 64)); 
				
		return answer;
	}
	
	public  Container or( BitmapContainer value2) {
		BitmapContainer value1 = this;
		BitmapContainer answer = new BitmapContainer();
		for(int k = 0; k<answer.bitmap.length;++k) //optimiser à min(last set bit of value1, value2)
			{ answer.bitmap[k]=value1.bitmap[k]|value2.bitmap[k];
			  answer.cardinality+=Long.bitCount(answer.bitmap[k]);
			}
		if(cardinality < 1024)
			return new ArrayContainer(answer);
		return answer;
	}
	
	public  Container xor(ArrayContainer value2) //intersect de 2 seq d'entiers
	{
		BitmapContainer value1 = this;
		BitmapContainer answer = new BitmapContainer();
		for(int k=0; k<value2.getCardinality(); ++k)
			if(!value1.contains(value2.content[k])) // si la val de la seq !exist on l'ajoute ds le bitmap
				{answer.bitmap[value2.content[k] / 64] |= (1l << (value2.content[k] % 64));
				 answer.cardinality++;
				}
			else answer.bitmap[value2.content[k] / 64] &= ~(1l << (value2.content[k] % 64));
		if (answer.cardinality==0) return null;
		if(answer.cardinality<1024) return new ArrayContainer(answer);
		return answer;
	}	
	
	public  Container xor( BitmapContainer value2) {
		BitmapContainer value1 = this;
		BitmapContainer answer = new BitmapContainer();
		for(int k = 0; k<answer.bitmap.length;++k)
			{answer.bitmap[k]=value1.bitmap[k] ^ value2.bitmap[k];
			 answer.cardinality+=Long.bitCount(answer.bitmap[k]);
			}		
		if(answer.cardinality==0) return null;	
		if(answer.cardinality < 1024)
			return new ArrayContainer(answer);
		return answer;
	}
	
	public void afficher() {
		int i=this.nextSetBit(0); 
		do { System.out.print(i+" p "); i=this.nextSetBit(i+1); }while(i>=0);
	}
}
