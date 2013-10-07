package me.lemire.roaringbitmap;

import java.util.Arrays;

public final class SpeedyArray implements Cloneable {
	Element[] array = null;
	int nbKeys = 0;
	static int initialCapacity = 4;
	
	public SpeedyArray() {
		this.array = new Element[initialCapacity];
	}
	
	public class Element {
		public short key;
		public Container value=null;
	
		public Element(short key, Container value) {
			this.key = key;
			this.value = value;
		}
	}
	
	public boolean ContainsKey(short x) {
		return (binarySearch(0, nbKeys, x) >= 0);
	}
	
	public void put(short key, Container value) {
		
		int i = binarySearch(0, nbKeys, key);
		
		if(i<0) { //if a new key
			extendArray();
			System.arraycopy(array, -i - 1, array, -i, nbKeys + i + 1);
			array[-i - 1] = new Element(key, value);
			nbKeys++;
		}
		else {	//When the key exists yet		
			this.array[i].value = value;
		}
	}
	
        public void extendArray() {
                // size + 1 could overflow
                if (this.nbKeys == this.array.length) {
                        int newcapacity;
                        if (this.array.length < 4) {
                                newcapacity = 4;
                        } else if (this.array.length < 1024) {
                                newcapacity = 2 * this.array.length; // grow
                                                                     // fast
                                                                     // initially
                        } else {
                                newcapacity = 5 * this.array.length / 4; // inspired
                                                                         // by
                                                                         // Go,
                                                                         // see
                                                                         // http://golang.org/src/pkg/runtime/slice.c#L131
                        }
                        this.array = Arrays.copyOf(this.array, newcapacity);
                }
        }
	
	public boolean remove(short key) {
		int i = binarySearch(0, nbKeys, key);
		if(i>=0) { //if a new key
			System.arraycopy(array, i+1, array, i, nbKeys - i - 1);
			nbKeys--;
			return true;
		}
		return false;
	}
	
	public void putEnd(short key, Container value) {
		extendArray();
		this.array[this.nbKeys++] = new Element(key, value);		
	}
	
	public Container get(short x) {
		int i = this.binarySearch(0, nbKeys, x);
		if(i<0) return null;
		return this.array[i].value;
	}
	
	public void InsertionSort() {
		for (int j=1; j<nbKeys; j++) {
			Element e = array[j];
			int key = Util.toIntUnsigned(e.key);			
			int i = j-1;
			
			while(i>0 && Util.toIntUnsigned(array[i].key)>key) {
				array[i+1] = array[i];
				i--;
			}
			array[i+1] = e;
		}
	}
	
	public boolean validateOrdering() {
		int i;
		for (i=1; i<nbKeys && Util.toIntUnsigned(array[i-1].key)<=Util.toIntUnsigned(array[i].key); i++);
		if(i==nbKeys) return true;
		return false;
	}
	
	public void clear() {
		this.nbKeys = 0;
	}
	
	public Element[] getArray() {
		return this.array;
	}
	
	public int getnbKeys() {
		return this.nbKeys;
	}
	
	@Override
        public SpeedyArray clone() throws CloneNotSupportedException {
		SpeedyArray sa;
		sa = (SpeedyArray) super.clone();
		sa.array = Arrays.copyOf(this.array, this.nbKeys);
		sa.nbKeys = this.nbKeys;
		return sa;
	}
	
	public static int linearSearch(Element[] array2, int nbKeys, short key){
		int i;
		for(i=0; i<nbKeys && array2[i].key!=key; i++); 
		if(i==nbKeys) return -1;
		return i;
	}
	
    public int binarySearch(int begin, int end, short key) {
            int low = begin;
            int high = end-1;
            int ikey = Util.toIntUnsigned(key);

            while (low <= high) {
                    int middleIndex = (low + high) >>> 1;
                    int middleValue = Util.toIntUnsigned(array[middleIndex].key);

                    if (middleValue < ikey)
                            low = middleIndex + 1;
                    else if (middleValue > ikey)
                            high = middleIndex - 1;
                    else
                            return middleIndex;
            }
            return -(low + 1);
    }
}
