package me.lemire.roaringbitmap;

public class SpeedyArray {
	Element[] array = null;
	int nbKeys = 0;
	
	public SpeedyArray(int capacity) {
		this.array = new Element[capacity];
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
		//return (linearSearch(array, nbKeys, x)>=0);
	}
	
	public void put(short key, Container value) {
		
		int i = //linearSearch(array, nbKeys, key);
				binarySearch(0, nbKeys, key);
		if(i<0) { //if a new key
			System.arraycopy(array, -i - 1, array, -i, nbKeys + i + 1);
			array[-i - 1] = new Element(key, value);
			nbKeys++;
		}
		else {	//When the key exist yet		
			this.array[i].value = value;
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
		this.array[this.nbKeys++] = new Element(key, value);		
	}
	
	public Container get(short x) {
		int i = //linearSearch(array, nbKeys, x);
				this.binarySearch(0, nbKeys, x);
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
	
	public Element[] getArray() {
		return this.array;
	}
	
	public int getnbKeys() {
		return this.nbKeys;
	}
	
	public SpeedyArray clone() {
		SpeedyArray sa = new SpeedyArray(this.nbKeys);
		sa.array = this.array.clone();
		sa.nbKeys = this.nbKeys;
		return sa;
	}
	
	public int linearSearch(Element[] array2, int nbKeys, short key){
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
