package me.lemire.roaringbitmap;

import java.util.Arrays;

public final class SpeedyArray implements Cloneable {
        Element[] array = null;
        int nbKeys = 0;
        final static int initialCapacity = 4;
        
        public SpeedyArray() {
                this.array = new Element[initialCapacity];
        }
        
        public final class Element {
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
                else {        //When the key exists yet                
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
                                newcapacity = 2 * this.array.length;
                        } else {
                                newcapacity = 5 * this.array.length / 4; 
                        }
                        this.array = Arrays.copyOf(this.array, newcapacity);
                }
        }
        
        public void removeAtIndex(int i) {
                System.arraycopy(array, i+1, array, i, nbKeys - i - 1);
                nbKeys--;
        }
        
        public boolean remove(short key) {
                int i = binarySearch(0, nbKeys, key);
                if(i>=0) { //if a new key
                        removeAtIndex(i);
                        return true;
                }
                return false;
        }
        
        public void putEnd(short key, Container value) {
                extendArray();
                this.array[this.nbKeys++] = new Element(key, value);                
        }
        
        public Container getContainer(short x) {
                int i = this.binarySearch(0, nbKeys, x);
                if(i<0) return null;
                return this.array[i].value;
        }
        public int getIndex(short x) {
                return this.binarySearch(0, nbKeys, x);
        }
        public Element getAtIndex(int i) {
                return this.array[i];
        }

        public void clear() {
                this.nbKeys = 0;
        }
        
        public Element[] getArray() {
                return this.array;
        }
        public int size() {
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
        
        
        public int binarySearch(int begin, int end, short key) {
                int low = begin;
                int high = end - 1;
                int ikey = Util.toIntUnsigned(key);

                while (low <= high) {
                        int middleIndex = (low + high) >>> 1;
                        int middleValue = Util
                                .toIntUnsigned(array[middleIndex].key);

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
