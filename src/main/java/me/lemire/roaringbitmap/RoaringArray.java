package me.lemire.roaringbitmap;

import java.util.Arrays;

public final class RoaringArray implements Cloneable {
        private final class Element {
                public Element(short key, Container value) {
                        this.key = key;
                        this.value = value;
                }

                public short key;

                public Container value = null;
        }

        public RoaringArray() {
                this.array = new Element[initialCapacity];
        }

        public void append(short key, Container value) {
                extendArray(1);
                this.array[this.size++] = new Element(key, value);
        }

        /**
         * Append copy of the one value from another array
         * 
         * @param sa
         * @param startingindex
         *                starting index in the other array
         * @param end
         */
        public void appendCopy(RoaringArray sa, int index) {
                extendArray(1);
                this.array[this.size++] = new Element(sa.array[index].key,
                        sa.array[index].value.clone());
        }

        /**
         * Append copies of the values from another array
         * 
         * @param sa
         * @param startingindex
         *                starting index in the other array
         * @param end
         */
        public void appendCopy(RoaringArray sa, int startingindex, int end) {
                extendArray(end - startingindex);
                for (int i = startingindex; i < end; ++i) {
                        this.array[this.size++] = new Element(sa.array[i].key,
                                sa.array[i].value.clone());
                }

        }

        public void clear() {
                this.array = null;
                this.size = 0;
        }

        @Override
        public RoaringArray clone() throws CloneNotSupportedException {
                RoaringArray sa;
                sa = (RoaringArray) super.clone();
                sa.array = Arrays.copyOf(this.array, this.size);
                sa.size = this.size;
                return sa;
        }

        public boolean ContainsKey(short x) {
                return (binarySearch(0, size, x) >= 0);
        }

        @Override
        public boolean equals(Object o) {
                if (o instanceof RoaringArray) {
                        RoaringArray srb = (RoaringArray) o;
                        if (srb.size != this.size)
                                return false;
                        for (int i = 0; i < srb.size; ++i) {
                                if (this.array[i].key != srb.array[i].key)
                                        return false;
                                if (!this.array[i].value
                                        .equals(srb.array[i].value))
                                        return false;
                        }
                        return true;
                }
                return false;
        }

        // make sure there is capacity for at least k more elements
        public void extendArray(int k) {
                // size + 1 could overflow
                if (this.size + k >= this.array.length) {
                        int newcapacity;
                        if (this.array.length < 1024) {
                                newcapacity = 2 * (this.size + k);
                        } else {
                                newcapacity = 5 * (this.size + k) / 4;
                        }
                        this.array = Arrays.copyOf(this.array, newcapacity);
                }
        }

        // involves a binary search
        public Container getContainer(short x) {
                int i = this.binarySearch(0, size, x);
                if (i < 0)
                        return null;
                return this.array[i].value;
        }

        public Container getContainerAtIndex(int i) {
                return this.array[i].value;
        }

        // involves a binary search
        public int getIndex(short x) {
                // before the binary search, we optimize for frequent cases
                if ((size == 0) || (array[size - 1].key == x))
                        return size - 1;
                // no luck we have to go through the list
                return this.binarySearch(0, size, x);
        }

        public short getKeyAtIndex(int i) {
                return this.array[i].key;
        }

        @Override
        public int hashCode() {
                return array.hashCode();
        }

        // insert a new key, it is assumed that it does not exist
        public void insertNewKeyValueAt(int i, short key, Container value) {
                extendArray(1);
                System.arraycopy(array, i, array, i + 1, size - i);
                array[i] = new Element(key, value);
                size++;
        }

        public boolean remove(short key) {
                int i = binarySearch(0, size, key);
                if (i >= 0) { // if a new key
                        removeAtIndex(i);
                        return true;
                }
                return false;
        }

        public void removeAtIndex(int i) {
                System.arraycopy(array, i + 1, array, i, size - i - 1);
                array[size - 1] = null;
                size--;
        }

        public void setContainerAtIndex(int i, Container c) {
                this.array[i].value = c;
        }

        public int size() {
                return this.size;
        }

        private int binarySearch(int begin, int end, short key) {
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

        private Element[] array = null;

        private int size = 0;

        final static int initialCapacity = 4;
}
