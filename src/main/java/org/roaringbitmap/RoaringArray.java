/*
 * Copyright 2013-2014 by Daniel Lemire, Owen Kaser and Samy Chambi
 * Licensed under the Apache License, Version 2.0.
 */
package org.roaringbitmap;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

/**
 * Specialized array to stored the containers used by a RoaringBitmap.
 * 
 */
public final class RoaringArray implements Cloneable, Externalizable {
        protected final class Element implements Cloneable {
                public Element(short key, Container value) {
                        this.key = key;
                        this.value = value;
                }

                public short key;

                public Container value = null;

                @Override
                public Element clone() {
                        Element c;
                        try {
                                c = (Element) super.clone();
                                c.key = this.key; // OFK: wouldn't Object's
                                                  // bitwise copy do this?
                                c.value = this.value.clone();
                                return c;
                        } catch (CloneNotSupportedException e) {
                                return null;
                        }
                }

        }

        protected RoaringArray() {
                this.array = new Element[initialCapacity];
        }

        protected void append(short key, Container value) {
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
        protected void appendCopy(RoaringArray sa, int index) {
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
        protected void appendCopy(RoaringArray sa, int startingindex, int end) {
                extendArray(end - startingindex);
                for (int i = startingindex; i < end; ++i) {
                        this.array[this.size++] = new Element(sa.array[i].key,
                                sa.array[i].value.clone());
                }

        }

        /**
         * Append copies of the values from another array, from the start
         * 
         * @param sa
         * @param stoppingKey
         *                any equal or larger key in other array will terminate
         *                copying
         */
        protected void appendCopiesUntil(RoaringArray sa, short stoppingKey) {
                int stopKey = Util.toIntUnsigned(stoppingKey);
                for (int i = 0; i < sa.size; ++i) {
                        if (Util.toIntUnsigned(sa.array[i].key) >= stopKey)
                                break;
                        extendArray(1);
                        this.array[this.size++] = new Element(sa.array[i].key,
                                sa.array[i].value.clone());
                }
        }

        /**
         * Append copies of the values AFTER a specified key (may or may not be
         * present) to end.
         * 
         * @param sa
         * @param beforeStart
         *                given key is the largest key that we won't copy
         */
        protected void appendCopiesAfter(RoaringArray sa, short beforeStart) {
                int startLocation = sa.getIndex(beforeStart);
                if (startLocation >= 0)
                        startLocation++;
                else
                        startLocation = -startLocation - 1;
                extendArray(sa.size - startLocation);

                for (int i = startLocation; i < sa.size; ++i) {
                        this.array[this.size++] = new Element(sa.array[i].key,
                                sa.array[i].value.clone());
                }
        }

        protected void clear() {
                this.array = null;
                this.size = 0;
        }

        @Override
        public RoaringArray clone() throws CloneNotSupportedException {
                RoaringArray sa;
                sa = (RoaringArray) super.clone();
                sa.array = Arrays.copyOf(this.array, this.size);
                for (int k = 0; k < this.size; ++k)
                        sa.array[k] = sa.array[k].clone();
                sa.size = this.size;
                return sa;
        }

        protected boolean ContainsKey(short x) {
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
        protected void extendArray(int k) {
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
        protected Container getContainer(short x) {
                int i = this.binarySearch(0, size, x);
                if (i < 0)
                        return null;
                return this.array[i].value;
        }

        protected Container getContainerAtIndex(int i) {
                return this.array[i].value;
        }

        // involves a binary search
        protected int getIndex(short x) {
                // before the binary search, we optimize for frequent cases
                if ((size == 0) || (array[size - 1].key == x))
                        return size - 1;
                // no luck we have to go through the list
                return this.binarySearch(0, size, x);
        }

        protected short getKeyAtIndex(int i) {
                return this.array[i].key;
        }

        @Override
        public int hashCode() {
                return array.hashCode();
        }

        // insert a new key, it is assumed that it does not exist
        protected void insertNewKeyValueAt(int i, short key, Container value) {
                extendArray(1);
                System.arraycopy(array, i, array, i + 1, size - i);
                array[i] = new Element(key, value);
                size++;
        }

        protected void resize(int newlength) {
                for (int k = newlength; k < this.size; ++k) {
                        this.array[k] = null;
                }
                this.size = newlength;
        }

        protected boolean remove(short key) {
                int i = binarySearch(0, size, key);
                if (i >= 0) { // if a new key
                        removeAtIndex(i);
                        return true;
                }
                return false;
        }

        protected void removeAtIndex(int i) {
                System.arraycopy(array, i + 1, array, i, size - i - 1);
                array[size - 1] = null;
                size--;
        }

        protected void setContainerAtIndex(int i, Container c) {
                this.array[i].value = c;
        }

        protected int size() {
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

        protected Element[] array = null;

        protected int size = 0;

        final static int initialCapacity = 4;

        @Override
        public void readExternal(ObjectInput in) throws IOException,
                ClassNotFoundException {
                this.clear();
                byte[] buffer = new byte[2];
                // little endian
                in.readFully(buffer);
                this.size = buffer[0] | (buffer[1] << 8);
                if ((this.array == null) || (this.array.length < this.size))
                        this.array = new Element[this.size];
                for (int k = 0; k < this.size; ++k) {
                        in.readFully(buffer);
                        short key = (short) (buffer[0] & 0xFF | ((buffer[1] & 0xFF) << 8));
                        boolean isbitmap = in.readBoolean();
                        Container val;
                        if (isbitmap) {
                                val = new BitmapContainer();
                                val.readExternal(in);
                        } else {
                                val = new ArrayContainer(0);
                                val.readExternal(in);
                        }
                        this.array[k] = new Element(key, val);
                }
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
                out.write((this.size >>> 0) & 0xFF);
                out.write((this.size >>> 8) & 0xFF);
                for (int k = 0; k < size; ++k) {
                        out.write((this.array[k].key >>> 0) & 0xFF);
                        out.write((this.array[k].key >>> 8) & 0xFF);
                        out.writeBoolean(this.array[k].value instanceof BitmapContainer);
                        array[k].value.writeExternal(out);
                }

        }

        private static final long serialVersionUID = 3L;

}
