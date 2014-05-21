/*
 * (c) Daniel Lemire, Owen Kaser, Samy Chambi, Jon Alvarado, Rory Graves, Björn Sperber
 * Licensed under the Apache License, Version 2.0.
 */
package org.roaringbitmap.buffer;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;
import java.util.Arrays;

/**
 * Specialized array to store the containers used by a RoaringBitmap. This class
 * is similar to org.roaringbitmap.RoaringArray but meant to be used with memory
 * mapping. This is not meant to be used by end users.
 * 
 * Objects of this class reside in RAM.
 */
public final class MutableRoaringArray implements Cloneable, Externalizable,
        PointableRoaringArray {

    protected static final class Element implements Cloneable,
            Comparable<Element> {

        final short key;

        MappeableContainer value;

        public Element(short key, MappeableContainer value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public Element clone() throws CloneNotSupportedException {
            Element c = (Element) super.clone();
            // key copied by clone
            c.value = this.value.clone();
            return c;
        }

        @Override
        public int compareTo(Element o) {
            return BufferUtil.toIntUnsigned(this.key)
                    - BufferUtil.toIntUnsigned(o.key);
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof Element) {
                Element e = (Element) o;
                return (e.key == key) && e.value.equals(value);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return key * 0xF0F0F0 + value.hashCode();
        }
    }

    protected static final int INITIAL_CAPACITY = 4;

    protected static final short SERIAL_COOKIE = 12345;

    private static final long serialVersionUID = 4L;

    Element[] array = null;

    int size = 0;

    protected MutableRoaringArray() {
        this.array = new Element[INITIAL_CAPACITY];
    }

    /**
     * Create a roaring array based on a previously serialized ByteBuffer. As
     * much as possible, the ByteBuffer is used as the backend, however if you
     * modify the content, the result is unspecified.
     * 
     * @param bb
     *            The source ByteBuffer
     */
    protected MutableRoaringArray(ByteBuffer bb) {
        bb.order(ByteOrder.LITTLE_ENDIAN);
        if (bb.getInt() != SERIAL_COOKIE)
            throw new RuntimeException("I failed to find the right cookie.");
        this.size = bb.getInt();
        // we fully read the meta-data array to RAM, but the containers
        // themselves are memory-mapped
        this.array = new Element[this.size];
        final short keys[] = new short[this.size];
        final int cardinalities[] = new int[this.size];
        final boolean isBitmap[] = new boolean[this.size];
        for (int k = 0; k < this.size; ++k) {
            keys[k] = bb.getShort();
            cardinalities[k] = BufferUtil.toIntUnsigned(bb.getShort()) + 1;
            isBitmap[k] = cardinalities[k] > MappeableArrayContainer.DEFAULT_MAX_SIZE;
        }
        for (int k = 0; k < this.size; ++k) {
            if (cardinalities[k] == 0)
                throw new RuntimeException("no");
            MappeableContainer val;
            if (isBitmap[k]) {
                final LongBuffer bitmapArray = bb.asLongBuffer().slice();
                bitmapArray.limit(MappeableBitmapContainer.MAX_CAPACITY / 64);
                bb.position(bb.position()
                        + MappeableBitmapContainer.MAX_CAPACITY / 8);
                val = new MappeableBitmapContainer(bitmapArray,
                        cardinalities[k]);
            } else {
                final ShortBuffer shortArray = bb.asShortBuffer().slice();
                shortArray.limit(cardinalities[k]);
                bb.position(bb.position() + cardinalities[k] * 2);
                val = new MappeableArrayContainer(shortArray, cardinalities[k]);
            }
            this.array[k] = new Element(keys[k], val);
        }
    }

    protected void append(short key, MappeableContainer value) {
        extendArray(1);
        this.array[this.size++] = new Element(key, value);
    }

    /**
     * Append copies of the values AFTER a specified key (may or may not be
     * present) to end.
     * 
     * @param highLowContainer
     *            the other array
     * @param beforeStart
     *            given key is the largest key that we won't copy
     */
    protected void appendCopiesAfter(PointableRoaringArray highLowContainer,
            short beforeStart) {

        int startLocation = highLowContainer.getIndex(beforeStart);
        if (startLocation >= 0)
            startLocation++;
        else
            startLocation = -startLocation - 1;
        extendArray(highLowContainer.size() - startLocation);

        for (int i = startLocation; i < highLowContainer.size(); ++i) {
            this.array[this.size++] = new Element(
                    highLowContainer.getKeyAtIndex(i), highLowContainer
                            .getContainerAtIndex(i).clone());
        }
    }

    /**
     * Append copies of the values from another array, from the start
     * 
     * @param highLowContainer
     *            the other array
     * @param stoppingKey
     *            any equal or larger key in other array will terminate copying
     */
    protected void appendCopiesUntil(PointableRoaringArray highLowContainer,
            short stoppingKey) {
        final int stopKey = BufferUtil.toIntUnsigned(stoppingKey);
        MappeableContainerPointer cp = highLowContainer.getContainerPointer();
        while (cp.hasContainer()) {
            if (BufferUtil.toIntUnsigned(cp.key()) >= stopKey)
                break;
            extendArray(1);
            this.array[this.size++] = new Element(cp.key(), cp.getContainer()
                    .clone());
            cp.advance();
        }
    }

    /**
     * Append copies of the values from another array
     * 
     * @param highLowContainer
     *            other array
     * @param startingIndex
     *            starting index in the other array
     * @param end
     *            last index array in the other array
     */
    protected void appendCopy(PointableRoaringArray highLowContainer,
            int startingIndex, int end) {
        extendArray(end - startingIndex);
        for (int i = startingIndex; i < end; ++i) {
            this.array[this.size++] = new Element(
                    highLowContainer.getKeyAtIndex(i), highLowContainer
                            .getContainerAtIndex(i).clone());
        }

    }

    protected void appendCopy(short key, MappeableContainer value) {
        extendArray(1);
        this.array[this.size++] = new Element(key, value.clone());
    }

    private int binarySearch(int begin, int end, short key) {
        int low = begin;
        int high = end - 1;
        final int ikey = BufferUtil.toIntUnsigned(key);

        while (low <= high) {
            final int middleIndex = (low + high) >>> 1;
            final int middleValue = BufferUtil
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

    protected void clear() {
        this.array = null;
        this.size = 0;
    }

    @Override
    public MutableRoaringArray clone() {
        MutableRoaringArray sa;
        try {
            sa = (MutableRoaringArray) super.clone();

            sa.array = Arrays.copyOf(this.array, this.size);
            for (int k = 0; k < this.size; ++k)
                sa.array[k] = sa.array[k].clone();
            sa.size = this.size;
            return sa;

        } catch (CloneNotSupportedException e) {
            return null;
        }
    }

    /**
     * Deserialize.
     * 
     * @param in
     *            the DataInput stream
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public void deserialize(DataInput in) throws IOException {
        this.clear();
        final byte[] buffer4 = new byte[4];
        final byte[] buffer = new byte[2];
        // little endian
        in.readFully(buffer4);
        final int cookie = (buffer4[0] & 0xFF) | ((buffer4[1] & 0xFF) << 8)
                | ((buffer4[2] & 0xFF) << 16) | ((buffer4[3] & 0xFF) << 24);
        if (cookie != SERIAL_COOKIE)
            throw new IOException("I failed to find the right cookie.");

        in.readFully(buffer4);
        this.size = (buffer4[0] & 0xFF) | ((buffer4[1] & 0xFF) << 8)
                | ((buffer4[2] & 0xFF) << 16) | ((buffer4[3] & 0xFF) << 24);
        if ((this.array == null) || (this.array.length < this.size))
            this.array = new Element[this.size];
        final short keys[] = new short[this.size];
        final int cardinalities[] = new int[this.size];
        final boolean isBitmap[] = new boolean[this.size];
        for (int k = 0; k < this.size; ++k) {
            in.readFully(buffer);
            keys[k] = (short) (buffer[0] & 0xFF | ((buffer[1] & 0xFF) << 8));
            in.readFully(buffer);
            cardinalities[k] = 1 + (buffer[0] & 0xFF | ((buffer[1] & 0xFF) << 8));
            isBitmap[k] = cardinalities[k] > MappeableArrayContainer.DEFAULT_MAX_SIZE;
        }
        for (int k = 0; k < this.size; ++k) {
            MappeableContainer val;
            if (isBitmap[k]) {
                final LongBuffer bitmapArray = LongBuffer
                        .allocate(MappeableBitmapContainer.MAX_CAPACITY / 64);
                final byte[] buf = new byte[8];
                // little endian
                for (int l = 0; l < bitmapArray.limit(); ++l) {
                    in.readFully(buf);
                    bitmapArray.put(l,
                            (((long) buf[7] << 56)
                                    + ((long) (buf[6] & 255) << 48)
                                    + ((long) (buf[5] & 255) << 40)
                                    + ((long) (buf[4] & 255) << 32)
                                    + ((long) (buf[3] & 255) << 24)
                                    + ((buf[2] & 255) << 16)
                                    + ((buf[1] & 255) << 8) + (buf[0] & 255)));
                }
                val = new MappeableBitmapContainer(bitmapArray,
                        cardinalities[k]);
            } else {
                final ShortBuffer shortArray = ShortBuffer
                        .allocate(cardinalities[k]);
                for (int l = 0; l < shortArray.limit(); ++l) {
                    in.readFully(buffer);
                    shortArray
                            .put(l,
                                    (short) (buffer[0] & 0xFF | ((buffer[1] & 0xFF) << 8)));
                }
                val = new MappeableArrayContainer(shortArray, cardinalities[k]);
            }
            this.array[k] = new Element(keys[k], val);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof MutableRoaringArray) {
            final MutableRoaringArray srb = (MutableRoaringArray) o;
            if (srb.size != this.size)
                return false;
            for (int i = 0; i < srb.size; ++i) {
                Element self = this.array[i];
                Element other = srb.array[i];
                if (self.key != other.key || !self.value.equals(other.value))
                    return false;
            }
            return true;
        }
        if (o instanceof ImmutableRoaringArray) {
            final ImmutableRoaringArray srb = (ImmutableRoaringArray) o;
            MappeableContainerPointer cp1 = srb.getContainerPointer();
            MappeableContainerPointer cp2 = srb.getContainerPointer();
            while (cp1.hasContainer()) {
                if (!cp2.hasContainer())
                    return false;
                if (cp1.key() != cp2.key())
                    return false;
                if (cp1.getCardinality() != cp2.getCardinality())
                    return false;
                if (!cp1.getContainer().equals(cp2.getContainer()))
                    return false;
            }
            if (cp2.hasContainer())
                return false;
            return true;
        }
        return false;
    }

    // make sure there is capacity for at least k more elements
    protected void extendArray(int k) {
        // size + 1 could overflow
        if (this.size + k >= this.array.length) {
            int newCapacity;
            if (this.array.length < 1024) {
                newCapacity = 2 * (this.size + k);
            } else {
                newCapacity = 5 * (this.size + k) / 4;
            }
            this.array = Arrays.copyOf(this.array, newCapacity);
        }
    }

    // involves a binary search
    public MappeableContainer getContainer(short x) {
        final int i = this.binarySearch(0, size, x);
        if (i < 0)
            return null;
        return this.array[i].value;
    }

    public MappeableContainer getContainerAtIndex(int i) {
        return this.array[i].value;
    }

    public MappeableContainerPointer getContainerPointer() {
        return new MappeableContainerPointer() {
            int k = 0;

            @Override
            public void advance() {
                ++k;

            }

            @Override
            public int compareTo(MappeableContainerPointer o) {
                if (key() != o.key())
                    return BufferUtil.toIntUnsigned(key())
                            - BufferUtil.toIntUnsigned(o.key());
                return o.getCardinality() - getCardinality();
            }

            @Override
            public int getCardinality() {
                return getContainer().getCardinality();
            }

            @Override
            public MappeableContainer getContainer() {
                if (k >= MutableRoaringArray.this.size)
                    return null;
                return MutableRoaringArray.this.array[k].value;
            }

            @Override
            public boolean hasContainer() {
                return k < MutableRoaringArray.this.size;
            }

            @Override
            public short key() {
                return MutableRoaringArray.this.array[k].key;

            }
        };

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
        int hashvalue = 0;
        for (int k = 0; k < this.size; ++k)
            hashvalue = 31 * hashvalue + array[k].hashCode();
        return hashvalue;
    }

    // insert a new key, it is assumed that it does not exist
    protected void insertNewKeyValueAt(int i, short key,
            MappeableContainer value) {
        extendArray(1);
        System.arraycopy(array, i, array, i + 1, size - i);
        array[i] = new Element(key, value);
        size++;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        deserialize(in);
    }

    protected void removeAtIndex(int i) {
        System.arraycopy(array, i + 1, array, i, size - i - 1);
        array[size - 1] = null;
        size--;
    }

    protected void resize(int newLength) {
        for (int k = newLength; k < this.size; ++k) {
            this.array[k] = null;
        }
        this.size = newLength;
    }

    /**
     * Serialize.
     * 
     * The current bitmap is not modified.
     * 
     * @param out
     *            the DataOutput stream
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public void serialize(DataOutput out) throws IOException {
        out.write(SERIAL_COOKIE & 0xFF);
        out.write((SERIAL_COOKIE >>> 8) & 0xFF);
        out.write((SERIAL_COOKIE >>> 16) & 0xFF);
        out.write((SERIAL_COOKIE >>> 24) & 0xFF);
        out.write(this.size & 0xFF);
        out.write((this.size >>> 8) & 0xFF);
        out.write((this.size >>> 16) & 0xFF);
        out.write((this.size >>> 24) & 0xFF);

        for (int k = 0; k < size; ++k) {
            out.write(this.array[k].key & 0xFF);
            out.write((this.array[k].key >>> 8) & 0xFF);
            out.write((this.array[k].value.getCardinality() - 1) & 0xFF);
            out.write(((this.array[k].value.getCardinality() - 1) >>> 8) & 0xFF);
        }
        for (int k = 0; k < size; ++k) {
            array[k].value.writeArray(out);
        }
    }

    /**
     * Report the number of bytes required for serialization.
     * 
     * @return the size in bytes
     */
    public int serializedSizeInBytes() {
        int count = 4 + 4 + 4 * size;
        for (int k = 0; k < size; ++k) {
            count += array[k].value.getArraySizeInBytes();
        }
        return count;
    }

    protected void setContainerAtIndex(int i, MappeableContainer c) {
        this.array[i].value = c;
    }

    public int size() {
        return this.size;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        serialize(out);
    }
}
