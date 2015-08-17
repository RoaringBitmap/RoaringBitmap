/*
 * (c) the authors
 * Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap.buffer;


import java.io.*;
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

    protected static final int INITIAL_CAPACITY = 4;

    protected static final short SERIAL_COOKIE_NO_RUNCONTAINER = 12346;
    protected static final short SERIAL_COOKIE = 12347;
    
    protected static final int NO_OFFSET_THRESHOLD = 4;

    private static final long serialVersionUID = 5L;  // TODO: OFK was 4L, not sure
    protected boolean mayHaveRunContainers = false;  // does not necessarily have them, after optimization


    short[] keys = null;
    MappeableContainer[] values = null;

    int size = 0;

    protected MutableRoaringArray() {
        this.keys = new short[INITIAL_CAPACITY];
        this.values = new MappeableContainer[INITIAL_CAPACITY];
    }


    protected void append(short key, MappeableContainer value) {
        extendArray(1);
        this.keys[this.size] = key;
        this.values[this.size] = value;
        this.size++;
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
            this.keys[this.size] = highLowContainer.getKeyAtIndex(i);
            this.values[this.size] = highLowContainer.getContainerAtIndex(i).clone();
            this.size++;
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
            this.keys[this.size] = cp.key();
            this.values[this.size] = cp.getContainer().clone();
            this.size++;
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
            this.keys[this.size] = highLowContainer.getKeyAtIndex(i);
            this.values[this.size] = highLowContainer.getContainerAtIndex(i).clone();
            this.size++;
        }
    }

    protected void appendCopy(short key, MappeableContainer value) {
        extendArray(1);
        this.keys[this.size] = key;
        this.values[this.size] = value.clone();
        this.size++;
    }

    private int binarySearch(int begin, int end, short key) {
        int low = begin;
        int high = end - 1;
        final int ikey = BufferUtil.toIntUnsigned(key);

        while (low <= high) {
            final int middleIndex = (low + high) >>> 1;
            final int middleValue = BufferUtil.toIntUnsigned(keys[middleIndex]);

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
        this.keys = null;
        this.values = null;
        this.size = 0;
    }

    @Override
    public MutableRoaringArray clone() {
        MutableRoaringArray sa;
        try {
            sa = (MutableRoaringArray) super.clone();

            // OFK: do we need runcontainer bitmap?  Guess not, this is just a directory
            // and each container knows what kind it is.
            sa.keys = Arrays.copyOf(this.keys, this.size);
            sa.values = Arrays.copyOf(this.values, this.size);
            for (int k = 0; k < this.size; ++k)
                sa.values[k] = sa.values[k].clone();
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
        // little endian
        final int cookie = Integer.reverseBytes(in.readInt());
        if ((cookie & 0xFFFF) != SERIAL_COOKIE && cookie != SERIAL_COOKIE_NO_RUNCONTAINER)
            throw new IOException("I failed to find the one of the right cookies.");
        this.size = ((cookie & 0xFFFF) == SERIAL_COOKIE) ? (cookie >>> 16) + 1: Integer.reverseBytes(in.readInt());
        if ((this.keys == null) || (this.keys.length < this.size)) {
            this.keys = new short[this.size];
            this.values = new MappeableContainer[this.size];
        }

        byte [] bitmapOfRunContainers = null;
        boolean hasrun = (cookie & 0xFFFF) == SERIAL_COOKIE; 
        if (hasrun) {
            bitmapOfRunContainers = new byte[ (size+7)/8];
            in.readFully(bitmapOfRunContainers);
        }

        final short keys[] = new short[this.size];
        final int cardinalities[] = new int[this.size];
        final boolean isBitmap[] = new boolean[this.size];
        for (int k = 0; k < this.size; ++k) {
            keys[k] = Short.reverseBytes(in.readShort());
            cardinalities[k] = 1 + (0xFFFF & Short.reverseBytes(in.readShort()));
            isBitmap[k] = cardinalities[k] > MappeableArrayContainer.DEFAULT_MAX_SIZE;
            if (bitmapOfRunContainers != null &&
                ( bitmapOfRunContainers[k/8] & (1<<(k%8))) != 0) {
                isBitmap[k] = false;
            }
        }
        if((! hasrun) || (this.size >= NO_OFFSET_THRESHOLD) ) {
            //skipping the offsets
            in.skipBytes(this.size*4);
        }
        //Reading the containers
        for (int k = 0; k < this.size; ++k) {
            MappeableContainer val;
            if (isBitmap[k]) {
                final LongBuffer bitmapArray = LongBuffer
                        .allocate(MappeableBitmapContainer.MAX_CAPACITY / 64);
                // little endian
                for (int l = 0; l < bitmapArray.limit(); ++l) {
                    bitmapArray.put(l,Long.reverseBytes(in.readLong()));
                }
                val = new MappeableBitmapContainer(bitmapArray,
                        cardinalities[k]);
            } else if (bitmapOfRunContainers != null && ((bitmapOfRunContainers[k/8] & (1<<(k%8))) != 0)) {
                int nbrruns = BufferUtil.toIntUnsigned(Short.reverseBytes(in.readShort()));
                final ShortBuffer shortArray = ShortBuffer
                        .allocate(2*nbrruns);
                for (int l = 0; l < shortArray.limit(); ++l) {
                    shortArray
                            .put(l,Short.reverseBytes(in.readShort()));
                }
                val = new MappeableRunContainer(shortArray, nbrruns);
            }

            else {
                final ShortBuffer shortArray = ShortBuffer
                        .allocate(cardinalities[k]);
                for (int l = 0; l < shortArray.limit(); ++l) {
                    shortArray
                            .put(l,Short.reverseBytes(in.readShort()));
                }
                val = new MappeableArrayContainer(shortArray, cardinalities[k]);
            }
            this.keys[k] = keys[k];
            this.values[k] = val;
        }
    }

    // make sure there is capacity for at least k more elements
    protected void extendArray(int k) {
        // size + 1 could overflow
        if (this.size + k >= this.keys.length) {
            int newCapacity;
            if (this.keys.length < 1024) {
                newCapacity = 2 * (this.size + k);
            } else {
                newCapacity = 5 * (this.size + k) / 4;
            }
            this.keys = Arrays.copyOf(this.keys, newCapacity);
            this.values = Arrays.copyOf(this.values, newCapacity);
        }
    }

    // involves a binary search
    public MappeableContainer getContainer(short x) {
        final int i = this.binarySearch(0, size, x);
        if (i < 0)
            return null;
        return this.values[i];
    }

    public MappeableContainer getContainerAtIndex(int i) {
        return this.values[i];
    }

    public MappeableContainerPointer getContainerPointer() {
        return getContainerPointer(0);
    }

    public MappeableContainerPointer getContainerPointer(final int startIndex) {
        return new MappeableContainerPointer() {
            int k = startIndex;

            @Override
            public void advance() {
                ++k;
            }

            @Override
            public void previous() {
                --k;
            }

            @Override
			public int compareTo(MappeableContainerPointer o) {
				if (key() != o.key())
					return BufferUtil.toIntUnsigned(key())
							- BufferUtil.toIntUnsigned(o.key());
                return o.getCardinality() - this.getCardinality();
			}

            @Override
            public int getCardinality() {
                return getContainer().getCardinality();
            }

            @Override
            public MappeableContainer getContainer() {
                if (k >= MutableRoaringArray.this.size)
                    return null;
                return MutableRoaringArray.this.values[k];
            }

            @Override
            public boolean hasContainer() {
                return 0 <= k & k < MutableRoaringArray.this.size;
            }

            @Override
            public short key() {
                return MutableRoaringArray.this.keys[k];

            }

            @Override
            public MappeableContainerPointer clone() {
                try {
                    return (MappeableContainerPointer) super.clone();
                } catch (CloneNotSupportedException e) {
                    return null;// will not happen
                }
            }

            @Override
            public boolean isBitmapContainer() {
                return getContainer() instanceof MappeableBitmapContainer;
            }

            @Override
            public boolean isRunContainer() {
                return getContainer() instanceof MappeableRunContainer;
            }

            
            @Override
            public int getSizeInBytes() {
                return getContainer().getArraySizeInBytes();
            }
        };

    }

    // involves a binary search
    public int getIndex(short x) {
        // before the binary search, we optimize for frequent cases
        if ((size == 0) || (keys[size - 1] == x))
            return size - 1;
        // no luck we have to go through the list
        return this.binarySearch(0, size, x);
    }

    public short getKeyAtIndex(int i) {
        return this.keys[i];
    }

    public int advanceUntil(short x, int pos) {
        int lower = pos + 1;

        // special handling for a possibly common sequential case
        if (lower >= size || BufferUtil.toIntUnsigned(keys[lower]) >= BufferUtil.toIntUnsigned(x)) {
            return lower;
        }

        int spansize = 1; // could set larger
        // bootstrap an upper limit

        while (lower + spansize < size && BufferUtil.toIntUnsigned(keys[lower + spansize]) < BufferUtil.toIntUnsigned(x))
            spansize *= 2; // hoping for compiler will reduce to shift
        int upper = (lower + spansize < size) ? lower + spansize : size - 1;

        // maybe we are lucky (could be common case when the seek ahead
        // expected to be small and sequential will otherwise make us look bad)
        if (keys[upper] == x) {
            return upper;
        }

        if (BufferUtil.toIntUnsigned(keys[upper]) < BufferUtil.toIntUnsigned(x)) {// means array has no item key >= x
            return size;
        }

        // we know that the next-smallest span was too small
        lower += (spansize / 2);

        // else begin binary search
        // invariant: array[lower]<x && array[upper]>x
        while (lower + 1 != upper) {
            int mid = (lower + upper) / 2;
            if (keys[mid] == x)
                return mid;
            else if (BufferUtil.toIntUnsigned(keys[mid]) < BufferUtil.toIntUnsigned(x))
                lower = mid;
            else
                upper = mid;
        }
        return upper;
    }

    @Override
    public int hashCode() {
        int hashvalue = 0;
        for (int k = 0; k < this.size; ++k)
            hashvalue = 31 * hashvalue + keys[k] * 0xF0F0F0 + values[k].hashCode();
        return hashvalue;
    }

    // insert a new key, it is assumed that it does not exist
    protected void insertNewKeyValueAt(int i, short key,
            MappeableContainer value) {
        extendArray(1);
        System.arraycopy(keys, i, keys, i + 1, size - i);
        System.arraycopy(values, i, values, i + 1, size - i);
        keys[i] = key;
        values[i] = value;
        size++;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        deserialize(in);
    }

    protected void removeAtIndex(int i) {
        System.arraycopy(keys, i + 1, keys, i, size - i - 1);
        keys[size - 1] = 0;
        System.arraycopy(values, i + 1, values, i, size - i - 1);
        values[size - 1] = null;
        size--;
    }

    protected void removeIndexRange(int begin, int end) {
        if(end <= begin) return;
        final int range = end - begin;
        System.arraycopy(keys, end, keys, begin, size - end);
        System.arraycopy(values, end, values, begin, size - end);
        for(int i = 1; i <= range; ++i) {
            keys[size - i] = 0;
            values[size - i] = null;
        }
        size -= range;
    }

    protected void resize(int newLength) {
        Arrays.fill(this.keys, newLength, this.size, (short) 0);
        Arrays.fill(this.values, newLength, this.size, null);
        this.size = newLength;
    }

    protected void copyRange(int begin, int end, int newBegin) {
        //assuming begin <= end and newBegin < begin
        final int range = end - begin;
        System.arraycopy(this.keys, begin, this.keys, newBegin, range);
        System.arraycopy(this.values, begin, this.values, newBegin, range);
    }
    
    @Override
    public boolean hasRunCompression() {
        for (int k=0; k < size; ++k) {
            MappeableContainer ck = values[k];
            if (ck instanceof MappeableRunContainer) return true;
        }
        return false;
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
        int startOffset=0;
        boolean hasrun = hasRunCompression();
        if (hasrun) {
            out.writeInt(Integer.reverseBytes(SERIAL_COOKIE | ((this.size-1) << 16)));
            byte [] bitmapOfRunContainers = new byte[ (size+7)/8];
            for (int i=0; i < size; ++i) {
                if (this.values[i] instanceof MappeableRunContainer) {
                    bitmapOfRunContainers[ i/8 ] |= (1 << (i%8));
                }
            }
            out.write(bitmapOfRunContainers);
            if(this.size < NO_OFFSET_THRESHOLD)
                startOffset =  4 + 4 * this.size + bitmapOfRunContainers.length;
           else 
                startOffset =  4 + 8 * this.size + bitmapOfRunContainers.length;
        }
        else {  // backwards compatibilility
            out.writeInt(Integer.reverseBytes(SERIAL_COOKIE_NO_RUNCONTAINER));
            out.writeInt(Integer.reverseBytes(size));
            startOffset = 4 + 4 + this.size*4 + this.size*4;
        }
        for (int k = 0; k < size; ++k) {
            out.writeShort(Short.reverseBytes(this.keys[k]));
            out.writeShort(Short.reverseBytes((short) ((this.values[k].getCardinality() - 1))));
        }
        if ((!hasrun) || (this.size >= NO_OFFSET_THRESHOLD)) {
            for (int k = 0; k < this.size; k++) {
                out.writeInt(Integer.reverseBytes(startOffset));
                startOffset = startOffset + values[k].getArraySizeInBytes();
            }
        }
        for (int k = 0; k < size; ++k) {
            values[k].writeArray(out);
        }

    }

    /**
     * Report the number of bytes required for serialization.
     *
     * @return the size in bytes
     */
    public int serializedSizeInBytes() {
        int count = headerSize();
        // for each container, we store cardinality (16 bits), key (16 bits) and location offset (32 bits).
        for (int k = 0; k < this.size; ++k) {
            count += values[k].getArraySizeInBytes();
        }
        return count;
    }
    

    protected int headerSize() {
        if (hasRunCompression()) { 
            if(size < NO_OFFSET_THRESHOLD) {// for small bitmaps, we omit the offsets
                return 4 + (size+7)/8 + 4 * size;
            }
            return  4 + (size+7)/8 + 8 * size;// - 4 because we pack the size with the cookie
        } else {
            return 4 + 4 + 8 * size;
        }
    }

    protected void setContainerAtIndex(int i, MappeableContainer c) {
        this.values[i] = c;
    }

    protected void replaceKeyAndContainerAtIndex(int i, short key, MappeableContainer c) {
        this.keys[i] = key;
        this.values[i] = c;
    }

    public int size() {
        return this.size;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        serialize(out);
    }

    @Override
    public int getCardinality(int i) {
        return getContainerAtIndex(i).getCardinality();
    }
}
