/*
 * (c) the authors
 * Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap.buffer;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;




/**
 * This is the underlying data structure for an ImmutableRoaringBitmap. This
 * class is not meant for end-users.
 * 
 */
public final class ImmutableRoaringArray implements PointableRoaringArray {

    protected static final short SERIAL_COOKIE = MutableRoaringArray.SERIAL_COOKIE;
    protected static final short SERIAL_COOKIE_NO_RUNCONTAINER = MutableRoaringArray.SERIAL_COOKIE_NO_RUNCONTAINER;
    private final static int startofrunbitmap = 4; // if there is a runcontainer bitmap

    ByteBuffer buffer;
    int size;

    protected int unsignedBinarySearch(short k) {
        int low = 0;
        int high = this.size - 1;
        final int ikey = BufferUtil.toIntUnsigned(k);
        while (low <= high) {
            final int middleIndex = (low + high) >>> 1;
            final int middleValue = getKey(middleIndex);
            if (middleValue < ikey)
                low = middleIndex + 1;
            else if (middleValue > ikey)
                high = middleIndex - 1;
            else
                return middleIndex;
        }
        return -(low + 1);
    }
    

    /**
     * Create an array based on a previously serialized ByteBuffer.
     * 
     * @param bbf The source ByteBuffer
     */

    protected ImmutableRoaringArray(ByteBuffer bbf) {
        buffer = bbf.slice();
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        final int cookie = buffer.getInt(0);
        if ((cookie & 0xFFFF) != SERIAL_COOKIE && cookie != SERIAL_COOKIE_NO_RUNCONTAINER)
            throw new RuntimeException("I failed to find one of the right cookies. "+ cookie);
        boolean hasRunContainers = ((cookie & 0xFFFF) == SERIAL_COOKIE); 
        this.size = hasRunContainers? (cookie >>> 16) + 1 : buffer.getInt(4);
        int theLimit = computeSerializedSizeInBytes();
        buffer.limit(theLimit);
    }


    @Override
    public  boolean hasRunCompression() {
        return (buffer.getInt(0) & 0xFFFF) == SERIAL_COOKIE;
    }

    // hasrun should be equal to hasRunCompression()
    protected int headerSize(boolean hasrun) {
        if (hasrun) { 
            if(size < MutableRoaringArray.NO_OFFSET_THRESHOLD) {// for small bitmaps, we omit the offsets
                return 4 + (size+7)/8 + 4 * size;
            }
            return  4 + (size+7)/8 + 8 * size;// - 4 because we pack the size with the cookie
        } else {
            return 4 + 4 + 8 * size;
        }
    }

    
    private int getStartOfKeys() {
        if (hasRunCompression()) { // info is in the buffer
            return 4 + (( this.size+7)/8);
        } else
            return 8;
    }

    // hasrun should be initialized with hasRunCompression()
    private boolean isRunContainer(int i, boolean hasrun) {
        if (hasrun) { // info is in the buffer
            int j = buffer.get(startofrunbitmap + i / 8);
            int mask = 1 << (i % 8);
            return (j & mask) != 0;
        } else
            return false;
    }
    

    
    private int computeSerializedSizeInBytes() {
        int CardinalityOfLastContainer = getCardinality(this.size - 1);
        int PositionOfLastContainer = getOffsetContainer(this.size - 1);
        int SizeOfLastContainer;
        boolean hasrun = hasRunCompression();
        if (isRunContainer(this.size - 1,hasrun)) {
            int nbrruns = BufferUtil.toIntUnsigned(buffer.getShort(PositionOfLastContainer));
            SizeOfLastContainer = BufferUtil.getSizeInBytesFromCardinalityEtc(0,nbrruns, true);
        }
        else {
            SizeOfLastContainer = BufferUtil.getSizeInBytesFromCardinalityEtc(CardinalityOfLastContainer,0,false);

        }
        return SizeOfLastContainer + PositionOfLastContainer;
    }

    @Override
    public ImmutableRoaringArray clone() {
        ImmutableRoaringArray sa;
        try {
            sa = (ImmutableRoaringArray) super.clone();
        } catch (CloneNotSupportedException e) {
            return null;// should never happen
        }
        return sa;
    }

    @Override
    public int getCardinality(int k) {
        return BufferUtil.toIntUnsigned(buffer.getShort(this.getStartOfKeys() + 4 * k + 2)) + 1;
    }

    // involves a binary search
    public MappeableContainer getContainer(short x) {
        final int i = unsignedBinarySearch(x);
        if (i < 0)
            return null;
        return getContainerAtIndex(i);
    }

    public MappeableContainer getContainerAtIndex(int i) {
    	int cardinality = getCardinality(i);
        boolean isBitmap = cardinality > MappeableArrayContainer.DEFAULT_MAX_SIZE; // if not a runcontainer
        buffer.position(getOffsetContainer(i));
        boolean hasrun = hasRunCompression();
        if (isRunContainer(i,hasrun))  {
            // first, we have a short giving the number of runs
            int nbrruns = BufferUtil.toIntUnsigned(buffer.getShort());
            final ShortBuffer shortArray = buffer.asShortBuffer().slice();
            shortArray.limit(2*nbrruns);
            return new MappeableRunContainer(shortArray,nbrruns);
        }
        if (isBitmap) {
            final LongBuffer bitmapArray = buffer.asLongBuffer().slice();
            bitmapArray.limit(MappeableBitmapContainer.MAX_CAPACITY / 64);            
            return new MappeableBitmapContainer(bitmapArray, cardinality);
        } else {
            final ShortBuffer shortArray = buffer.asShortBuffer().slice();
            shortArray.limit(cardinality);
            return new MappeableArrayContainer(shortArray, cardinality);
        }
    }
    
    private int getOffsetContainerSlow(int k) {
        boolean hasrun = hasRunCompression();
        int pos = this.headerSize(hasrun);
        for(int z = 0; z < k; ++z) {
            if (isRunContainer(z,hasrun))  {
                int nbrruns = BufferUtil.toIntUnsigned(buffer.getShort(pos));
                int SizeOfLastContainer = BufferUtil.getSizeInBytesFromCardinalityEtc(0,nbrruns, true);
                pos += SizeOfLastContainer;
            } else {
                int CardinalityOfLastContainer = this.getCardinality(z);
                int SizeOfLastContainer = BufferUtil.getSizeInBytesFromCardinalityEtc(CardinalityOfLastContainer,0,false);
                pos +=  SizeOfLastContainer;
            }                    
        }
        return pos;
    }


    private int getOffsetContainer(int k) {
        if (hasRunCompression()) { // account for size of runcontainer bitmap
            if(this.size < MutableRoaringArray.NO_OFFSET_THRESHOLD) {
                // we do it the hard way
                return getOffsetContainerSlow(k);
            }
            return buffer.getInt(4 + 4*this.size +  (( this.size+7)/8) + 4*k);
        }
        else {
            return buffer.getInt(4 + 4 + 4*this.size + 4*k);
        }
    }

    public MappeableContainerPointer getContainerPointer() {
        return getContainerPointer(0);
    }
    
    /**
     * Returns true if this bitmap is empty.
     * @return true if empty
     */
    public boolean isEmpty() {
        return this.size == 0;
    }

    public MappeableContainerPointer getContainerPointer(final int startIndex) {        
        final boolean hasrun = isEmpty() ? false: hasRunCompression();
        return new MappeableContainerPointer() {
            int k = startIndex;

            @Override
            public int getSizeInBytes() {
                // might be a tad expensive
                if (ImmutableRoaringArray.this.isRunContainer(k, hasrun)) {
                    int pos = getOffsetContainer(k);
                    int nbrruns = BufferUtil
                            .toIntUnsigned(buffer.getShort(pos));
                    return BufferUtil.getSizeInBytesFromCardinalityEtc(0,
                            nbrruns, true);
                } else {
                    int CardinalityOfLastContainer = getCardinality();
                    return BufferUtil.getSizeInBytesFromCardinalityEtc(
                            CardinalityOfLastContainer, 0, false);
                }
            }

            
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
            public boolean isBitmapContainer() {
                if(ImmutableRoaringArray.this.isRunContainer(k, hasrun))
                    return false;
                return getCardinality() >MappeableArrayContainer.DEFAULT_MAX_SIZE;
            }
            

            @Override
            public boolean isRunContainer() {
                return ImmutableRoaringArray.this.isRunContainer(k, hasrun);
            }

            @Override
            public int getCardinality() {
                return ImmutableRoaringArray.this.getCardinality(k);
            }

            @Override
            public MappeableContainer getContainer() {
                if (k >= ImmutableRoaringArray.this.size)
                    return null;
                return ImmutableRoaringArray.this.getContainerAtIndex(k);
            }

            @Override
            public boolean hasContainer() {
                return 0 <= k & k < ImmutableRoaringArray.this.size;
            }

            @Override
            public short key() {
                return ImmutableRoaringArray.this.getKeyAtIndex(k);

            }
            

            @Override
            public MappeableContainerPointer clone() {
                try {
                    return (MappeableContainerPointer) super.clone();
                } catch (CloneNotSupportedException e) {
                    return null;// will not happen
                }
            }
        };
    }

    private int getKey(int k) {
    	return BufferUtil.toIntUnsigned(buffer.getShort(getStartOfKeys() + 4 * k));
    }

    // involves a binary search
    public int getIndex(short x) {
        return unsignedBinarySearch(x);
    }

    public short getKeyAtIndex(int i) {
        return buffer.getShort(4 * i + getStartOfKeys());
    }

    public int advanceUntil(short x, int pos) {
        int lower = pos + 1;

        // special handling for a possibly common sequential case
        if (lower >= size || getKey(lower) >= BufferUtil.toIntUnsigned(x)) {
            return lower;
        }

        int spansize = 1; // could set larger
        // bootstrap an upper limit

        while (lower + spansize < size && getKey(lower + spansize) < BufferUtil.toIntUnsigned(x))
            spansize *= 2; // hoping for compiler will reduce to shift
        int upper = (lower + spansize < size) ? lower + spansize : size - 1;

        // maybe we are lucky (could be common case when the seek ahead
        // expected to be small and sequential will otherwise make us look bad)
        if (getKey(upper) == BufferUtil.toIntUnsigned(x)) {
            return upper;
        }

        if (getKey(upper) < BufferUtil.toIntUnsigned(x)) {// means array has no item key >= x
            return size;
        }

        // we know that the next-smallest span was too small
        lower += (spansize / 2);

        // else begin binary search
        // invariant: array[lower]<x && array[upper]>x
        while (lower + 1 != upper) {
            int mid = (lower + upper) / 2;
            if (getKey(mid) == BufferUtil.toIntUnsigned(x))
                return mid;
            else if (getKey(mid) < BufferUtil.toIntUnsigned(x))
                lower = mid;
            else
                upper = mid;
        }
        return upper;
    }

    @Override
    public int hashCode() {
        MappeableContainerPointer cp = this.getContainerPointer();
        int hashvalue = 0;
        while (cp.hasContainer()) {
            int th = cp.key() * 0xF0F0F0 + cp.getContainer().hashCode();
            hashvalue = 31 * hashvalue + th;
        }
        return hashvalue;
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
        if(buffer.hasArray()) {
            out.write(buffer.array(), buffer.arrayOffset(), buffer.limit());
        } else {
            ByteBuffer tmp = buffer.duplicate();
            tmp.position(0);
            WritableByteChannel channel = Channels.newChannel((OutputStream ) out);
            channel.write(tmp);
        }
    }
    /**
     * @return the size that the data structure occupies on disk
     */
    public int serializedSizeInBytes() {
        return buffer.limit();
    }

    public int size() {
        return this.size;
    }
}
