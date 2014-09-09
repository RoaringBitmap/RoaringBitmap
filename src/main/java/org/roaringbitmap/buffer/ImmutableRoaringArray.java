package org.roaringbitmap.buffer;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;
import com.carrotsearch.sizeof.RamUsageEstimator;

/**
 * This is the underlying data structure for an ImmutableRoaringBitmap. This
 * class is not meant for end-users.
 * 
 */
public final class ImmutableRoaringArray implements PointableRoaringArray {

    protected static final short SERIAL_COOKIE = MutableRoaringArray.SERIAL_COOKIE;

    protected int unsignedBinarySearch(short k) {
        int low = 0;
        int high = this.size - 1;
        int ikey = BufferUtil.toIntUnsigned(k);
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
    
    ByteBuffer buffer;
    int size;
    
    private final static int startofkeyscardinalities = 8;

    /**
     * Create an array based on a previously serialized ByteBuffer.
     * 
     * @param bbf The source ByteBuffer
     */
    protected ImmutableRoaringArray(ByteBuffer bbf) {
        buffer = bbf;
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        if (buffer.getInt() != SERIAL_COOKIE)
            throw new RuntimeException("I failed to find the right cookie.");
        this.size = buffer.getInt();
        //int lastContainerOffset = buffer.getInt(4 + 4 + 4*this.size + 4*this.size - 4);
        //buffer.limit(lastContainerOffset + BufferUtil
          //      .getSizeInBytesFromCardinality(getCardinality(this.size - 1)));
    }

    public long bufferMemoryUsage(){
        long size = RamUsageEstimator.sizeOf(this.buffer);
        return size;
    }

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
    public boolean equals(Object o) {
        if (o instanceof ImmutableRoaringArray) {
            final ImmutableRoaringArray srb = (ImmutableRoaringArray) o;
            return srb.buffer.equals(this.buffer);
        }

        if (o instanceof MutableRoaringArray) {
            final MutableRoaringArray srb = (MutableRoaringArray) o;
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
                cp1.advance();
                cp2.advance();
            }
            if (cp2.hasContainer())
                return false;
            return true;
        }
        return false;
    }

    private int getCardinality(int k) {
        return BufferUtil.toIntUnsigned(buffer.getShort(startofkeyscardinalities + 4 * k + 2)) + 1;
    }

    // involves a binary search
    public MappeableContainer getContainer(short x) {
        final int i = unsignedBinarySearch(x);
        if (i < 0)
            return null;
        return getContainerAtIndex(i);
    }

    public MappeableContainer getContainerAtIndex(int i) {

        boolean isBitmap = getCardinality(i) > MappeableArrayContainer.DEFAULT_MAX_SIZE;
        buffer.position(getOffsetContainer(i));
        if (isBitmap) {
            final LongBuffer bitmapArray = buffer.asLongBuffer().slice();
            bitmapArray.limit(MappeableBitmapContainer.MAX_CAPACITY / 64);
            return new MappeableBitmapContainer(bitmapArray, getCardinality(i));
        } else {
            final ShortBuffer shortArray = buffer.asShortBuffer().slice();

            shortArray.limit(getCardinality(i));
            return new MappeableArrayContainer(shortArray, getCardinality(i));
        }

    }
    
    public int getOffsetContainer(int k){
    	return buffer.getInt(4 + 4 + 4*this.size + 4*k);
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
                return k < ImmutableRoaringArray.this.size;
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
        return BufferUtil.toIntUnsigned(buffer.getShort(startofkeyscardinalities + 4 * k));
    }

    // involves a binary search
    public int getIndex(short x) {
        return unsignedBinarySearch(x);
    }

    public short getKeyAtIndex(int i) {
        return buffer.getShort(4*i + startofkeyscardinalities);
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
            out.write(buffer.array());
        } else {
            ByteBuffer tmp = buffer.duplicate();
            tmp.position(0);
            byte[] bytes = new byte[256];
            while(tmp.remaining() > bytes.length) {
                tmp.get(bytes);
                out.write(bytes);
            }
            int left = tmp.remaining();
            tmp.get(bytes,0,left);
            out.write(bytes, 0, left);
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
