package org.roaringbitmap;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Simple bitset-like container.
 *
 */
public final class BitmapContainer extends Container implements Cloneable,
        Serializable {

        /**
         * Create a bitmap container with all bits set to false
         */
        public BitmapContainer() {
                this.cardinality = 0;
                this.bitmap = new long[maxcapacity / 64]; 
        }
       
        private BitmapContainer(int newcardinality, long[] newbitmap) {
                this.cardinality = newcardinality;
                this.bitmap = Arrays.copyOf(newbitmap, newbitmap.length);
        }

        @Override
        public Container add(final short i) {
                final int x = Util.toIntUnsigned(i);
                final long previous = bitmap[x / 64];
                bitmap[x / 64] |= (1l << x);
                cardinality += (previous ^ bitmap[x / 64]) >>> x;
                return this;
        }

        @Override
        public ArrayContainer and(final ArrayContainer value2) {
                final ArrayContainer answer = new ArrayContainer(value2.content.length);
                for (int k = 0; k < value2.getCardinality(); ++k)
                        if (this.contains(value2.content[k]))
                                answer.content[answer.cardinality++] = value2.content[k];
                return answer;
        }

        @Override
        public Container and(final BitmapContainer value2) {
                int newcardinality = 0;
                for (int k = 0; k < this.bitmap.length; ++k) {
                        newcardinality += Long.bitCount(this.bitmap[k] & value2.bitmap[k]);
                }
                if(newcardinality > ArrayContainer.DEFAULTMAXSIZE) {
                        final BitmapContainer answer = new BitmapContainer();
                        for (int k = 0; k < answer.bitmap.length; ++k) {
                                answer.bitmap[k] = this.bitmap[k] & value2.bitmap[k];
                        }
                        answer.cardinality = newcardinality;
                        return answer;
                }
                ArrayContainer ac = new ArrayContainer(newcardinality);
                Util.fillArrayAND(ac.content, this.bitmap, value2.bitmap);
                ac.cardinality = newcardinality;
                return ac;
        }

        @Override
        public Container andNot(final ArrayContainer value2) {
                final BitmapContainer answer = clone();
                for (int k = 0; k < value2.cardinality; ++k) {
                        final int i = Util.toIntUnsigned(value2.content[k]) >>> 6;
                        answer.bitmap[i] = answer.bitmap[i]
                                & (~(1l << value2.content[k]));
                        answer.cardinality -= (answer.bitmap[i] ^ this.bitmap[i]) >>> value2.content[k];
                }
                if (answer.cardinality <= ArrayContainer.DEFAULTMAXSIZE)
                        return answer.toArrayContainer();
                return answer;
        }

        @Override
        public Container andNot(final BitmapContainer value2) {
                int newcardinality = 0;
                for (int k = 0; k < this.bitmap.length; ++k) {
                        newcardinality += Long.bitCount(this.bitmap[k] & (~value2.bitmap[k]));
                }
                if(newcardinality > ArrayContainer.DEFAULTMAXSIZE) {
                        final BitmapContainer answer = new BitmapContainer();
                        for (int k = 0; k < answer.bitmap.length; ++k) {
                                answer.bitmap[k] = this.bitmap[k] & (~value2.bitmap[k]);
                        }
                        answer.cardinality = newcardinality;
                        return answer;
                }
                ArrayContainer ac = new ArrayContainer(newcardinality);
                Util.fillArrayANDNOT(ac.content, this.bitmap, value2.bitmap);
                ac.cardinality = newcardinality;
                return ac;
        }

        @Override
        public void clear() {
                if (cardinality != 0) {
                        cardinality = 0;
                        Arrays.fill(bitmap, 0);
                }
        }

        @Override
        public BitmapContainer clone() {
                return new BitmapContainer(this.cardinality,this.bitmap);
        }

        @Override
        public boolean contains(final short i) {
                final int x = Util.toIntUnsigned(i);
                return (bitmap[x / 64] & (1l << x)) != 0;
        }

        @Override
        public boolean equals(Object o) {
                if (o instanceof BitmapContainer) {
                        BitmapContainer srb = (BitmapContainer) o;
                        if (srb.cardinality != this.cardinality)
                                return false;
                        return Arrays.equals(this.bitmap, srb.bitmap);
                } 
                return false;
        }

        @Override
        public void fillLeastSignificant16bits(int[] x, int i, int mask) {
                int pos = i;
                for (int k = 0; k < bitmap.length; ++k) {
                        long bitset = bitmap[k];
                        while (bitset != 0) {
                                long t = bitset & -bitset;
                                x[pos++] =  (k * 64 + Long.bitCount(t - 1)) | mask;
                                bitset ^= t;
                        }
                }                
        }

        @Override
        public int getCardinality() {
                return cardinality;
        }



        @Override
        public ShortIterator getShortIterator() {
                return new ShortIterator() {
                        @Override
                        public boolean hasNext() {
                                return i >= 0;
                        }

                        @Override
                        public short next() {
                                j =  i;
                                i = BitmapContainer.this.nextSetBit(i + 1);
                                return (short) j;
                        }
                        
                        @Override
                        public void remove() {
                                BitmapContainer.this.remove((short) j);
                        }

                        int i = BitmapContainer.this.nextSetBit(0);

                        int j;

                };

        }

        @Override
        public int getSizeInBytes() {
                return this.bitmap.length * 8;
        }

        @Override
        public int hashCode() {
                return Arrays.hashCode(this.bitmap);
        }

        @Override
        public Container iand(final ArrayContainer B2) {
                return B2.and(this);// no inplace possible
        }

        @Override
        public Container iand(final BitmapContainer B2) {
                int newcardinality = 0;
                for (int k = 0; k < this.bitmap.length; ++k) {
                        newcardinality += Long.bitCount(this.bitmap[k] & B2.bitmap[k]);
                }
                if(newcardinality > ArrayContainer.DEFAULTMAXSIZE) {
                        for (int k = 0; k < this.bitmap.length; ++k) {
                                this.bitmap[k] = this.bitmap[k] & B2.bitmap[k];
                        }
                        this.cardinality = newcardinality;
                        return this;
                }
                ArrayContainer ac = new ArrayContainer(newcardinality);
                Util.fillArrayAND(ac.content, this.bitmap, B2.bitmap);
                ac.cardinality = newcardinality;
                return ac;
        }

        @Override
        public Container iandNot(final ArrayContainer B2) {
                for (int k = 0; k < B2.cardinality; ++k) {
                        this.remove(B2.content[k]);
                }
                if (cardinality <= ArrayContainer.DEFAULTMAXSIZE)
                        return this.toArrayContainer();
                return this;
        }

        @Override
        public Container iandNot(final BitmapContainer B2) {
                int newcardinality = 0;
                for (int k = 0; k < this.bitmap.length; ++k) {
                        newcardinality += Long.bitCount(this.bitmap[k] & (~B2.bitmap[k]));
                }
                if(newcardinality > ArrayContainer.DEFAULTMAXSIZE) {
                        for (int k = 0; k < this.bitmap.length; ++k) {
                                this.bitmap[k] = this.bitmap[k] & (~B2.bitmap[k]);
                        }
                        this.cardinality = newcardinality;
                        return this;
                }
                ArrayContainer ac = new ArrayContainer(newcardinality);
                Util.fillArrayANDNOT(ac.content, this.bitmap, B2.bitmap);
                ac.cardinality = newcardinality;
                return ac;
        }

        @Override
        public BitmapContainer ior(final ArrayContainer value2) {
                for (int k = 0; k < value2.cardinality; ++k) {
                        final int i = Util.toIntUnsigned(value2.content[k]) >>> 6;
                        this.cardinality += ((~this.bitmap[i]) & (1l << value2.content[k])) >>> value2.content[k];
                        this.bitmap[i] |= (1l << value2.content[k]);
                }
                return this;
        }

        @Override
        public Container ior(final BitmapContainer B2) {
                this.cardinality = 0;
                for (int k = 0; k < this.bitmap.length; k++) {
                        this.bitmap[k] |= B2.bitmap[k];
                        this.cardinality += Long.bitCount(this.bitmap[k]);
                }
                return this;
        }

        @Override
        public Iterator<Short> iterator() {
                return new Iterator<Short>() {
                        @Override
                        public boolean hasNext() {
                                return i >= 0;
                        }

                        @Override
                        public Short next() {
                                j = i;
                                i = BitmapContainer.this.nextSetBit(i + 1);
                                return new Short((short) j);
                        }

                        @Override
                        public void remove() {
                                BitmapContainer.this.remove((short) j);
                        }

                        int i = BitmapContainer.this.nextSetBit(0);

                        int j;

                };
        }

        @Override
        public Container ixor(final ArrayContainer value2) {
                for (int k = 0; k < value2.getCardinality(); ++k) {
                        final int index = Util.toIntUnsigned(value2.content[k]) >>> 6;
                        this.cardinality += 1 - 2 * ((this.bitmap[index] & (1l << value2.content[k])) >>> value2.content[k]);
                        this.bitmap[index] ^= (1l << value2.content[k]);
                }
                if (this.cardinality <= ArrayContainer.DEFAULTMAXSIZE) {
                        return this.toArrayContainer();
                }
                return this;
        }

        @Override
        public Container ixor(BitmapContainer B2) {
                int newcardinality = 0;
                for (int k = 0; k < this.bitmap.length; ++k) {
                        newcardinality += Long.bitCount(this.bitmap[k] ^ B2.bitmap[k]);
                }
                if(newcardinality > ArrayContainer.DEFAULTMAXSIZE) {
                        for (int k = 0; k < this.bitmap.length; ++k) {
                                this.bitmap[k] = this.bitmap[k] ^ B2.bitmap[k];
                        }
                        this.cardinality = newcardinality;
                        return this;
                }
                ArrayContainer ac = new ArrayContainer(newcardinality);
                Util.fillArrayXOR(ac.content, this.bitmap, B2.bitmap);
                ac.cardinality = newcardinality;
                return ac;
        }
        /**
         * Find the  index of the next set bit greater or equal to i, returns -1
         * if none found.
         * @param i starting index
         * @return index of the next set bit 
         */
        public int nextSetBit(final int i) {
                int x = i / 64;
                if (x >= bitmap.length)
                        return -1;
                long w = bitmap[x];
                w >>>= i;
                if (w != 0) {
                        return i + Long.numberOfTrailingZeros(w);
                }
                ++x;
                for (; x < bitmap.length; ++x) {
                        if (bitmap[x] != 0) {
                                return x * 64
                                        + Long.numberOfTrailingZeros(bitmap[x]);
                        }
                }
                return -1;
        }
        /**
         * Find the  index of the next unset bit greater or equal to i, returns -1
         * if none found.
         * @param i starting index
         * @return index of the next unset bit 
         */
        public short nextUnsetBit(final int i) {
                int x = i / 64;
                long w = ~bitmap[x];
                w >>>= i;
                if (w != 0) {
                        return (short) (i + Long.numberOfTrailingZeros(w));
                }
                ++x;
                for (; x < bitmap.length; ++x) {
                        if (bitmap[x] != ~0) {
                                return (short) (x * 64 + Long
                                        .numberOfTrailingZeros(~bitmap[x]));
                        }
                }
                return -1;
        }
        
        @Override
        public BitmapContainer or(final ArrayContainer value2) {
                final BitmapContainer answer = clone();
                for (int k = 0; k < value2.cardinality; ++k) {
                        final int i = Util.toIntUnsigned(value2.content[k]) >>> 6;
                        answer.cardinality += ((~answer.bitmap[i]) & (1l << value2.content[k])) >>> value2.content[k];
                        answer.bitmap[i] = answer.bitmap[i]
                                | (1l << value2.content[k]);
                }
                return answer;
        }

        @Override
        public Container or(final BitmapContainer value2) {
                if (USEINPLACE) {
                        BitmapContainer value1 = this.clone();
                        return value1.ior(value2);
                }
                final BitmapContainer answer = new BitmapContainer();
                answer.cardinality = 0;
                for (int k = 0; k < answer.bitmap.length; ++k) {
                        answer.bitmap[k] = this.bitmap[k] | value2.bitmap[k];
                        answer.cardinality += Long.bitCount(answer.bitmap[k]);
                }
                return answer;
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException,
                ClassNotFoundException {
                byte[] buffer = new byte[8];
                // little endian
                this.cardinality = 0;
                for(int k = 0; k < bitmap.length; ++k) {
                        in.readFully(buffer);
                        bitmap[k] = (((long)buffer[7] << 56) +
                                ((long)(buffer[6] & 255) << 48) +
                                ((long)(buffer[5] & 255) << 40) +
                                ((long)(buffer[4] & 255) << 32) +
                                ((long)(buffer[3] & 255) << 24) +
                                ((buffer[2] & 255) << 16) +
                                ((buffer[1] & 255) <<  8) +
                                ((buffer[0] & 255) <<  0));
                        this.cardinality += Long.bitCount(bitmap[k]);
                }
        }

        @Override
        public Container remove(final short i) {
                final int x = Util.toIntUnsigned(i);
                if (cardinality == ArrayContainer.DEFAULTMAXSIZE) {// this is
                                                                   // the
                                                                   // uncommon
                                                                   // path
                        if ((bitmap[x / 64] & (1l << x)) != 0) {
                                --cardinality;
                                bitmap[x / 64] &= ~(1l << x);
                                return this.toArrayContainer();
                        }
                }
                cardinality -= (bitmap[x / 64] & (1l << x)) >>> x;
                bitmap[x / 64] &= ~(1l << x);
                return this;
        }

        /**
         * Copies the data to an array container
         * @return the array container
         */
        public ArrayContainer toArrayContainer() {
                ArrayContainer ac = new ArrayContainer(cardinality);
                ac.loadData(this);
                return ac;
        }

        @Override
        public String toString() {
                StringBuffer sb = new StringBuffer();
                sb.append("{");
                int i = this.nextSetBit(0);
                while (i >= 0) {
                        sb.append(i);
                        i = this.nextSetBit(i + 1);
                        if (i >= 0)
                                sb.append(",");
                }
                sb.append("}");
                return sb.toString();
        }

        @Override
        public void trim() {
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
                byte[] buffer = new byte[8];
                // little endian
                for(long w: bitmap) {
                        buffer[0] = (byte)(w >>> 0);
                        buffer[1] = (byte)(w >>> 8);
                        buffer[2] = (byte)(w >>> 16);
                        buffer[3] = (byte)(w >>> 24);
                        buffer[4] = (byte)(w >>> 32);
                        buffer[5] = (byte)(w >>> 40);
                        buffer[6] = (byte)(w >>>  48);
                        buffer[7] = (byte)(w >>>  56);
                        out.write(buffer, 0, 8);
                }
        }

        @Override
        public Container xor(final ArrayContainer value2) {
                final BitmapContainer answer = clone();
                for (int k = 0; k < value2.getCardinality(); ++k) {
                        final int index = Util.toIntUnsigned(value2.content[k]) >>> 6;
                        answer.cardinality += 1 - 2 * ((answer.bitmap[index] & (1l << value2.content[k])) >>> value2.content[k]);
                        answer.bitmap[index] = answer.bitmap[index]
                                ^ (1l << value2.content[k]);
                }
                if (answer.cardinality <= ArrayContainer.DEFAULTMAXSIZE)
                        return answer.toArrayContainer();
                return answer;
        }
        
        @Override
        public Container xor(BitmapContainer value2) {
                int newcardinality = 0;
                for (int k = 0; k < this.bitmap.length; ++k) {
                        newcardinality += Long.bitCount(this.bitmap[k] ^ value2.bitmap[k]);
                }
                if(newcardinality > ArrayContainer.DEFAULTMAXSIZE) {
                        final BitmapContainer answer = new BitmapContainer();
                        for (int k = 0; k < answer.bitmap.length; ++k) {
                                answer.bitmap[k] = this.bitmap[k] ^ value2.bitmap[k];
                        }
                        answer.cardinality = newcardinality;
                        return answer;
                }
                ArrayContainer ac = new ArrayContainer(newcardinality);
                Util.fillArrayXOR(ac.content, this.bitmap, value2.bitmap);
                ac.cardinality = newcardinality;
                return ac;
        }

        /**
         * Fill the array with set bits
         * 
         * @param array
         *                container (should be large enoug)
         */
        protected void fillArray(final int[] array) {
                int pos = 0;
                for (int k = 0; k < bitmap.length; ++k) {
                        long bitset = bitmap[k];
                        while (bitset != 0) {
                                long t = bitset & -bitset;
                                array[pos++] = k * 64 + Long.bitCount(t - 1);
                                bitset ^= t;
                        }
                }
        }

        /**
         * Fill the array with set bits
         * 
         * @param array
         *                container (should be large enoug)
         */
        protected void fillArray(final short[] array) {
                int pos = 0;
                for (int k = 0; k < bitmap.length; ++k) {
                        long bitset = bitmap[k];
                        while (bitset != 0) {
                                long t = bitset & -bitset;
                                array[pos++] = (short)( k * 64 + Long.bitCount(t - 1));
                                bitset ^= t;
                        }
                }
        }
        
        protected void loadData(final ArrayContainer arrayContainer) {
                this.cardinality = arrayContainer.cardinality;
                for (int k = 0; k < arrayContainer.cardinality; ++k) {
                        final short x = arrayContainer.content[k];
                        bitmap[Util.toIntUnsigned(x) / 64] |= (1l << x);
                }
        }

        long[] bitmap;
        
        int cardinality;

        private static final long serialVersionUID = 2L;

        private static boolean USEINPLACE = true; // optimization flag

        protected static int maxcapacity = 1<<16;

}
