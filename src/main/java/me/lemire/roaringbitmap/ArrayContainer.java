package me.lemire.roaringbitmap;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

public final class ArrayContainer extends Container implements Cloneable,
        Serializable {

        public ArrayContainer() {
                this(DEFAULTINITSIZE);
        }

        public ArrayContainer(final int capacity) {
                content = new short[capacity];
        }

        /**
         * running time is in O(n) time if insert is not in order.
         * 
         */
        @Override
        public Container add(final short x) {
                // Transform the ArrayContainer to a BitmapContainer
                // when cardinality = DEFAULTMAXSIZE
                if (cardinality >= DEFAULTMAXSIZE) {
                        BitmapContainer a = ContainerFactory
                                .transformToBitmapContainer(this);
                        a.add(x);
                        return a;
                }
                if ((cardinality == 0)
                        || (Util.toIntUnsigned(x) > Util
                                .toIntUnsigned(content[cardinality - 1]))) {
                        if (cardinality >= this.content.length)
                                increaseCapacity();
                        content[cardinality++] = x;
                        return this;
                }
                int loc = Util
                        .unsigned_binarySearch(content, 0, cardinality, x);
                if (loc < 0) {
                        if (cardinality >= this.content.length)
                                increaseCapacity();
                        // insertion : shift the elements > x by one position to
                        // the right
                        // and put x in it's appropriate place
                        System.arraycopy(content, -loc - 1, content, -loc,
                                cardinality + loc + 1);
                        content[-loc - 1] = x;
                        ++cardinality;
                }
                return this;
        }

        @Override
        public ArrayContainer and(final ArrayContainer value2) {

                ArrayContainer value1 = this;
                final int desiredcapacity = Math.min(value1.getCardinality(),
                        value2.getCardinality());
                ArrayContainer answer = ContainerFactory.getArrayContainer();
                if (answer.content.length < desiredcapacity)
                        answer.content = new short[desiredcapacity];
                answer.cardinality = Util
                        .unsigned_intersect2by2(value1.content,
                                value1.getCardinality(), value2.content,
                                value2.getCardinality(), answer.content);
                return answer;
        }

        @Override
        public Container and(BitmapContainer x) {
                return x.and(this);
        }

        @Override
        public ArrayContainer andNot(final ArrayContainer value2) {
                ArrayContainer value1 = this;
                final int desiredcapacity = value1.getCardinality();
                ArrayContainer answer = ContainerFactory.getArrayContainer();
                if (answer.content.length < desiredcapacity)
                        answer.content = new short[desiredcapacity];
                answer.cardinality = Util.unsigned_difference(value1.content,
                        value1.getCardinality(), value2.content,
                        value2.getCardinality(), answer.content);
                return answer;
        }

        @Override
        public ArrayContainer andNot(BitmapContainer value2) {
                final ArrayContainer answer = ContainerFactory
                        .getArrayContainer();
                if (answer.content.length < this.content.length)
                        answer.content = new short[this.content.length];
                int pos = 0;
                for (int k = 0; k < cardinality; ++k)
                        if (!value2.contains(this.content[k]))
                                answer.content[pos++] = this.content[k];
                answer.cardinality = pos;
                return answer;
        }

        @Override
        public void clear() {
                cardinality = 0;
        }

        @Override
        public ArrayContainer clone() {
                final ArrayContainer x = (ArrayContainer) super.clone();
                x.cardinality = this.cardinality;
                x.content = Arrays.copyOf(content, content.length);
                return x;
        }

        @Override
        public boolean contains(final short x) {
                return Util.unsigned_binarySearch(content, 0, cardinality, x) >= 0;
        }

        @Override
        public boolean equals(Object o) {
                if (o instanceof ArrayContainer) {
                        ArrayContainer srb = (ArrayContainer) o;
                        if (srb.cardinality != this.cardinality)
                                return false;
                        for (int i = 0; i < this.cardinality; ++i) {
                                if (this.content[i] != srb.content[i])
                                        return false;
                        }
                        return true;
                }
                return false;
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
                                return pos < ArrayContainer.this.cardinality;
                        }

                        @Override
                        public short next() {
                                return ArrayContainer.this.content[pos++];
                        }

                        int pos = 0;
                };
        }

        @Override
        public int getSizeInBits() {
                return this.cardinality * 16 + 32;
        }

        @Override
        public int getSizeInBytes() {
                return this.cardinality * 2 + 4;

        }

        @Override
        public ArrayContainer iand(final ArrayContainer value2) {
                ArrayContainer value1 = this;
                value1.cardinality = Util
                        .unsigned_intersect2by2(value1.content,
                                value1.getCardinality(), value2.content,
                                value2.getCardinality(), value1.content);
                return this;
        }

        @Override
        public Container iand(BitmapContainer value2) {
                int pos = 0;
                for (int k = 0; k < cardinality; ++k)
                        if (value2.contains(this.content[k]))
                                this.content[pos++] = this.content[k];
                cardinality = pos;
                return this;
        }

        @Override
        public ArrayContainer iandNot(final ArrayContainer value2) {
                this.getCardinality();
                this.cardinality = Util.unsigned_difference(this.content,
                        this.getCardinality(), value2.content,
                        value2.getCardinality(), this.content);
                return this;
        }

        @Override
        public ArrayContainer iandNot(BitmapContainer value2) {
                int pos = 0;
                for (int k = 0; k < cardinality; ++k)
                        if (!value2.contains(this.content[k]))
                                this.content[pos++] = this.content[k];
                this.cardinality = pos;
                return this;
        }

        public ArrayContainer iandNOT(final ArrayContainer value2) {
                cardinality = Util.unsigned_difference(content, cardinality,
                        value2.content, value2.getCardinality(), content);
                return this;
        }

        public Container iandNOT(BitmapContainer value2) {
                int pos = 0;
                for (int k = 0; k < cardinality; ++k)
                        if (!value2.contains(this.content[k]))
                                this.content[pos++] = this.content[k];
                cardinality = pos;
                return this;
        }

        @Override
        public Container ior(final ArrayContainer value2) {
                // Using inPlace operations on arrays is very expensive. Each
                // modification needs O(n) shifts
                final ArrayContainer value1 = this;
                int tailleAC = value1.getCardinality()
                        + value2.getCardinality();
                final int desiredcapacity = tailleAC > 65535 ? 65535 : tailleAC;
                short[] newContent = new short[desiredcapacity];
                int card = Util.unsigned_union2by2(value1.content,
                        value1.getCardinality(), value2.content,
                        value2.getCardinality(), newContent);
                this.content = newContent;
                this.cardinality = card;
                if (this.cardinality >= DEFAULTMAXSIZE)
                        return ContainerFactory
                                .transformToBitmapContainer(this);
                return this;
        }

        @Override
        public Container ior(BitmapContainer x) {
                return x.or(this);
        }

        @Override
        public Iterator<Short> iterator() {
                return new Iterator<Short>() {
                        @Override
                        public boolean hasNext() {
                                return pos < ArrayContainer.this.cardinality;
                        }

                        @Override
                        public Short next() {
                                return new Short(
                                        ArrayContainer.this.content[pos++]);
                        }

                        @Override
                        public void remove() {
                                ArrayContainer.this.remove((short) (pos - 1));
                                pos--;
                        }

                        short pos = 0;
                };
        }

        @Override
        public Container ixor(final ArrayContainer value2) {
                final ArrayContainer value1 = this;
                final int lentgh = value1.getCardinality()
                        + value2.getCardinality();
                final int desiredcapacity = lentgh <= 65536 ? lentgh : 65536;
                short[] newContent = new short[desiredcapacity];
                int card = Util.unsigned_exclusiveunion2by2(value1.content,
                        value1.getCardinality(), value2.content,
                        value2.getCardinality(), newContent);
                this.content = newContent;
                this.cardinality = card;
                if (this.cardinality >= DEFAULTMAXSIZE)
                        return ContainerFactory
                                .transformToBitmapContainer(this);
                return this;
        }

        @Override
        public Container ixor(BitmapContainer x) {
                return x.xor(this);
        }

        public void loadData(final BitmapContainer bitmapContainer) {
                if (content.length < bitmapContainer.cardinality)
                        content = new short[bitmapContainer.cardinality];
                this.cardinality = bitmapContainer.cardinality;
                int pos = 0;
                for (int i = bitmapContainer.nextSetBit(0); i >= 0; i = bitmapContainer
                        .nextSetBit(i + 1)) {
                        content[pos++] = (short) i;
                }
                if (pos != this.cardinality)
                        throw new RuntimeException("bug " + pos + " "
                                + this.cardinality);
        }

        @Override
        public Container or(final ArrayContainer value2) {
                final ArrayContainer value1 = this;
                int tailleAC = value1.getCardinality()
                        + value2.getCardinality();
                final int desiredcapacity = tailleAC > 65535 ? 65535 : tailleAC;
                ArrayContainer answer = ContainerFactory.getArrayContainer();
                if (answer.content.length < desiredcapacity)
                        answer.content = new short[desiredcapacity];
                answer.cardinality = Util.unsigned_union2by2(value1.content,
                        value1.getCardinality(), value2.content,
                        value2.getCardinality(), answer.content);
                if (answer.cardinality >= DEFAULTMAXSIZE)
                        return ContainerFactory
                                .transformToBitmapContainer(answer);
                return answer;
        }

        @Override
        public Container or(BitmapContainer x) {
                return x.or(this);
        }

        @Override
        public Container remove(final short x) {
                final int loc = Util.unsigned_binarySearch(content, 0,
                        cardinality, x);
                if (loc >= 0) {
                        // insertion
                        System.arraycopy(content, loc + 1, content, loc,
                                cardinality - loc - 1);
                        --cardinality;
                }
                return this;
        }

        @Override
        public String toString() {
                if (this.cardinality == 0)
                        return "{}";
                StringBuffer sb = new StringBuffer();
                sb.append("{");
                for (int i = 0; i < this.cardinality - 1; i++) {
                        sb.append(this.content[i]);
                        sb.append(",");
                }
                sb.append(this.content[this.cardinality - 1]);
                sb.append("}");
                return sb.toString();
        }

        @Override
        public void trim() {
                this.content = Arrays.copyOf(this.content, this.cardinality);
        }

        @Override
        public Container xor(final ArrayContainer value2) {
                final ArrayContainer value1 = this;
                final int desiredcapacity = Math.min(value1.getCardinality()
                        + value2.getCardinality(), 65536);
                ArrayContainer answer = ContainerFactory.getArrayContainer();
                if (answer.content.length < desiredcapacity)
                        answer.content = new short[desiredcapacity];
                answer.cardinality = Util
                        .unsigned_exclusiveunion2by2(value1.content,
                                value1.getCardinality(), value2.content,
                                value2.getCardinality(), answer.content);
                if (answer.cardinality >= DEFAULTMAXSIZE)
                        return ContainerFactory
                                .transformToBitmapContainer(answer);
                return answer;
        }

        @Override
        public Container xor(BitmapContainer x) {
                return x.xor(this);
        }

        private void increaseCapacity() {
                int newcapacity = this.content.length * 5 / 4;
                if (newcapacity > ArrayContainer.DEFAULTMAXSIZE)
                        newcapacity = ArrayContainer.DEFAULTMAXSIZE;
                this.content = Arrays.copyOf(this.content, newcapacity);
        }

        public short[] content;
        int cardinality = 0;
        private static final int DEFAULTINITSIZE = 4;

        private static final long serialVersionUID = 1L;

        protected static final int DEFAULTMAXSIZE = 4096;

}
