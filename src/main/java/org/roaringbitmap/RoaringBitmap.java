package org.roaringbitmap;

import java.io.Serializable;
import java.util.Iterator;


public final class RoaringBitmap implements Cloneable, Serializable,
        Iterable<Integer> {

        public RoaringBitmap() {
                highlowcontainer = new RoaringArray();
        }


        /**
         * set the value to "true", whether it already appears on not.
         */
        public void add(final int x) {
                final short hb = Util.highbits(x);
                final int i = highlowcontainer.getIndex(hb);
                if (i >= 0) {
                        highlowcontainer.setContainerAtIndex(
                                i,
                                highlowcontainer.getContainerAtIndex(i).add(
                                        Util.lowbits(x)));
                } else {
                        final ArrayContainer newac = ContainerFactory
                                .getArrayContainer();
                        highlowcontainer.insertNewKeyValueAt(-i - 1, hb,
                                newac.add(Util.lowbits(x)));
                }
        }

        public void and(final RoaringBitmap x2) {
                int pos1 = 0, pos2 = 0;
                int length1 = highlowcontainer.size();
                final int length2 = x2.highlowcontainer.size();
                /*
                 * TODO: This could be optimized quite a bit when one bitmap is
                 * much smaller than the other one.
                 */
                main: if (pos1 < length1 && pos2 < length2) {
                        short s1 = highlowcontainer.getKeyAtIndex(pos1);
                        short s2 = x2.highlowcontainer.getKeyAtIndex(pos2);
                        do {
                                if (s1 < s2) {
                                        highlowcontainer.removeAtIndex(pos1);
                                        --length1;
                                        if (pos1 == length1)
                                                break main;
                                        s1 = highlowcontainer
                                                .getKeyAtIndex(pos1);
                                } else if (s1 > s2) {
                                        pos2++;
                                        if (pos2 == length2)
                                                break main;
                                        s2 = x2.highlowcontainer
                                                .getKeyAtIndex(pos2);
                                } else {
                                        final Container C = highlowcontainer
                                                .getContainerAtIndex(pos1)
                                                .and(x2.highlowcontainer
                                                        .getContainerAtIndex(pos2));
                                        if (C.getCardinality() > 0) {
                                                this.highlowcontainer
                                                        .setContainerAtIndex(
                                                                pos1, C);
                                                pos1++;
                                        } else {
                                                highlowcontainer
                                                        .removeAtIndex(pos1);
                                                --length1;
                                        }
                                        pos2++;
                                        if ((pos1 == length1)
                                                || (pos2 == length2))
                                                break main;
                                        s1 = highlowcontainer
                                                .getKeyAtIndex(pos1);
                                        s2 = x2.highlowcontainer
                                                .getKeyAtIndex(pos2);
                                }
                        } while (true);
                }
        }

        public void andNot(final RoaringBitmap x2) {
                int pos1 = 0, pos2 = 0;
                int length1 = highlowcontainer.size();
                final int length2 = x2.highlowcontainer.size();
                main: if (pos1 < length1 && pos2 < length2) {
                        short s1 = highlowcontainer.getKeyAtIndex(pos1);
                        short s2 = x2.highlowcontainer.getKeyAtIndex(pos2);
                        do {
                                if (s1 < s2) {
                                        pos1++;
                                        if (pos1 == length1)
                                                break main;
                                        s1 = highlowcontainer
                                                .getKeyAtIndex(pos1);
                                } else if (s1 > s2) {
                                        pos2++;
                                        if (pos2 == length2) {
                                                break main;
                                        }
                                        s2 = x2.highlowcontainer
                                                .getKeyAtIndex(pos2);
                                } else {
                                        final Container C = highlowcontainer
                                                .getContainerAtIndex(pos1)
                                                .andNot(x2.highlowcontainer
                                                        .getContainerAtIndex(pos2));
                                        if (C.getCardinality() > 0) {
                                                this.highlowcontainer
                                                        .setContainerAtIndex(
                                                                pos1, C);
                                                pos1++;
                                        } else {
                                                highlowcontainer
                                                        .removeAtIndex(pos1);
                                                --length1;
                                        }
                                        pos2++;
                                        if ((pos1 == length1)
                                                || (pos2 == length2))
                                                break main;
                                        s1 = highlowcontainer
                                                .getKeyAtIndex(pos1);
                                        s2 = x2.highlowcontainer
                                                .getKeyAtIndex(pos2);
                                }
                        } while (true);
                }
        }

        /**
         * reset to an empty bitmap; result occupies as much space a newly
         * created bitmap.
         */
        public void clear() {
                highlowcontainer = new RoaringArray(); // lose references
        }

        @Override
        public RoaringBitmap clone() {
                try {
                        final RoaringBitmap x = (RoaringBitmap) super.clone();
                        x.highlowcontainer = highlowcontainer.clone();
                        return x;
                } catch (final CloneNotSupportedException e) {
                        e.printStackTrace();
                        throw new RuntimeException(
                                "shouldn't happen with clone");
                }
        }

        public boolean contains(final int x) {
                final short hb = Util.highbits(x);
                final Container C = highlowcontainer.getContainer(hb);
                if (C == null)
                        return false;
                return C.contains(Util.lowbits(x));
        }

        /**
         * public Container getContainer(short key) { return
         * this.highlowcontainer.getContainer(key); }
         */

        public boolean containsKey(short key) {
                return this.highlowcontainer.ContainsKey(key);
        }

        @Override
        public boolean equals(Object o) {
                if (o instanceof RoaringBitmap) {
                        final RoaringBitmap srb = (RoaringBitmap) o;
                        return srb.highlowcontainer
                                .equals(this.highlowcontainer);
                }
                return false;
        }


        public int getCardinality() {
                int size = 0;
                for (int i = 0; i < this.highlowcontainer.size(); i++) {
                        size += this.highlowcontainer.getContainerAtIndex(i)
                                .getCardinality();
                }
                return size;
        }

        public int getSizeInBytes() {
                int size = 8;
                for (int i = 0; i < this.highlowcontainer.size(); i++) {
                        final Container c = this.highlowcontainer
                                .getContainerAtIndex(i);
                        size += 2 + c.getSizeInBytes();
                }
                return size;
        }

        /**
         * iterate over the positions of the true values. 
         * 
         * @return the iterator
         */
        @Override
        public Iterator<Integer> iterator() {
                return new Iterator<Integer>() {
                        @Override
                        public boolean hasNext() {
                                return pos < RoaringBitmap.this.highlowcontainer
                                        .size();
                        }

                        public Iterator<Integer> init() {
                                if (pos < RoaringBitmap.this.highlowcontainer
                                        .size()) {
                                        iter = RoaringBitmap.this.highlowcontainer
                                                .getContainerAtIndex(pos)
                                                .iterator();
                                        hs = Util
                                                .toIntUnsigned(RoaringBitmap.this.highlowcontainer
                                                        .getKeyAtIndex(pos)) << 16;
                                }
                                return this;
                        }

                        @Override
                        public Integer next() {
                                x = Util.toIntUnsigned(iter.next()) | hs;
                                if (!iter.hasNext()) {
                                        ++pos;
                                        init();
                                }
                                return x;
                        }

                        @Override
                        public void remove() {
                                if ((x & hs) == hs) {// still in same container
                                        iter.remove();
                                } else {
                                        RoaringBitmap.this.remove(x);
                                }
                        }

                        int hs = 0;

                        Iterator<Short> iter;

                        short pos = 0;

                        int x;

                }.init();
        }

        public void or(final RoaringBitmap x2) {
                int pos1 = 0, pos2 = 0;
                int length1 = highlowcontainer.size();
                final int length2 = x2.highlowcontainer.size();
                main: if (pos1 < length1 && pos2 < length2) {
                        short s1 = highlowcontainer.getKeyAtIndex(pos1);
                        short s2 = x2.highlowcontainer.getKeyAtIndex(pos2);

                        while (true) {
                                if (s1 < s2) {
                                        pos1++;
                                        if (pos1 == length1) {
                                                break main;
                                        }
                                        s1 = highlowcontainer
                                                .getKeyAtIndex(pos1);
                                } else if (s1 > s2) {
                                        highlowcontainer
                                                .insertNewKeyValueAt(
                                                        pos1,
                                                        s2,
                                                        x2.highlowcontainer
                                                                .getContainerAtIndex(pos2));
                                        pos1++;
                                        length1++;
                                        pos2++;
                                        if (pos2 == length2) {
                                                break main;
                                        }
                                        s2 = x2.highlowcontainer
                                                .getKeyAtIndex(pos2);
                                } else {
                                        this.highlowcontainer
                                                .setContainerAtIndex(
                                                        pos1,
                                                        highlowcontainer
                                                                .getContainerAtIndex(
                                                                        pos1)
                                                                .or(x2.highlowcontainer
                                                                        .getContainerAtIndex(pos2)));
                                        pos1++;
                                        pos2++;
                                        if ((pos1 == length1)
                                                || (pos2 == length2)) {
                                                break main;
                                        }
                                        s1 = highlowcontainer
                                                .getKeyAtIndex(pos1);
                                        s2 = x2.highlowcontainer
                                                .getKeyAtIndex(pos2);
                                }
                        }
                }
                if (pos1 == length1) {
                        highlowcontainer.appendCopy(x2.highlowcontainer, pos2,
                                length2);
                }
        }

        public void remove(final int x) {
                final short hb = Util.highbits(x);
                final int i = highlowcontainer.getIndex(hb);
                if (i < 0)
                        return;
                highlowcontainer.setContainerAtIndex(i, highlowcontainer
                        .getContainerAtIndex(i).remove(Util.lowbits(x)));
                if (highlowcontainer.getContainerAtIndex(i).getCardinality() == 0)
                        highlowcontainer.removeAtIndex(i);
        }

        public int[] toArray() {
                final int[] array = new int[this.getCardinality()];
                int pos = 0, pos2 = 0;
                while (pos < this.highlowcontainer.size()) {
                        final int hs = Util.toIntUnsigned(this.highlowcontainer
                                .getKeyAtIndex(pos)) << 16;
                        final ShortIterator si = this.highlowcontainer
                                .getContainerAtIndex(pos++).getShortIterator();

                        while (si.hasNext()) {
                                array[pos2++] = hs
                                        | Util.toIntUnsigned(si.next());
                        }
                }
                return array;
        }

        /**
         * A string describing the bitmap.
         * 
         * @return the string
         */
        @Override
        public String toString() {
                final StringBuffer answer = new StringBuffer();
                final IntIterator i = this.getIntIterator();
                answer.append("{");
                if (i.hasNext())
                        answer.append(i.next());
                while (i.hasNext()) {
                        answer.append(",");
                        answer.append(i.next());
                }
                answer.append("}");
                return answer.toString();
        }

        public void trim() {
                for (int i = 0; i < this.highlowcontainer.size(); i++) {
                        this.highlowcontainer.getContainerAtIndex(i).trim();
                }
        }

        public void xor(final RoaringBitmap x2) {
                int pos1 = 0, pos2 = 0;
                int length1 = highlowcontainer.size();
                final int length2 = x2.highlowcontainer.size();

                main: if (pos1 < length1 && pos2 < length2) {
                        short s1 = highlowcontainer.getKeyAtIndex(pos1);
                        short s2 = x2.highlowcontainer.getKeyAtIndex(pos2);

                        while (true) {
                                if (s1 < s2) {
                                        pos1++;
                                        if (pos1 == length1) {
                                                break main;
                                        }
                                        s1 = highlowcontainer
                                                .getKeyAtIndex(pos1);
                                } else if (s1 > s2) {
                                        pos2++;
                                        if (pos2 == length2) {
                                                break main;
                                        }
                                        s2 = x2.highlowcontainer
                                                .getKeyAtIndex(pos2);
                                } else {
                                        final Container C = highlowcontainer
                                                .getContainerAtIndex(pos1)
                                                .xor(x2.highlowcontainer
                                                        .getContainerAtIndex(pos2));
                                        if (C.getCardinality() > 0) {
                                                this.highlowcontainer
                                                        .setContainerAtIndex(
                                                                pos1, C);
                                                pos1++;
                                        } else {
                                                highlowcontainer
                                                        .removeAtIndex(pos1);
                                                --length1;
                                        }
                                        pos2++;
                                        if ((pos1 == length1)
                                                || (pos2 == length2)) {
                                                break main;
                                        }
                                        s1 = highlowcontainer
                                                .getKeyAtIndex(pos1);
                                        s2 = x2.highlowcontainer
                                                .getKeyAtIndex(pos2);
                                }
                        }
                }
                if (pos1 == length1) {
                        highlowcontainer.appendCopy(x2.highlowcontainer, pos2,
                                length2);
                }
        }

        private IntIterator getIntIterator() {
                return new IntIterator() {
                        @Override
                        public boolean hasNext() {
                                return pos < RoaringBitmap.this.highlowcontainer
                                        .size();
                        }

                        public IntIterator init() {
                                if (pos < RoaringBitmap.this.highlowcontainer
                                        .size()) {
                                        iter = RoaringBitmap.this.highlowcontainer
                                                .getContainerAtIndex(pos)
                                                .iterator();
                                        hs = Util
                                                .toIntUnsigned(RoaringBitmap.this.highlowcontainer
                                                        .getKeyAtIndex(pos)) << 16;
                                }
                                return this;
                        }

                        @Override
                        public int next() {
                                x = Util.toIntUnsigned(iter.next()) | hs;
                                if (!iter.hasNext()) {
                                        ++pos;
                                        init();
                                }
                                return x;
                        }

                        @Override
                        public void remove() {
                                if ((x & hs) == hs) {// still in same container
                                        iter.remove();
                                } else {
                                        RoaringBitmap.this.remove(x);
                                }
                        }

                        int hs = 0;

                        Iterator<Short> iter;

                        short pos = 0;

                        int x;

                }.init();
        }

        public static RoaringBitmap and(final RoaringBitmap x1,
                final RoaringBitmap x2) {
                final RoaringBitmap answer = new RoaringBitmap();
                int pos1 = 0, pos2 = 0;
                final int length1 = x1.highlowcontainer.size(), length2 = x2.highlowcontainer
                        .size();

                /*
                 * TODO: This could be optimized quite a bit when one bitmap is
                 * much smaller than the other one.
                 */
                main: if (pos1 < length1 && pos2 < length2) {
                        short s1 = x1.highlowcontainer.getKeyAtIndex(pos1);
                        short s2 = x2.highlowcontainer.getKeyAtIndex(pos2);
                        do {
                                if (s1 < s2) {
                                        pos1++;
                                        if (pos1 == length1)
                                                break main;
                                        s1 = x1.highlowcontainer
                                                .getKeyAtIndex(pos1);
                                } else if (s1 > s2) {
                                        pos2++;
                                        if (pos2 == length2)
                                                break main;
                                        s2 = x2.highlowcontainer
                                                .getKeyAtIndex(pos2);
                                } else {
                                        final Container C = x1.highlowcontainer
                                                .getContainerAtIndex(pos1)
                                                .and(x2.highlowcontainer
                                                        .getContainerAtIndex(pos2));
                                        if (C.getCardinality() > 0)
                                                answer.highlowcontainer.append(
                                                        s1, C);
                                        pos1++;
                                        pos2++;
                                        if ((pos1 == length1)
                                                || (pos2 == length2))
                                                break main;
                                        s1 = x1.highlowcontainer
                                                .getKeyAtIndex(pos1);
                                        s2 = x2.highlowcontainer
                                                .getKeyAtIndex(pos2);
                                }
                        } while (true);
                }
                return answer;
        }

        public static RoaringBitmap andNot(final RoaringBitmap x1,
                final RoaringBitmap x2) {
                final RoaringBitmap answer = new RoaringBitmap();
                int pos1 = 0, pos2 = 0;
                final int length1 = x1.highlowcontainer.size(), length2 = x2.highlowcontainer
                        .size();
                main: if (pos1 < length1 && pos2 < length2) {
                        short s1 = x1.highlowcontainer.getKeyAtIndex(pos1);
                        short s2 = x2.highlowcontainer.getKeyAtIndex(pos2);
                        do {
                                if (s1 < s2) {
                                        answer.highlowcontainer.appendCopy(
                                                x1.highlowcontainer, pos1);
                                        pos1++;
                                        if (pos1 == length1)
                                                break main;
                                        s1 = x1.highlowcontainer
                                                .getKeyAtIndex(pos1);
                                } else if (s1 > s2) {
                                        pos2++;
                                        if (pos2 == length2) {
                                                break main;
                                        }
                                        s2 = x2.highlowcontainer
                                                .getKeyAtIndex(pos2);
                                } else {
                                        final Container C = x1.highlowcontainer
                                                .getContainerAtIndex(pos1)
                                                .andNot(x2.highlowcontainer
                                                        .getContainerAtIndex(pos2));
                                        if (C.getCardinality() > 0)
                                                answer.highlowcontainer.append(
                                                        s1, C);
                                        pos1++;
                                        pos2++;
                                        if ((pos1 == length1)
                                                || (pos2 == length2))
                                                break main;
                                        s1 = x1.highlowcontainer
                                                .getKeyAtIndex(pos1);
                                        s2 = x2.highlowcontainer
                                                .getKeyAtIndex(pos2);
                                }
                        } while (true);
                }
                if (pos2 == length2) {
                        answer.highlowcontainer.appendCopy(x1.highlowcontainer,
                                pos1, length1);
                }
                return answer;
        }

        public static RoaringBitmap bitmapOf(int... dat) {
                final RoaringBitmap ans = new RoaringBitmap();
                for (final int i : dat)
                        ans.add(i);
                return ans;
        }

        public static RoaringBitmap or(final RoaringBitmap x1,
                final RoaringBitmap x2) {
                final RoaringBitmap answer = new RoaringBitmap();
                int pos1 = 0, pos2 = 0;
                final int length1 = x1.highlowcontainer.size(), length2 = x2.highlowcontainer
                        .size();
                main: if (pos1 < length1 && pos2 < length2) {
                        short s1 = x1.highlowcontainer.getKeyAtIndex(pos1);
                        short s2 = x2.highlowcontainer.getKeyAtIndex(pos2);

                        while (true) {
                                if (s1 < s2) {
                                        answer.highlowcontainer.appendCopy(
                                                x1.highlowcontainer, pos1);
                                        pos1++;
                                        if (pos1 == length1) {
                                                break main;
                                        }
                                        s1 = x1.highlowcontainer
                                                .getKeyAtIndex(pos1);
                                } else if (s1 > s2) {
                                        answer.highlowcontainer.appendCopy(
                                                x2.highlowcontainer, pos2);
                                        pos2++;
                                        if (pos2 == length2) {
                                                break main;
                                        }
                                        s2 = x2.highlowcontainer
                                                .getKeyAtIndex(pos2);
                                } else {
                                        answer.highlowcontainer
                                                .append(s1,
                                                        x1.highlowcontainer
                                                                .getContainerAtIndex(
                                                                        pos1)
                                                                .or(x2.highlowcontainer
                                                                        .getContainerAtIndex(pos2)));
                                        pos1++;
                                        pos2++;
                                        if ((pos1 == length1)
                                                || (pos2 == length2)) {
                                                break main;
                                        }
                                        s1 = x1.highlowcontainer
                                                .getKeyAtIndex(pos1);
                                        s2 = x2.highlowcontainer
                                                .getKeyAtIndex(pos2);
                                }
                        }
                }
                if (pos1 == length1) {
                        answer.highlowcontainer.appendCopy(x2.highlowcontainer,
                                pos2, length2);
                } else if (pos2 == length2) {
                        answer.highlowcontainer.appendCopy(x1.highlowcontainer,
                                pos1, length1);
                }
                return answer;
        }

        public static RoaringBitmap xor(final RoaringBitmap x1,
                final RoaringBitmap x2) {
                final RoaringBitmap answer = new RoaringBitmap();
                int pos1 = 0, pos2 = 0;
                final int length1 = x1.highlowcontainer.size(), length2 = x2.highlowcontainer
                        .size();

                main: if (pos1 < length1 && pos2 < length2) {
                        short s1 = x1.highlowcontainer.getKeyAtIndex(pos1);
                        short s2 = x2.highlowcontainer.getKeyAtIndex(pos2);

                        while (true) {
                                if (s1 < s2) {
                                        answer.highlowcontainer.appendCopy(
                                                x1.highlowcontainer, pos1);
                                        pos1++;
                                        if (pos1 == length1) {
                                                break main;
                                        }
                                        s1 = x1.highlowcontainer
                                                .getKeyAtIndex(pos1);
                                } else if (s1 > s2) {
                                        answer.highlowcontainer.appendCopy(
                                                x2.highlowcontainer, pos2);
                                        pos2++;
                                        if (pos2 == length2) {
                                                break main;
                                        }
                                        s2 = x2.highlowcontainer
                                                .getKeyAtIndex(pos2);
                                } else {
                                        final Container C = x1.highlowcontainer
                                                .getContainerAtIndex(pos1)
                                                .xor(x2.highlowcontainer
                                                        .getContainerAtIndex(pos2));
                                        if (C.getCardinality() > 0)
                                                answer.highlowcontainer.append(
                                                        s1, C);
                                        pos1++;
                                        pos2++;
                                        if ((pos1 == length1)
                                                || (pos2 == length2)) {
                                                break main;
                                        }
                                        s1 = x1.highlowcontainer
                                                .getKeyAtIndex(pos1);
                                        s2 = x2.highlowcontainer
                                                .getKeyAtIndex(pos2);
                                }
                        }
                }
                if (pos1 == length1) {
                        answer.highlowcontainer.appendCopy(x2.highlowcontainer,
                                pos2, length2);
                } else if (pos2 == length2) {
                        answer.highlowcontainer.appendCopy(x1.highlowcontainer,
                                pos1, length1);
                }

                return answer;
        }

        public RoaringArray highlowcontainer = null;

        private static final long serialVersionUID = 3L;

}
