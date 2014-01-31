package me.lemire.roaringbitmap;

import java.io.Serializable;
import java.util.Iterator;

/**
 * TODO: This class should be renamed RoaringBitmap.
 */
public final class SpeedyRoaringBitmap implements Cloneable, Serializable, Iterable<Integer>  {

        public SpeedyRoaringBitmap() {
                highlowcontainer = new SpeedyArray();
        }

        /**
         * set the value to "true", whether it already appears on not.
         * proxy for set
         */
        public void add(final int x) {
                set(x);
        }

        /**
         * reset to an empty bitmap; result occupies as much space a newly
         * created bitmap.
         */
        public void clear() {
                highlowcontainer = new SpeedyArray(); // lose references
        }

        @Override
        public SpeedyRoaringBitmap clone() {
                try {
                        SpeedyRoaringBitmap x = (SpeedyRoaringBitmap) super
                                .clone();
                        x.highlowcontainer = highlowcontainer.clone();
                        return x;
                } catch (CloneNotSupportedException e) {
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
                if (o instanceof SpeedyRoaringBitmap) {
                        SpeedyRoaringBitmap srb = (SpeedyRoaringBitmap) o;
                        return srb.highlowcontainer
                                .equals(this.highlowcontainer);
                }
                return false;
        }

        public int getCardinality() {
                int size = 0;
                for (int i = 0; i < this.getNbNodes(); i++) {
                        size += this.highlowcontainer.getContainerAtIndex(i)
                                .getCardinality();
                }
                return size;
        }

        public int[] getIntegers() {
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
         * return an array that contains the number of short integers in each
         * node. The number of short integers of the i th node will be stocked
         * in the array's position i.
         * 
         * @return int[] number of short integers per node
         */
        public int[] getIntsPerNode() {
                /**
                 * TODO: More of a debugging routine. Should be removed in the
                 * final version or move to Util.
                 */
                int nb[] = new int[this.highlowcontainer.size()], pos = 0;
                for (int i = 0; i < this.highlowcontainer.size(); i++)
                        nb[pos++] = this.highlowcontainer
                                .getContainerAtIndex(i).getCardinality();
                return nb;
        }

        public int getNbNodes() {
                /**
                 * TODO: More of a debugging routine. Should be removed in the
                 * final version or moved to Util.
                 */
                return this.highlowcontainer.size();
        }

        public int getSizeInBytes() {
                int size = 8;
                for (int i = 0; i < this.highlowcontainer.size(); i++) {
                        Container c = this.highlowcontainer
                                .getContainerAtIndex(i);
                        size += 2 + c.getSizeInBytes();
                }
                return size;
        }

        /**
         * iterate over the positions of the true values. TODO: test this
         * (include remove)
         * 
         * @return the iterator
         */
        public Iterator<Integer> iterator() {
                return new Iterator<Integer>() {
                        @Override
                        public boolean hasNext() {
                                return pos < SpeedyRoaringBitmap.this.highlowcontainer
                                        .size();
                        }

                        public Iterator<Integer> init() {
                                if (pos < SpeedyRoaringBitmap.this.highlowcontainer
                                        .size()) {
                                        iter = SpeedyRoaringBitmap.this.highlowcontainer
                                                .getContainerAtIndex(pos)
                                                .iterator();
                                        hs = Util
                                                .toIntUnsigned(SpeedyRoaringBitmap.this.highlowcontainer
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
                                        SpeedyRoaringBitmap.this.remove(x);
                                }
                        }

                        int hs = 0;

                        Iterator<Short> iter;

                        short pos = 0;

                        int x;

                }.init();
        }
        
        // inplace
        public void and(final SpeedyRoaringBitmap x2) {
                int pos1 = 0, pos2 = 0;
                int length1 = highlowcontainer.size(), length2 = x2.highlowcontainer
                        .size();
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
                                        if(pos1==length1)
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
                                        Container C = Util.inPlaceAND(highlowcontainer
                                                        .getContainerAtIndex(pos1),
                                                        x2.highlowcontainer
                                                                .getContainerAtIndex(pos2));
                                        if (C.getCardinality() > 0) {
                                                this.highlowcontainer.setContainerAtIndex(pos1, C);
                                                pos1++;
                                        } else {
                                                highlowcontainer.removeAtIndex(pos1);
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

        // inplace
        public void andNot(final SpeedyRoaringBitmap x2) {
                int pos1 = 0, pos2 = 0;
                int length1 = highlowcontainer.size(), length2 = x2.highlowcontainer
                        .size();
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
                                        Container C = Util
                                                .inPlaceANDNOT(
                                                        highlowcontainer
                                                                .getContainerAtIndex(pos1),
                                                        x2.highlowcontainer
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
        
        // inplace
        public void or(final SpeedyRoaringBitmap x2) {
                int pos1 = 0, pos2 = 0;
                int length1 = highlowcontainer.size(), length2 = x2.highlowcontainer
                        .size();
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
                                        highlowcontainer.insertNewKeyValueAt(pos1, s2, x2.highlowcontainer.getContainerAtIndex(pos2));
                                        pos1++;
                                        length1++;
                                        pos2++;
                                        if (pos2 == length2) {
                                                break main;
                                        }
                                        s2 = x2.highlowcontainer
                                                .getKeyAtIndex(pos2);
                                } else {
                                        this.highlowcontainer.setContainerAtIndex(pos1, Util.inPlaceOR(highlowcontainer
                                                .getContainerAtIndex(pos1), x2.highlowcontainer
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
                        highlowcontainer.appendCopy(x2.highlowcontainer,
                                pos2, length2);
                }
        }

        // inplace
        public void xor(final SpeedyRoaringBitmap x2) {
                int pos1 = 0, pos2 = 0;
                int length1 = highlowcontainer.size(), length2 = x2.highlowcontainer
                        .size();

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
                                        Container C = Util.inPlaceXOR(highlowcontainer
                                                .getContainerAtIndex(pos1), x2.highlowcontainer
                                                .getContainerAtIndex(pos2));
                                        if(C.getCardinality()>0) {
                                                this.highlowcontainer.setContainerAtIndex(pos1, C);
                                                pos1++;
                                        } else {
                                                highlowcontainer.removeAtIndex(pos1);
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
                        highlowcontainer.appendCopy(x2.highlowcontainer,
                                pos2, length2);
                }
        }


        public void remove(final int x) {
                final short hb = Util.highbits(x);
                int i = highlowcontainer.getIndex(hb);
                if (i < 0)
                        return;
                highlowcontainer.setContainerAtIndex(i, highlowcontainer
                        .getContainerAtIndex(i).remove(Util.lowbits(x)));
                if (highlowcontainer.getContainerAtIndex(i).getCardinality() == 0)
                        highlowcontainer.removeAtIndex(i);
        }

        /**
         * set the value to "true", whether it already appears on not.
         */
        public void set(final int x) {
                final short hb = Util.highbits(x);
                final int i = highlowcontainer.getIndex(hb);
                if (i >= 0) {
                        highlowcontainer.setContainerAtIndex(
                                i,
                                highlowcontainer.getContainerAtIndex(i).add(
                                        Util.lowbits(x)));
                } else {
                        ArrayContainer newac = ContainerFactory
                                .getArrayContainer();
                        highlowcontainer.insertNewKeyValueAt(-i-1,hb, newac.add(Util.lowbits(x)));
                }
        }

        @Override
        public String toString() {
                /**
                 * TODO: This toString is really more of a debugging routine.
                 * Should be removed in the final version.
                 */
                int nbNodes = this.highlowcontainer.size();
                int nbArrayC = 0, nbBitmapC = 0, minAC = 1024, maxAC = 0, minBC = 65535, maxBC = 0, card, avAC = 0, avBC = 0;
                for (int i = 0; i < this.highlowcontainer.size(); i++) {
                        Container C = this.highlowcontainer
                                .getContainerAtIndex(i);
                        if (C instanceof ArrayContainer) {
                                nbArrayC++;
                                card = C.getCardinality();
                                if (card < minAC)
                                        minAC = C.getCardinality();
                                if (card > maxAC)
                                        maxAC = C.getCardinality();
                                avAC += card;
                        } else {
                                nbBitmapC++;
                                card = C.getCardinality();
                                if (C.getCardinality() < minBC)
                                        minBC = C.getCardinality();
                                if (C.getCardinality() > maxBC)
                                        maxBC = C.getCardinality();
                                avBC += card;
                        }
                }
                try {
                        avAC /= nbArrayC;
                } catch (ArithmeticException e) {
                        avAC = 0;
                }
                try {
                        avBC /= nbBitmapC;
                } catch (ArithmeticException e) {
                        avBC = 0;
                }
                String desc = "We have " + nbNodes + " nodes with " + nbArrayC
                        + " ArrayContainers min : " + minAC + " max : " + maxAC
                        + " average : " + avAC + " and " + nbBitmapC
                        + " BitmapContainers min : " + minBC + " max : "
                        + maxBC + " avBC : " + avBC;
                return desc;
        }

        public void trim() {
                for (int i = 0; i < this.highlowcontainer.size(); i++) {
                        this.highlowcontainer.getContainerAtIndex(i).trim();
                }
        }

        public void validate() {
                /**
                 * TODO: More of a debugging routine. Should be pruned before
                 * release.
                 */
                for (int i = 0; i < this.highlowcontainer.size(); i++)
                        this.highlowcontainer.getContainerAtIndex(i).validate();
        }

        public static SpeedyRoaringBitmap and(final SpeedyRoaringBitmap x1,
                final SpeedyRoaringBitmap x2) {
                final SpeedyRoaringBitmap answer = new SpeedyRoaringBitmap();
                int pos1 = 0, pos2 = 0;
                int length1 = x1.highlowcontainer.size(), length2 = x2.highlowcontainer
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
                                        Container C = Util
                                                .and(x1.highlowcontainer
                                                        .getContainerAtIndex(pos1),
                                                        x2.highlowcontainer
                                                                .getContainerAtIndex(pos2));
                                        if (C.getCardinality() > 0)
                                                answer.highlowcontainer.append(
                                                        s1, C);
                                        pos1++;
                                        pos2++;
                                        if ((pos1 == length1) ||(pos2 == length2))
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

        public static SpeedyRoaringBitmap andNot(final SpeedyRoaringBitmap x1,
                final SpeedyRoaringBitmap x2) {
                final SpeedyRoaringBitmap answer = new SpeedyRoaringBitmap();
                int pos1 = 0, pos2 = 0;
                int length1 = x1.highlowcontainer.size(), length2 = x2.highlowcontainer
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
                                        Container C = Util
                                                .andNot(x1.highlowcontainer
                                                        .getContainerAtIndex(pos1),
                                                        x2.highlowcontainer
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

        public static SpeedyRoaringBitmap or(final SpeedyRoaringBitmap x1,
                final SpeedyRoaringBitmap x2) {
                final SpeedyRoaringBitmap answer = new SpeedyRoaringBitmap();
                int pos1 = 0, pos2 = 0;
                int length1 = x1.highlowcontainer.size(), length2 = x2.highlowcontainer
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
                                                        Util.or(x1.highlowcontainer
                                                                .getContainerAtIndex(pos1),
                                                                x2.highlowcontainer
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

        public static SpeedyRoaringBitmap xor(final SpeedyRoaringBitmap x1,
                final SpeedyRoaringBitmap x2) {
                final SpeedyRoaringBitmap answer = new SpeedyRoaringBitmap();
                int pos1 = 0, pos2 = 0;
                int length1 = x1.highlowcontainer.size(), length2 = x2.highlowcontainer
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
                                        Container C = Util.xor(
                                                x1.highlowcontainer
                                                        .getContainerAtIndex(pos1),
                                                x2.highlowcontainer
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

        public SpeedyArray highlowcontainer = null;

        private static final long serialVersionUID = 3L;

}
