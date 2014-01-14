package me.lemire.roaringbitmap;

import java.io.Serializable;
import java.util.Iterator;
import me.lemire.roaringbitmap.SpeedyArray.Element;

/**
 * TODO: This class should be renamed RoaringBitmap.
 */
public final class SpeedyRoaringBitmap implements Cloneable, Serializable {

        /**
	 * 
	 */
        private static final long serialVersionUID = 3L;
        public SpeedyArray highlowcontainer = null;

        public SpeedyRoaringBitmap() {
                highlowcontainer = new SpeedyArray();
        }

        /**
         * set the value to "true", whether it already appears on not.
         */
        public void add(final int x) {
                set(x);
        }

        /**
         * set the value to "true", whether it already appears on not.
         */
        public void set(final int x) {
                final short hb = Util.highbits(x);
                int i = highlowcontainer.getIndex(hb);
                if (i >= 0) {
                        highlowcontainer.getAtIndex(i).value = highlowcontainer.getAtIndex(i).value.add(Util.lowbits(x));
                } else {
                        ArrayContainer newac = ContainerFactory
                                .getArrayContainer();
                        highlowcontainer.put(hb, newac.add(Util.lowbits(x)));
                }
        }

        public void validate() {
                /**
                 * TODO: More of a debugging routine. Should be pruned before release.
                 */
                for (int i = 0; i < this.highlowcontainer.size(); i++)
                        this.highlowcontainer.getAtIndex(i).value.validate();
        }
        

        public static SpeedyRoaringBitmap and(final SpeedyRoaringBitmap x1,
                final SpeedyRoaringBitmap x2) {
                final SpeedyRoaringBitmap answer = new SpeedyRoaringBitmap();
                int pos1 = 0, pos2 = 0;
                int length1 = x1.highlowcontainer.size(), length2 = x2.highlowcontainer
                        .size();

                /*
                 * TODO: This could be optimized quite a bit when one bitmap is much
                 * smaller than the other one.
                 */
                main: if (pos1 < length1 && pos2 < length2) {
                        short s1 = x1.highlowcontainer.getAtIndex(pos1).key;
                        short s2 = x2.highlowcontainer.getAtIndex(pos2).key;
                        do {
                                if (s1 < s2) {
                                        pos1++;
                                        if (pos1 == length1)
                                                break main;
                                        s1 = x1.highlowcontainer.getAtIndex(pos1).key;
                                } else if (s1 > s2) {
                                        pos2++;
                                        if (pos2 == length2)
                                                break main;
                                        s2 = x2.highlowcontainer.getAtIndex(pos2).key;
                                } else {
                                        Container C = Util.and(
                                                x1.highlowcontainer.getAtIndex(pos1).value,
                                                x2.highlowcontainer.getAtIndex(pos2).value);
                                        if (C.getCardinality() > 0)
                                                answer.highlowcontainer.putEnd(
                                                        s1, C);
                                        pos1++;
                                        pos2++;
                                        if (pos1 == length1)
                                                break main;
                                        if (pos2 == length2)
                                                break main;
                                        s1 = x1.highlowcontainer.getAtIndex(pos1).key;
                                        s2 = x2.highlowcontainer.getAtIndex(pos2).key;
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
                        short s1 = x1.highlowcontainer.getAtIndex(pos1).key;
                        short s2 = x2.highlowcontainer.getAtIndex(pos2).key;
                        do {
                                if (s1 < s2) {
                                        answer.highlowcontainer
                                        .putEnd(s1,
                                                x1.highlowcontainer
                                                        .getAtIndex(pos1).value);
                                        pos1++;
                                        if (pos1 == length1)
                                                break main;
                                        s1 = x1.highlowcontainer.getAtIndex(pos1).key;
                                } else if (s1 > s2) {
                                        pos2++;
                                        if (pos2 == length2) {
                                                do {
                                                        answer.highlowcontainer
                                                                .putEnd(s1,
                                                                        x1.highlowcontainer
                                                                                .getAtIndex(pos1).value);
                                                        pos1++;
                                                        if (pos1 == length1)
                                                                break;
                                                        s1 = x1.highlowcontainer
                                                                .getAtIndex(pos1).key;
                                                } while (true);
                                                break main;
                                        }
                                        s2 = x2.highlowcontainer.getAtIndex(pos2).key;
                                } else {
                                        Container C = Util.andNot(
                                                x1.highlowcontainer.getAtIndex(pos1).value,
                                                x2.highlowcontainer.getAtIndex(pos2).value);
                                        if (C.getCardinality() > 0)
                                                answer.highlowcontainer.putEnd(
                                                        s1, C);
                                        pos1++;
                                        pos2++;
                                        if (pos1 == length1)
                                                break main;
                                        if (pos2 == length2){
                                                s1 = x1.highlowcontainer
                                                        .getAtIndex(pos1).key;
                                                do {

                                                        answer.highlowcontainer
                                                                .putEnd(s1,
                                                                        x1.highlowcontainer
                                                                                .getAtIndex(pos1).value);
                                                        pos1++;
                                                        if (pos1 == length1)
                                                                break;
                                                        s1 = x1.highlowcontainer
                                                                .getAtIndex(pos1).key;
                                                } while (true);
                                                break main;
                                        }
                                        s1 = x1.highlowcontainer.getAtIndex(pos1).key;
                                        s2 = x2.highlowcontainer.getAtIndex(pos2).key;
                                }
                        } while (true);
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
                        short s1 = x1.highlowcontainer.getAtIndex(pos1).key;
                        short s2 = x2.highlowcontainer.getAtIndex(pos2).key;

                        while (true) {
                                if (s1 < s2) {
                                        answer.highlowcontainer.putEnd(s1,
                                                x1.highlowcontainer.getAtIndex(pos1).value);
                                        pos1++;
                                        if (pos1 == length1) {
                                                do {
                                                        answer.highlowcontainer
                                                                .putEnd(s2,
                                                                        x2.highlowcontainer
                                                                                .getAtIndex(pos2).value);
                                                        pos2++;
                                                        if (pos2 == length2)
                                                                break;
                                                        s2 = x2.highlowcontainer
                                                                .getAtIndex(pos2).key;
                                                } while (true);
                                                break main;
                                        }
                                        s1 = x1.highlowcontainer.getAtIndex(pos1).key;
                                } else if (s1 > s2) {
                                        answer.highlowcontainer.putEnd(s2,
                                                x2.highlowcontainer.getAtIndex(pos2).value);
                                        pos2++;
                                        if (pos2 == length2) {
                                                do {
                                                        answer.highlowcontainer
                                                                .putEnd(s1,
                                                                        x1.highlowcontainer
                                                                                .getAtIndex(pos1).value);
                                                        pos1++;
                                                        if (pos1 == length1)
                                                                break;
                                                        s1 = x1.highlowcontainer
                                                                .getAtIndex(pos1).key;
                                                } while (true);
                                                break main;
                                        }
                                        s2 = x2.highlowcontainer.getAtIndex(pos2).key;
                                } else {
                                        // nbOR++;
                                        answer.highlowcontainer
                                                .putEnd(s1,
                                                        Util.or(x1.highlowcontainer.getAtIndex(pos1).value,
                                                                x2.highlowcontainer.getAtIndex(pos2).value));
                                        pos1++;
                                        pos2++;
                                        if (pos1 == length1) {
                                                while (pos2 < length2) {
                                                        s2 = x2.highlowcontainer
                                                                .getAtIndex(pos2).key;
                                                        answer.highlowcontainer
                                                                .putEnd(s2,
                                                                        x2.highlowcontainer
                                                                                .getAtIndex(pos2).value);
                                                        pos2++;
                                                }
                                                break main;
                                        }

                                        if (pos2 == length2) {
                                                while (pos1 < length1) {
                                                        s1 = x1.highlowcontainer
                                                                .getAtIndex(pos1).key;
                                                        answer.highlowcontainer
                                                                .put(s1,
                                                                        x1.highlowcontainer
                                                                                .getAtIndex(pos1).value);
                                                        pos1++;
                                                }
                                                break main;
                                        }
                                        s1 = x1.highlowcontainer.getAtIndex(pos1).key;
                                        s2 = x2.highlowcontainer.getAtIndex(pos2).key;
                                }
                        }
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
                        short s1 = x1.highlowcontainer.getAtIndex(pos1).key;
                        short s2 = x2.highlowcontainer.getAtIndex(pos2).key;

                        while (true) {
                                if (s1 < s2) {
                                        answer.highlowcontainer.putEnd(s1,
                                                x1.highlowcontainer.getAtIndex(pos1).value);
                                        pos1++;
                                        if (pos1 == length1) {
                                                do {
                                                        answer.highlowcontainer
                                                                .putEnd(s2,
                                                                        x2.highlowcontainer
                                                                                .getAtIndex(pos2).value);
                                                        pos2++;
                                                        if (pos2 == length2)
                                                                break;
                                                        s2 = x2.highlowcontainer
                                                                .getAtIndex(pos2).key;
                                                } while (true);
                                                break main;
                                        }
                                        s1 = x1.highlowcontainer.getAtIndex(pos1).key;
                                } else if (s1 > s2) {
                                        answer.highlowcontainer.putEnd(s2,
                                                x2.highlowcontainer.getAtIndex(pos2).value);
                                        pos2++;
                                        if (pos2 == length2) {
                                                do {
                                                        answer.highlowcontainer
                                                                .putEnd(s1,
                                                                        x1.highlowcontainer.getAtIndex(pos1).value);
                                                        pos1++;
                                                        if (pos1 == length1)
                                                                break;
                                                        s1 = x1.highlowcontainer
                                                                .getAtIndex(pos1).key;
                                                } while (true);
                                                break main;
                                        }
                                        s2 = x2.highlowcontainer.getAtIndex(pos2).key;
                                } else {
                                        // nbXOR++;
                                        answer.highlowcontainer.putEnd(s1, Util
                                                .xor(x1.highlowcontainer.getAtIndex(pos1).value, x2.highlowcontainer.getAtIndex(pos2).value));
                                        pos1++;
                                        pos2++;
                                        if (pos1 == length1) {
                                                while (pos2 < length2) {
                                                        s2 = x2.highlowcontainer
                                                                .getAtIndex(pos2).key;
                                                        answer.highlowcontainer
                                                                .putEnd(s2,
                                                                        x2.highlowcontainer
                                                                                .getAtIndex(pos2).value);
                                                        pos2++;
                                                }
                                                break main;
                                        }

                                        if (pos2 == length2) {
                                                while (pos1 < length1) {
                                                        s1 = x1.highlowcontainer
                                                                .getAtIndex(pos1).key;
                                                        answer.highlowcontainer
                                                                .putEnd(s1,
                                                                        x1.highlowcontainer
                                                                                .getAtIndex(pos1).value);
                                                        pos1++;
                                                }
                                                break main;
                                        }
                                        s1 = x1.highlowcontainer.getAtIndex(pos1).key;
                                        s2 = x2.highlowcontainer.getAtIndex(pos2).key;
                                }
                        }
                }
                return answer;
        }
        /**
         * iterate over the positions of the true values. 
         * TODO: test this (include remove)
         * @return the iterator
         */
        public Iterator<Integer> iterator() {
                return new Iterator<Integer>() {
                        short pos = 0;
                        int hs = 0;
                        int x;
                        Iterator<Short> iter;

                        public Iterator<Integer> init() {
                                if (pos < SpeedyRoaringBitmap.this.highlowcontainer
                                        .size()) {
                                        iter = SpeedyRoaringBitmap.this.highlowcontainer
                                                .getAtIndex(pos).value.iterator();
                                        hs = Util
                                                .toIntUnsigned(SpeedyRoaringBitmap.this.highlowcontainer
                                                        .getArray()[pos].key) << 16;
                                }
                                return this;
                        }

                        @Override
                        public boolean hasNext() {
                                return pos < SpeedyRoaringBitmap.this.highlowcontainer
                                        .size();
                        }

                        @Override
                        public Integer next() {
                                x = Util.toIntUnsigned(iter.next())
                                        | hs;
                                if (!iter.hasNext()) {
                                        ++pos;
                                        init();
                                }
                                return x;
                        }

                        @Override
                        public void remove() {
                              if((x & hs) == hs) {// still in same container
                                      iter.remove();
                              } else {
                                      SpeedyRoaringBitmap.this.remove(x);
                              }
                        }

                }.init();
        }

        public int[] getIntegers() {
                final int[] array = new int[this.getCardinality()];
                int pos = 0, pos2 = 0;
                while (pos < this.highlowcontainer.size()) {
                        final int hs = Util.toIntUnsigned(this.highlowcontainer
                                .getArray()[pos].key) << 16;
                        final ShortIterator si = this.highlowcontainer
                                .getArray()[pos++].value.getShortIterator();

                        while (si.hasNext()) {
                                array[pos2++] = hs
                                        | Util.toIntUnsigned(si.next());
                        }
                }
                return array;
        }

        public void remove(final int x) {
                final short hb = Util.highbits(x);
                int i = highlowcontainer.getIndex(hb);
                if (i < 0)
                        return;
                highlowcontainer.getAtIndex(i).value = highlowcontainer.getAtIndex(i).value.remove(Util.lowbits(x));
                if (highlowcontainer.getAtIndex(i).value.getCardinality() == 0)
                        highlowcontainer.removeAtIndex(i);
        }

        public boolean contains(final int x) {
                final short hb = Util.highbits(x);
                final Container C = highlowcontainer.getContainer(hb);
                if (C == null)
                        return false;
                return C.contains(Util.lowbits(x));
        }

        public int getSizeInBytes() {
                int size = 8;
                for (int i = 0; i < this.highlowcontainer.size(); i++) {
                        Container c = this.highlowcontainer.getArray()[i].value;
                        size += 2 + c.getSizeInBytes();
                }
                return size;
        }

        public void trim() {
                for (int i = 0; i < this.highlowcontainer.size(); i++) {
                        this.highlowcontainer.getArray()[i].value.trim();
                }
        }

        public Container getContainer(short key) {
                return this.highlowcontainer.getContainer(key);
        }

        public boolean containsKey(short key) {
                return this.highlowcontainer.ContainsKey(key);
        }

        public Element[] getArray() {
                /**
                 * TODO: More of a debugging routine. Should be removed in the final version.
                 */
                return this.highlowcontainer.getArray();
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
                 * TODO: More of a debugging routine. 
                 * Should be removed in the final version or move to Util.
                 */
                int nb[] = new int[this.highlowcontainer.size()], pos = 0;
                for (int i = 0; i < this.highlowcontainer.size(); i++)
                        nb[pos++] = this.highlowcontainer.getArray()[i].value
                                .getCardinality();
                return nb;
        }

        public int getNbNodes() {
                /**
                 * TODO: More of a debugging routine. 
                 * Should be removed in the final version or moved to Util.
                 */
                return this.highlowcontainer.size();
        }

        public int getCardinality() {
                int size = 0;
                for (int i = 0; i < this.getNbNodes(); i++) {
                        size += this.highlowcontainer.getArray()[i].value
                                .getCardinality();
                }
                return size;
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

        @Override
        public String toString() {
                /**
                 * TODO: This toString is really more
                 * of a debugging routine. Should be removed in the final version.
                 */
                int nbNodes = this.highlowcontainer.size();
                int nbArrayC = 0, nbBitmapC = 0, minAC = 1024, maxAC = 0, minBC = 65535, maxBC = 0, card, avAC = 0, avBC = 0;
                for (int i = 0; i < this.highlowcontainer.size(); i++) {
                        Container C = this.highlowcontainer.getArray()[i].value;
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


}
