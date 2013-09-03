package me.lemire.roaringbitmap;

import java.util.Iterator;
import java.util.Map.Entry;

public final class Util {
        public static short highbits(int x) {
                return (short) (x >>> 16);
        }

        public static short lowbits(int x) {
                return (short) (x & 0xFFFF);
        }

        protected static Container and(Container value1, Container value2) {
                if (value1 instanceof ArrayContainer) {
                        if (value2 instanceof ArrayContainer)
                                return ((ArrayContainer) value1)
                                        .and((ArrayContainer) value2);
                        return ((BitmapContainer) value2)
                                .and((ArrayContainer) value1);
                }
                if (value2 instanceof ArrayContainer)
                        return ((BitmapContainer) value1)
                                .and((ArrayContainer) value2);
                return ((BitmapContainer) value2).and((BitmapContainer) value1);
        }

        protected static Container or(Container value1, Container value2) {
                if (value1 instanceof ArrayContainer) {
                        if (value2 instanceof ArrayContainer)
                                return ((ArrayContainer) value1)
                                        .or((ArrayContainer) value2);
                        return ((BitmapContainer) value2)
                                .or((ArrayContainer) value1);
                }
                if (value2 instanceof ArrayContainer)
                        return ((BitmapContainer) value1)
                                .or((ArrayContainer) value2);
                return ((BitmapContainer) value2).or((BitmapContainer) value1);
        }

        protected static Container xor(Container value1, Container value2) {
                if (value1 instanceof ArrayContainer) {
                        if (value2 instanceof ArrayContainer)
                                return ((ArrayContainer) value1)
                                        .xor((ArrayContainer) value2);
                        return ((BitmapContainer) value2)
                                .xor((ArrayContainer) value1);
                }
                if (value2 instanceof ArrayContainer)
                        return ((BitmapContainer) value1)
                                .xor((ArrayContainer) value2);
                return ((BitmapContainer) value2).xor((BitmapContainer) value1);
        }

        public final static int toIntUnsigned(short x) {
                return x & 0xFFFF;
        }

        protected static int unsigned_intersect2by2(final short[] set1,
                final int length1, final short[] set2, final int length2,
                final short[] buffer) {
                if ((0 == length1) || (0 == length2))
                        return 0;
                int k1 = 0;
                int k2 = 0;
                int pos = 0;

                mainwhile: while (true) {
                        if (toIntUnsigned(set2[k2]) < toIntUnsigned(set1[k1])) {
                                do {
                                        ++k2;
                                        if (k2 == length2)
                                                break mainwhile;
                                } while (toIntUnsigned(set2[k2]) < toIntUnsigned(set1[k1]));
                        }
                        if (toIntUnsigned(set1[k1]) < toIntUnsigned(set2[k2])) {
                                do {
                                        ++k1;
                                        if (k1 == length1)
                                                break mainwhile;
                                } while (toIntUnsigned(set1[k1]) < toIntUnsigned(set2[k2]));
                        } else {
                                // (set2[k2] == set1[k1])
                                buffer[pos++] = set1[k1];
                                ++k1;
                                if (k1 == length1)
                                        break;
                                ++k2;
                                if (k2 == length2)
                                        break;
                        		}
                }
                return pos;
        }

        static protected int unsigned_union2by2(final short[] set1,
                final int length1, final short[] set2, final int length2,
                final short[] buffer) {
                int pos = 0;
                int k1 = 0, k2 = 0;
                if (0 == length1) {
                        for (int k = 0; k < length1; ++k)
                                buffer[k] = set1[k];
                        return length1;
                }
                if (0 == length2) {
                        for (int k = 0; k < length2; ++k)
                                buffer[k] = set2[k];
                        return length2;
                }
                while (true) {
                        if (toIntUnsigned(set1[k1]) < toIntUnsigned(set2[k2])) {
                                buffer[pos++] = set1[k1];
                                ++k1;
                                if (k1 >= length1) {
                                        for (; k2 < length2; ++k2)
                                                buffer[pos++] = set2[k2];
                                        break;
                                }
                        } else if (toIntUnsigned(set1[k1]) == toIntUnsigned(set2[k2])) {
                                buffer[pos++] = set1[k1];
                                ++k1;
                                ++k2;
                                if (k1 >= length1) {
                                        for (; k2 < length2; ++k2)
                                                buffer[pos++] = set2[k2];
                                        break;
                                }
                                if (k2 >= length2) {
                                        for (; k1 < length1; ++k1)
                                                buffer[pos++] = set1[k1];
                                        break;
                                }
                        } else {// if (set1[k1]>set2[k2])
                                buffer[pos++] = set2[k2];
                                ++k2;
                                if (k2 >= length2) {
                                        for (; k1 < length1; ++k1)
                                                buffer[pos++] = set1[k1];
                                        break;
                                }
                        }
                }
                return pos;
        }

        protected static int unsigned_binarySearch(short[] array, int begin,
                int end, short k) {
                int low = begin;
                int high = end - 1;
                int ikey = toIntUnsigned(k);

                while (low <= high) {
                        int middleIndex = (low + high) >>> 1;
                        int middleValue = toIntUnsigned(array[middleIndex]);

                        if (middleValue < ikey)
                                low = middleIndex + 1;
                        else if (middleValue > ikey)
                                high = middleIndex - 1;
                        else
                                return middleIndex;
                }
                return -(low + 1);
        }

        static protected int unsigned_exclusiveunion2by2(final short[] set1,
                final int length1, final short[] set2, final int length2,
                final short[] buffer) {
                int pos = 0;
                int k1 = 0, k2 = 0;
                if (0 == length1) {
                        for (int k = 0; k < length1; ++k)
                                buffer[k] = set1[k];
                        return length1;
                }
                if (0 == length2) {
                        for (int k = 0; k < length2; ++k)
                                buffer[k] = set2[k];
                        return length2;
                }
                while (true) {
                        if (toIntUnsigned(set1[k1]) < toIntUnsigned(set2[k2])) {
                                buffer[pos++] = set1[k1];
                                ++k1;
                                if (k1 >= length1) {
                                        for (; k2 < length2; ++k2)
                                                buffer[pos++] = set2[k2];
                                        break;
                                }
                        } else if (toIntUnsigned(set1[k1]) == toIntUnsigned(set2[k2])) {
                                ++k1;
                                ++k2;
                                if (k1 >= length1) {
                                        for (; k2 < length2; ++k2)
                                                buffer[pos++] = set2[k2];
                                        break;
                                }
                                if (k2 >= length2) {
                                        for (; k1 < length1; ++k1)
                                                buffer[pos++] = set1[k1];
                                        break;
                                }
                        } else {// if (val1>val2)
                                buffer[pos++] = set2[k2];
                                ++k2;
                                if (k2 >= length2) {
                                        for (; k1 < length1; ++k1)
                                                buffer[pos++] = set1[k1];
                                        break;
                                }
                        }
                }
                return pos;
        }
        
        public static int getAverageNbIntsPerNode(RoaringBitmap rb)
        {
                final Iterator<Entry<Short, Container>> p1 = rb.highlowcontainer
                                .entrySet().iterator();
                Entry<Short, Container> s;
                int average = 0;
                do{
                        s=p1.next();
                        // we add the 16 highbits of a node and the size of its leafs
                        average += rb.highlowcontainer.get(s.getKey()).getCardinality();                      
                }while(p1.hasNext());
                average /= rb.highlowcontainer.size();
                return average; 
        }

        public static void display(RoaringBitmap x) {
                final Iterator<Entry<Short, Container>> p1 = x.highlowcontainer.entrySet().iterator();
                Entry<Short, Container> s1;

                while (p1.hasNext()) {
                        s1 = p1.next();
                        System.out.println("\n" + s1.getKey().shortValue());
                        Container c = s1.getValue();
                        System.out.println(c.toString());
                }
        }



}
