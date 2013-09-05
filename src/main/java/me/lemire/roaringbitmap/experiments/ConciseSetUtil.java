package me.lemire.roaringbitmap.experiments;

import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;

import it.uniroma3.mat.extendedset.intset.ConciseSet;

public class ConciseSetUtil {
        protected static int sizeInBytes(ConciseSet cs) {
                return (int) (cs.size() * cs.collectionCompressionRatio()) * 4;
        }

        protected static ConciseSet fastOR(ConciseSet... bitmaps) {
                PriorityQueue<ConciseSet> pq = new PriorityQueue<ConciseSet>(
                        bitmaps.length, new Comparator<ConciseSet>() {
                                @Override
                                public int compare(ConciseSet a, ConciseSet b) {
                                        return sizeInBytes(a) - sizeInBytes(b);
                                }
                        });
                for (ConciseSet x : bitmaps) {
                        pq.add(x.clone());
                }
                while (pq.size() > 1) {
                        ConciseSet x1 = pq.poll();
                        ConciseSet x2 = pq.poll();
                        x1.union(x2);
                        pq.add(x1);
                }
                return pq.poll();
        }

        protected static ConciseSet fastXOR(ConciseSet... bitmaps) {
                PriorityQueue<ConciseSet> pq = new PriorityQueue<ConciseSet>(
                        bitmaps.length, new Comparator<ConciseSet>() {
                                @Override
                                public int compare(ConciseSet a, ConciseSet b) {
                                        return sizeInBytes(a) - sizeInBytes(b);
                                }
                        });
                for (ConciseSet x : bitmaps) {
                        pq.add(x.clone());
                }
                while (pq.size() > 1) {
                        ConciseSet x1 = pq.poll();
                        ConciseSet x2 = pq.poll();
                        x1.symmetricDifference(x2);
                        pq.add(x1);
                }
                return pq.poll();
        }

        protected static ConciseSet fastAND(ConciseSet... bitmaps) {
                if (bitmaps.length == 0)
                        return new ConciseSet();
                ConciseSet[] array = Arrays.copyOf(bitmaps, bitmaps.length);
                Arrays.sort(array, new Comparator<ConciseSet>() {
                        @Override
                        public int compare(ConciseSet a, ConciseSet b) {
                                return sizeInBytes(a) - sizeInBytes(b);
                        }
                });
                ConciseSet answer = array[0].clone();
                for (int k = 1; k < array.length; ++k)
                        answer.intersection(array[k]);
                return answer;
        }

}
