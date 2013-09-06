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
                PriorityQueue<ConciseSetPointer> pq = new PriorityQueue<ConciseSetPointer>(
                        bitmaps.length, new Comparator<ConciseSetPointer>() {
                                @Override
                                public int compare(ConciseSetPointer a, ConciseSetPointer b) {
                                        return sizeInBytes(a.cs) - sizeInBytes(b.cs);
                                }
                        });
                for (ConciseSet x : bitmaps) {
                        pq.add(new ConciseSetPointer(x, true));
                }
                while (pq.size() > 1) {
                        ConciseSetPointer x1 = pq.poll();
                        ConciseSetPointer x2 = pq.poll();
                        if(x1.needsCloning)
                                x1.cs = x1.cs.clone();
                        x1.cs.union(x2.cs);
                        x1.needsCloning = false;
                        pq.add(x1);
                }
                return pq.poll().cs;
        }

        protected static ConciseSet fastXOR(ConciseSet... bitmaps) {
                PriorityQueue<ConciseSetPointer> pq = new PriorityQueue<ConciseSetPointer>(
                        bitmaps.length, new Comparator<ConciseSetPointer>() {
                                @Override
                                public int compare(ConciseSetPointer a, ConciseSetPointer b) {
                                        return sizeInBytes(a.cs) - sizeInBytes(b.cs);
                                }
                        });
                for (ConciseSet x : bitmaps) {
                        pq.add(new ConciseSetPointer(x, true));
                }
                while (pq.size() > 1) {
                        ConciseSetPointer x1 = pq.poll();
                        ConciseSetPointer x2 = pq.poll();
                        if(x1.needsCloning)
                                x1.cs = x1.cs.clone();
                        x1.cs.symmetricDifference(x2.cs);
                        x1.needsCloning = false;
                        pq.add(x1);
                }
                return pq.poll().cs;
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

class ConciseSetPointer {
        ConciseSet cs;
        boolean needsCloning;
        public ConciseSetPointer(ConciseSet c, boolean mustclone) {
                needsCloning = mustclone;
                cs = c;
                
        }
}
