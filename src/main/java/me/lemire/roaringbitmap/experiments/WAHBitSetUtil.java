package me.lemire.roaringbitmap.experiments;

import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;

import org.devbrat.util.WAHBitSet;


public class WAHBitSetUtil {
        protected static int sizeInBytes(WAHBitSet cs) {
                return (int) (cs.memSize() * 4);
        }

        protected static WAHBitSet fastOR(WAHBitSet... bitmaps) {
                PriorityQueue<WAHBitSet> pq = new PriorityQueue<WAHBitSet>(
                        bitmaps.length, new Comparator<WAHBitSet>() {
                                @Override
                                public int compare(WAHBitSet a, WAHBitSet b) {
                                        return sizeInBytes(a) - sizeInBytes(b);
                                }
                        });
                for (WAHBitSet x : bitmaps) {
                        pq.add(x);
                }
                while (pq.size() > 1) {
                        WAHBitSet x1 = pq.poll();
                        WAHBitSet x2 = pq.poll();
                        x1.or(x2);
                        pq.add(x1);
                }
                return pq.poll();
        }


        protected static WAHBitSet fastAND(WAHBitSet... bitmaps) {
                if (bitmaps.length == 0)
                        return new WAHBitSet();
                WAHBitSet[] array = Arrays.copyOf(bitmaps, bitmaps.length);
                Arrays.sort(array, new Comparator<WAHBitSet>() {
                        @Override
                        public int compare(WAHBitSet a, WAHBitSet b) {
                                return sizeInBytes(a) - sizeInBytes(b);
                        }
                });
                WAHBitSet answer = array[0];
                for (int k = 1; k < array.length; ++k)
                        answer = answer.and(array[k]);
                return answer;
        }

}
