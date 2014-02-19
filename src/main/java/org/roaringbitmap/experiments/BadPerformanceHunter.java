package org.roaringbitmap.experiments;

import java.util.BitSet;
import org.roaringbitmap.RoaringBitmap;


/**
 * The purpose of this case is to identify cases where roaring fares poorly as
 * far as intersection speed goes.
 * 
 * @author Daniel Lemire
 * 
 */
public class BadPerformanceHunter {
        static int bogus = 0;

        static long timeIntersection(BitSet bs1, BitSet bs2) {
                long duration = Long.MAX_VALUE;
                for (int k = 0; k < 10; ++k) {
                        long bef = System.currentTimeMillis();
                        BitSet bs1c = (BitSet) bs1.clone();
                        bs1c.and(bs2);
                        bogus += bs1c.size();
                        long aft = System.currentTimeMillis();
                        if (aft - bef < duration)
                                duration = aft - bef;
                }
                return duration;
        }

        static long timeIntersection(RoaringBitmap bs1, RoaringBitmap bs2) {
                long duration = Long.MAX_VALUE;
                for (int k = 0; k < 10; ++k) {
                        long bef = System.currentTimeMillis();
                        bogus += RoaringBitmap.and(bs1, bs2).getSizeInBytes();
                        long aft = System.currentTimeMillis();
                        if (aft - bef < duration)
                                duration = aft - bef;
                }
                return duration;
        }

        /**
         * @param args
         */
        public static void main(String[] args) {
                RealDataRetriever dataSrc = new RealDataRetriever(args[0]);
                String dataset = args[1];
                int NTRIALS = Integer.parseInt(args[2]);
                System.out.println(NTRIALS + " tests on " + dataset);
                double worse = 1;
                for (int i = 0; i < NTRIALS; ++i) {
                        try {
                                int[] data1 = dataSrc.fetchBitPositions(
                                        dataset, 2 * i);
                                int[] data2 = dataSrc.fetchBitPositions(
                                        dataset, 2 * i + 1);

                                if (data1.length < 1024)
                                        continue;
                                if (data2.length < 1024)
                                        continue;

                                BitSet ans1 = new BitSet();
                                for (int j : data1)
                                        ans1.set(j);
                                BitSet ans2 = new BitSet();
                                for (int j : data2)
                                        ans2.set(j);
                                long bstime = timeIntersection(ans1, ans2);

                                RoaringBitmap rr1 = RoaringBitmap
                                        .bitmapOf(data1);
                                RoaringBitmap rr2 = RoaringBitmap
                                        .bitmapOf(data2);
                                long rrtime = timeIntersection(rr1, rr2);
                                if (rrtime / bstime > worse) {
                                        System.out.println("index = " + i);
                                        System.out.println("cardinality = "
                                                + data1.length + " "
                                                + data2.length);
                                        System.out
                                                .println("bstime = " + bstime);
                                        System.out
                                                .println("rrtime = " + rrtime);
                                        worse = rrtime / bstime;
                                        System.out.println("ratio = " + worse);
                                }
                        } catch (java.lang.RuntimeException e) {
                        }

                }

        }
}
