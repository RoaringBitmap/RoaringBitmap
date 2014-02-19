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
                for (int k = 0; k < 3; ++k) {
                        long bef = System.nanoTime();
                        for(int j = 0; j < 100; ++j) {
                          BitSet bs1c = (BitSet) bs1.clone();
                          bs1c.and(bs2);
                          bogus += bs1c.size();
                        }
                        long aft = System.nanoTime();
                        if (aft - bef < duration)
                                duration = aft - bef;
                }
                return duration;
        }

        static long timeIntersection(RoaringBitmap bs1, RoaringBitmap bs2) {
                long duration = Long.MAX_VALUE;
                for (int k = 0; k < 3; ++k) {
                        long bef = System.nanoTime();
                        for(int j = 0; j < 100; ++j) {
                           bogus += RoaringBitmap.and(bs1, bs2).getSizeInBytes();
                        }
                        long aft = System.nanoTime();
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
                int counter = 0;
                for (int i = 0; i < NTRIALS; ++i) {
                        try {
                                int[] data1 = dataSrc.fetchBitPositions(
                                        dataset, 2 * i);
                                int[] data2 = dataSrc.fetchBitPositions(
                                        dataset, 2 * i + 1);
                                counter ++;

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
                                double bstime = timeIntersection(ans1, ans2);

                                RoaringBitmap rr1 = RoaringBitmap
                                        .bitmapOf(data1);
                                RoaringBitmap rr2 = RoaringBitmap
                                        .bitmapOf(data2);
                                double rrtime = timeIntersection(rr1, rr2);
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
                System.out.println("Processed "+counter+" pairs");

        }
}
