package me.lemire.roaringbitmap.experiments.colantonio;

import java.text.DecimalFormat;
import java.util.BitSet;
import me.lemire.roaringbitmap.SpeedyRoaringBitmap;
import it.uniroma3.mat.extendedset.intset.ConciseSet;

/**
 * 
 * This a reproduction of the benchmark used by Colantonio and Di Pietro,
 * Concise: Compressed 'n' Composable Integer Set
 * 
 * While they report "Compression" as the ratio between the number of 32-bit words
 * required to represent the compressed bitmap and the cardinality
 * of the integer set, we report the number of bits per integer.
 * 
 *  Like them, we use "Density" mean  the ratio between the cardinality
 * of the set and the number range. 
 * 
 * Like them, we "Max/Cardinality" to mean the ratio
 * between the maximal value (i.e., the number range) and the cardinality
 * of the set—that is, the inverse of the density.
 *
 *  
 * Time measurement are expressed in nanoseconds. Each experiment is
 * performed 100 times, and the average reported.
 *  
 * @author Daniel Lemire
 * 
 */
public class Benchmark {

        /**
         * @param a an array of integers
         * @return a bitset representing the provided integers
         */
        public static BitSet toBitSet(final int[] a) {
                BitSet bs = new BitSet();
                for (int x : a)
                        bs.set(x);
                return bs;
        }

        /**
         * @param a an array of integers
         * @return a ConciseSet representing the provided integers
         */
        public static ConciseSet toConciseSet(final int[] a) {
                ConciseSet cs = new ConciseSet();
                for (int x : a)
                        cs.add(x);
                return cs;
        }
        
        /**
         * @param a an array of integers
         * @return a SpeedyRoaringBitmap representing the provided integers
         */
        public static SpeedyRoaringBitmap toSpeedyRoaringBitmap(int[] a) {
                SpeedyRoaringBitmap rr = new SpeedyRoaringBitmap();
                for (int x : a)
                        rr.add(x);
                return rr;
        }

        /**
         * @param a an array of integers
         * @return a ConciseSet (in WAH mode) representing the provided integers
         */
        public static ConciseSet toWAHConciseSet(int[] a) {
                ConciseSet cs = new ConciseSet(true);
                for (int x : a)
                        cs.add(x);
                return cs;
        }

        /**
         * @param args command line arguments
         */
        public static void main(final String[] args) {
                System.out.println("# This benchmark emulates what Colantonio and Di Pietro,");
                System.out.println("#  did in Concise: Compressed 'n' Composable Integer Set");
                System.out.println("########");
                System.out.println("# "+System.getProperty("java.vendor")+" "+System.getProperty("java.version")+" "+System.getProperty("java.vm.name"));
                System.out.println("# "+System.getProperty("os.name")+" "+System.getProperty("os.arch")+" "+System.getProperty("os.version"));
                System.out.println("# processors: "+Runtime.getRuntime().availableProcessors());
                System.out.println("# max mem.: "+Runtime.getRuntime().maxMemory());
                System.out.println("########");
                uniformtest(false, 10, 10000);//warming up
                zipfiantest(false, 10, 10000);//warming up
                int N = 100000;
                int TIMES = 100;
                System.out.println("# starting uniform test, please wait... (go grab a coffee)");
                uniformtest(true, TIMES, N);
                System.out.println();
                System.out.println("# starting zipfian test, please wait... (go grab a coffee)");
                zipfiantest(true, TIMES, N);
                System.out.println();
       }

        /**
         * @param verbose whether to print out the result
         * @param TIMES how many times should we run each test
         * @param N size of the sets
         */
        public static void uniformtest(final boolean verbose, final int TIMES, final int N) {
                if (!verbose)
                        System.out
                                .println("# running a dry run (can take a long time)");
                int bogus = 0;
                long bef, aft;
                DecimalFormat df = new DecimalFormat("0.000E0");
                DecimalFormat dfb = new DecimalFormat("000.0");
                if (verbose)
                        System.out
                                .println("### uniform test (intersection times in ns)");
                if (verbose)
                        System.out
                                .println("# (first columns are timings, then bits/int)");
                if (verbose)
                        System.out
                                .println("# density\tbitset\t\tconcise\t\twah\t\tspeedyroaring" +
                                		"\t\t\t\tbitset\t\tconcise\t\twah\t\tspeedyroaring");
                DataGenerator gen = new DataGenerator(N);
                for (double d = 0.005; d <= 0.999; d *= 1.2) {
                        double[] timings = new double[4];
                        double[] storageinbits = new double[4];

                        for (int times = 0; times < TIMES; ++times) {
                                int[] v1 = gen.getUniform(d);
                                int[] v2 = gen.getUniform(d);
                                //
                                BitSet b1 = toBitSet(v1);
                                BitSet b2 = toBitSet(v2);
                                storageinbits[0] += b1.size() + b2.size();
                                bef = System.nanoTime();
                                b1.and(b2);
                                aft = System.nanoTime();
                                bogus += b1.length();
                                timings[0] += aft - bef;
                                //
                                ConciseSet cs1 = toConciseSet(v1);
                                ConciseSet cs2 = toConciseSet(v2);
                                bef = System.nanoTime();
                                cs1.intersection(cs2);
                                aft = System.nanoTime();
                                bogus += cs1.size();
                                timings[1] += aft - bef;
                                storageinbits[1] += cs1.size()
                                        * cs1.collectionCompressionRatio() * 4
                                        * 8;
                                storageinbits[1] += cs2.size()
                                        * cs2.collectionCompressionRatio() * 4
                                        * 8;

                                //
                                ConciseSet wah1 = toWAHConciseSet(v1);
                                ConciseSet wah2 = toWAHConciseSet(v2);
                                bef = System.nanoTime();
                                wah1.intersection(wah2);
                                aft = System.nanoTime();
                                bogus += wah1.size();
                                timings[2] += aft - bef;
                                storageinbits[2] += wah1.size()
                                        * wah1.collectionCompressionRatio() * 4
                                        * 8;
                                storageinbits[2] += wah2.size()
                                        * wah2.collectionCompressionRatio() * 4
                                        * 8;
                                //
                                SpeedyRoaringBitmap rb1 = toSpeedyRoaringBitmap(v1);
                                SpeedyRoaringBitmap rb2 = toSpeedyRoaringBitmap(v2);
                                bef = System.nanoTime();
                                rb1.inPlaceAND(rb2);
                                aft = System.nanoTime();
                                bogus += rb1.getCardinality();
                                timings[3] += aft - bef;
                                storageinbits[3] += rb1.getSizeInBytes() * 8;
                                storageinbits[3] += rb2.getSizeInBytes() * 8;

                        }
                        if (verbose)
                                System.out.print(df.format(d) + "\t"
                                        + df.format(timings[0] / TIMES)
                                        + "\t\t"
                                        + df.format(timings[1] / TIMES)
                                        + "\t\t"
                                        + df.format(timings[2] / TIMES)
                                        + "\t\t"
                                        + df.format(timings[3] / TIMES));
                        if (verbose)
                                System.out.println("\t\t\t"
                                        + dfb.format(storageinbits[0]
                                                / (2 * TIMES * gen.N))
                                        + "   "
                                        + dfb.format(storageinbits[1]
                                                / (2 * TIMES * gen.N))
                                        + "   "
                                        + dfb.format(storageinbits[2]
                                                / (2 * TIMES * gen.N))
                                        + "   "
                                        + dfb.format(storageinbits[3]
                                                / (2 * TIMES * gen.N)));

                }
                System.out.println("#ignore = " + bogus);
        }
        
        /**
         * @param verbose whether to print out the result
         * @param TIMES how many times should we run each test
         * @param N size of the sets
         */
        public static void zipfiantest(boolean verbose, int TIMES, int N) {
                if (!verbose)
                        System.out.println("# running a dry run");
                int bogus = 0;
                long bef, aft;
                boolean out = false;
                DecimalFormat df = new DecimalFormat("0.000E0");
                DecimalFormat dfb = new DecimalFormat("000.0");

                if (verbose)
                        System.out
                                .println("### zipfian test (intersection times in ns)");
                if (verbose)
                        System.out
                                .println("# (first columns are timings, then bits/int)");
                if (verbose)
                	System.out
                    .println("# density\tbitset\t\tconcise\t\twah\t\tspeedyroaring" +
                    		"\t\t\t\tbitset\t\tconcise\t\twah\t\tspeedyroaring");
                DataGenerator gen = new DataGenerator(N);
                //This strategy gives too big ints that leads to overheads with the ConciseSet library 
               // for (double max = 1.2 * gen.N; max <= gen.N * 2 * 10000; max *= 1.9) { 
                for (double d = 0.005; d <= 0.999; d *= 1.2){
                        double[] timings = new double[4];
                        double[] storageinbits = new double[4];
                        int max = (int) (N / d);
                        
                        for (int times = 0; times < TIMES; ++times) {
                                int[] v1 = gen.getZipfian(max);
                                int[] v2 = gen.getZipfian(max);
                                //
                                try {
                                        BitSet b1 = toBitSet(v1);
                                        BitSet b2 = toBitSet(v2);
                                        storageinbits[0] += b1.size()
                                                + b2.size();
                                        bef = System.nanoTime();
                                        b1.and(b2);
                                        aft = System.nanoTime();
                                        bogus += b1.length();
                                        timings[0] += aft - bef;
                                } catch (java.lang.OutOfMemoryError e) {
                                        timings[0] = Integer.MAX_VALUE;
                                        out = true;
                                }
                                //
                                ConciseSet cs1 = toConciseSet(v1);
                                ConciseSet cs2 = toConciseSet(v2);
                                storageinbits[1] += cs1.size()
                                        * cs1.collectionCompressionRatio() * 4
                                        * 8;
                                storageinbits[1] += cs2.size()
                                        * cs2.collectionCompressionRatio() * 4
                                        * 8;
                                bef = System.nanoTime();
                                cs1.intersection(cs2);
                                aft = System.nanoTime();
                                bogus += cs1.size();
                                timings[1] += aft - bef;
                                //
                                ConciseSet wah1 = toWAHConciseSet(v1);
                                ConciseSet wah2 = toWAHConciseSet(v2);
                                storageinbits[2] += wah1.size()
                                        * wah1.collectionCompressionRatio() * 4
                                        * 8;
                                storageinbits[2] += wah2.size()
                                        * wah2.collectionCompressionRatio() * 4
                                        * 8;
                                bef = System.nanoTime();
                                wah1.intersection(wah2);
                                aft = System.nanoTime();
                                bogus += wah1.size();
                                timings[2] += aft - bef;
                                //
                                SpeedyRoaringBitmap rb1 = toSpeedyRoaringBitmap(v1);
                                SpeedyRoaringBitmap rb2 = toSpeedyRoaringBitmap(v2);
                                bef = System.nanoTime();
                                rb1.inPlaceAND(rb2);
                                aft = System.nanoTime();
                                bogus += rb1.getCardinality();
                                timings[3] += aft - bef;
                                storageinbits[3] += rb1.getSizeInBytes() * 8;
                                storageinbits[3] += rb2.getSizeInBytes() * 8;
                        }
                        if (verbose)
                                System.out.print(df.format(max / gen.N)
                                        + "\t\t"
                                        + df.format(timings[0] / TIMES)
                                        + "\t\t"
                                        + df.format(timings[1] / TIMES)
                                        + "\t\t"
                                        + df.format(timings[2] / TIMES)
                                        + "\t\t"
                                        + df.format(timings[3] / TIMES));
                        if (verbose)
                                System.out.println("\t\t\t"
                                        + dfb.format(storageinbits[0]
                                                / (TIMES * gen.N))
                                        + "   "
                                        + dfb.format(storageinbits[1]
                                                / (TIMES * gen.N))
                                        + "   "
                                        + dfb.format(storageinbits[2]
                                                / (TIMES * gen.N))
                                        + "   "
                                        + dfb.format(storageinbits[3]
                                                / (TIMES * gen.N)));

                }
                if (out)
                        System.out
                                .println("# you got OutOfMemoryError, please grant more memory");
                System.out.println("#ignore = " + bogus);
        }

}

