package me.lemire.roaringbitmap.experiments.colantonio;

import java.text.DecimalFormat;
import java.util.Arrays;
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
 * of the set that is, the inverse of the density.
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
                int N = 100000;
                DataGenerator gen = new DataGenerator(N);
                int TIMES = 100;
                gen.setUniform();
                test(gen,true, TIMES);
                gen.setZipfian();
                test(gen,true, TIMES);
                System.out.println();
       }

        /**
         * @param gen data generator
         * @param verbose whether to print out the result
         * @param TIMES how many times should we run each test
         */
        public static void test(final DataGenerator gen, final boolean verbose, final int TIMES) {
                if (!verbose)
                        System.out
                                .println("# running a dry run (can take a long time)");
                int bogus = 0;
                long bef, aft;
                DecimalFormat df = new DecimalFormat("0.000E0");
                DecimalFormat dfb = new DecimalFormat("000.0");
                if (verbose)
                        if(gen.is_zipfian())
                                System.out
                                .println("### zipfian test");
                         else
                              System.out
                                .println("### uniform test");
                if (verbose)
                        System.out
                                .println("# first columns are timings [intersection times in ns], then bits/int");
                if (verbose)
                        System.out
                                .println("# density\tbitset\t\tconcise\t\twah\t\tspeedyroaring" +
                                		"\t\tbitset\t\tconcise\t\twah\t\tspeedyroaring");
                for (double d = 0.001; d <= 0.999; d *= 1.2) {
                        double[] timings = new double[4];
                        double[] storageinbits = new double[4];

                        for (int times = 0; times < TIMES; ++times) {
                        	int[] v1, v2;
                        	if(!gen.is_zipfian()) { //Uniform tests
                                v1 = gen.getUniform(d);
                                v2 = gen.getUniform(d);
                        	}
                        	else { //Zipfian tests
                        		v1=gen.getZipfian(d);
                        		v2=gen.getZipfian(d);
                        	}
                                //
                                BitSet borig1 = toBitSet(v1);
                                BitSet b2 = toBitSet(v2);
                                storageinbits[0] += borig1.size() + b2.size();
                                bef = System.nanoTime();
                                BitSet b1 = (BitSet) borig1.clone(); // for fair comparison (not inplace)
                                b1.and(b2);
                                aft = System.nanoTime();
                                bogus += b1.length();
                                b2 = null;
                                timings[0] += aft - bef;
                                //
                                ConciseSet cs1 = toConciseSet(v1);
                                ConciseSet cs2 = toConciseSet(v2);
                                bef = System.nanoTime();
                                cs1 = cs1.intersection(cs2);
                                aft = System.nanoTime();
                                // we verify the answer
                                if(!Arrays.equals(cs1.toArray(), toArray(b1)))
                                        throw new RuntimeException("bug");
                                bogus += cs1.size();
                                timings[1] += aft - bef;
                                storageinbits[1] += cs1.size()
                                        * cs1.collectionCompressionRatio() * 4
                                        * 8;
                                storageinbits[1] += cs2.size()
                                        * cs2.collectionCompressionRatio() * 4
                                        * 8;
                                cs1 = null;
                                cs2 = null;
                                //
                                ConciseSet wah1 = toWAHConciseSet(v1);
                                ConciseSet wah2 = toWAHConciseSet(v2);
                                bef = System.nanoTime();
                                wah1 = wah1.intersection(wah2);
                                aft = System.nanoTime();
                                // we verify the answer
                                if(!Arrays.equals(wah1.toArray(), toArray(b1)))
                                        throw new RuntimeException("bug");
                                bogus += wah1.size();
                                timings[2] += aft - bef;
                                storageinbits[2] += wah1.size()
                                        * wah1.collectionCompressionRatio() * 4
                                        * 8;
                                storageinbits[2] += wah2.size()
                                        * wah2.collectionCompressionRatio() * 4
                                        * 8;
                                wah1 = null;
                                wah2 = null;
                                //
                                SpeedyRoaringBitmap rb1 = toSpeedyRoaringBitmap(v1);
                                SpeedyRoaringBitmap rb2 = toSpeedyRoaringBitmap(v2);
                                bef = System.nanoTime();
                                rb1 = SpeedyRoaringBitmap.and(rb1,rb2);
                                aft = System.nanoTime();
                                // we verify the answer
                                if(!Arrays.equals(rb1.getIntegers(), toArray(b1)))
                                        throw new RuntimeException("bug");
                                bogus += rb1.getCardinality();
                                timings[3] += aft - bef;
                                storageinbits[3] += rb1.getSizeInBytes() * 8;
                                storageinbits[3] += rb2.getSizeInBytes() * 8;
                                rb1 = null;
                                rb2 = null;

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
                                        + "\t\t"
                                        + dfb.format(storageinbits[1]
                                                / (2 * TIMES * gen.N))
                                        + "\t\t"
                                        + dfb.format(storageinbits[2]
                                                / (2 * TIMES * gen.N))
                                        + "\t\t"
                                        + dfb.format(storageinbits[3]
                                                / (2 * TIMES * gen.N)));

                }
                System.out.println("#ignore = " + bogus);
        }
        
        private static int[] toArray(final BitSet bs) {
                int[] a = new int[bs.cardinality()];
                int pos = 0;
                for (int x = bs.nextSetBit(0); x >= 0; x = bs.nextSetBit(x + 1))
                        a[pos++] = x;
                return a;
        }

}

