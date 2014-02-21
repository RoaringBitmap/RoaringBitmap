package org.roaringbitmap.experiments.colantonio;

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Locale;
import java.util.Random;

import org.roaringbitmap.RoaringBitmap;

import net.sourceforge.sizeof.SizeOf;
import it.uniroma3.mat.extendedset.intset.ConciseSet;

/**
 * 
 * This a reproduction of the benchmark used by Colantonio and Di Pietro,
 * Concise: Compressed 'n' Composable Integer Set
 * 
 * While they report "Compression" as the ratio between the number of 32-bit
 * words required to represent the compressed bitmap and the cardinality of the
 * integer set, we report the number of bits per integer.
 * 
 * Like them, we use "Density" mean the ratio between the cardinality of the set
 * and the number range.
 * 
 * Like them, we "Max/Cardinality" to mean the ratio between the maximal value
 * (i.e., the number range) and the cardinality of the set that is, the inverse
 * of the density.
 * 
 * 
 * Time measurement are expressed in nanoseconds. Each experiment is performed
 * 100 times, and the average reported.
 * 
 * @author Daniel Lemire
 * 
 */
public class AsymmetricBenchmark {

        /**
         * @param a
         *                an array of integers
         * @return a bitset representing the provided integers
         */
        public static BitSet toBitSet(final int[] a) {
                BitSet bs = new BitSet();
                for (int x : a)
                        bs.set(x);
                return bs;
        }

        /**
         * @param a
         *                an array of integers
         * @return a ConciseSet representing the provided integers
         */
        public static ConciseSet toConciseSet(final int[] a) {
                ConciseSet cs = new ConciseSet();
                for (int x : a)
                        cs.add(x);
                return cs;
        }

        /**
         * @param a
         *                an array of integers
         * @return a SpeedyRoaringBitmap representing the provided integers
         */
        public static RoaringBitmap toSpeedyRoaringBitmap(int[] a) {
                RoaringBitmap rr = new RoaringBitmap();
                for (int x : a)
                        rr.add(x);
                return rr;
        }

        /**
         * @param a
         *                an array of integers
         * @return a ConciseSet (in WAH mode) representing the provided integers
         */
        public static ConciseSet toWAHConciseSet(int[] a) {
                ConciseSet cs = new ConciseSet(true);
                for (int x : a)
                        cs.add(x);
                return cs;
        }

        /**
         * @param args
         *                command line arguments
         */
        public static void main(final String[] args) {
                Locale.setDefault(Locale.US);
                System.out
                        .println("# This benchmark is a variation on  what Colantonio and Di Pietro,");
                System.out
                        .println("#  did in Concise: Compressed 'n' Composable Integer Set");
                System.out.println("#  Except that (1) we aggregate bitmaps of different density");
                System.out.println("#              (2) check bits instead of removing them");
                System.out.println("########");
                System.out.println("# " + System.getProperty("java.vendor")
                        + " " + System.getProperty("java.version") + " "
                        + System.getProperty("java.vm.name"));
                System.out.println("# " + System.getProperty("os.name") + " "
                        + System.getProperty("os.arch") + " "
                        + System.getProperty("os.version"));
                System.out.println("# processors: "
                        + Runtime.getRuntime().availableProcessors());
                System.out.println("# max mem.: "
                        + Runtime.getRuntime().maxMemory());
                System.out.println("########");
                int N = 100000;
                boolean sizeof = true;
                try {
                        SizeOf.setMinSizeToLog(0);
                        SizeOf.skipStaticField(true);
                        // SizeOf.skipFinalField(true);
                        SizeOf.deepSizeOf(args);
                } catch (IllegalStateException e) {
                        sizeof = false;
                        System.out
                                .println("# disabling sizeOf, run  -javaagent:lib/SizeOf.jar or equiv. to enable");
                }

                DataGenerator gen = new DataGenerator(N);
                int TIMES = 100;
                gen.setUniform();
                test(gen, false, TIMES, sizeof);
                test(gen, true, TIMES, sizeof);
                System.out.println();
                gen.setZipfian();
                test(gen, false, TIMES, sizeof);
                test(gen, true, TIMES, sizeof);
                System.out.println();
        }


        /**
         * @param gen
         *                data generator
         * @param verbose
         *                whether to print out the result
         * @param TIMES
         *                how many times should we run each test
         * @param sizeof
         *                whether to use the sizeOf library.
         */
        public static void test(final DataGenerator gen, final boolean verbose,
                final int TIMES, boolean sizeof) {
                if (!verbose)
                        System.out
                                .println("# running a dry run (can take a long time)");
                int bogus = 0;
                long bef, aft;
                DecimalFormat df = new DecimalFormat("0.000E0");
                DecimalFormat dfb = new DecimalFormat("000.0");
                if (verbose)
                        if (gen.is_zipfian())
                                System.out.println("### zipfian test");
                        else
                                System.out.println("### uniform test");
                if (verbose)
                        System.out
                                .println("# first columns are timings [intersection times in ns], then insert in ns, "
                                        + "then check times in ns, then bits/int");
                if (verbose && sizeof)
                        System.out
                                .println("# For size (last columns), first column is estimated, second is sizeof");
                if (verbose)
                        System.out
                                .print("# density\tbitset\t\tconcise\t\twah\t\troar"
                                        + "\t\t\tbitset\t\tconcise\t\twah\t\tspeedyroaring"
                                        + "\t\t\tbitset\t\tconcise\t\twah\t\tspeedyroaring");
                if (verbose)
                        if (sizeof)
                                System.out
                                        .println("\t\tbitset\tbitset\tconcise\tconcise\twah\twah\troar\troar");
                        else
                                System.out
                                        .println("\t\tbitset\t\tconcise\t\twah\t\troar");
                for (double d = 0.0009765625; d < 1.000; d *= 2) {
                        double[] timings = new double[4];
                        double[] storageinbits = new double[4];
                        double[] truestorageinbits = new double[4];
                        double[] appendTimes = new double[4];
                        double[] checkTimes = new double[4];
                        Random r = new Random(0);

                        for (int times = 0; times < TIMES; ++times) {
                                int[] v1 = gen.getRandomArray(d);
                                int[] v2 = gen.getRandomArray((1 - d)
                                        * r.nextDouble() + d); // v2 is randomly denser
                                // BitSet
                                // Append times
                                bef = System.nanoTime();
                                BitSet borig1 = toBitSet(v1); // we will clone
                                                              // it
                                aft = System.nanoTime();
                                bogus += borig1.length();
                                appendTimes[0] += aft - bef;
                                BitSet b2 = toBitSet(v2);
                                // Storage
                                storageinbits[0] += borig1.size() + b2.size();
                                if (sizeof)
                                        truestorageinbits[0] += SizeOf
                                                .deepSizeOf(borig1)
                                                * 8
                                                + SizeOf.deepSizeOf(b2) * 2;
                                // And times.
                                bef = System.nanoTime();
                                BitSet b1 = (BitSet) borig1.clone(); // for fair
                                                                     // comparison
                                                                     // (not
                                                                     // inplace)
                                b1.and(b2);
                                aft = System.nanoTime();
                                timings[0] += aft - bef;
                                bogus += b1.length();
                                // Random access times
                                int toCheck = v1[gen.rand.nextInt(gen.N)];
                                bef = System.nanoTime();
                                b2.get(toCheck);
                                aft = System.nanoTime();
                                checkTimes[0] += aft - bef;
                                bogus += borig1.size();
                                int[] b2withcheck = toArray(b2);
                                borig1 = null;
                                b2 = null;
                                int[] trueintersection = toArray(b1);
                                b1 = null;
                                // Concise
                                // Append times
                                bef = System.nanoTime();
                                ConciseSet cs1 = toConciseSet(v1);
                                aft = System.nanoTime();
                                bogus += cs1.size();
                                appendTimes[1] += aft - bef;
                                ConciseSet cs2 = toConciseSet(v2);
                                storageinbits[1] += cs1.size()
                                        * cs1.collectionCompressionRatio() * 4
                                        * 8;
                                storageinbits[1] += cs2.size()
                                        * cs2.collectionCompressionRatio() * 4
                                        * 8;
                                if (sizeof)
                                        truestorageinbits[1] += SizeOf
                                                .deepSizeOf(cs1)
                                                * 8
                                                + SizeOf.deepSizeOf(cs2) * 2;
                                bef = System.nanoTime();
                                cs1 = cs1.intersection(cs2);
                                aft = System.nanoTime();
                                // we verify the answer
                                if(cs1.toArray() == null) {
                                        if(trueintersection.length != 0)
                                                throw new RuntimeException("bug");
                                } else if (!Arrays.equals(cs1.toArray(),
                                        trueintersection))
                                        throw new RuntimeException("bug");
                                bogus += cs1.size();
                                timings[1] += aft - bef;
                                // check times
                                bef = System.nanoTime();
                                cs2.contains(toCheck);
                                aft = System.nanoTime();
                                if (!Arrays
                                        .equals(cs2.toArray(), b2withcheck))
                                        throw new RuntimeException("bug");
                                checkTimes[1] += aft - bef;
                                bogus += cs1.size();
                                cs1 = null;
                                cs2 = null;
                                // WAHConcise
                                // Append times
                                bef = System.nanoTime();
                                ConciseSet wah1 = toWAHConciseSet(v1);
                                aft = System.nanoTime();
                                bogus += wah1.size();
                                appendTimes[2] += aft - bef;
                                ConciseSet wah2 = toWAHConciseSet(v2);
                                // Storage
                                storageinbits[2] += wah1.size()
                                        * wah1.collectionCompressionRatio() * 4
                                        * 8;
                                storageinbits[2] += wah2.size()
                                        * wah2.collectionCompressionRatio() * 4
                                        * 8;
                                if (sizeof)
                                        truestorageinbits[2] += SizeOf
                                                .deepSizeOf(wah1)
                                                * 8
                                                + SizeOf.deepSizeOf(wah2) * 2;
                                // Intersect times
                                bef = System.nanoTime();
                                wah1 = wah1.intersection(wah2);
                                aft = System.nanoTime();
                                // we verify the answer
                                if(wah1.toArray() == null) {
                                        if(trueintersection.length != 0)
                                                throw new RuntimeException("bug");
                                } else if (!Arrays.equals(wah1.toArray(),
                                        trueintersection))
                                        throw new RuntimeException("bug");
                                bogus += wah1.size();
                                timings[2] += aft - bef;
                                // check times
                                bef = System.nanoTime();
                                wah2.contains(toCheck);
                                aft = System.nanoTime();
                                if (!Arrays.equals(wah2.toArray(),
                                        b2withcheck))
                                        throw new RuntimeException("bug");
                                checkTimes[2] += aft - bef;
                                bogus += wah1.size();
                                wah1 = null;
                                wah2 = null;
                                // SpeedyRoaringBitmap
                                // Append times
                                bef = System.nanoTime();
                                RoaringBitmap rb1 = toSpeedyRoaringBitmap(v1);
                                aft = System.nanoTime();
                                bogus += rb1.getCardinality();
                                appendTimes[3] += aft - bef;
                                RoaringBitmap rb2 = toSpeedyRoaringBitmap(v2);
                                // Storage
                                storageinbits[3] += rb1.getSizeInBytes() * 8;
                                storageinbits[3] += rb2.getSizeInBytes() * 8;
                                if (sizeof)
                                        truestorageinbits[3] += SizeOf
                                                .deepSizeOf(rb1)
                                                * 8
                                                + SizeOf.deepSizeOf(rb2) * 2;
                                // Intersect times
                                bef = System.nanoTime();
                                rb1 = RoaringBitmap.and(rb1, rb2);
                                aft = System.nanoTime();
                                // we verify the answer
                                if (!Arrays.equals(rb1.toArray(),
                                        trueintersection))
                                        throw new RuntimeException("bug");
                                bogus += rb1.getCardinality();
                                timings[3] += aft - bef;
                                // check times
                                bef = System.nanoTime();
                                rb2.contains(toCheck);
                                aft = System.nanoTime();
                                if (!Arrays
                                        .equals(rb2.toArray(), b2withcheck))
                                        throw new RuntimeException("bug");
                                checkTimes[3] += aft - bef;
                                bogus += rb1.getCardinality();
                                rb1 = null;
                                rb2 = null;
                        }
                        if (verbose) {
                                System.out.print(df.format(d) + "\t"
                                        + df.format(timings[0] / TIMES)
                                        + "\t\t"
                                        + df.format(timings[1] / TIMES)
                                        + "\t\t"
                                        + df.format(timings[2] / TIMES)
                                        + "\t\t"
                                        + df.format(timings[3] / TIMES));
                                System.out.print("\t\t\t"
                                        + df.format(appendTimes[0]
                                                / (TIMES * gen.N))
                                        + "\t\t"
                                        + df.format(appendTimes[1]
                                                / (TIMES * gen.N))
                                        + "\t\t"
                                        + df.format(appendTimes[2]
                                                / (TIMES * gen.N))
                                        + "\t\t"
                                        + df.format(appendTimes[3]
                                                / (TIMES * gen.N)));
                                System.out.print("\t\t\t\t"
                                        + df.format(checkTimes[0] / TIMES)
                                        + "\t\t"
                                        + df.format(checkTimes[1] / TIMES)
                                        + "\t\t"
                                        + df.format(checkTimes[2] / TIMES)
                                        + "\t\t"
                                        + df.format(checkTimes[3] / TIMES));
                        }
                        if (verbose)
                                if (sizeof)
                                        System.out
                                                .println("\t\t\t\t"
                                                        + dfb.format(storageinbits[0]
                                                                / (2 * TIMES * gen.N))
                                                        + "\t"
                                                        + dfb.format(truestorageinbits[0]
                                                                / (2 * TIMES * gen.N))
                                                        + "\t"
                                                        + dfb.format(storageinbits[1]
                                                                / (2 * TIMES * gen.N))
                                                        + "\t"
                                                        + dfb.format(truestorageinbits[1]
                                                                / (2 * TIMES * gen.N))
                                                        + "\t"
                                                        + dfb.format(storageinbits[2]
                                                                / (2 * TIMES * gen.N))
                                                        + "\t"
                                                        + dfb.format(truestorageinbits[2]
                                                                / (2 * TIMES * gen.N))
                                                        + "\t"
                                                        + dfb.format(storageinbits[3]
                                                                / (2 * TIMES * gen.N))
                                                        + "\t"
                                                        + dfb.format(truestorageinbits[3]
                                                                / (2 * TIMES * gen.N)));
                                else
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
