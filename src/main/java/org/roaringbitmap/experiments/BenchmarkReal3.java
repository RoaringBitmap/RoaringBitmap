package org.roaringbitmap.experiments;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.roaringbitmap.RoaringBitmap;

import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.googlecode.javaewah32.EWAHCompressedBitmap32;

import net.sourceforge.sizeof.SizeOf;
import it.uniroma3.mat.extendedset.intset.ConciseSet;

/**
 * O. Kaser's benchmark over real data modified by D. Lemire
 * so that it processes bitmaps 3-by-3
 * 
 */
public class BenchmarkReal3 {
        static final String AND = "AND";
        static final String OR = "OR";
        static final String XOR = "XOR";
        static final String[] ops = { AND, OR, XOR };
        static final String EWAH32 = "EWAH32";
        static final String EWAH64 = "EWAH64";
        static final String CONCISE = "CONCISE";
        static final String WAH = "WAH";
        static final String BITSET = "BITSET";
        static final String ROARING = "ROARING";
        static final String[] formats = { EWAH32, EWAH64, CONCISE, WAH, BITSET,
                ROARING };

        static int junk = 0; // to fight optimizer.

        static long LONG_ENOUGH_NS = 1000L * 1000L * 1000L;

        @SuppressWarnings("javadoc")
        public static void main(final String[] args) {

                Locale.setDefault(Locale.US);

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

                String dataset = args[1];
                int NTRIALS = Integer.parseInt(args[2]);
                System.out.println(NTRIALS + " tests on " + dataset);
                // for future use...

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

                RealDataRetriever dataSrc = new RealDataRetriever(args[0]);
                HashMap<String, Double> totalTimes = new HashMap<String, Double>();
                HashMap<String, Double> totalSizes = new HashMap<String, Double>();
                for (String op : ops)
                        for (String format : formats) {
                                totalTimes.put(op + ";" + format, 0.0);
                                totalSizes.put(format, 0.0); // done more than
                                                             // necessary
                        }

                for (int i = 0; i < NTRIALS; ++i)
                        for (String op : ops)
                                for (String format : formats)
                                        test(op, format, totalTimes,
                                                totalSizes, sizeof,
                                                dataSrc.fetchBitPositions(
                                                        dataset, 3 * i),
                                                dataSrc.fetchBitPositions(
                                                        dataset, 3 * i + 1),
                                                dataSrc.fetchBitPositions(
                                                        dataset, 3 * i + 2));

                if (sizeof) {
                        System.out.println("Size ratios");
                        double baselineSize = totalSizes.get(ROARING);
                        for (String format : formats) {
                                double thisSize = totalSizes.get(format);
                                System.out.printf("%s\t%5.2f\n", format,
                                        thisSize / baselineSize);
                        }
                        System.out.println();
                }

                System.out.println("Time ratios");

                for (String op : ops) {
                        double baseline = totalTimes.get(op + ";" + ROARING);

                        System.out.println("baseline is " + baseline);
                        System.out.println(op);
                        System.out.println();
                        for (String format : formats) {
                                double ttime = totalTimes
                                        .get(op + ";" + format);
                                if (ttime != 0.0)
                                        System.out.printf("%s\t%s\t%5.2f\n",
                                                format, op, ttime / baseline);
                        }
                }
                System.out.println("ignore this " + junk);
        }

        static BitSet toBitSet(int[] dat) {
                BitSet ans = new BitSet();
                for (int i : dat)
                        ans.set(i);
                return ans;
        }

        static ConciseSet toConcise(int[] dat) {
                ConciseSet ans = new ConciseSet();
                for (int i : dat)
                        ans.add(i);
                return ans;
        }

        static ConciseSet toConciseWAH(int[] dat) {
                ConciseSet ans = new ConciseSet(true);
                for (int i : dat)
                        ans.add(i);
                return ans;
        }

        /*
         * What follows is ugly and repetitive, but it has the virtue of being
         * straightforward.
         */

        static void test(String op, String format,
                Map<String, Double> totalTimes, Map<String, Double> totalSizes,
                boolean sizeof, int[] data1, int[] data2, int[] data3) {
                String timeKey = op + ";" + format;
                String spaceKey = format;


                /***************************************************************************/
                if (format.equals(ROARING)) {
                        final RoaringBitmap bm1 = RoaringBitmap.bitmapOf(data1);
                        final RoaringBitmap bm2 = RoaringBitmap.bitmapOf(data2);
                        final RoaringBitmap bm3 = RoaringBitmap.bitmapOf(data3);
                        bm1.trim();
                        bm2.trim();
                        bm3.trim();
                        if (sizeof) {
                                long theseSizesInBits = 8 * (SizeOf
                                        .deepSizeOf(bm1) + SizeOf
                                        .deepSizeOf(bm2)+ SizeOf
                                        .deepSizeOf(bm3));
                                totalSizes.put(spaceKey, theseSizesInBits
                                        + totalSizes.get(spaceKey));
                        }
                        double thisTime = 0.0;
                        if (op.equals(AND)) {
                                thisTime = avgSeconds(new Computation() {
                                        @Override
                                        public void compute() {
                                                RoaringBitmap result = RoaringBitmap
                                                        .and(bm1, bm2);
                                                result.and(bm3);
                                                BenchmarkReal3.junk += result
                                                        .getCardinality(); // cheap
                                        }
                                });
                                totalTimes.put(timeKey,
                                        thisTime + totalTimes.get(timeKey));
                        } else if (op.equals(OR)) {
                                thisTime = avgSeconds(new Computation() {
                                        @Override
                                        public void compute() {
                                                RoaringBitmap result = RoaringBitmap
                                                        .or(bm1, bm2);
                                                result.or(bm3);
                                                BenchmarkReal3.junk += result
                                                        .getCardinality(); // cheap
                                        }
                                });
                                totalTimes.put(timeKey,
                                        thisTime + totalTimes.get(timeKey));
                        } else if (op.equals(XOR)) {
                                thisTime = avgSeconds(new Computation() {
                                        @Override
                                        public void compute() {
                                                RoaringBitmap result = RoaringBitmap
                                                        .xor(bm1, bm2);
                                                result.xor(bm3);
                                                BenchmarkReal3.junk += result
                                                        .getCardinality(); // cheap
                                        }
                                });
                                totalTimes.put(timeKey,
                                        thisTime + totalTimes.get(timeKey));
                        } else
                                throw new RuntimeException("unknown op " + op);
                }
                /***************************************************************************/
                else if (format.equals(BITSET)) {
                        final BitSet bm1 = toBitSet(data1);
                        final BitSet bm2 = toBitSet(data2);
                        final BitSet bm3 = toBitSet(data3);
                        if (sizeof) {
                                long theseSizesInBits = 8 * (SizeOf
                                        .deepSizeOf(bm1) + SizeOf
                                        .deepSizeOf(bm2) + SizeOf
                                        .deepSizeOf(bm3));
                                totalSizes.put(spaceKey, theseSizesInBits
                                        + totalSizes.get(spaceKey));
                        }
                        double thisTime = 0.0;
                        if (op.equals(AND)) {
                                thisTime = avgSeconds(new Computation() {
                                        @Override
                                        public void compute() {
                                                BitSet result;
                                                result = (BitSet) bm1.clone();
                                                result.and(bm2);
                                                result.and(bm3);
                                                BenchmarkReal3.junk += result
                                                        .size(); // cheap
                                        }
                                });
                                totalTimes.put(timeKey,
                                        thisTime + totalTimes.get(timeKey));
                        } else if (op.equals(OR)) {
                                thisTime = avgSeconds(new Computation() {
                                        @Override
                                        public void compute() {
                                                BitSet result;
                                                result = (BitSet) bm1.clone();
                                                result.or(bm2);
                                                result.or(bm3);
                                                BenchmarkReal3.junk += result
                                                        .size(); // cheap
                                        }
                                });
                                totalTimes.put(timeKey,
                                        thisTime + totalTimes.get(timeKey));
                        } else if (op.equals(XOR)) {
                                thisTime = avgSeconds(new Computation() {
                                        @Override
                                        public void compute() {
                                                BitSet result;
                                                result = (BitSet) bm1.clone();
                                                result.xor(bm2);
                                                result.xor(bm3);
                                                BenchmarkReal3.junk += result
                                                        .size(); // cheap
                                        }
                                });
                                totalTimes.put(timeKey,
                                        thisTime + totalTimes.get(timeKey));
                        } else
                                throw new RuntimeException("unknown op " + op);
                }
                /***************************************************************************/
                else if (format.equals(WAH)) {
                        final ConciseSet bm1 = toConciseWAH(data1);
                        final ConciseSet bm2 = toConciseWAH(data2);
                        final ConciseSet bm3 = toConciseWAH(data3);
                        if (sizeof) {
                                long theseSizesInBits = 8 * (SizeOf
                                        .deepSizeOf(bm1) + SizeOf
                                        .deepSizeOf(bm2)+ SizeOf
                                        .deepSizeOf(bm3));
                                totalSizes.put(spaceKey, theseSizesInBits
                                        + totalSizes.get(spaceKey));
                        }
                        double thisTime = 0.0;
                        if (op.equals(AND)) {
                                thisTime = avgSeconds(new Computation() {
                                        @Override
                                        public void compute() {
                                                ConciseSet result = bm1.intersection(bm2);
                                                result = result.intersection(bm3);
                                                BenchmarkReal3.junk += result
                                                        .isEmpty() ? 1 : 0; // cheap???
                                        }
                                });
                                totalTimes.put(timeKey,
                                        thisTime + totalTimes.get(timeKey));
                        }
                        if (op.equals(OR)) {
                                thisTime = avgSeconds(new Computation() {
                                        @Override
                                        public void compute() {
                                                ConciseSet result = bm1.union(bm2);
                                                result = result.union(bm3);

                                                BenchmarkReal3.junk += result
                                                        .isEmpty() ? 1 : 0; // dunno
                                                                            // if
                                                                            // cheap
                                                                            // enough
                                        }
                                });
                                totalTimes.put(timeKey,
                                        thisTime + totalTimes.get(timeKey));
                        }
                        if (op.equals(XOR)) {
                                thisTime = avgSeconds(new Computation() {
                                        @Override
                                        public void compute() {
                                                ConciseSet result = bm1.symmetricDifference(bm2);
                                                result = result.symmetricDifference(bm3);

                                                BenchmarkReal3.junk += result
                                                        .isEmpty() ? 1 : 0; // cheap???
                                        }
                                });
                                totalTimes.put(timeKey,
                                        thisTime + totalTimes.get(timeKey));
                        }
                }
                /***************************************************************************/
                else if (format.equals(EWAH32)) {
                        final EWAHCompressedBitmap32 bm1 = EWAHCompressedBitmap32
                                .bitmapOf(data1);
                        final EWAHCompressedBitmap32 bm2 = EWAHCompressedBitmap32
                                .bitmapOf(data2);
                        final EWAHCompressedBitmap32 bm3 = EWAHCompressedBitmap32
                                .bitmapOf(data3);
                        bm1.trim();
                        bm2.trim();
                        bm3.trim();
                        if (sizeof) {
                                long theseSizesInBits = 8 * (SizeOf
                                        .deepSizeOf(bm1) + SizeOf
                                        .deepSizeOf(bm2)+ SizeOf
                                        .deepSizeOf(bm3));
                                totalSizes.put(spaceKey, theseSizesInBits
                                        + totalSizes.get(spaceKey));
                        }
                        double thisTime = 0.0;
                        if (op.equals(AND)) {
                                thisTime = avgSeconds(new Computation() {
                                        @Override
                                        public void compute() {
                                                EWAHCompressedBitmap32 result = EWAHCompressedBitmap32
                                                        .and(bm1, bm2,bm3);
                                                BenchmarkReal3.junk += result
                                                        .sizeInBits(); // cheap
                                        }
                                });
                                totalTimes.put(timeKey,
                                        thisTime + totalTimes.get(timeKey));
                        } else if (op.equals(OR)) {
                                thisTime = avgSeconds(new Computation() {
                                        @Override
                                        public void compute() {
                                                EWAHCompressedBitmap32 result = EWAHCompressedBitmap32
                                                        .or(bm1, bm2,bm3);
                                                BenchmarkReal3.junk += result
                                                        .sizeInBits(); // cheap
                                        }
                                });
                                totalTimes.put(timeKey,
                                        thisTime + totalTimes.get(timeKey));
                        } else if (op.equals(XOR)) {
                                thisTime = avgSeconds(new Computation() {
                                        @Override
                                        public void compute() {
                                                EWAHCompressedBitmap32 result = EWAHCompressedBitmap32
                                                        .xor(bm1, bm2,bm3);
                                                BenchmarkReal3.junk += result
                                                        .sizeInBits(); // cheap
                                        }
                                });
                                totalTimes.put(timeKey,
                                        thisTime + totalTimes.get(timeKey));
                        } else
                                throw new RuntimeException("unknown op " + op);
                }
                /***************************************************************************/
                else if (format.equals(EWAH64)) {
                        final EWAHCompressedBitmap bm1 = EWAHCompressedBitmap
                                .bitmapOf(data1);
                        final EWAHCompressedBitmap bm2 = EWAHCompressedBitmap
                                .bitmapOf(data2);
                        final EWAHCompressedBitmap bm3 = EWAHCompressedBitmap
                                .bitmapOf(data3);
                        bm1.trim();
                        bm2.trim();
                        bm3.trim();
                        if (sizeof) {
                                long theseSizesInBits = 8 * (SizeOf
                                        .deepSizeOf(bm1) + SizeOf
                                        .deepSizeOf(bm2) + SizeOf
                                        .deepSizeOf(bm3));
                                totalSizes.put(spaceKey, theseSizesInBits
                                        + totalSizes.get(spaceKey));
                        }
                        double thisTime = 0.0;
                        if (op.equals(AND)) {
                                thisTime = avgSeconds(new Computation() {
                                        @Override
                                        public void compute() {
                                                EWAHCompressedBitmap result = EWAHCompressedBitmap
                                                        .and(bm1, bm2, bm3);
                                                BenchmarkReal3.junk += result
                                                        .sizeInBits(); // cheap
                                        }
                                });
                                totalTimes.put(timeKey,
                                        thisTime + totalTimes.get(timeKey));
                        } else if (op.equals(OR)) {
                                thisTime = avgSeconds(new Computation() {
                                        @Override
                                        public void compute() {
                                                EWAHCompressedBitmap result = EWAHCompressedBitmap
                                                        .or(bm1, bm2, bm3);
                                                BenchmarkReal3.junk += result
                                                        .sizeInBits(); // cheap
                                        }
                                });
                                totalTimes.put(timeKey,
                                        thisTime + totalTimes.get(timeKey));
                        } else if (op.equals(XOR)) {
                                thisTime = avgSeconds(new Computation() {
                                        @Override
                                        public void compute() {
                                                EWAHCompressedBitmap result = EWAHCompressedBitmap
                                                        .xor(bm1, bm2, bm3);
                                                BenchmarkReal3.junk += result
                                                        .sizeInBits(); // cheap
                                        }
                                });
                                totalTimes.put(timeKey,
                                        thisTime + totalTimes.get(timeKey));
                        } else
                                throw new RuntimeException("unknown op " + op);
                }

                /***************************************************************************/
                else if (format.equals(CONCISE)) {
                        final ConciseSet bm1 = toConcise(data1);
                        final ConciseSet bm2 = toConcise(data2);
                        final ConciseSet bm3 = toConcise(data3);
                        if (sizeof) {
                                long theseSizesInBits = 8 * (SizeOf
                                        .deepSizeOf(bm1) + SizeOf
                                        .deepSizeOf(bm2) + SizeOf
                                        .deepSizeOf(bm3));
                                totalSizes.put(spaceKey, theseSizesInBits
                                        + totalSizes.get(spaceKey));
                        }
                        double thisTime = 0.0;
                        if (op.equals(AND)) {
                                thisTime = avgSeconds(new Computation() {
                                        @Override
                                        public void compute() {
                                                ConciseSet result = bm1.intersection(bm2);
                                                result = result.intersection(bm3);
                                                BenchmarkReal3.junk += result
                                                        .isEmpty() ? 1 : 0; // cheap???
                                        }
                                });
                                totalTimes.put(timeKey,
                                        thisTime + totalTimes.get(timeKey));
                        }
                        if (op.equals(OR)) {
                                thisTime = avgSeconds(new Computation() {
                                        @Override
                                        public void compute() {
                                                ConciseSet result = bm1.union(bm2);
                                                result = result.union(bm3);

                                                BenchmarkReal3.junk += result
                                                        .isEmpty() ? 1 : 0; // dunno
                                                                            // if
                                                                            // cheap
                                                                            // enough
                                        }
                                });
                                totalTimes.put(timeKey,
                                        thisTime + totalTimes.get(timeKey));
                        }
                        if (op.equals(XOR)) {
                                thisTime = avgSeconds(new Computation() {
                                        @Override
                                        public void compute() {
                                                ConciseSet result = bm1.symmetricDifference(bm2);
                                                result = result.symmetricDifference(bm3);

                                                BenchmarkReal3.junk += result
                                                        .isEmpty() ? 1 : 0; // cheap???
                                        }
                                });
                                totalTimes.put(timeKey,
                                        thisTime + totalTimes.get(timeKey));
                        }
                }
        }

        static double avgSeconds(Computation toDo) {
                int ntrials = 1;
                long elapsedNS = 0L;
                long start, stop;
                do {
                        ntrials *= 2;
                        start = System.nanoTime();
                        for (int i = 0; i < ntrials; ++i) {
                                // might have to do something to stop hoisting
                                // here
                                toDo.compute();
                        }
                        stop = System.nanoTime();
                        elapsedNS = stop - start;
                } while (elapsedNS < LONG_ENOUGH_NS);
                /* now things are very hot, so do an actual timing */
                start = System.nanoTime();
                for (int i = 0; i < ntrials; ++i) {
                        // danger, optimizer??
                        toDo.compute();
                }
                stop = System.nanoTime();
                return (stop - start) / (ntrials * 1e+9); // ns to s
        }

        abstract static class Computation {
                public abstract void compute(); // must mess with "junk"
        }

}