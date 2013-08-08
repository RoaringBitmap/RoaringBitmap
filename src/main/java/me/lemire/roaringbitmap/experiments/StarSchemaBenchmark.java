package me.lemire.roaringbitmap.experiments;

import it.uniroma3.mat.extendedset.intset.ConciseSet;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.TreeMap;

import javax.swing.JFileChooser;

import me.lemire.roaringbitmap.RoaringBitmap;

import org.devbrat.util.WAHBitSet;

import au.com.bytecode.opencsv.CSVReader;

import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.googlecode.javaewah32.EWAHCompressedBitmap32;

public class StarSchemaBenchmark {

        static ArrayList<TreeMap<String, IntArray>> TreeBitmapIdx;
        static int nbBitmaps = 0;

        /**
         * @param args
         * @throws IOException
         */
        public static void main(String[] args) throws IOException {
                System.out.println("Starting building :");
                if (args.length > 0) {
                        StarSchemaBenchmark.BuildingSSBbitmaps(args[0]);
                } else {
                        StarSchemaBenchmark.BuildingSSBbitmaps(null);
                }

                System.out.println("Starting experiments :");
                DecimalFormat df = new DecimalFormat("0.###");
                int repeat = 3;
                StarSchemaBenchmark.testRoaringBitmap(repeat, df);
                StarSchemaBenchmark.testBitSet(repeat, df);
                StarSchemaBenchmark.testConciseSet(repeat, df);
                StarSchemaBenchmark.testWAH32(repeat, df);
                StarSchemaBenchmark.testEWAH64(repeat, df);
                StarSchemaBenchmark.testEWAH32(repeat, df);
        }

        public static void BuildingSSBbitmaps(String path) throws IOException {

                if (path == null) {
                        do {
                                JFileChooser file = new JFileChooser();
                                int val = file.showOpenDialog(null);
                                if (val == JFileChooser.APPROVE_OPTION) {
                                        path = file.getSelectedFile()
                                                .getAbsolutePath();
                                        break;
                                }
                        } while (true);
                }
                int row = 0;
                nbBitmaps = 0;
                TreeBitmapIdx = new ArrayList<TreeMap<String, IntArray>>();
                Reader source_file = new FileReader(
                        path);
                try {
                        CSVReader reader = new CSVReader(source_file);
                        
                        String[] entries;
                        if ((entries = reader.readNext())!= null) {
                                for (String e : entries) {
                                        TreeMap<String, IntArray> x = new TreeMap<String, IntArray>();
                                        IntArray ia = new IntArray(1);
                                        nbBitmaps++;
                                        ia.add(row);
                                        x.put(e, ia);
                                        TreeBitmapIdx.add(x);
                                }
                                ++row;
                        }
                        System.out.println("Detected "+TreeBitmapIdx.size()+" columns.");
                        

                        while ((entries = reader.readNext())!= null) {
                                for (int k = 0; k < entries.length; ++k) {
                                        TreeMap<String, IntArray> x = TreeBitmapIdx
                                                .get(k);
                                        if(x == null) continue; // this happens if the column has been disabled
                                        if (x.containsKey(entries[k]))
                                                x.get(entries[k]).add(row);
                                        else {
                                                IntArray ia = new IntArray();
                                                ia.add(row);
                                                nbBitmaps++;
                                                x.put(entries[k], ia);
                                        }
                                }
                                ++row;
                                if (row % 1000000 == 0) {
                                        for(int c = 0; c < TreeBitmapIdx.size();++c) {
                                                TreeMap<String, IntArray> x = TreeBitmapIdx
                                                        .get(c);
                                                if(x == null) continue;
                                                if(x.size() * 10 >= row) {
                                                        System.out.println("Disabling high cardinality column "+c+" got "+x.size()+ " distinct values over "+row+" rows");
                                                        nbBitmaps -= x.size();
                                                        TreeBitmapIdx.set(c,null);
                                                }
                                        }
                                        System.out.println("read " + row
                                                + " rows, got "+nbBitmaps+" distinct values");
                                }
                        }

                } finally {
                        source_file.close();
                }
                System.out.println("processed "+row+" rows and "+nbBitmaps+" distinct values");
                for (int D = 0; D < TreeBitmapIdx.size(); ++D) {
                        TreeMap<String, IntArray> x = TreeBitmapIdx.get(D);
                        if(x==null) {
                                System.out.println("Column "+D+" has been disabled (high cardinality) ");
                                continue;
                        }
                        System.out.println("Column "+D+" has "+x.size()+" distinct values");
                        for (IntArray ia : x.values()) {
                                ia.trim();// recovering memory
                        }
                }
        }

        public static void testRoaringBitmap(int repeat, DecimalFormat df) {
                System.out.println("# RoaringBitmap on Star Schema Benchmark");
                System.out
                        .println("# size, construction time, time to recover set bits, "
                                + "time to compute unions (OR), intersections (AND) "
                                + "and exclusive unions (XOR) ");

                long bef, aft;
                String line = "";
                /**
                 * Bogus is essential otherwise the compiler could optimize away
                 * the computation.
                 */
                int bogus = 0;
                int N = nbBitmaps, k, r;
                int size = 0;
                // Calculating the construction time
                bef = System.currentTimeMillis();
                RoaringBitmap[] bitmap = new RoaringBitmap[N];
                {
                        int pos = 0;
                        for (int D = 0; D < TreeBitmapIdx.size(); ++D) {
                                if(TreeBitmapIdx.get(D) == null) continue; // disabled
                                for (IntArray ia : TreeBitmapIdx.get(D)
                                        .values()) {
                                        bitmap[pos] = new RoaringBitmap();
                                        for (int v = 0; v < ia.size(); ++v) {
                                                bitmap[pos].set(v);
                                        }
                                        ++pos;
                                }
                        }
                }
                aft = System.currentTimeMillis();

                // Validating that ArrayContainers contents are sorted
                // and BitmapContainers cardinalities are corrects
                for (RoaringBitmap rb : bitmap)
                        rb.validate();

                // Calculating the size
                for (k = 0; k < N; k++)
                        size += bitmap[k].getSizeInBytes();

                line += "\t" + size / 1024;
                line += "\t" + df.format((aft - bef) / 1000.0);

                // uncompressing
                bef = System.currentTimeMillis();
                for (r = 0; r < repeat; ++r)
                        for (k = 0; k < N; ++k) {
                                int[] array = bitmap[k].getIntegers();
                                bogus += array.length;
                        }
                aft = System.currentTimeMillis();
                line += "\t" + df.format((aft - bef) / 1000.0);

                {
                        RoaringBitmap bitmapor1 = bitmap[0];
                        bitmapor1.validate();
                        for (k = 1; k < N; ++k) {
                                bitmapor1 = RoaringBitmap.or(bitmapor1,
                                        bitmap[k]);
                                bitmapor1.validate();
                        }

                        int[] array = bitmapor1.getIntegers();
                        bogus += array.length;
                }

                // logical or + retrieval
                bef = System.currentTimeMillis();
                for (r = 0; r < repeat; ++r) {
                        RoaringBitmap bitmapor1 = bitmap[0];
                        for (k = 1; k < N; ++k)
                                bitmapor1 = RoaringBitmap.or(bitmapor1,
                                        bitmap[k]);

                        int[] array = bitmapor1.getIntegers();
                        bogus += array.length;
                }
                aft = System.currentTimeMillis();
                line += "\t" + df.format((aft - bef) / 1000.0);

                {
                        RoaringBitmap bitmapand1 = bitmap[0];
                        bitmapand1.validate();
                        for (k = 1; k < N; ++k) {
                                bitmapand1 = RoaringBitmap.and(bitmapand1,
                                        bitmap[k]);
                                bitmapand1.validate();
                        }
                        int[] array = bitmapand1.getIntegers();
                        bogus += array.length;
                }

                // logical and + retrieval
                bef = System.currentTimeMillis();
                for (r = 0; r < repeat; ++r) {
                        RoaringBitmap bitmapand1 = bitmap[0];
                        for (k = 1; k < N; ++k) {
                                bitmapand1 = RoaringBitmap.and(bitmapand1,
                                        bitmap[k]);
                        }
                        int[] array = bitmapand1.getIntegers();
                        bogus += array.length;
                }
                aft = System.currentTimeMillis();
                line += "\t" + df.format((aft - bef) / 1000.0);

                {
                        RoaringBitmap bitmapxor1 = bitmap[0];
                        for (k = 1; k < N; ++k) {
                                bitmapxor1 = RoaringBitmap.xor(bitmapxor1,
                                        bitmap[k]);
                                bitmapxor1.validate();
                        }
                        int[] array = bitmapxor1.getIntegers();
                        bogus += array.length;
                }

                // logical xor + retrieval
                bef = System.currentTimeMillis();
                for (r = 0; r < repeat; ++r) {
                        RoaringBitmap bitmapxor1 = bitmap[0];
                        for (k = 1; k < N; ++k)
                                bitmapxor1 = RoaringBitmap.xor(bitmapxor1,
                                        bitmap[k]);

                        int[] array = bitmapxor1.getIntegers();
                        bogus += array.length;
                }
                aft = System.currentTimeMillis();
                line += "\t" + df.format((aft - bef) / 1000.0);

                System.out.println(line);
                System.out.println("# ignore this " + bogus);
        }

        public static void testBitSet(int repeat, DecimalFormat df) {
                System.out.println("# BitSet");
                System.out
                        .println("# size, construction time, time to recover set bits, "
                                + "time to compute unions (OR), intersections (AND) "
                                + "and exclusive unions (XOR) ");
                long bef, aft;
                String line = "";
                int N = nbBitmaps;
                int size = 0;
                bef = System.currentTimeMillis();

                BitSet[] bitmap = new BitSet[N];
                {
                        int pos = 0;
                        for (int D = 0; D < TreeBitmapIdx.size(); ++D) {
                                if(TreeBitmapIdx.get(D) == null) continue; // disabled
                                for (IntArray ia : TreeBitmapIdx.get(D)
                                        .values()) {
                                        bitmap[pos] = new BitSet();
                                        for (int v = 0; v < ia.size(); ++v) {
                                                bitmap[pos].set(v);
                                        }
                                        ++pos;
                                }
                        }
                }
                aft = System.currentTimeMillis();

                // Calculating the size in bytes
                for (int k = 0; k < N; k++)
                        size += bitmap[k].size() / 8;

                line += "\t" + size / 1024;
                line += "\t" + df.format((aft - bef) / 1000.0);

                // uncompressing
                bef = System.currentTimeMillis();
                for (int r = 0; r < repeat; ++r)
                        for (int k = 0; k < N; ++k) {
                                int[] array = new int[bitmap[k].cardinality()];
                                int pos = 0;
                                for (int i = bitmap[k].nextSetBit(0); i >= 0; i = bitmap[k]
                                        .nextSetBit(i + 1)) {
                                        array[pos++] = i;
                                }
                        }
                aft = System.currentTimeMillis();
                line += "\t" + df.format((aft - bef) / 1000.0);

                // Creating and filling the 2nd bitset index

                // logical or + retrieval
                bef = System.currentTimeMillis();
                for (int r = 0; r < repeat; ++r) {
                        BitSet bitmapor1 = (BitSet) bitmap[0].clone();
                        for (int k = 1; k < N; ++k) {
                                bitmapor1.or(bitmap[k]);
                        }
                        int[] array = new int[bitmapor1.cardinality()];
                        int pos = 0;
                        for (int i = bitmapor1.nextSetBit(0); i >= 0; i = bitmapor1
                                .nextSetBit(i + 1)) {
                                array[pos++] = i;
                        }

                }
                aft = System.currentTimeMillis();
                line += "\t" + df.format((aft - bef) / 1000.0);

                // logical and + retrieval
                bef = System.currentTimeMillis();
                for (int r = 0; r < repeat; ++r) {
                        BitSet bitmapand1 = (BitSet) bitmap[0].clone();
                        for (int k = 1; k < N; ++k) {
                                bitmapand1.and(bitmap[k]);
                        }
                        int[] array = new int[bitmapand1.cardinality()];
                        int pos = 0;
                        for (int i = bitmapand1.nextSetBit(0); i >= 0; i = bitmapand1
                                .nextSetBit(i + 1)) {
                                array[pos++] = i;
                        }
                }
                aft = System.currentTimeMillis();
                line += "\t" + df.format((aft - bef) / 1000.0);

                // logical xor + retrieval
                bef = System.currentTimeMillis();
                for (int r = 0; r < repeat; ++r) {
                        BitSet bitmapxor1 = (BitSet) bitmap[0].clone();
                        for (int k = 1; k < N; ++k) {
                                bitmapxor1.xor(bitmap[k]);
                        }
                        int[] array = new int[bitmapxor1.cardinality()];
                        int pos = 0;
                        for (int i = bitmapxor1.nextSetBit(0); i >= 0; i = bitmapxor1
                                .nextSetBit(i + 1)) {
                                array[pos++] = i;
                        }
                }
                aft = System.currentTimeMillis();
                line += "\t" + df.format((aft - bef) / 1000.0);

                System.out.println(line);
        }

        public static void testConciseSet(int repeat, DecimalFormat df) {
                System.out
                        .println("# ConciseSet 32 bit using the extendedset_2.2 library");
                System.out
                        .println("# size, construction time, time to recover set bits, time to compute unions  and intersections ");
                long bef, aft;
                String line = "";
                int bogus = 0;
                int N = nbBitmaps;
                int size = 0;
                bef = System.currentTimeMillis();

                ConciseSet[] bitmap = new ConciseSet[N];
                {
                        int pos = 0;
                        for (int D = 0; D < TreeBitmapIdx.size(); ++D) {
                                if(TreeBitmapIdx.get(D) == null) continue; // disabled
                                for (IntArray ia : TreeBitmapIdx.get(D)
                                        .values()) {
                                        bitmap[pos] = new ConciseSet();
                                        for (int v = 0; v < ia.size(); ++v) {
                                                bitmap[pos].add(v);
                                        }
                                        ++pos;
                                }
                        }
                }
                aft = System.currentTimeMillis();

                // Calculating the size
                for (int k = 0; k < N; k++)
                        size += (int) (bitmap[k].size() * bitmap[k]
                                .collectionCompressionRatio()) * 4;

                line += "\t" + size / 1024;
                line += "\t" + df.format((aft - bef) / 1000.0);

                // uncompressing
                bef = System.currentTimeMillis();
                for (int r = 0; r < repeat; ++r)
                        for (int k = 0; k < N; ++k) {
                                int[] array = bitmap[k].toArray();
                                bogus += array.length;
                        }
                aft = System.currentTimeMillis();
                line += "\t" + df.format((aft - bef) / 1000.0);

                // logical or + retrieval
                bef = System.currentTimeMillis();
                for (int r = 0; r < repeat; ++r) {
                        ConciseSet bitmapor1 = bitmap[0].clone();
                        for (int k = 1; k < N; ++k) {
                                bitmapor1 = bitmapor1.union(bitmap[k]);
                        }
                        int[] array = bitmapor1.toArray();
                        bogus += array.length;
                }
                aft = System.currentTimeMillis();
                line += "\t" + df.format((aft - bef) / 1000.0);

                // logical and + retrieval
                bef = System.currentTimeMillis();
                for (int r = 0; r < repeat; ++r) {
                        ConciseSet bitmapand1 = bitmap[0].clone();
                        for (int k = 1; k < N; ++k) {
                                bitmapand1 = bitmapand1.intersection(bitmap[k]);
                        }
                        int[] array = bitmapand1.toArray();
                        if (array != null)
                                bogus += array.length;
                }
                aft = System.currentTimeMillis();
                line += "\t" + df.format((aft - bef) / 1000.0);
                // logical xor + retrieval
                bef = System.currentTimeMillis();
                for (int r = 0; r < repeat; ++r) {
                        ConciseSet bitmapand1 = bitmap[0].clone();
                        for (int k = 1; k < N; ++k) {
                                bitmapand1 = bitmapand1
                                        .symmetricDifference(bitmap[k]);
                        }
                        int[] array = bitmapand1.toArray();
                        if (array != null)
                                bogus += array.length;
                }
                aft = System.currentTimeMillis();
                line += "\t" + df.format((aft - bef) / 1000.0);

                System.out.println(line);
                System.out.println("# ignore this " + bogus);

        }

        public static void testWAH32(int repeat, DecimalFormat df) {
                System.out
                        .println("# WAH 32 bit using the compressedbitset library");
                System.out
                        .println("# size, construction time, time to recover set bits, "
                                + "time to compute unions (OR), intersections (AND)");
                long bef, aft;
                int bogus = 0;
                String line = "";
                int N = nbBitmaps;
                int size = 0;

                bef = System.currentTimeMillis();

                WAHBitSet[] bitmap = new WAHBitSet[N];
                {
                        int pos = 0;
                        for (int D = 0; D < TreeBitmapIdx.size(); ++D) {
                                if(TreeBitmapIdx.get(D) == null) continue; // disabled
                                for (IntArray ia : TreeBitmapIdx.get(D)
                                        .values()) {
                                        bitmap[pos] = new WAHBitSet();
                                        for (int v = 0; v < ia.size(); ++v) {
                                                bitmap[pos].set(v);
                                        }
                                        ++pos;
                                }
                        }
                }
                aft = System.currentTimeMillis();

                for (int k = 0; k < N; k++)
                        size += bitmap[k].memSize() * 4;

                line += "\t" + size / 1024;
                line += "\t" + df.format((aft - bef) / 1000.0);

                // uncompressing
                bef = System.currentTimeMillis();
                for (int r = 0; r < repeat; ++r)
                        for (int k = 0; k < N; ++k) {
                                int[] array = new int[bitmap[k].cardinality()];
                                int c = 0;
                                for (@SuppressWarnings("unchecked")
                                Iterator<Integer> i = bitmap[k].iterator(); i
                                        .hasNext(); array[c++] = i.next()
                                        .intValue()) {
                                }
                                bogus += c;
                        }
                aft = System.currentTimeMillis();
                line += "\t" + df.format((aft - bef) / 1000.0);

                // logical or + retrieval
                bef = System.currentTimeMillis();
                for (int r = 0; r < repeat; ++r) {
                        WAHBitSet bitmapor1 = bitmap[0];
                        for (int k = 0; k < N; ++k) {
                                bitmapor1 = bitmapor1.or(bitmap[k]);
                        }
                        int[] array = new int[bitmapor1.cardinality()];
                        int c = 0;
                        for (@SuppressWarnings("unchecked")
                        Iterator<Integer> i = bitmapor1.iterator(); i.hasNext(); array[c++] = i
                                .next().intValue()) {
                        }
                        bogus += array[array.length - 1];
                }
                aft = System.currentTimeMillis();
                line += "\t" + df.format((aft - bef) / 1000.0);

                // logical and + retrieval
                bef = System.currentTimeMillis();
                for (int r = 0; r < repeat; ++r) {
                        WAHBitSet bitmapand1 = bitmap[0];
                        for (int k = 1; k < N; ++k) {
                                bitmapand1 = bitmapand1.and(bitmap[k]);
                        }
                        int[] array = new int[bitmapand1.cardinality()];
                        int c = 0;
                        for (@SuppressWarnings("unchecked")
                        Iterator<Integer> i = bitmapand1.iterator(); i
                                .hasNext(); array[c++] = i.next().intValue()) {
                        }
                        if (array.length > 0)
                                bogus += array[array.length - 1];
                }
                aft = System.currentTimeMillis();
                line += "\t" + df.format((aft - bef) / 1000.0);

                System.out.println(line);
                System.out.println("# ignore this " + bogus);

        }

        public static void testEWAH64(int repeat, DecimalFormat df) {
                System.out.println("# EWAH 64bits using the javaewah library");
                System.out
                        .println("# size, construction time, time to recover set bits, "
                                + "time to compute unions (OR), intersections (AND) "
                                + "and exclusive unions (XOR) ");
                long bef, aft;
                String line = "";
                int bogus = 0;
                int N = nbBitmaps;
                int size = 0;

                bef = System.currentTimeMillis();

                EWAHCompressedBitmap[] ewah = new EWAHCompressedBitmap[N];
                {
                        int pos = 0;
                        for (int D = 0; D < TreeBitmapIdx.size(); ++D) {
                                if(TreeBitmapIdx.get(D) == null) continue; // disabled
                                for (IntArray ia : TreeBitmapIdx.get(D)
                                        .values()) {
                                        ewah[pos] = new EWAHCompressedBitmap();
                                        for (int v = 0; v < ia.size(); ++v) {
                                                ewah[pos].set(v);
                                        }
                                        ++pos;
                                }
                        }
                }

                aft = System.currentTimeMillis();

                // Calculating size
                for (int k = 0; k < N; k++)
                        size += ewah[k].sizeInBytes();

                line += "\t" + size / 1024;
                line += "\t" + df.format((aft - bef) / 1000.0);

                // uncompressing
                bef = System.currentTimeMillis();
                for (int r = 0; r < repeat; ++r)
                        for (int k = 0; k < N; ++k) {
                                int[] array = ewah[k].toArray();
                                bogus += array.length;
                        }
                aft = System.currentTimeMillis();
                line += "\t" + df.format((aft - bef) / 1000.0);

                // fast logical or + retrieval
                bef = System.currentTimeMillis();
                for (int r = 0; r < repeat; ++r) {
                        EWAHCompressedBitmap ewahor1 = EWAHCompressedBitmap
                                .or(Arrays.copyOf(ewah, N));
                        int[] array = ewahor1.toArray();
                        bogus += array.length;
                }
                aft = System.currentTimeMillis();
                line += "\t" + df.format((aft - bef) / 1000.0);

                // fast logical and + retrieval
                bef = System.currentTimeMillis();
                for (int r = 0; r < repeat; ++r) {
                        EWAHCompressedBitmap ewahand1 = EWAHCompressedBitmap
                                .and(Arrays.copyOf(ewah, N));
                        int[] array = ewahand1.toArray();
                        bogus += array.length;
                }
                aft = System.currentTimeMillis();
                line += "\t" + df.format((aft - bef) / 1000.0);

                // fast logical xor + retrieval
                bef = System.currentTimeMillis();
                for (int r = 0; r < repeat; ++r) {
                        EWAHCompressedBitmap ewahxor1 = EWAHCompressedBitmap
                                .and(Arrays.copyOf(ewah, N));
                        int[] array = ewahxor1.toArray();
                        bogus += array.length;
                }
                aft = System.currentTimeMillis();
                line += "\t" + df.format((aft - bef) / 1000.0);

                System.out.println(line);
                System.out.println("# ignore this " + bogus);
        }

        public static void testEWAH32(int repeat, DecimalFormat df) {
                System.out.println("# EWAH 32-bit using the javaewah library");
                System.out
                        .println("# size, construction time, time to recover set bits, "
                                + "time to compute unions (OR), intersections (AND) "
                                + "and exclusive unions (XOR) ");
                long bef, aft;
                String line = "";
                long bogus = 0;
                int N = nbBitmaps, k, r;
                int size = 0;

                bef = System.currentTimeMillis();

                EWAHCompressedBitmap32[] ewah = new EWAHCompressedBitmap32[N];
                {
                        int pos = 0;
                        for (int D = 0; D < TreeBitmapIdx.size(); ++D) {
                                if(TreeBitmapIdx.get(D) == null) continue; // disabled
                                for (IntArray ia : TreeBitmapIdx.get(D)
                                        .values()) {
                                        ewah[pos] = new EWAHCompressedBitmap32();
                                        for (int v = 0; v < ia.size(); ++v) {
                                                ewah[pos].set(v);
                                        }
                                        ++pos;
                                }
                        }
                }

                aft = System.currentTimeMillis();

                // Calculating size
                for (k = 0; k < N; k++)
                        size += ewah[k].sizeInBytes();

                line += "\t" + size / 1024;
                line += "\t" + df.format((aft - bef) / 1000.0);

                // uncompressing
                bef = System.currentTimeMillis();
                for (r = 0; r < repeat; ++r)
                        for (k = 0; k < N; ++k) {
                                int[] array = ewah[k].toArray();
                                bogus += array.length;
                        }
                aft = System.currentTimeMillis();
                line += "\t" + df.format((aft - bef) / 1000.0);

                // fast logical or + retrieval
                bef = System.currentTimeMillis();
                for (r = 0; r < repeat; ++r) {
                        EWAHCompressedBitmap32 ewahor1 = EWAHCompressedBitmap32
                                .or(Arrays.copyOf(ewah, N));
                        int[] array = ewahor1.toArray();
                        bogus += array.length;
                }
                aft = System.currentTimeMillis();
                line += "\t" + df.format((aft - bef) / 1000.0);

                // fast logical and + retrieval
                bef = System.currentTimeMillis();
                for (r = 0; r < repeat; ++r) {
                        EWAHCompressedBitmap32 ewahand1 = EWAHCompressedBitmap32
                                .and(Arrays.copyOf(ewah, N));
                        int[] array = ewahand1.toArray();
                        bogus += array.length;
                }
                aft = System.currentTimeMillis();
                line += "\t" + df.format((aft - bef) / 1000.0);

                // fast logical xor + retrieval
                bef = System.currentTimeMillis();
                for (r = 0; r < repeat; ++r) {
                        EWAHCompressedBitmap32 ewahxor1 = EWAHCompressedBitmap32
                                .xor(Arrays.copyOf(ewah, N));
                        int[] array = ewahxor1.toArray();
                        bogus += array.length;
                }
                aft = System.currentTimeMillis();
                line += "\t" + df.format((aft - bef) / 1000.0);

                System.out.println(line);
                System.out.println("# ignore this " + bogus);

        }

        /*
         * @Override public String toString() { StringBuffer sb = new
         * StringBuffer(); final Iterator<Entry<String, TreeMap<String,
         * ArrayList<Integer>>>> H = TreeBitmapIdx .entrySet().iterator();
         * 
         * Entry<String, TreeMap<String, ArrayList<Integer>>> sh; Entry<String,
         * ArrayList<Integer>> s; int counter = 0;
         * 
         * while (H.hasNext()) { sh = H.next(); sb.append(sh.getKey() + "\n");
         * final Iterator<Entry<String, ArrayList<Integer>>> I = sh
         * .getValue().entrySet().iterator(); while (I.hasNext()) { // s =
         * sh.getValue().firstEntry(); s = I.next(); counter++;
         * sb.append(s.getKey() + " :: " + s.getValue().toString() + "\n"); } }
         * System.out.println("cardinality = " + counter); return sb.toString();
         * }
         */
}
