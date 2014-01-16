package me.lemire.roaringbitmap.experiments.colantonio;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Locale;

import net.sourceforge.sizeof.SizeOf;
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
	private static String line = "";
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
        		Locale.setDefault(Locale.US);
                line+="# This benchmark emulates what Colantonio and Di Pietro,\n";
                line+="#  did in Concise: Compressed 'n' Composable Integer Set\n";
                line+="########\n";
                line+="# "+System.getProperty("java.vendor")+" "+System.getProperty("java.version")+" "+System.getProperty("java.vm.name")+"\n";
                line+="# "+System.getProperty("os.name")+" "+System.getProperty("os.arch")+" "+System.getProperty("os.version")+"\n";
                line+="# processors: "+Runtime.getRuntime().availableProcessors()+"\n";
                line+="# max mem.: "+Runtime.getRuntime().maxMemory()+"\n";
                line+="########\n";                
                int N = 100000;
                boolean sizeof = true;
                try {
                        SizeOf.setMinSizeToLog(0);
                        SizeOf.skipStaticField(true);
                        //SizeOf.skipFinalField(true);
                        SizeOf.deepSizeOf(args);
                } catch (IllegalStateException e) {
                        sizeof = false;
                        line+="# disabling sizeOf, run  -javaagent:lib/SizeOf.jar or equiv. to enable\n";
                }
                System.out.print(line);
                DataGenerator gen = new DataGenerator(N);
                int TIMES = 100;
                gen.setUniform();
                test(gen,true, TIMES,sizeof);
                gen.setZipfian();
                test(gen,true, TIMES,sizeof);
                
                //Creating a file and printing results on it
            try {
            		String folderPath;
            		folderPath = (args[0]==null) ? "" : args[0];
            		folderPath+="/tree/master/scripts";
            		new File(folderPath).mkdirs();
                	File file = new File(folderPath+"/Benchmark.txt");
					FileWriter fw = new FileWriter(file.getAbsoluteFile());
					BufferedWriter bw = new BufferedWriter(fw);
					bw.write(line);
					bw.close();
				} catch (IOException e) {e.printStackTrace();}
       }

        /**
         * @param gen data generator
         * @param verbose whether to print out the result
         * @param TIMES how many times should we run each test
         */
        public static void test(final DataGenerator gen, final boolean verbose, final int TIMES, boolean sizeof) {        	
        		String newTest = "";
        		if (!verbose)
                        System.out
                                .println("# running a dry run (can take a long time)");
                int bogus = 0;
                long bef, aft;
                DecimalFormat df = new DecimalFormat("0.000E0");
                DecimalFormat dfb = new DecimalFormat("000.0");
                if (verbose)
                        if(gen.is_zipfian())
                                newTest+="### zipfian test\n";
                         else
                        	 newTest+="### uniform test\n";
                if (verbose)
                	newTest+="# first columns are timings [intersection times in ns], then append times in ns, " +
                                    "then removes times in ns, then bits/int\n";
                if(verbose && sizeof)
                	newTest+="# For size (last columns), first column is estimated, second is sizeof\n";
                if (verbose)
                	newTest+="# density\tbitset\t\tconcise\t\twah\t\troar"+
                                    "\t\t\tbitset\t\tconcise\t\twah\t\tspeedyroaring"+
                                    "\t\t\tbitset\t\tconcise\t\twah\t\tspeedyroaring";
                if(verbose)
                        if(sizeof) 
                        	newTest+="\t\tbitset\tbitset\tconcise\tconcise\twah\twah\troar\troar";
                        else
                        	newTest+="\t\tbitset\t\tconcise\t\twah\t\troar"; 
                if(verbose){
                	System.out.println(newTest);
                	line+=newTest;
                }
                
                for (double d = 0.001; d <= 0.999; d *= 1.2) {
                		String newLine ="";
                        double[] timings = new double[4];
                        double[] storageinbits = new double[4];
                        double[] truestorageinbits = new double[4];
                        double[] appendTimes = new double[4];
                        double[] removeTimes = new double[4];
                        
                        for (int times = 0; times < TIMES; ++times) {
                                int[] v1 = gen.getRandomArray(d);
                                int[] v2 = gen.getRandomArray(d);
                                //BitSet
                                //Append times
                                bef = System.nanoTime();
                                BitSet borig1 = toBitSet(v1); // we will clone it
                                aft = System.nanoTime();
                                bogus += borig1.length();
                                appendTimes[0] += aft-bef;
                                BitSet b2 = toBitSet(v2);
                                //Storage
                                storageinbits[0] += borig1.size() + b2.size();
                                if(sizeof) truestorageinbits[0] += SizeOf.deepSizeOf(borig1)*8 
                                    + SizeOf.deepSizeOf(b2)*2;  
                                //And times.
                                bef = System.nanoTime();
                                BitSet b1 = (BitSet) borig1.clone(); // for fair comparison (not inplace)
                                b1.and(b2);
                                aft = System.nanoTime();
                                timings[0] += aft - bef;
                                bogus += b1.length();
                                //Remove times
                                int toRemove = v1[gen.rand.nextInt(gen.N)];
                                bef = System.nanoTime();
                                b2.clear(toRemove);
                                aft = System.nanoTime();
                                removeTimes[0] += aft-bef;
                                bogus += borig1.size();
                                int[] b2withremoval = toArray(b2);
                                borig1 = null;
                                b2 = null;
                                int[] trueintersection = toArray(b1);
                                b1 = null;
                                // Concise
                                //Append times
                                bef = System.nanoTime();
                                ConciseSet cs1 = toConciseSet(v1);
                                aft = System.nanoTime();
                                bogus += cs1.size();
                                appendTimes[1] += aft-bef;
                                ConciseSet cs2 = toConciseSet(v2);
                                storageinbits[1] += cs1.size()
                                        * cs1.collectionCompressionRatio() * 4
                                        * 8;
                                storageinbits[1] += cs2.size()
                                        * cs2.collectionCompressionRatio() * 4
                                        * 8;
                                if(sizeof) truestorageinbits[1] += SizeOf.deepSizeOf(cs1)*8 
                                          + SizeOf.deepSizeOf(cs2)*2;                              
                                bef = System.nanoTime();
                                cs1 = cs1.intersection(cs2);
                                aft = System.nanoTime();
                                // we verify the answer
                                if(!Arrays.equals(cs1.toArray(), trueintersection))
                                        throw new RuntimeException("bug");
                                bogus += cs1.size();
                                timings[1] += aft - bef;
                                //Removal times
                                bef = System.nanoTime();
                                cs2.remove(toRemove);
                                aft = System.nanoTime();
                                if(!Arrays.equals(cs2.toArray(), b2withremoval)) throw new RuntimeException("bug");
                                removeTimes[1] += aft-bef;
                                bogus += cs1.size();
                                cs1 = null;
                                cs2 = null;
                                //WAHConcise 
                                //Append times
                                bef = System.nanoTime();
                                ConciseSet wah1 = toWAHConciseSet(v1);
                                aft = System.nanoTime();
                                bogus += wah1.size();
                                appendTimes[2] += aft-bef;                                
                                ConciseSet wah2 = toWAHConciseSet(v2);
                                //Storage
                                storageinbits[2] += wah1.size()
                                        * wah1.collectionCompressionRatio() * 4
                                        * 8;
                                storageinbits[2] += wah2.size()
                                        * wah2.collectionCompressionRatio() * 4
                                        * 8;
                                if(sizeof) truestorageinbits[2] += SizeOf.deepSizeOf(wah1)*8 
                                    + SizeOf.deepSizeOf(wah2)*2;  
                                //Intersect times
                                bef = System.nanoTime();
                                wah1 = wah1.intersection(wah2);
                                aft = System.nanoTime();
                                //we verify the answer
                                if(!Arrays.equals(wah1.toArray(), trueintersection))
                                        throw new RuntimeException("bug");
                                bogus += wah1.size();
                                timings[2] += aft - bef;
                                //Removing times
                                bef = System.nanoTime();
                                wah2.remove(toRemove);
                                aft = System.nanoTime();
                                if(!Arrays.equals(wah2.toArray(), b2withremoval)) throw new RuntimeException("bug");
                                removeTimes[2] += aft-bef;
                                bogus += wah1.size();
                                wah1 = null;
                                wah2 = null;
                                //SpeedyRoaringBitmap
                                //Append times
                                bef = System.nanoTime();
                                SpeedyRoaringBitmap rb1 = toSpeedyRoaringBitmap(v1);
                                aft = System.nanoTime();
                                bogus += rb1.getCardinality();
                                appendTimes[3] += aft-bef;
                                SpeedyRoaringBitmap rb2 = toSpeedyRoaringBitmap(v2);
                                //Storage
                                storageinbits[3] += rb1.getSizeInBytes() * 8;
                                storageinbits[3] += rb2.getSizeInBytes() * 8;
                                if(sizeof) truestorageinbits[3] += SizeOf.deepSizeOf(rb1)*8 + SizeOf.deepSizeOf(rb2)*2;                              
                                //Intersect times
                                bef = System.nanoTime();
                                rb1 = SpeedyRoaringBitmap.and(rb1,rb2);
                                aft = System.nanoTime();
                                // we verify the answer
                                if(!Arrays.equals(rb1.getIntegers(), trueintersection))
                                        throw new RuntimeException("bug");
                                bogus += rb1.getCardinality();
                                timings[3] += aft - bef;
                                //Remove times
                                bef = System.nanoTime();
                                rb2.remove(toRemove);
                                aft = System.nanoTime();
                                if(!Arrays.equals(rb2.getIntegers(), b2withremoval)) throw new RuntimeException("bug");
                                removeTimes[3] += aft-bef;
                                bogus += rb1.getCardinality();
                                rb1 = null;
                                rb2 = null;
                        }
                        
                        if (verbose) {
                                newLine+= df.format(d)+"\t"
                                        + df.format(timings[0] / TIMES)
                                        + "\t\t"
                                        + df.format(timings[1] / TIMES)
                                        + "\t\t"
                                        + df.format(timings[2] / TIMES)
                                        + "\t\t"
                                        + df.format(timings[3] / TIMES);
                                newLine+= "\t\t\t"
                                        + df.format(appendTimes[0] / (TIMES * gen.N))
                                        + "\t\t"
                                        + df.format(appendTimes[1] / (TIMES * gen.N))
                                        + "\t\t"
                                        + df.format(appendTimes[2] / (TIMES * gen.N))
                                        + "\t\t"
                                        + df.format(appendTimes[3] / (TIMES * gen.N));
                                newLine += "\t\t\t\t"
                                        + df.format(removeTimes[0] / TIMES)
                                        + "\t\t"
                                        + df.format(removeTimes[1] / TIMES)
                                        + "\t\t"
                                        + df.format(removeTimes[2] / TIMES)
                                        + "\t\t"
                                        + df.format(removeTimes[3] / TIMES);
                        }
                        if (verbose)
                                if (sizeof)
                                	newLine += "\t\t\t"
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
                                                        / (2 * TIMES * gen.N));
                                else
                                	newLine += "\t\t\t"
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
                                                        / (2 * TIMES * gen.N));
                    if(verbose) {
                        System.out.println(newLine);
                        line+=newLine+"\n";
                    }                    
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

