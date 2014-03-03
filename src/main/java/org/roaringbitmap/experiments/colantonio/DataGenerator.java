package org.roaringbitmap.experiments.colantonio;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
/**
 * 
 * This is a data generator object reproducing the models used
 * by Colantonio and Di Pietro, Concise: Compressed n Composable Integer Set
 * 
 * @author Daniel Lemire
 *
 */
public class DataGenerator {
        Random rand = new Random();
        int N;
        boolean zipfian = false;

        /**
         * Will generate arrays with default size (100000)
         */
        public DataGenerator() {
                N = 100000;
        }
        
        /**
         * @param n size of the arrays
         */
        public DataGenerator(final int n) {
                if(n<1) throw new IllegalArgumentException("number of ints should be positive");
                N = n;
        }
        
        /**
         * @return whether the data generator is in zipfian mode
         */
        public boolean is_zipfian() {
                return zipfian;
        }
        
        /**
         * set the data generator in uniform  mode
         */
        public void setUniform() {
                zipfian = false;
        }
        
        /**
         * set the data generator in Zipfian mode
         */
        public void setZipfian() {
                zipfian = true;
        }
        /**
         * Generate a random array (sorted integer set)
         * 
         * @param d should vary from 0 to 0.999
         * @return an array with a uniform or zipfian distribution
         */
        public int[] getRandomArray(double d) {
                if(zipfian)
                        return getZipfian(d);
                return getUniform(d);
        }
        /**
         * Generate a random array (sorted integer set)
         * 
         * @param d should vary from 0 to 1.000
         * @return an array with a uniform distribution
         */
        public int[] getUniform(double d) {
                ////////////////// (from arxiv version)
                //at each generation of a pseudo-random number a in [0, 1), 
                //in uniform sets an integer corresponding to floor(a * max) was added,
                //where max = 
                //105/d by varying d (the density) from 0.001 to 0.999.
                //////////////////
                if((d<0) || (d>1.000)) throw new IllegalArgumentException("parameter should be in [0.005,0.999]");
                if(d>=0.99) {
                        int[] answer = new int[N];
                        for(int k = 0; k<N;++k) answer[k] = k;
                        return answer;
                }
                final HashSet<Integer> hash = new HashSet<Integer>();
                final double max = N / d;
                
                while(hash.size()<N) {
                        final double a = rand.nextDouble();
                        final int x = (int) Math.floor(a * max);
                        hash.add(x);
                }
                int c = 0;
                int[] answer = new int[N];
                for(int x : hash)
                        answer[c++] = x;
                Arrays.sort(answer);
                return answer;
        }
        
        /**
         * Generate a random array (sorted integer set). 
         * 
         * This is what Colantonio and Di Pietro called Zipfian.
         * 
         * @param d should vary from 0 to 1.000
         * @return an array with a "Zipfian" distribution
         */
        public int[] getZipfian(double d) {
                ////////////////// (from arXiv version)
                //Similarly, in Zipfian sets, at each number generation, an 
                //integer corresponding to floor(max * a^4) was added, where
                //max in [1.2 * 10^ 5, 6 * 10^9]. In this way, we generated skewed
                //data such that most of the integers were concentrated to lower 
                //values, while numbers with high values were very sparse.                 
                //////////////////
                ////////////////
                // in  Colantonio and Di Pietro's arXiv version of their  paper at the 
                // end of page 3, they clearly state that it is a^4
                // However, the version that they published (IPL) states a^2.
                ////////////////
                if((d<0) || (d>1.000)) throw new IllegalArgumentException("parameter should be in [0.005,0.999]");
                if(d>=0.99) {
                        int[] answer = new int[N];
                        for(int k = 0; k<N;++k) answer[k] = k;
                        return answer;
                }
                final double max = N / d;
                HashSet<Integer> hash = new HashSet<Integer>();
                int loopcount = 0;
                while(hash.size()<N) {
                        final double a = rand.nextDouble();
                        final int x = (int) Math.floor(a*a * max);
                        hash.add(x);
                        if(loopcount++ > 10*N) throw new RuntimeException("zipfian generation is too expensive");
                }
                int c = 0;
                int[] answer = new int[N];
                for(int x : hash)
                        answer[c++] = x;
                Arrays.sort(answer);
                return answer;
        }

}


