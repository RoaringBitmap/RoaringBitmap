package me.lemire.roaringbitmap.experiments.colantonio;

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
         * Generate a random array (sorted integer set)
         * 
         * @param d should vary from 0.005 to 0.999
         * @return an array with a uniform distribution
         */
        public int[] getUniform(double d) {
                //////////////////
                //at each generation of a pseudo-random number a in [0, 1), 
                //in uniform sets an integer corresponding to floor(a * max) was added,
                //where max = 
                //105/d by varying d (the density) from 0.005 to 0.999.
                //////////////////
                if((d<0.005) || (d>0.999)) throw new IllegalArgumentException("parameter should be in [0.005,0.999]");
                HashSet<Integer> hash = new HashSet<Integer>();
                double max = N / d;
                while(hash.size()<N) {
                        double a = rand.nextDouble();
                        int x = (int) Math.floor(a * max);
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
         * Generate a random array (sorted integer set)
         * @param max parameter which should be larger integer set size
         * @return an array with a uniform distribution
         */
        public int[] getZipfian(double max) {
                //////////////////
                //Similarly, in Zipfian sets, at each number generation, an 
                //integer corresponding to floor(max * a^4) was added, where
                //max in [1.2 * 10^ 5, 6 * 10^9]. In this way, we generated skewed
                //data such that most of the integers were concentrated to lower 
                //values, while numbers with high values were very sparse.                 HashSet<Integer> hash = new HashSet<Integer>();
                //////////////////
                if(max<=N) 
                        throw new IllegalArgumentException("parameter should be larger than N");
                HashSet<Integer> hash = new HashSet<Integer>();
                int loopcount = 0;
                while(hash.size()<N) {
                        double a = rand.nextDouble();
                        int x = (int) Math.floor(a*a * max);
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


