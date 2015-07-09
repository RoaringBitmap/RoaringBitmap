package org.roaringbitmap.runcontainer;

import org.openjdk.jmh.annotations.*;
import org.roaringbitmap.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)

public class BasicContainerBenchmark {

	@Benchmark
	public int andRunContainer(BenchmarkState benchmarkState) {
		return benchmarkState.rc1.and(benchmarkState.rc2).getCardinality();
	}

	@Benchmark
	public int andNormalContainer(BenchmarkState benchmarkState) {
		return benchmarkState.ac1.and(benchmarkState.ac2).getCardinality();
	}
	   
    @State(Scope.Benchmark)
    public static class BenchmarkState {
        @Param({"1", "16", "32", "48", "52", "64"})
        public static int bitsetperword1;
        @Param({"1", "16", "32", "48", "52", "64"})
        public static int bitsetperword2;

       final RunContainer rc1, rc2;
       final ArrayContainer ac1, ac2;
       Random rand = new Random();

       /**
        * generates randomly N distinct integers from 0 to Max.
        */
       int[] generateUniformHash(int N, int Max) {
               if (N > Max)
                       throw new RuntimeException("not possible");
               if(N > Max/2) {
            	   return negate(generateUniformHash(Max-N, Max),Max);
               }
               int[] ans = new int[N];
               HashSet<Integer> s = new HashSet<Integer>();
               while (s.size() < N)
                       s.add(new Integer(rand.nextInt(Max)));
               Iterator<Integer> i = s.iterator();
               for (int k = 0; k < N; ++k)
                       ans[k] = i.next().intValue();
               Arrays.sort(ans);
               return ans;
       }
       /**
       * output all integers from the range [0,Max) that are not
       * in the array
       */
       static int[] negate(int[] x, int Max) {
               int[] ans = new int[Max - x.length];
               int i = 0;
               int c = 0;
               for (int j = 0; j < x.length; ++j) {
                       int v = x[j];
                       for (; i < v; ++i)
                               ans[c++] = i;
                       ++i;
               }
               while (c < ans.length)
                       ans[c++] = i++;
               return ans;
       }
       
       public static void fillMeUp(Container c, int[] values) {
    	   for(int k = 0; k < values.length; ++k)
    		   c = c.add((short)values[k]);
       }
       
       public BenchmarkState() {
    	 int max = 1<<16;
    	 int[] values1 = generateUniformHash(bitsetperword1 * max  / 64, max);
    	 int[] values2 = generateUniformHash(bitsetperword2 * max  / 64, max);

    	 rc1 = new RunContainer();
    	 fillMeUp(rc1, values1);
    	 
    	 rc2 = new RunContainer();
    	 fillMeUp(rc2, values2);
    	 
    	 ac1 = new ArrayContainer();
    	 fillMeUp(ac1, values1);
    	 
    	 ac2 = new ArrayContainer();
    	 if( !rc1.and(rc2).equals(ac1.and(ac2))) 
    		 throw new RuntimeException("ands do not match");
	   }
    }

}
