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

public class BasicOrContainerBenchmark {

	@Benchmark
	public int orRunContainer(BenchmarkState benchmarkState) {
		return benchmarkState.rc1.or(benchmarkState.rc2).getCardinality();
	}

	@Benchmark
	public int orNormalContainer(BenchmarkState benchmarkState) {
		return benchmarkState.ac1.or(benchmarkState.ac2).getCardinality();
	}
	   
    @State(Scope.Benchmark)
    public static class BenchmarkState {        
	   public int bitsetperword1 = 32;
	   public int bitsetperword2 = 63;

       Container rc1, rc2, ac1, ac2;
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
       
       public static Container fillMeUp(Container c, int[] values) {
    	   if(values.length == 0)
    		   throw new RuntimeException("You are trying to create an empty bitmap! ");
    	   for(int k = 0; k < values.length; ++k)
    		   c = c.add((short)values[k]);
    	   if(c.getCardinality() != values.length)
    		   throw new RuntimeException("add failure");
    	   System.out.println("Generated container of size "+c.getSizeInBytes());
    	   return c;
       }
       
       public BenchmarkState() {
      	 final int max = 1<<16;
      	 final int howmanywords = ( 1 << 16 ) / 64;
      	 int[] values1 = generateUniformHash(bitsetperword1 * howmanywords, max);
      	 int[] values2 = generateUniformHash(bitsetperword2 * howmanywords, max);
      	 

    	 rc1 = new RunContainer();
    	 rc1 = fillMeUp(rc1, values1);
    	 
    	 rc2 = new RunContainer();
    	 rc2 = fillMeUp(rc2, values2);
    	 
    	 ac1 = new ArrayContainer();
    	 ac1 = fillMeUp(ac1, values1);
    	 
    	 ac2 = new ArrayContainer();
    	 ac2 = fillMeUp(ac2, values2);

    	 if( !rc1.equals(ac1)) 
    		 throw new RuntimeException("first containers do not match");

    	 if( !rc2.equals(ac2)) 
    		 throw new RuntimeException("second containers do not match");

    	 
    	 if( !rc1.or(rc2).equals(ac1.or(ac2))) 
    		 throw new RuntimeException("ors do not match");
	   }
    }

}
