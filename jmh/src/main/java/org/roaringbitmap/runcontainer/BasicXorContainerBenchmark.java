package org.roaringbitmap.runcontainer;

import org.openjdk.jmh.annotations.*;
import org.roaringbitmap.*;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)

public class BasicXorContainerBenchmark {

	@Benchmark
	public int xorBitmapContainerVSRunContainerContainer(BenchmarkState benchmarkState) {
		if(benchmarkState.rc2.serializedSizeInBytes() > benchmarkState.ac2.serializedSizeInBytes())
			throw new RuntimeException("Can't expect run containers to win if they are larger.");
		return benchmarkState.ac1.xor(benchmarkState.rc2).getCardinality();
	}
	
	@Benchmark
	public int xorBitmapContainerVSBitmapContainer(BenchmarkState benchmarkState) {
		return benchmarkState.ac1.xor(benchmarkState.ac2).getCardinality();
	}
	   

	@Benchmark
	public int part2_xorRunContainerVSRunContainerContainer(BenchmarkState benchmarkState) {
		if(benchmarkState.rc2.serializedSizeInBytes() > benchmarkState.ac2.serializedSizeInBytes())
			throw new RuntimeException("Can't expect run containers to win if they are larger.");
		if(benchmarkState.rc3.serializedSizeInBytes() > benchmarkState.ac3.serializedSizeInBytes())
			throw new RuntimeException("Can't expect run containers to win if they are larger.");
		return benchmarkState.rc3.xor(benchmarkState.rc2).getCardinality();
	}
	
	@Benchmark
	public int part2_xorBitmapContainerVSBitmapContainer2(BenchmarkState benchmarkState) {
		return benchmarkState.ac3.xor(benchmarkState.ac2).getCardinality();
	}

	
	@State(Scope.Benchmark)
    public static class BenchmarkState {        
	   public int bitsetperword1 = 32;
	   public int bitsetperword2 = 63;

       Container rc1, rc2, rc3, ac1, ac2, ac3;
       Random rand = new Random();

       
       public BenchmarkState() {
      	 final int max = 1<<16;
      	 final int howmanywords = ( 1 << 16 ) / 64;
      	 int[] values1 = RandomUtil.generateUniformHash(rand,bitsetperword1 * howmanywords, max);
      	 int[] values2 = RandomUtil.generateUniformHash(rand,bitsetperword2 * howmanywords, max);
      	 int[] values3 = RandomUtil.generateCrazyRun(rand, max);
      	 

    	 rc1 = new RunContainer();
    	 rc1 = RandomUtil.fillMeUp(rc1, values1);
    	 
    	 rc2 = new RunContainer();
    	 rc2 = RandomUtil.fillMeUp(rc2, values2);

    	 rc3 = new RunContainer();
    	 rc3 = RandomUtil.fillMeUp(rc3, values3);
    	 
    	 ac1 = new ArrayContainer();
    	 ac1 = RandomUtil.fillMeUp(ac1, values1);
    	 
    	 ac2 = new ArrayContainer();
    	 ac2 = RandomUtil.fillMeUp(ac2, values2);

    	 ac3 = new ArrayContainer();
    	 ac3 = RandomUtil.fillMeUp(ac3, values3);
    	 
    	 if( !rc1.equals(ac1)) 
    		 throw new RuntimeException("first containers do not match");

    	 if( !rc2.equals(ac2)) 
    		 throw new RuntimeException("second containers do not match");

    	 if( !rc1.xor(rc2).equals(ac1.xor(ac2))) 
    		 throw new RuntimeException("xors do not match");
    	 if( !ac1.xor(rc2).equals(ac1.xor(ac2))) 
    		 throw new RuntimeException("xors do not match");
       }
    }

}
