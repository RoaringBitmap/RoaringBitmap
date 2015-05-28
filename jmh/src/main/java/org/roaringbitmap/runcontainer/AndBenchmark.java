package org.roaringbitmap.runcontainer;

import org.openjdk.jmh.annotations.*;
import org.roaringbitmap.*;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class AndBenchmark {

   @Benchmark
   public int andNotSmallBitmap(BenchmarkState benchmarkState) {
       return benchmarkState.rc1.and(benchmarkState.bc1).getCardinality();
   }

   @Benchmark
   public int andNotBigBitmap(BenchmarkState benchmarkState) {
       return benchmarkState.rc2.and(benchmarkState.bc1).getCardinality();
   }

   @Benchmark
   public int andNotSmallBitmapold(BenchmarkState benchmarkState) {
       return benchmarkState.rc1.andold(benchmarkState.bc1).getCardinality();
   }

   @Benchmark
   public int andNotBigBitmapold(BenchmarkState benchmarkState) {
       return benchmarkState.rc2.andold(benchmarkState.bc1).getCardinality();
   }

   
   @State(Scope.Benchmark)
   public static class BenchmarkState {

      final RunContainer rc1;
      final RunContainer rc2;
      final BitmapContainer bc1;


      public BenchmarkState() {
		 rc1 = new RunContainer();
         for(int k = 10; k < 20; ++k )
            for(int j = 0; j < 50; ++j)
              rc1.add((short)(k * 100 + j));
         bc1 = new BitmapContainer();
         for(int k = 0; k < 20000; ++k)
            bc1.add((short)k);
		 rc2 = new RunContainer();
         for(int k = 0; k < 120; ++k )
            for(int j = 0; j < 50; ++j)
              rc2.add((short)(k * 100 + j));

      }

   }

}