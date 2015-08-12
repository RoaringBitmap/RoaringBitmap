package org.roaringbitmap.runcontainer;

import it.uniroma3.mat.extendedset.intset.ConciseSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.roaringbitmap.Container;
import org.roaringbitmap.FastAggregation;
import org.roaringbitmap.RoaringBitmap;

import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.googlecode.javaewah32.EWAHCompressedBitmap32;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class AllRunHorizontalOrBenchmark {

    static ConciseSet toConcise(int[] dat) {
        ConciseSet ans = new ConciseSet();
        for (int i : dat) {
            ans.add(i);
        }
        return ans;
    }


    static ConciseSet toWAH(int[] dat) {
        ConciseSet ans = new ConciseSet(true);
        for (int i : dat) {
            ans.add(i);
        }
        return ans;
    }

    
    @Benchmark
    public int OrARunContainer(BenchmarkState benchmarkState) {
        RoaringBitmap base = benchmarkState.rc.get(0);
        for(int k = 1; k < benchmarkState.rc.size(); ++k)
            base = RoaringBitmap.or(base, benchmarkState.rc.get(k));
        return base.getCardinality();
    }
    
    @Benchmark
    public int OrBitmapContainer(BenchmarkState benchmarkState) {
        RoaringBitmap base = benchmarkState.ac.get(0);
        for(int k = 1; k < benchmarkState.ac.size(); ++k)
            base = RoaringBitmap.or(base, benchmarkState.ac.get(k));
        return base.getCardinality();
    }
    
    @Benchmark
    public int OrConcise(BenchmarkState benchmarkState) {
        ConciseSet base = benchmarkState.cc.get(0);
        for(int k = 1; k < benchmarkState.cc.size(); ++k)
            base.union(benchmarkState.cc.get(k));
        return base.size();
    }
    
    @Benchmark
    public int OrWAH(BenchmarkState benchmarkState) {
        ConciseSet base = benchmarkState.wah.get(0);
        for(int k = 1; k < benchmarkState.wah.size(); ++k)
            base.union(benchmarkState.wah.get(k));
        return base.size();
    }
    
    @Benchmark
	public int horizontalOrARunContainer(BenchmarkState benchmarkState) {
		return FastAggregation.naive_or(benchmarkState.rc.iterator())
	               .getCardinality();
	}
	
  
	@Benchmark
	public int horizontalOrBitmapContainer(BenchmarkState benchmarkState) {
        return FastAggregation.naive_or(benchmarkState.ac.iterator())
                .getCardinality();
	}

    @Benchmark
    public int fastOrARunContainer(BenchmarkState benchmarkState) {
        RoaringBitmap[] v = new RoaringBitmap[benchmarkState.rc.size()];
        return FastAggregation.or((RoaringBitmap[]) benchmarkState.rc.toArray(v))
                   .getCardinality();
    }
    
  
    @Benchmark
    public int fastOrBitmapContainer(BenchmarkState benchmarkState) {
        RoaringBitmap[] v = new RoaringBitmap[benchmarkState.rc.size()];
        return FastAggregation.or((RoaringBitmap[]) benchmarkState.ac.toArray(v))
                .getCardinality();
    }
    
    @Benchmark
    public int horizontalOr_EWAH(BenchmarkState benchmarkState) {
        EWAHCompressedBitmap[] a = new EWAHCompressedBitmap[benchmarkState.ewah.size()];
        EWAHCompressedBitmap bitmapor = EWAHCompressedBitmap.or(benchmarkState.ewah.toArray(a)); 
        int answer = bitmapor.cardinality();
        return answer;

    }

    @Benchmark
    public int horizontalOr_EWAH32(BenchmarkState benchmarkState) {
        EWAHCompressedBitmap32[] a = new EWAHCompressedBitmap32[benchmarkState.ewah32.size()];
        EWAHCompressedBitmap32 bitmapor = EWAHCompressedBitmap32.or(benchmarkState.ewah32.toArray(a)); 
        int answer = bitmapor.cardinality();
        return answer;
    }
    
    @State(Scope.Benchmark)
    public static class BenchmarkState {        
 	  
        ArrayList<RoaringBitmap> rc = new  ArrayList<RoaringBitmap>();
        ArrayList<RoaringBitmap> ac = new  ArrayList<RoaringBitmap>();
        ArrayList<ConciseSet> cc = new  ArrayList<ConciseSet>();
        ArrayList<ConciseSet> wah = new  ArrayList<ConciseSet>();
        List<EWAHCompressedBitmap> ewah = new ArrayList<EWAHCompressedBitmap>();
        List<EWAHCompressedBitmap32> ewah32 = new ArrayList<EWAHCompressedBitmap32>();

        Random rand = new Random();
        Container aggregate;

        
        public BenchmarkState() {
            int N = 30;
            Random rand = new Random(1234);
            for (int k = 0; k < N; ++k) {
                RoaringBitmap rb = new RoaringBitmap();
                int start = rand.nextInt(10000);

                for (int z = 0; z < 50; ++z) {
                    int end = start + rand.nextInt(10000);
                    rb.add(start, end);
                    start = end + rand.nextInt(1000);
                }
                cc.add(toConcise(rb.toArray()));
                wah.add(toWAH(rb.toArray()));

                ac.add(rb);
                
                rb = rb.clone();
                rb.runOptimize();
                rc.add(rb);
                ewah.add(EWAHCompressedBitmap.bitmapOf(rb.toArray()));
                ewah32.add(EWAHCompressedBitmap32.bitmapOf(rb.toArray()));

            }
        }
    }

}
