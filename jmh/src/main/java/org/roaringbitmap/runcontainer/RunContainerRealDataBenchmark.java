package org.roaringbitmap.runcontainer;


import it.uniroma3.mat.extendedset.intset.ConciseSet;

import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.roaringbitmap.FastAggregation;
import org.roaringbitmap.RoaringBitmap;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class RunContainerRealDataBenchmark {
    static ConciseSet toConcise(int[] dat) {
        ConciseSet ans = new ConciseSet();
        for (int i : dat) {
            ans.add(i);
        }
        return ans;
    }
  
	@Benchmark
	public int horizontalOr_RunContainer(BenchmarkState benchmarkState) {
		return FastAggregation.horizontal_or(benchmarkState.rc.iterator())
				.getCardinality();
	}

	@Benchmark
	public int horizontalOr_BitmapContainer(BenchmarkState benchmarkState) {
		return FastAggregation.horizontal_or(benchmarkState.ac.iterator())
				.getCardinality();
	}
	
	@Benchmark
	public int horizontalOr_Concise(BenchmarkState benchmarkState) {
		ConciseSet bitmapor = benchmarkState.cc.get(0);
		for (int j = 1; j < benchmarkState.cc.size() ; ++j) {
			bitmapor = bitmapor.union(benchmarkState.cc.get(j));
		}
		return bitmapor.size();
	}

	@Benchmark
	public int pairwiseAnd_RunContainer(BenchmarkState benchmarkState) {
		int total = 0;
		for(int k = 0; k + 1 < benchmarkState.rc.size(); ++k)
			total += RoaringBitmap.and(benchmarkState.rc.get(k),benchmarkState.rc.get(k+1)).getCardinality();
		if(total !=benchmarkState.totaland )
			throw new RuntimeException("bad pairwise and result");
		return total;
	}

	@Benchmark
	public int pairwiseAnd_NormalContainer(BenchmarkState benchmarkState) {
		int total = 0;
		for(int k = 0; k + 1 < benchmarkState.rc.size(); ++k)
			total += RoaringBitmap.and(benchmarkState.ac.get(k),benchmarkState.ac.get(k+1)).getCardinality();
		if(total !=benchmarkState.totaland )
			throw new RuntimeException("bad pairwise and result");
		return total;
	}
	
	@Benchmark
	public int pairwiseAnd_Concise(BenchmarkState benchmarkState) {
		int total = 0;
		for(int k = 0; k + 1 < benchmarkState.cc.size(); ++k)
			total += benchmarkState.cc.get(k).intersection(benchmarkState.cc.get(k+1)).size();
		if(total !=benchmarkState.totaland )
			throw new RuntimeException("bad pairwise and result");
		return total;
	}

	@Benchmark
	public int pairwiseAndNot_RunContainer(BenchmarkState benchmarkState) {
		int total = 0;
		for(int k = 0; k + 1 < benchmarkState.rc.size(); ++k)
			total += RoaringBitmap.andNot(benchmarkState.rc.get(k),benchmarkState.rc.get(k+1)).getCardinality();
		if(total !=benchmarkState.totalandnot )
			throw new RuntimeException("bad pairwise andNot result");
		return total;
	}

	@Benchmark
	public int pairwiseAndNot_NormalContainer(BenchmarkState benchmarkState) {
		int total = 0;
		for(int k = 0; k + 1 < benchmarkState.rc.size(); ++k)
			total += RoaringBitmap.andNot(benchmarkState.ac.get(k),benchmarkState.ac.get(k+1)).getCardinality();
		if(total !=benchmarkState.totalandnot )
			throw new RuntimeException("bad pairwise andNot result");
		return total;
	}
	
	@Benchmark
	public int pairwiseAndNot_Concise(BenchmarkState benchmarkState) {
		int total = 0;
		for(int k = 0; k + 1 < benchmarkState.cc.size(); ++k)
			total += benchmarkState.cc.get(k).difference(benchmarkState.cc.get(k+1)).size();
		if(total !=benchmarkState.totalandnot )
			throw new RuntimeException("bad pairwise andNot result");
		return total;
	}

	
	@Benchmark
	public int pairwiseOr_RunContainer(BenchmarkState benchmarkState) {
		int total = 0;
		for(int k = 0; k + 1 < benchmarkState.rc.size(); ++k)
			total += RoaringBitmap.or(benchmarkState.rc.get(k),benchmarkState.rc.get(k+1)).getCardinality();
		if(total != benchmarkState.totalor )
			throw new RuntimeException("bad pairwise or result");
		return total;
	}
	
	@Benchmark
	public int pairwiseOr_NormalContainer(BenchmarkState benchmarkState) {
		int total = 0;
		for(int k = 0; k + 1 < benchmarkState.rc.size(); ++k)
			total += RoaringBitmap.or(benchmarkState.ac.get(k),benchmarkState.ac.get(k+1)).getCardinality();
		if(total != benchmarkState.totalor )
			throw new RuntimeException("bad pairwise or result");
		return total;
	}
	
	@Benchmark
	public int pairwiseOr_Concise(BenchmarkState benchmarkState) {
		int total = 0;
		for(int k = 0; k + 1 < benchmarkState.cc.size(); ++k)
			total += benchmarkState.cc.get(k).union(benchmarkState.cc.get(k+1)).size();
		if(total != benchmarkState.totalor )
			throw new RuntimeException("bad pairwise or result");
		return total;
	}

	@Benchmark
	public int pairwiseXor_RunContainer(BenchmarkState benchmarkState) {
		int total = 0;
		for(int k = 0; k + 1 < benchmarkState.rc.size(); ++k)
			total += RoaringBitmap.xor(benchmarkState.rc.get(k),benchmarkState.rc.get(k+1)).getCardinality();
		if(total != benchmarkState.totalxor )
			throw new RuntimeException("bad pairwise xor result");
		return total;
	}
	
	@Benchmark
	public int pairwiseXor_NormalContainer(BenchmarkState benchmarkState) {
		int total = 0;
		for(int k = 0; k + 1 < benchmarkState.rc.size(); ++k)
			total += RoaringBitmap.xor(benchmarkState.ac.get(k),benchmarkState.ac.get(k+1)).getCardinality();
		if(total != benchmarkState.totalxor )
			throw new RuntimeException("bad pairwise xor result");
		return total;
	}
	
	@Benchmark
	public int pairwiseXor_Concise(BenchmarkState benchmarkState) {
		int total = 0;
		for(int k = 0; k + 1 < benchmarkState.cc.size(); ++k)
			total += benchmarkState.cc.get(k).symmetricDifference(benchmarkState.cc.get(k+1)).size();
		if(total != benchmarkState.totalxor )
			throw new RuntimeException("bad pairwise xor result");
		return total;
	}

	@State(Scope.Benchmark)
	public static class BenchmarkState {

		int totalandnot = 0;
		int totaland = 0;
		int totalor = 0;
		int totalxor = 0;

		ArrayList<RoaringBitmap> rc = new ArrayList<RoaringBitmap>();
		ArrayList<RoaringBitmap> ac = new ArrayList<RoaringBitmap>();
		ArrayList<ConciseSet> cc = new ArrayList<ConciseSet>();

		public BenchmarkState() {
			File folder = new File("src/main/resources/dimension_033");
			System.out.println("Loading files from "+folder.getAbsolutePath());
			RealDataRetriever dataRetriever = new RealDataRetriever(folder);
			int normalsize = 0;
			int runsize = 0;
			int concisesize = 0;
			for (File datafile : folder.listFiles()) {
				int[] data = dataRetriever.fetchBitPositions(datafile);
				RoaringBitmap basic = RoaringBitmap.bitmapOf(data);
				RoaringBitmap opti = ((RoaringBitmap) basic.clone());
				opti.runOptimize();
				ConciseSet concise = toConcise(data);
				rc.add(opti);
				ac.add(basic);
				cc.add(concise);
				normalsize += basic.serializedSizeInBytes();
				runsize += opti.serializedSizeInBytes();
				concisesize += (int) (concise.size() * concise
						.collectionCompressionRatio()) * 4;
			}
			System.out.println("==============");
			System.out.println("Run size = "+runsize+" / normal size = "+normalsize+" / concise size = "+concisesize);
			System.out.println("==============");
			// compute pairwise AND and OR
			for(int k = 0; k + 1 < rc.size(); ++k) {
				totalandnot += RoaringBitmap.andNot(rc.get(k),rc.get(k+1)).getCardinality();
				totaland += RoaringBitmap.and(rc.get(k),rc.get(k+1)).getCardinality();
				totalor += RoaringBitmap.or(rc.get(k),rc.get(k+1)).getCardinality();
				totalxor += RoaringBitmap.xor(rc.get(k),rc.get(k+1)).getCardinality();
			}
		}

	}

}