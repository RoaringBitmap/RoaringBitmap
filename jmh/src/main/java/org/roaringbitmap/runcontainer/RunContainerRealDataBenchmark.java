package org.roaringbitmap.runcontainer;


import it.uniroma3.mat.extendedset.intset.ConciseSet;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.roaringbitmap.FastAggregation;
import org.roaringbitmap.IntIteratorFlyweight;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.BufferFastAggregation;
import org.roaringbitmap.buffer.BufferIntIteratorFlyweight;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

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
	public int horizontalOr_RoaringWithRun(BenchmarkState benchmarkState) {
		return FastAggregation.horizontal_or(benchmarkState.rc.iterator())
				.getCardinality();
	}

	@Benchmark
	public int horizontalOr_Roaring(BenchmarkState benchmarkState) {
		return FastAggregation.horizontal_or(benchmarkState.ac.iterator())
				.getCardinality();
	}

	@Benchmark
    public int horizontalOr_MutableRoaringWithRun(BenchmarkState benchmarkState) {
        return BufferFastAggregation.horizontal_or(benchmarkState.mrc.iterator())
                .getCardinality();
    }

    @Benchmark
    public int horizontalOr_MutableRoaring(BenchmarkState benchmarkState) {
        return BufferFastAggregation.horizontal_or(benchmarkState.mac.iterator())
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
	public int pairwiseAnd_RoaringWithRun(BenchmarkState benchmarkState) {
		int total = 0;
		for(int k = 0; k + 1 < benchmarkState.rc.size(); ++k)
			total += RoaringBitmap.and(benchmarkState.rc.get(k),benchmarkState.rc.get(k+1)).getCardinality();
		if(total !=benchmarkState.totaland )
			throw new RuntimeException("bad pairwise and result");
		return total;
	}

	@Benchmark
	public int pairwiseAnd_Roaring(BenchmarkState benchmarkState) {
		int total = 0;
		for(int k = 0; k + 1 < benchmarkState.ac.size(); ++k)
			total += RoaringBitmap.and(benchmarkState.ac.get(k),benchmarkState.ac.get(k+1)).getCardinality();
		if(total !=benchmarkState.totaland )
			throw new RuntimeException("bad pairwise and result");
		return total;
	}

    @Benchmark
    public int pairwiseAnd_MutableRoaringWithRun(BenchmarkState benchmarkState) {
        int total = 0;
        for (int k = 0; k + 1 < benchmarkState.mrc.size(); ++k)
            total += MutableRoaringBitmap.and(benchmarkState.mrc.get(k),
                    benchmarkState.mrc.get(k + 1)).getCardinality();
        if (total != benchmarkState.totaland)
            throw new RuntimeException("bad pairwise and result");
        return total;
    }

    @Benchmark
    public int pairwiseAnd_MutableRoaring(BenchmarkState benchmarkState) {
        int total = 0;
        for (int k = 0; k + 1 < benchmarkState.mac.size(); ++k)
            total += MutableRoaringBitmap.and(benchmarkState.mac.get(k),
                    benchmarkState.mac.get(k + 1)).getCardinality();
        if (total != benchmarkState.totaland)
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
	public int pairwiseAndNot_RoaringWithRun(BenchmarkState benchmarkState) {
		int total = 0;
		for(int k = 0; k + 1 < benchmarkState.rc.size(); ++k)
			total += RoaringBitmap.andNot(benchmarkState.rc.get(k),benchmarkState.rc.get(k+1)).getCardinality();
		if(total !=benchmarkState.totalandnot )
			throw new RuntimeException("bad pairwise andNot result");
		return total;
	}

	@Benchmark
	public int pairwiseAndNot_Roaring(BenchmarkState benchmarkState) {
		int total = 0;
		for(int k = 0; k + 1 < benchmarkState.ac.size(); ++k)
			total += RoaringBitmap.andNot(benchmarkState.ac.get(k),benchmarkState.ac.get(k+1)).getCardinality();
		if(total !=benchmarkState.totalandnot )
			throw new RuntimeException("bad pairwise andNot result");
		return total;
	}

    @Benchmark
    public int pairwiseAndNot_MutableRoaringWithRun(BenchmarkState benchmarkState) {
        int total = 0;
        for (int k = 0; k + 1 < benchmarkState.mrc.size(); ++k)
            total += MutableRoaringBitmap.andNot(benchmarkState.mrc.get(k),
                    benchmarkState.mrc.get(k + 1)).getCardinality();
        if (total != benchmarkState.totalandnot)
            throw new RuntimeException("bad pairwise andNot result");
        return total;
    }

    @Benchmark
    public int pairwiseAndNot_MutableRoaring(BenchmarkState benchmarkState) {
        int total = 0;
        for (int k = 0; k + 1 < benchmarkState.mac.size(); ++k)
            total += MutableRoaringBitmap.andNot(benchmarkState.mac.get(k),
                    benchmarkState.mac.get(k + 1)).getCardinality();
        if (total != benchmarkState.totalandnot)
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
	public int pairwiseOr_RoaringWithRun(BenchmarkState benchmarkState) {
		int total = 0;
		for(int k = 0; k + 1 < benchmarkState.rc.size(); ++k)
			total += RoaringBitmap.or(benchmarkState.rc.get(k),benchmarkState.rc.get(k+1)).getCardinality();
		if(total != benchmarkState.totalor )
			throw new RuntimeException("bad pairwise or result");
		return total;
	}
	
	@Benchmark
	public int pairwiseOr_Roaring(BenchmarkState benchmarkState) {
		int total = 0;
		for(int k = 0; k + 1 < benchmarkState.ac.size(); ++k)
			total += RoaringBitmap.or(benchmarkState.ac.get(k),benchmarkState.ac.get(k+1)).getCardinality();
		if(total != benchmarkState.totalor )
			throw new RuntimeException("bad pairwise or result");
		return total;
	}
	

    
    @Benchmark
    public int pairwiseOr_MutableRoaringWithRun(BenchmarkState benchmarkState) {
        int total = 0;
        for(int k = 0; k + 1 < benchmarkState.mrc.size(); ++k)
            total += MutableRoaringBitmap.or(benchmarkState.mrc.get(k),benchmarkState.mrc.get(k+1)).getCardinality();
        if(total != benchmarkState.totalor )
            throw new RuntimeException("bad pairwise or result");
        return total;
    }
    
    @Benchmark
    public int pairwiseOr_MutableRoaring(BenchmarkState benchmarkState) {
        int total = 0;
        for(int k = 0; k + 1 < benchmarkState.mac.size(); ++k)
            total += MutableRoaringBitmap.or(benchmarkState.mac.get(k),benchmarkState.mac.get(k+1)).getCardinality();
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
	public int pairwiseXor_RoaringWithRun(BenchmarkState benchmarkState) {
		int total = 0;
		for(int k = 0; k + 1 < benchmarkState.rc.size(); ++k)
			total += RoaringBitmap.xor(benchmarkState.rc.get(k),benchmarkState.rc.get(k+1)).getCardinality();
		if(total != benchmarkState.totalxor )
			throw new RuntimeException("bad pairwise xor result");
		return total;
	}
	
	@Benchmark
	public int pairwiseXor_Roaring(BenchmarkState benchmarkState) {
		int total = 0;
		for(int k = 0; k + 1 < benchmarkState.ac.size(); ++k)
			total += RoaringBitmap.xor(benchmarkState.ac.get(k),benchmarkState.ac.get(k+1)).getCardinality();
		if(total != benchmarkState.totalxor )
			throw new RuntimeException("bad pairwise xor result");
		return total;
	}
	
	@Benchmark
    public int pairwiseXor_MutableRoaringWithRun(BenchmarkState benchmarkState) {
        int total = 0;
        for(int k = 0; k + 1 < benchmarkState.mrc.size(); ++k)
            total += MutableRoaringBitmap.xor(benchmarkState.mrc.get(k),benchmarkState.mrc.get(k+1)).getCardinality();
        if(total != benchmarkState.totalxor )
            throw new RuntimeException("bad pairwise xor result");
        return total;
    }
    
    @Benchmark
    public int pairwiseXor_MutableRoaring(BenchmarkState benchmarkState) {
        int total = 0;
        for(int k = 0; k + 1 < benchmarkState.mac.size(); ++k)
            total += MutableRoaringBitmap.xor(benchmarkState.mac.get(k),benchmarkState.mac.get(k+1)).getCardinality();
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

    @Benchmark
    public int iterate_RoaringWithRun(BenchmarkState benchmarkState) {
        int total = 0;
        for (int k = 0; k < benchmarkState.rc.size(); ++k) {
            RoaringBitmap rb = benchmarkState.rc.get(k);
            org.roaringbitmap.IntIterator i = rb.getIntIterator();
            while(i.hasNext())
                total += i.next();
        }
        return total;
    }

    @Benchmark
    public int iterate_RoaringWithRun_flyweight(BenchmarkState benchmarkState) {
        int total = 0;
        IntIteratorFlyweight i = new IntIteratorFlyweight();
        for (int k = 0; k < benchmarkState.rc.size(); ++k) {
            RoaringBitmap rb = benchmarkState.rc.get(k);
            i.wrap(rb);
            while(i.hasNext())
                total += i.next();
        }
        return total;
    }

    
    @Benchmark
    public int iterate_Roaring(BenchmarkState benchmarkState) {
        int total = 0;
        for (int k = 0; k < benchmarkState.ac.size(); ++k) {
            RoaringBitmap rb = benchmarkState.ac.get(k);
            org.roaringbitmap.IntIterator i = rb.getIntIterator();
            while(i.hasNext())
                total += i.next();
        }
        return total;

    }
    @Benchmark
    public int iterate_Roaring_flyweight(BenchmarkState benchmarkState) {
        int total = 0;
        IntIteratorFlyweight i = new IntIteratorFlyweight();
        for (int k = 0; k < benchmarkState.ac.size(); ++k) {
            RoaringBitmap rb = benchmarkState.ac.get(k);
            i.wrap(rb);
            while(i.hasNext())
                total += i.next();
        }
        return total;
    }    

    @Benchmark
    public int iterate_MutableRoaringWithRun(BenchmarkState benchmarkState) {
        int total = 0;
        for (int k = 0; k < benchmarkState.mrc.size(); ++k) {
            ImmutableRoaringBitmap rb = benchmarkState.mrc.get(k);
            org.roaringbitmap.IntIterator i = rb.getIntIterator();
            while(i.hasNext())
                total += i.next();
        }
        return total;
    }

    @Benchmark
    public int iterate_MutableRoaringWithRun_flyweight(BenchmarkState benchmarkState) {
        int total = 0;
        BufferIntIteratorFlyweight i = new BufferIntIteratorFlyweight();
        for (int k = 0; k < benchmarkState.mrc.size(); ++k) {
            ImmutableRoaringBitmap rb = benchmarkState.mrc.get(k);
            i.wrap(rb);
            while(i.hasNext())
                total += i.next();
        }
        return total;
    }

    
    @Benchmark
    public int iterate_MutableRoaring(BenchmarkState benchmarkState) {
        int total = 0;
        for (int k = 0; k < benchmarkState.mac.size(); ++k) {
            ImmutableRoaringBitmap rb = benchmarkState.mac.get(k);
            org.roaringbitmap.IntIterator i = rb.getIntIterator();
            while(i.hasNext())
                total += i.next();
        }
        return total;

    }
    @Benchmark
    public int iterate_MutableRoaring_flyweight(BenchmarkState benchmarkState) {
        int total = 0;
        BufferIntIteratorFlyweight i = new BufferIntIteratorFlyweight();
        for (int k = 0; k < benchmarkState.mac.size(); ++k) {
            ImmutableRoaringBitmap rb = benchmarkState.mac.get(k);
            i.wrap(rb);
            while(i.hasNext())
                total += i.next();
        }
        return total;
    }        

    @Benchmark
    public int iterate_Concise(BenchmarkState benchmarkState) {
        int total = 0;
        for (int k = 0; k < benchmarkState.rc.size(); ++k) {
            ConciseSet cs = benchmarkState.cc.get(k);
            it.uniroma3.mat.extendedset.intset.IntSet.IntIterator i = cs.iterator();
            while(i.hasNext())
                total += i.next();
        }
        return total;
    }

    @Benchmark
    public int toarray_RoaringWithRun(BenchmarkState benchmarkState) {
        int total = 0;
        for (int k = 0; k < benchmarkState.rc.size(); ++k) {
            RoaringBitmap rb = benchmarkState.rc.get(k);
            total += rb.toArray().length;
        }
        return total;
    }

    @Benchmark
    public int toarray_Roaring(BenchmarkState benchmarkState) {
        int total = 0;
        for (int k = 0; k < benchmarkState.ac.size(); ++k) {
            RoaringBitmap rb = benchmarkState.ac.get(k);
            total += rb.toArray().length;
        }
        return total;

    }


    @Benchmark
    public int toarray_MutableRoaringWithRun(BenchmarkState benchmarkState) {
        int total = 0;
        for (int k = 0; k < benchmarkState.mrc.size(); ++k) {
            ImmutableRoaringBitmap rb = benchmarkState.mrc.get(k);
            total += rb.toArray().length;
        }
        return total;
    }

    @Benchmark
    public int toarray_MutableRoaring(BenchmarkState benchmarkState) {
        int total = 0;
        for (int k = 0; k < benchmarkState.mac.size(); ++k) {
            ImmutableRoaringBitmap rb = benchmarkState.mac.get(k);
            total += rb.toArray().length;
        }
        return total;

    }    

    @Benchmark
    public int toarray_Concise(BenchmarkState benchmarkState) {
        int total = 0;
        for (int k = 0; k < benchmarkState.rc.size(); ++k) {
            ConciseSet cs = benchmarkState.cc.get(k);
            total += cs.toArray().length;
        }
        return total;
    }

	@State(Scope.Benchmark)
	public static class BenchmarkState {
	    String basedir = "src/main/resources/real-roaring-dataset/";
	    @Param ({// putting the data sets in alpha. order
	        "census-income",/* "census1881", 
	        "dimension_008", "dimension_003", "dimension_033",  
	        "uscensus2000", "weather_sept_85", 
	        "wikileaks-noquotes"*/})
	    String foldername;

		int totalandnot = 0;
		int totaland = 0;
		int totalor = 0;
		int totalxor = 0;

		ArrayList<RoaringBitmap> rc = new ArrayList<RoaringBitmap>();
		ArrayList<RoaringBitmap> ac = new ArrayList<RoaringBitmap>();
        ArrayList<ImmutableRoaringBitmap> mrc = new ArrayList<ImmutableRoaringBitmap>();
        ArrayList<ImmutableRoaringBitmap> mac = new ArrayList<ImmutableRoaringBitmap>();

		ArrayList<ConciseSet> cc = new ArrayList<ConciseSet>();
	    
		public BenchmarkState() {
		}
		
	    @Setup		
        public void setup() throws IOException {
	        ZipRealDataRetriever folder = new ZipRealDataRetriever(basedir + foldername + ".zip");
	        System.out.println();
	        System.out
                    .println("Loading files from " + folder.getName());
            int normalsize = 0;
            int runsize = 0;
            int concisesize = 0;
            long stupidarraysize = 0;
            long stupidbitmapsize = 0;
            int totalcount = 0;
            int numberofbitmaps = 0;
            int universesize = 0;

            DecimalFormat df = new DecimalFormat("0.###");
            for (String datafile : folder.files()) {
                int[] data = folder.fetchBitPositions(datafile);
                numberofbitmaps++;
                if(universesize < data[data.length - 1 ])
                    universesize = data[data.length - 1 ];
                stupidarraysize += 8 + data.length * 4L;
                stupidbitmapsize += 8 + (data[data.length - 1] + 63L) / 64 * 8; 
                totalcount += data.length;
                MutableRoaringBitmap mbasic = MutableRoaringBitmap.bitmapOf(data);
                MutableRoaringBitmap mopti = ((MutableRoaringBitmap) mbasic.clone());
                mopti.runOptimize();

                RoaringBitmap basic = RoaringBitmap.bitmapOf(data);
                RoaringBitmap opti = ((RoaringBitmap) basic.clone());
                opti.runOptimize();
                ConciseSet concise = toConcise(data);
                rc.add(opti);
                ac.add(basic);
                mrc.add(mopti);
                mac.add(mbasic);
                cc.add(concise);
                if(basic.serializedSizeInBytes() != mbasic.serializedSizeInBytes())
                    throw new RuntimeException("size mismatch");
                if(opti.serializedSizeInBytes() != mopti.serializedSizeInBytes())
                    throw new RuntimeException("size mismatch");
                normalsize += basic.serializedSizeInBytes();
                runsize += opti.serializedSizeInBytes();
                concisesize += (int) (concise.size() * concise
                        .collectionCompressionRatio()) * 4;
            }
            /***
             * This is a hack. JMH does not allow us to report
             * anything directly ourselves, so we do it forcefully.
             */
            System.out.println();
            System.out.println("==============");
            System.out.println("Number of bitmaps = " + numberofbitmaps
                    + " total count = " + totalcount
                    + " universe size = "+universesize);
            System.out.println("Average bits per bitmap = " 
                    + df.format(totalcount * 1.0 / numberofbitmaps));

            System.out.println("(in bytes) Run size = " + runsize 
                    + " / normal size = " + normalsize 
                    + " / concise size = " + concisesize
                    + " / stupid array size = " + stupidarraysize
                    + " / stupid bitmap size = "+ stupidbitmapsize);
            System.out.println("Bits per int: Run  = "
                    + df.format(runsize * 8.0 / totalcount)
                    + " / normal size = "
                    + df.format(normalsize * 8.0 / totalcount)
                    + " / concise size = "
                    + df.format(concisesize * 8.0 / totalcount)
                    + " / stupid array size = "
                    + df.format(stupidarraysize * 8.0 / totalcount)
                    + " / stupid bitmap size = "+ df.format(stupidbitmapsize * 8.0 / totalcount));
            int bestofroaring = runsize < normalsize ? runsize : normalsize;
            System.out.println(" Average savings due to Roaring per bitmap (can be neg.) : "
                    + df.format((concisesize - bestofroaring) * 1.0 / numberofbitmaps ) 
                    + " bytes" );
            System.out.println("==============");
            System.out.println();
            // compute pairwise AND and OR
            for (int k = 0; k + 1 < rc.size(); ++k) {
                totalandnot += RoaringBitmap.andNot(rc.get(k), rc.get(k + 1))
                        .getCardinality();
                totaland += RoaringBitmap.and(rc.get(k), rc.get(k + 1))
                        .getCardinality();
                totalor += RoaringBitmap.or(rc.get(k), rc.get(k + 1))
                        .getCardinality();
                totalxor += RoaringBitmap.xor(rc.get(k), rc.get(k + 1))
                        .getCardinality();
            }
        }

	}

}
