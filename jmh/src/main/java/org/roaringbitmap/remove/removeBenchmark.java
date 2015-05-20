package org.roaringbitmap.remove;

import org.openjdk.jmh.annotations.*;
import org.roaringbitmap.RoaringBitmap;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class removeBenchmark {	
	int nbInts = 1<<20;
	int step = 4;
	RoaringBitmap bitmap1;
	
	 @Setup
	 public void setup() {		 
		 nbInts=1<<25;
		 step=4;
		 bitmap1 = new RoaringBitmap();
		 for(int i=0; i<nbInts; i++)
			 bitmap1.add(i);
	 }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public RoaringBitmap originalRemove() {
    	RoaringBitmap rb = bitmap1.clone();
    	for(int i=0; i<nbInts; i+=step)
    		rb.remove(i);
        return rb;
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public RoaringBitmap checkRemove() {
    	RoaringBitmap rb = bitmap1.clone();
    	for(int i=0; i<nbInts; i+=step)
    		rb.checkedRemove(i);
        return rb;
    }
    
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public RoaringBitmap clone(){
    	return bitmap1.clone();
    }
}
