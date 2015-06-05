package org.roaringbitmap.add;

import org.openjdk.jmh.annotations.*;
import org.roaringbitmap.RoaringBitmap;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class addBenchmark {
	
	int nbInts;
	int step;
	
	 @Setup
	 public void setup() {
		 nbInts=1<<20;
		 step=4;
	 }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public RoaringBitmap originalAdd() {
    	RoaringBitmap rb = new RoaringBitmap();
    	for(int i=0; i<nbInts; i+=step)
    		rb.add(i);
        return rb;
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public RoaringBitmap checkAdd() {
    	RoaringBitmap rb = new RoaringBitmap();
    	for(int i=0; i<nbInts; i+=step)
    		rb.checkedAdd(i);
        return rb;
    }
}
