package org.roaringbitmap;

import static org.roaringbitmap.buffer.BenchmarkConsumers.CONSOLE_CONSUMER;
import static org.roaringbitmap.buffer.BenchmarkConsumers.H2_CONSUMER;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;

public class TestBenchmarkIterator2 {
    

    @Rule
    public TestRule benchmarkRun = new BenchmarkRule(CONSOLE_CONSUMER,
            H2_CONSUMER);
    
    static RoaringBitmap rb;
    static int result = 0;

    @BenchmarkOptions(callgc = false,warmupRounds = 10,benchmarkRounds = 10)
    @Test
    public void testStandard() {
            IntIterator i = rb.getIntIterator();
            while (i.hasNext()) {
                result += i.next();
            }
    }
    
    @BenchmarkOptions(callgc = false,warmupRounds = 10,benchmarkRounds = 10)
    @Test
    public void testFlyweight() {
            IntIteratorFlyweight i = new IntIteratorFlyweight();
            i.wrap(rb);
            while (i.hasNext()) {
                result += i.next();
            }
    }


   @AfterClass
    public static void result() throws IOException {
        System.out.println("Ignore: "+result);
    }

    @BeforeClass
    public static void prepare() throws IOException {
        rb = new RoaringBitmap();
        for(int k = 0; k < (1<<30); k+= 32)
            rb.add(k);
     }


}
