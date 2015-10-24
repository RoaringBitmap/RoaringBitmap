package org.roaringbitmap.realdata;

import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class RealDataBenchmarkWideAndNaiveTest extends RealDataBenchmarkSanityTest {

    @Test
    public void test() throws Exception {
        RealDataBenchmarkWideAndNaive bench = new RealDataBenchmarkWideAndNaive();
        assertEquals(0, bench.wideAnd_naive(bs));
    }

}
