package org.roaringbitmap.realdata;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RealDataBenchmarkWideAndNaiveTest extends RealDataBenchmarkSanityTest {

  @Override
  protected void doTest(String dataset, String type, boolean immutable) {
    RealDataBenchmarkWideAndNaive bench = new RealDataBenchmarkWideAndNaive();
    assertEquals(0, bench.wideAnd_naive(bs));
  }
}
