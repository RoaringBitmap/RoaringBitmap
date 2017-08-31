package org.roaringbitmap;

import static org.roaringbitmap.RealDataset.CENSUS1881;
import static org.roaringbitmap.RealDataset.CENSUS1881_SRT;
import static org.roaringbitmap.RealDataset.CENSUS_INCOME;
import static org.roaringbitmap.RealDataset.CENSUS_INCOME_SRT;
import static org.roaringbitmap.RealDataset.DIMENSION_003;
import static org.roaringbitmap.RealDataset.DIMENSION_008;
import static org.roaringbitmap.RealDataset.DIMENSION_033;
import static org.roaringbitmap.RealDataset.USCENSUS2000;
import static org.roaringbitmap.RealDataset.WEATHER_SEPT_85;
import static org.roaringbitmap.RealDataset.WEATHER_SEPT_85_SRT;
import static org.roaringbitmap.RealDataset.WIKILEAKS_NOQUOTES;
import static org.roaringbitmap.RealDataset.WIKILEAKS_NOQUOTES_SRT;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;

import org.ehcache.sizeof.SizeOf;
import org.ehcache.sizeof.impl.AgentSizeOf;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.zaxxer.sparsebits.SparseBitSet;

import it.uniroma3.mat.extendedset.intset.ConciseSet;

@RunWith(Parameterized.class)
public class RealDataMemoryBenchmark {

  private static final SizeOf sizeOf = AgentSizeOf.newInstance(false, true);

  @Parameters(name = "{0}")
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][] {{CENSUS_INCOME}, {CENSUS1881}, {DIMENSION_008},
        {DIMENSION_003}, {DIMENSION_033}, {USCENSUS2000}, {WEATHER_SEPT_85}, {WIKILEAKS_NOQUOTES},
        {CENSUS_INCOME_SRT}, {CENSUS1881_SRT}, {WEATHER_SEPT_85_SRT}, {WIKILEAKS_NOQUOTES_SRT}});
  }

  @Parameter
  public String dataset;

  @Test
  public void benchmark() throws Exception {
    ZipRealDataRetriever dataRetriever = new ZipRealDataRetriever(dataset);

    long optiSize = 0;
    long conciseSize = 0;
    long wahSize = 0;
    long sparseSize = 0;
    long bitsetSize = 0;

    for (int[] data : dataRetriever.fetchBitPositions()) {
      RoaringBitmap basic = RoaringBitmap.bitmapOf(data);
      basic.runOptimize();
      ConciseSet concise = toConcise(data);
      ConciseSet w = toWAH(data);
      SparseBitSet ss = toSparseBitSet(data);
      optiSize += sizeOf.deepSizeOf(basic);
      conciseSize += sizeOf.deepSizeOf(concise);
      wahSize += sizeOf.deepSizeOf(w);
      sparseSize += sizeOf.deepSizeOf(ss);
      bitsetSize += sizeOf.deepSizeOf(toBitSet(data));
    }

    System.out.println();
    System.out.println("==============");
    System.out
        .println(dataset + " / bitset size = " + RealDataDetailedBenchmark.humanReadable(bitsetSize)
            + " / Roaring size = " + RealDataDetailedBenchmark.humanReadable(optiSize)
            + " / concise size = " + RealDataDetailedBenchmark.humanReadable(conciseSize)
            + " / WAH size = " + RealDataDetailedBenchmark.humanReadable(wahSize)
            + " / SparseBitSet size = " + RealDataDetailedBenchmark.humanReadable(sparseSize));
    System.out.println("==============");
  }

  private static BitSet toBitSet(int[] dat) {
    BitSet ans = new BitSet();
    for (int i : dat) {
      ans.set(i);
    }
    return ans;
  }

  private static ConciseSet toConcise(int[] dat) {
    ConciseSet ans = new ConciseSet();
    for (int i : dat) {
      ans.add(i);
    }
    return ans;
  }

  public SparseBitSet toSparseBitSet(int[] dat) {
    SparseBitSet r = new SparseBitSet();
    for (int i : dat) {
      r.set(i);
    }
    return r;
  }

  private static ConciseSet toWAH(int[] dat) {
    ConciseSet ans = new ConciseSet(true);
    for (int i : dat) {
      ans.add(i);
    }
    return ans;
  }


}
