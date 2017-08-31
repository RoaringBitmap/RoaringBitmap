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

import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Collection;

import org.ehcache.sizeof.SizeOf;
import org.ehcache.sizeof.impl.AgentSizeOf;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class RealDataDetailedBenchmark {

  private static final SizeOf sizeOf = AgentSizeOf.newInstance(false, true);

  @Parameters(name = "{0}")
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][] {{CENSUS_INCOME}, {CENSUS1881}, {DIMENSION_008},
        {DIMENSION_003}, {DIMENSION_033}, {USCENSUS2000}, {WEATHER_SEPT_85}, {WIKILEAKS_NOQUOTES},
        {CENSUS_INCOME_SRT}, {CENSUS1881_SRT}, {WEATHER_SEPT_85_SRT}, {WIKILEAKS_NOQUOTES_SRT}});
  }

  @Parameter
  public String dataset;

  public static int getContainerCountByName(RoaringBitmap rb, String containername) {
    ContainerPointer cp = rb.getContainerPointer();
    Container c = null;
    int answer = 0;
    while ((c = cp.getContainer()) != null) {
      if (c.getContainerName().equals(containername))
        ++answer;
      cp.advance();
    }
    return answer;
  }


  public static int getContainerCardinalityByName(RoaringBitmap rb, String containername) {
    ContainerPointer cp = rb.getContainerPointer();
    Container c = null;
    int answer = 0;
    while ((c = cp.getContainer()) != null) {
      if (c.getContainerName().equals(containername))
        answer += c.getCardinality();
      cp.advance();
    }
    return answer;
  }

  public static int getContainerSizeInBytesByName(RoaringBitmap rb, String containername) {
    ContainerPointer cp = rb.getContainerPointer();
    Container c = null;
    int answer = 0;
    while ((c = cp.getContainer()) != null) {
      if (c.getContainerName().equals(containername))
        answer += c.serializedSizeInBytes();
      cp.advance();
    }
    return answer;
  }


  @Test
  public void benchmark() throws Exception {
    ZipRealDataRetriever dataRetriever = new ZipRealDataRetriever(dataset);

    long basicSize = 0;
    long optiSize = 0;

    String containerNames[] = {"bitmap", "array", "run"};

    long basicCount[] = {0, 0, 0};
    long basicCardinality[] = {0, 0, 0};
    long basicSizeInBytes[] = {0, 0, 0};
    long optiCount[] = {0, 0, 0};
    long optiCardinality[] = {0, 0, 0};
    long optiSizeInBytes[] = {0, 0, 0};

    for (int[] data : dataRetriever.fetchBitPositions()) {
      RoaringBitmap basic = RoaringBitmap.bitmapOf(data);
      RoaringBitmap opti = basic.clone();
      opti.runOptimize();

      basicSize += sizeOf.deepSizeOf(basic);
      optiSize += sizeOf.deepSizeOf(opti);
      for (int i = 0; i < containerNames.length; ++i) {
        basicCount[i] += getContainerCountByName(basic, containerNames[i]);
        optiCount[i] += getContainerCountByName(opti, containerNames[i]);
        basicCardinality[i] += getContainerCardinalityByName(basic, containerNames[i]);
        optiCardinality[i] += getContainerCardinalityByName(opti, containerNames[i]);
        basicSizeInBytes[i] += getContainerSizeInBytesByName(basic, containerNames[i]);
        optiSizeInBytes[i] += getContainerSizeInBytesByName(opti, containerNames[i]);

      }
    }
    NumberFormat percentFormat = NumberFormat.getPercentInstance();
    percentFormat.setMaximumFractionDigits(1);
    System.out.println();
    System.out.println("==============");
    System.out.println(dataset + " / Run size = " + humanReadable(optiSize) + " / normal size = "
        + humanReadable(basicSize));
    System.out.println("-- Roaring without runs data");
    System.out.println("  Memory usage : " + humanReadable(basicSize));

    System.out.print("  container counts:");
    for (int i = 0; i < containerNames.length; ++i) {
      System.out.print(containerNames[i] + ":" + basicCount[i] + " ("
          + percentFormat.format(percentage(basicCount, i)) + ")\t\t");
    }
    System.out.println();

    System.out.print("  container cardinality:");
    for (int i = 0; i < containerNames.length; ++i) {
      System.out.print(containerNames[i] + ":" + basicCardinality[i] + " ("
          + percentFormat.format(percentage(basicCardinality, i)) + ")\t\t");
    }
    System.out.println();

    System.out.print("  container size in bytes:");
    for (int i = 0; i < containerNames.length; ++i) {
      System.out.print(containerNames[i] + ":" + basicSizeInBytes[i] + " ("
          + percentFormat.format(percentage(basicSizeInBytes, i)) + ")\t\t");
    }
    System.out.println();

    System.out.println("-- Roaring+Run data");
    System.out.println("  Memory usage : " + humanReadable(optiSize));
    System.out.print("  container counts:");
    for (int i = 0; i < containerNames.length; ++i) {
      System.out.print(containerNames[i] + ":" + optiCount[i] + " ("
          + percentFormat.format(percentage(optiCount, i)) + ")\t\t");
    }
    System.out.println();

    System.out.print("  container cardinality:");
    for (int i = 0; i < containerNames.length; ++i) {
      System.out.print(containerNames[i] + ":" + optiCardinality[i] + " ("
          + percentFormat.format(percentage(optiCardinality, i)) + ")\t\t");
    }
    System.out.println();

    System.out.print("  container size in bytes:");
    for (int i = 0; i < containerNames.length; ++i) {
      System.out.print(containerNames[i] + ":" + optiSizeInBytes[i] + " ("
          + percentFormat.format(percentage(optiSizeInBytes, i)) + ")\t\t");
    }
    System.out.println();


    System.out.println("==============");
  }

  public static String humanReadable(long optiSize) {
    if (optiSize < 1024) {
      return optiSize + "b";
    } else if (optiSize < 1024 * 1024) {
      return (optiSize / 1024) + "Kb";
    } else {
      return (optiSize / (1024 * 1024)) + "Mb";
    }
  }

  public static double percentage(long[] array, int i) {
    long sum = 0;
    for (int j = 0; j < array.length; ++j)
      sum += array[j];
    return array[i] * 1.0 / sum;
  }
}
