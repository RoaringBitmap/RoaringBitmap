package org.roaringbitmap;

import net.sourceforge.sizeof.SizeOf;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import java.text.*;

import java.util.Arrays;
import java.util.Collection;

import static org.roaringbitmap.RealDataset.*;

@RunWith(Parameterized.class)
public class RealDataDetailedBenchmark {

    @Parameters(name = "{0}")
    public static Collection<Object[]> params() {
        return Arrays.asList(
                new Object[][] {
                        { CENSUS_INCOME },
                        { CENSUS1881 },
                        { DIMENSION_008 },
                        { DIMENSION_003 },
                        { DIMENSION_033 },
                        { USCENSUS2000 },
                        { WEATHER_SEPT_85 },
                        { WIKILEAKS_NOQUOTES },
                        { CENSUS_INCOME_SRT },
                        { CENSUS1881_SRT },
                        { WEATHER_SEPT_85_SRT },
                        { WIKILEAKS_NOQUOTES_SRT }
                });
    }

    @Parameter
    public String dataset;
    
    public static int getContainerCountByName(RoaringBitmap rb, String containername) {
      ContainerPointer  cp = rb.getContainerPointer();
      Container c = null;
      int answer = 0;
      while( (c = cp.getContainer()) != null) {
        if(c.getContainerName().equals(containername)) ++ answer;
        cp.advance();
      }
      return answer;
    }
    

    public static int getContainerCardinalityByName(RoaringBitmap rb, String containername) {
      ContainerPointer  cp = rb.getContainerPointer();
      Container c = null;
      int answer = 0;
      while( (c = cp.getContainer()) != null) {
        if(c.getContainerName().equals(containername))  
          answer += c.getCardinality();
        cp.advance();
      }
      return answer;
    }

    public static int getContainerSizeInBytesByName(RoaringBitmap rb, String containername) {
      ContainerPointer  cp = rb.getContainerPointer();
      Container c = null;
      int answer = 0;
      while( (c = cp.getContainer()) != null) {
        if(c.getContainerName().equals(containername))  
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
        
        String containerNames[] = {"bitmap","array","run"};

        long basicCount[] = {0,0,0};
        long basicCardinality[] = {0,0,0};
        long basicSizeInBytes[] = {0,0,0};
        long optiCount[] = {0,0,0};
        long optiCardinality[] = {0,0,0};
        long optiSizeInBytes[] = {0,0,0};

        for (int[] data : dataRetriever.fetchBitPositions()) {
            RoaringBitmap basic = RoaringBitmap.bitmapOf(data);
            RoaringBitmap opti = basic.clone();
            opti.runOptimize();

            basicSize += SizeOf.deepSizeOf(basic);
            optiSize += SizeOf.deepSizeOf(opti);
            for(int i = 0 ; i < containerNames.length; ++i) {
              basicCount[i] += getContainerCountByName(basic,containerNames[i]);
              optiCount[i] += getContainerCountByName(opti,containerNames[i]);
              basicCardinality[i] += getContainerCardinalityByName(basic,containerNames[i]);
              optiCardinality[i] += getContainerCardinalityByName(opti,containerNames[i]);
              basicSizeInBytes[i] += getContainerSizeInBytesByName(basic,containerNames[i]);
              optiSizeInBytes[i] += getContainerSizeInBytesByName(opti,containerNames[i]);

            }
        }
        NumberFormat percentFormat = NumberFormat.getPercentInstance();
        percentFormat.setMaximumFractionDigits(1);
        System.out.println();
        System.out.println("==============");
        System.out.println(dataset + " / Run size = " + SizeOf.humanReadable(optiSize)
                                   + " / normal size = " + SizeOf.humanReadable(basicSize));
        System.out.println("-- Roaring without runs data");
        System.out.println("  Memory usage : "+SizeOf.humanReadable(basicSize));
        
        System.out.print("  container counts:");
        for(int i = 0 ; i < containerNames.length; ++i) {
          System.out.print(containerNames[i]+":"+basicCount[i]+" ("+percentFormat.format(percentage(basicCount,i))+")\t\t");
        }
        System.out.println();

        System.out.print("  container cardinality:");
        for(int i = 0 ; i < containerNames.length; ++i) {
          System.out.print(containerNames[i]+":"+basicCardinality[i]+" ("+percentFormat.format(percentage(basicCardinality,i))+")\t\t");
        }
        System.out.println();

        System.out.print("  container size in bytes:");
        for(int i = 0 ; i < containerNames.length; ++i) {
          System.out.print(containerNames[i]+":"+basicSizeInBytes[i]+" ("+percentFormat.format(percentage(basicSizeInBytes,i))+")\t\t");
        }
        System.out.println();
        
        System.out.println("-- Roaring+Run data");
        System.out.println("  Memory usage : "+SizeOf.humanReadable(optiSize));
        System.out.print("  container counts:");
        for(int i = 0 ; i < containerNames.length; ++i) {
          System.out.print(containerNames[i]+":"+optiCount[i]+" ("+percentFormat.format(percentage(optiCount,i))+")\t\t");
        }
        System.out.println();

        System.out.print("  container cardinality:");
        for(int i = 0 ; i < containerNames.length; ++i) {
          System.out.print(containerNames[i]+":"+optiCardinality[i]+" ("+percentFormat.format(percentage(optiCardinality,i))+")\t\t");
        }
        System.out.println();

        System.out.print("  container size in bytes:");
        for(int i = 0 ; i < containerNames.length; ++i) {
          System.out.print(containerNames[i]+":"+optiSizeInBytes[i]+" ("+percentFormat.format(percentage(optiSizeInBytes,i))+")\t\t");
        }
        System.out.println();

        
        System.out.println("==============");
    }
    
    public static double percentage(long[] array, int i) {
      long sum = 0;
      for (int j = 0; j < array.length; ++j)
        sum += array[j];
      return array[i] * 1.0 / sum;
    }


}
