package org.roaringbitmap;
import org.ehcache.sizeof.SizeOf;
import org.ehcache.sizeof.impl.AgentSizeOf;
import java.util.*;
import org.junit.Test;

/**
* Bitmaps (even compressed bitmaps) are not useful in the "sparse" scenario where
* we have a few random looking integers within a large range of values. One should
* use a simple array or another data structure.
*/
public class AdversarialMemoryUsage {
  private static final SizeOf sizeOf = AgentSizeOf.newInstance(false, true);

  @Test
  public void benchmark()  {
    int N = 200;
    int setsize = 1000;
    long normalSize = 0;
    long optiSize = 0;
    long arraySize = 0;
    long estimated = 0;
    long estimatedopti = 0;
    long valueCount = 0;
    System.out.println("Running adversarial use case.");
    for (int k = 0; k < setsize; k++) {
      int[] array = new int[N];
      Random r = new Random();
      for(int m = 0; m < N; m++) {
            array[m] = r.nextInt(); // collision are possible but unlikely
      }
      RoaringBitmap basic = RoaringBitmap.bitmapOf(array);
      long thiscard = basic.getLongCardinality();
      valueCount += thiscard;
      normalSize += sizeOf.deepSizeOf(basic);
      estimated += basic.getLongSizeInBytes();

      basic.runOptimize();
      basic.trim();
      estimatedopti += basic.getLongSizeInBytes();
      optiSize += sizeOf.deepSizeOf(basic);
      arraySize += sizeOf.deepSizeOf(array);
    }
    System.out.println("roaring bits/value = "+normalSize * 8.0 / valueCount);
    System.out.println("roaring bits/value [estimated] = "+estimated * 8.0 / valueCount);

    System.out.println("roaring (run) bits/value = "+optiSize * 8.0 / valueCount);
    System.out.println("roaring bits (run)/value [estimated] = "+estimatedopti * 8.0 / valueCount);

    System.out.println("Ratio measured/estimated = "+optiSize * 1.0 / estimatedopti);

    System.out.println("array bits/value = "+arraySize * 8.0 / valueCount);

  }


    @Test
    public void bad_benchmark()  {
      int N = 1;
      int setsize = 1000;
      long normalSize = 0;
      long optiSize = 0;
      long arraySize = 0;
      long estimated = 0;
      long estimatedopti = 0;
      long valueCount = 0;
      System.out.println("Running adversarial use case.");
      for (int k = 0; k < setsize; k++) {
        int[] array = new int[N];
        Random r = new Random();
        for(int m = 0; m < N; m++) {
              array[m] = r.nextInt(); // collision are possible but unlikely
        }
        RoaringBitmap basic = RoaringBitmap.bitmapOf(array);
        long thiscard = basic.getLongCardinality();
        valueCount += thiscard;
        normalSize += sizeOf.deepSizeOf(basic);
        estimated += basic.getLongSizeInBytes();

        basic.runOptimize();
        basic.trim();
        estimatedopti += basic.getLongSizeInBytes();
        optiSize += sizeOf.deepSizeOf(basic);
        arraySize += sizeOf.deepSizeOf(array);
      }
      System.out.println("roaring bits/value = "+normalSize * 8.0 / valueCount);
      System.out.println("roaring bits/value [estimated] = "+estimated * 8.0 / valueCount);

      System.out.println("roaring (run) bits/value = "+optiSize * 8.0 / valueCount);
      System.out.println("roaring bits (run)/value [estimated] = "+estimatedopti * 8.0 / valueCount);

      System.out.println("Ratio measured/estimated = "+optiSize * 1.0 / estimatedopti);

      System.out.println("array bits/value = "+arraySize * 8.0 / valueCount);

    }
}
