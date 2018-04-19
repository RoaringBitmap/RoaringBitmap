package org.roaringbitmap.allocations;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.roaringbitmap.FastAggregation;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.IntIteratorFlyweight;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RealDataset;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.ZipRealDataRetriever;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;

public class IterationAllocationsTest extends AllocationSuite {

  private static RoaringBitmap bitmap_a;

  @BeforeClass
  public static void loadBitmaps() throws IOException, URISyntaxException {
    final int[] data = takeSortedAndDistinct(new Random(0xcb000a2b9b5bdfb6l), 100000);
    bitmap_a = RoaringBitmap.bitmapOf(data);
  }

  @Test
  public void measureBoxedIterationAllocation() {
    printAllocations(() -> {
      Iterator<Integer> intIterator = bitmap_a.iterator();
      long result = 0;
      while (intIterator.hasNext()) {
        result += intIterator.next();

      }
      // A small check for iterator consistency
      Assert.assertEquals(407, result % 1024);
    });
  }

  @Test
  public void measureStandardIterationAllocation() {
    printAllocations(() -> {
      IntIterator intIterator = bitmap_a.getIntIterator();
      long result = 0;
      while (intIterator.hasNext()) {
        result += intIterator.next();

      }
      // A small check for iterator consistency
      Assert.assertEquals(407, result % 1024);
    });
  }


  @Test
  public void measureFlyWeightIterationAllocation() {
    printAllocations(() -> {
      IntIteratorFlyweight intIterator = new IntIteratorFlyweight();
      intIterator.wrap(bitmap_a);

      long result = 0;
      while (intIterator.hasNext()) {
        result += intIterator.next();

      }
      // A small check for iterator consistency
      Assert.assertEquals(407, result % 1024);
    });
  }

  @Before
  public void preloadClasspath() {
    //warm up, initialize class path
    nop(() ->{
      Iterator<Integer> iter = bitmap_a.iterator();
      PeekableIntIterator intIter = bitmap_a.getIntIterator();
      iter.hasNext();
      iter.next();
      IntIteratorFlyweight intIterator = new IntIteratorFlyweight();
      intIterator.wrap(bitmap_a);
    });
  }

  private static int[] takeSortedAndDistinct(Random source, int count) {

    LinkedHashSet<Integer> ints = new LinkedHashSet<Integer>(count);

    for (int size = 0; size < count; size++) {
      int next;
      do {
        next = Math.abs(source.nextInt());
      } while (!ints.add(next));
    }

    int[] unboxed = toArray(ints);
    Arrays.sort(unboxed);
    return unboxed;
  }

  private static int[] toArray(LinkedHashSet<Integer> integers) {
    int[] ints = new int[integers.size()];
    int i = 0;
    for (Integer n : integers) {
      ints[i++] = n;
    }
    return ints;
  }

}
