package org.roaringbitmap;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.roaringbitmap.allocations.AllocationSuite;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class AggregationAllocationsTest extends AllocationSuite {

  private static List<List<RoaringBitmap>> datasets = new ArrayList<>();

  @Before
  public void preloadClasspath() {
    //warm up, initialize class path
    Iterator<RoaringBitmap> iterator = datasets.get(0).iterator();
    nop(() -> FastAggregation.and(iterator));
    nop(() -> {
      for (List<RoaringBitmap> bitmaps : datasets) {
        for (int i = 0; i + 1 < bitmaps.size(); i += 2) {
          RoaringBitmap.andNot(bitmaps.get(i), bitmaps.get(i + 1));
        }
      }
    });
  }

  @BeforeClass
  public static void loadBitmaps() throws IOException, URISyntaxException {
    for (String dataset : RealDataset.ALL) {
      List<RoaringBitmap> localBitmaps = new ArrayList<>();
      new ZipRealDataRetriever(dataset).fetchBitPositions().forEach((data) -> {
            RoaringBitmap bitmap = RoaringBitmap.bitmapOf(data);
            bitmap.trim();
            bitmap.runOptimize();
            localBitmaps.add(bitmap);
          }
      );
      datasets.add(localBitmaps);
    }
  }

  @Test
  public void andNotAllocations() {
    printAllocations(() -> {
      for (List<RoaringBitmap> bitmaps : datasets) {
        for (int i = 0; i + 1 < bitmaps.size(); i += 2) {
          RoaringBitmap.andNot(bitmaps.get(i), bitmaps.get(i + 1));
        }
      }
    });
  }

  @Test
  public void fastOrAllocations() {
    printAllocations(() -> {
      for (List<RoaringBitmap> bitmaps : datasets) {
        FastAggregation.or(bitmaps.iterator());
      }
    });
  }


  @Test
  public void fastAndAllocations() {
    printAllocations(() -> {
      for (List<RoaringBitmap> bitmaps : datasets) {
        FastAggregation.and(bitmaps.iterator());
      }
    });
  }

}
