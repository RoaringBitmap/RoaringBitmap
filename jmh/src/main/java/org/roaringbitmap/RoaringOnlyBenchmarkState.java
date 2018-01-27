package org.roaringbitmap;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import static org.roaringbitmap.realdata.wrapper.BitmapFactory.*;

@State(Scope.Benchmark)
public abstract class RoaringOnlyBenchmarkState {

  public List<RoaringBitmap> bitmaps;
  public List<RoaringBitmap> onlyArrayContainers;
  public List<RoaringBitmap> onlyRunContainers;
  public List<RoaringBitmap> onlyBitmapContainers;

  // All tests relying on the same dataset should be run consecutively: the cache will maintain in
  // memory the associated int arrays
  private static final Cache<String, List<int[]>> DATASET_CACHE =
      CacheBuilder.newBuilder().maximumSize(1).build();

  public void setup(final String dataset) throws Exception {

    bitmaps = new ArrayList<RoaringBitmap>();
    onlyArrayContainers = new ArrayList<RoaringBitmap>();
    onlyRunContainers = new ArrayList<RoaringBitmap>();
    onlyBitmapContainers = new ArrayList<RoaringBitmap>();

    List<int[]> ints = DATASET_CACHE.get(dataset, new Callable<List<int[]>>() {

      @Override
      public List<int[]> call() throws Exception {
        System.out.println("Loading" + dataset);
        ZipRealDataRetriever dataRetriever = new ZipRealDataRetriever(dataset);
        return Lists.newArrayList(dataRetriever.fetchBitPositions());
      }
    });

    for (int[] data : ints) {
      RoaringBitmap roaring = RoaringBitmap.bitmapOf(data);
      roaring.runOptimize();
      bitmaps.add(roaring);

    }
    //Add bitmaps with only RunContainers preserved
    for (RoaringBitmap rb : bitmaps) {
      RoaringBitmap runOnly = new RoaringBitmap();
      ContainerPointer cp = rb.getContainerPointer();
      while (cp.getContainer() != null) {
        if (cp.isRunContainer()) {
          runOnly.highLowContainer.append(cp.key(), cp.getContainer());
        }
        cp.advance();
      }
      onlyRunContainers.add(runOnly);
    }

    //Add bitmaps with only ArrayContainers preserved
    for (RoaringBitmap rb : bitmaps) {
      RoaringBitmap clone = rb.clone();
      RoaringBitmap arrayOnly = new RoaringBitmap();
      clone.removeRunCompression(); //more containers preserved
      ContainerPointer cp = clone.getContainerPointer();
      while (cp.getContainer() != null) {
        if (!cp.isBitmapContainer()) {
          arrayOnly.highLowContainer.append(cp.key(), cp.getContainer());
        }
        cp.advance();
      }
      onlyArrayContainers.add(arrayOnly);
    }

    //Add bitmaps with only BitmapContainers preserved
    for (RoaringBitmap rb : bitmaps) {
      RoaringBitmap clone = rb.clone();
      RoaringBitmap bitmapOnly = new RoaringBitmap();
      clone.removeRunCompression(); //more containers preserved
      ContainerPointer cp = clone.getContainerPointer();
      while (cp.getContainer() != null) {
        if (cp.isBitmapContainer()) {
          bitmapOnly.highLowContainer.append(cp.key(), cp.getContainer());
        }
        cp.advance();
      }
      onlyBitmapContainers.add(bitmapOnly);
    }
  }

  @TearDown
  public void tearDown() {
  }

}
