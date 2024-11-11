package org.roaringbitmap;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

@State(Scope.Benchmark)
public abstract class RoaringOnlyBenchmarkState {

  public List<RoaringBitmap> bitmaps;
  public List<RoaringBitmap> onlyArrayContainers;
  public List<RoaringBitmap> onlyRunContainers;
  public List<RoaringBitmap> onlyBitmapContainers;

  public List<ImmutableRoaringBitmap> immutableBitmaps;
  public List<ImmutableRoaringBitmap> immutableOnlyArrayContainers;
  public List<ImmutableRoaringBitmap> immutableOnlyRunContainers;
  public List<ImmutableRoaringBitmap> immutableOnlyBitmapContainers;

  // All tests relying on the same dataset should be run consecutively: the cache will maintain in
  // memory the associated int arrays
  private static final Cache<String, List<int[]>> DATASET_CACHE =
      CacheBuilder.newBuilder().maximumSize(1).build();

  public void setup(final String dataset) throws Exception {

    bitmaps = new ArrayList<>();
    onlyArrayContainers = new ArrayList<>();
    onlyRunContainers = new ArrayList<>();
    onlyBitmapContainers = new ArrayList<>();

    immutableBitmaps = new ArrayList<>();
    immutableOnlyArrayContainers = new ArrayList<>();
    immutableOnlyRunContainers = new ArrayList<>();
    immutableOnlyBitmapContainers = new ArrayList<>();

    List<int[]> ints =
        DATASET_CACHE.get(
            dataset,
            new Callable<List<int[]>>() {

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
      immutableBitmaps.add(roaring.toMutableRoaringBitmap());
    }
    // Add bitmaps with only RunContainers preserved
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
      immutableOnlyRunContainers.add(runOnly.toMutableRoaringBitmap());
    }

    // Add bitmaps with only ArrayContainers preserved
    for (RoaringBitmap rb : bitmaps) {
      RoaringBitmap clone = rb.clone();
      RoaringBitmap arrayOnly = new RoaringBitmap();
      clone.removeRunCompression(); // more containers preserved
      ContainerPointer cp = clone.getContainerPointer();
      while (cp.getContainer() != null) {
        if (!cp.isBitmapContainer()) {
          arrayOnly.highLowContainer.append(cp.key(), cp.getContainer());
        }
        cp.advance();
      }
      onlyArrayContainers.add(arrayOnly);
      immutableOnlyArrayContainers.add(arrayOnly.toMutableRoaringBitmap());
    }

    // Add bitmaps with only BitmapContainers preserved
    for (RoaringBitmap rb : bitmaps) {
      RoaringBitmap clone = rb.clone();
      RoaringBitmap bitmapOnly = new RoaringBitmap();
      clone.removeRunCompression(); // more containers preserved
      ContainerPointer cp = clone.getContainerPointer();
      while (cp.getContainer() != null) {
        if (cp.isBitmapContainer()) {
          bitmapOnly.highLowContainer.append(cp.key(), cp.getContainer());
        }
        cp.advance();
      }
      onlyBitmapContainers.add(bitmapOnly);
      immutableOnlyBitmapContainers.add(bitmapOnly.toMutableRoaringBitmap());
    }
  }

  @TearDown
  public void tearDown() {}
}
