package org.roaringbitmap;

public class OrderedWriters {

  public static OrderedWriter denseWriter(final RoaringBitmap roaringBitmap) {
    return new DenseOrderedWriter(roaringBitmap);
  }

  public static OrderedWriter sparseWriter(final RoaringBitmap roaringBitmap) {
    return new SparseOrderedWriter(roaringBitmap);
  }

  public static OrderedWriter create(final RoaringBitmap roaringBitmap) {
    return denseWriter(roaringBitmap);
  }
}
