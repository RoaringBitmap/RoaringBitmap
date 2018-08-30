package org.roaringbitmap;

public class AdaptiveOrderedWriter implements OrderedWriter {
  private SparseOrderedWriter sparseWriter;
  private DenseOrderedWriter denseWriter;
  private boolean sparse;

  public AdaptiveOrderedWriter(RoaringBitmap underlying) {
    this.sparseWriter = new SparseOrderedWriter(underlying);
    this.sparse = true;
  }

  AdaptiveOrderedWriter(RoaringBitmap underlying, int size) {
    this.sparseWriter = new SparseOrderedWriter(underlying, size);
    this.sparse = true;
  }

  @Override
  public void add(int value) {
    if (sparse) {
      try {
        sparseWriter.add(value);
      } catch (IllegalStateException e) {
        denseWriter = sparseWriter.transfer(denseWriter);
        denseWriter.add(value);
        sparse = false;
      }
    } else {
      denseWriter.add(value);
      if (!denseWriter.isDirty()) {
        sparse = true;
      }
    }
  }

  @Override
  public void flush() {
    if (sparse) {
      sparseWriter.flush();
    } else {
      denseWriter.flush();
    }
  }

  @Override
  public boolean isDirty() {
    if (sparse) {
      return sparseWriter.isDirty();
    } else {
      return denseWriter.isDirty();
    }
  }

  @Override
  public void clear() {
    sparseWriter.clear();
    denseWriter.clear();
  }
}
