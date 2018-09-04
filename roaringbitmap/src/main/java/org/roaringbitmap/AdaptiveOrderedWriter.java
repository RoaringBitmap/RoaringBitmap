package org.roaringbitmap;

public class AdaptiveOrderedWriter implements OrderedWriter {
  private SparseOrderedWriter sparseWriter;
  private DenseOrderedWriter denseWriter;
  private boolean sparse;

  public AdaptiveOrderedWriter(RoaringBitmap underlying) {
    this.sparseWriter = new SparseOrderedWriter(underlying);
    this.sparse = true;
  }

  public AdaptiveOrderedWriter(RoaringBitmap underlying, int size) {
    this.sparseWriter = new SparseOrderedWriter(underlying, size);
    this.sparse = true;
  }

  @Override
  public void add(int value) {
    if (sparse) {
      try {
        sparseWriter.add(value);
      } catch (IndexOutOfBoundsException e) {
        denseWriter = sparseWriter.transfer();
        denseWriter.add(value);
        sparse = false;
      }
    } else {
      denseWriter.add(value);
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

}
