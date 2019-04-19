package org.roaringbitmap;

import static org.roaringbitmap.Util.toIntUnsigned;

public final class RunBatchIterator implements ContainerBatchIterator {

  private RunContainer runs;
  private int run = 0;
  private int cursor = 0;

  public RunBatchIterator(RunContainer runs) {
    wrap(runs);
  }

  @Override
  public int next(int key, int[] buffer) {
    int consumed = 0;
    do {
      int runStart = toIntUnsigned(runs.getValue(run));
      int runLength = toIntUnsigned(runs.getLength(run));
      int chunkStart = runStart + cursor;
      int chunkEnd = chunkStart + Math.min(runLength - cursor, buffer.length - consumed - 1);
      int chunk = chunkEnd - chunkStart + 1;
      for (int i = 0; i < chunk; ++i) {
        buffer[consumed + i] = key + chunkStart + i;
      }
      consumed += chunk;
      if (runStart + runLength == chunkEnd) {
        ++run;
        cursor = 0;
      } else {
        cursor += chunk;
      }
    } while (consumed < buffer.length && run != runs.numberOfRuns());
    return consumed;
  }

  @Override
  public boolean hasNext() {
    return run < runs.numberOfRuns();
  }

  @Override
  public ContainerBatchIterator clone() {
    try {
      return (ContainerBatchIterator)super.clone();
    } catch (CloneNotSupportedException e) {
      // won't happen
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void releaseContainer() {
    runs = null;
  }

  void wrap(RunContainer runs) {
    this.runs = runs;
    this.run = 0;
    this.cursor = 0;
  }
}
