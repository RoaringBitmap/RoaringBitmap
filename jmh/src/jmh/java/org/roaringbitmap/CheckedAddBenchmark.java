package org.roaringbitmap;

import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 3, timeUnit = TimeUnit.MILLISECONDS, time = 2000)
@Measurement(iterations = 5, timeUnit = TimeUnit.MILLISECONDS, time = 2000)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class CheckedAddBenchmark {

  private static final int valuesCount = 10_000_000;
  @Param({"HALF_DUPLICATED", "UNIQUE"})
  public String inputData;

  @Param({"RUN", "BITMAP"})
  public String containerType;

  public RoaringBitmap bitmap;

  public int[] a;

  @Setup(Level.Invocation)
  public void setUp() {
    a = new int[valuesCount];
    if (inputData.equals("HALF_DUPLICATED")) {
      for (int i = 0; i < valuesCount; i++) {
        a[i] = i >> 2;
      }
    }
    if (inputData.equals("UNIQUE")) {
      for (int i = 0; i < valuesCount; i++) {
        a[i] = i;
      }
    }
    bitmap = RoaringBitmap.bitmapOf(a);
    if (containerType.equals("RUN")) {
      bitmap.runOptimize();
      if (!(bitmap.getContainerPointer().getContainer() instanceof RunContainer)) {
        throw new IllegalStateException("Container is not run for " + inputData + " " + containerType);
      }
    } else {
      if (!(bitmap.getContainerPointer().getContainer() instanceof BitmapContainer)) {
        throw new IllegalStateException("Container is not bitmap for " + inputData + " " + containerType);
      }
    }
  }

  @Benchmark
  public boolean optimized() {
    boolean contains = false;
    for (int i = 0; i < valuesCount; i++) {
      contains ^= bitmap.checkedAdd(a[i]);
    }
    return contains;
  }

  @Benchmark
  public boolean original() {
    boolean contains = false;
    for (int i = 0; i < valuesCount; i++) {
      contains ^= checkedAddOriginal(a[i]);
    }
    return contains;
  }

  public boolean checkedAddOriginal(final int x) {
    final char hb = Util.highbits(x);
    final int i = bitmap.highLowContainer.getIndex(hb);
    if (i >= 0) {
      Container c = bitmap.highLowContainer.getContainerAtIndex(i);
      int oldCard = c.getCardinality();
      // we need to keep the newContainer if a switch between containers type
      // occur, in order to get the new cardinality
      Container newCont = c.add(Util.lowbits(x));
      bitmap.highLowContainer.setContainerAtIndex(i, newCont);
      if (newCont.getCardinality() > oldCard) {
        return true;
      }
    } else {
      final ArrayContainer newac = new ArrayContainer();
      bitmap.highLowContainer.insertNewKeyValueAt(-i - 1, hb, newac.add(Util.lowbits(x)));
      return true;
    }
    return false;
  }
}