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
  public InputDataType inputData;

  @Param({"RUN", "BITMAP"})
  public ContainerType containerType;

  public RoaringBitmap bitmap;

  public int[] a;

  public enum InputDataType {
    @SuppressWarnings("unused")
    HALF_DUPLICATED {
      @Override
      public int getValue(int i) {
        // leads to pairs of same value, resulting bitmap has half size, it does not affect
        // benchmark results
        return i >> 2;
      }
    },
    @SuppressWarnings("unused")
    UNIQUE {
      @Override
      public int getValue(int i) {
        return i;
      }
    };

    protected abstract int getValue(int i);

    public int[] getValues() {
      int[] a = new int[valuesCount];
      for (int i = 0; i < valuesCount; i++) {
        a[i] = getValue(i);
      }
      return a;
    }
  }

  public enum ContainerType {
    @SuppressWarnings("unused")
    RUN {
      @Override
      public RoaringBitmap createBitmap(int[] values) {
        RoaringBitmap bitmap = RoaringBitmap.bitmapOf(values);
        bitmap.runOptimize();
        if (!(bitmap.getContainerPointer().getContainer() instanceof RunContainer)) {
          throw new IllegalStateException("Container is not run!!!");
        }
        return bitmap;
      }
    },
    @SuppressWarnings("unused")
    BITMAP {
      @Override
      public RoaringBitmap createBitmap(int[] values) {
        RoaringBitmap bitmap = RoaringBitmap.bitmapOf(values);
        if (!(bitmap.getContainerPointer().getContainer() instanceof BitmapContainer)) {
          throw new IllegalStateException("Container is not bitmap!!!");
        }
        return bitmap;
      }
    };

    public abstract RoaringBitmap createBitmap(int[] values);
  }

  @Setup(Level.Invocation)
  public void setUp() {
    a = inputData.getValues();
    bitmap = containerType.createBitmap(a);
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

      //noinspection RedundantIfStatement
      if (newCont.getCardinality() > oldCard) {
        return true;
      }
    } else {
      @SuppressWarnings("SpellCheckingInspection") final ArrayContainer newac =
          new ArrayContainer();
      bitmap.highLowContainer.insertNewKeyValueAt(-i - 1, hb, newac.add(Util.lowbits(x)));
      return true;
    }
    return false;
  }
}