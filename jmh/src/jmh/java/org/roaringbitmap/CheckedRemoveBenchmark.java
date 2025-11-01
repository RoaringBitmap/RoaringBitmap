package org.roaringbitmap;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 3, timeUnit = TimeUnit.MILLISECONDS, time = 2000)
@Measurement(iterations = 5, timeUnit = TimeUnit.MILLISECONDS, time = 5000)
@OutputTimeUnit(TimeUnit.SECONDS)
public class CheckedRemoveBenchmark {

  private static final int VALUES_COUNT = 10_000_000;

  @Param({
      "HUNDRED_IN_ONE_RUN",
      "THOUSAND_IN_ONE_RUN",
      "ALL_IN_ONE_RUN"
  })
  public InputDataType inputData;

  @Param({"RUN", "BITMAP"})
  public ContainerType containerType;

  public RoaringBitmap bitmap;

  public int[] a;

  public enum InputDataType {
    @SuppressWarnings("unused")
    THOUSAND_IN_ONE_RUN {
      @Override
      public int getValue(int i) {
        return i / 1000 + i;
      }
    },
    @SuppressWarnings("unused")
    HUNDRED_IN_ONE_RUN {
      @Override
      public int getValue(int i) {
        return i / 100 + i;
      }
    },
    @SuppressWarnings("unused")
    ALL_IN_ONE_RUN {
      @Override
      public int getValue(int i) {
        return i;
      }
    };

    protected abstract int getValue(int i);

    public int[] getValues() {
      int[] a = new int[VALUES_COUNT];
      for (int i = 0; i < VALUES_COUNT; i++) {
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

  @Setup(Level.Trial)
  public void prepareArray() {
    a = inputData.getValues();
  }

  @Setup(Level.Invocation)
  public void createBitmap() {
    bitmap = containerType.createBitmap(a);
  }

  @Benchmark
  public void optimized(Blackhole blackhole) {
    boolean contains = false;
    for (int i = 0; i < VALUES_COUNT; i++) {
      contains ^= bitmap.checkedRemove(a[i]);
    }
    blackhole.consume(contains);
  }

  @Benchmark
  public void original(Blackhole blackhole) {
    boolean contains = false;
    for (int i = 0; i < VALUES_COUNT; i++) {
      contains ^= checkedRemoveOriginal(a[i]);
    }
    blackhole.consume(contains);
  }

  public boolean checkedRemoveOriginal(final int x) {
    final char hb = Util.highbits(x);
    final int i = bitmap.highLowContainer.getIndex(hb);
    if (i < 0) {
      return false;
    }
    Container C = bitmap.highLowContainer.getContainerAtIndex(i);
    int oldcard = C.getCardinality();
    C = C.remove(Util.lowbits(x));
    int newcard = C.getCardinality();
    if (newcard == oldcard) {
      return false;
    }
    if (newcard > 0) {
      bitmap.highLowContainer.setContainerAtIndex(i, C);
    } else {
      bitmap.highLowContainer.removeAtIndex(i);
    }
    return true;
  }
}
