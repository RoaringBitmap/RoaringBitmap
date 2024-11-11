package org.roaringbitmap.iteration;

import org.roaringbitmap.IntIterator;
import org.roaringbitmap.IntIteratorFlyweight;
import org.roaringbitmap.ReverseIntIteratorFlyweight;
import org.roaringbitmap.RoaringBitmap;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Created by Borislav Ivanov on 4/2/15.
 */
@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class IteratorsBenchmark32 {

  @Benchmark
  public int testBoxed_a(BenchmarkState benchmarkState) {
    Iterator<Integer> intIterator = benchmarkState.bitmap_a.iterator();
    int result = 0;
    while (intIterator.hasNext()) {
      result = intIterator.next();
    }
    return result;
  }

  @Benchmark
  public int testStandard_a(BenchmarkState benchmarkState) {

    IntIterator intIterator = benchmarkState.bitmap_a.getIntIterator();
    int result = 0;
    while (intIterator.hasNext()) {
      result = intIterator.next();
    }
    return result;
  }

  @Benchmark
  public int testFlyweight_a(BenchmarkState benchmarkState) {

    IntIteratorFlyweight intIterator = benchmarkState.flyweightIterator;

    intIterator.wrap(benchmarkState.bitmap_a);

    int result = 0;
    while (intIterator.hasNext()) {
      result = intIterator.next();
    }
    return result;
  }

  @Benchmark
  public int testBoxed_b(BenchmarkState benchmarkState) {
    Iterator<Integer> intIterator = benchmarkState.bitmap_b.iterator();
    int result = 0;
    while (intIterator.hasNext()) {
      result = intIterator.next();
    }
    return result;
  }

  @Benchmark
  public int testStandard_b(BenchmarkState benchmarkState) {

    IntIterator intIterator = benchmarkState.bitmap_b.getIntIterator();
    int result = 0;
    while (intIterator.hasNext()) {
      result = intIterator.next();
    }
    return result;
  }

  @Benchmark
  public int testFlyweight_b(BenchmarkState benchmarkState) {

    IntIteratorFlyweight intIterator = benchmarkState.flyweightIterator;

    intIterator.wrap(benchmarkState.bitmap_b);

    int result = 0;
    while (intIterator.hasNext()) {
      result = intIterator.next();
    }
    return result;
  }

  @Benchmark
  public int testBoxed_c(BenchmarkState benchmarkState) {
    Iterator<Integer> intIterator = benchmarkState.bitmap_c.iterator();
    int result = 0;
    while (intIterator.hasNext()) {
      result = intIterator.next();
    }
    return result;
  }

  @Benchmark
  public int testStandard_c(BenchmarkState benchmarkState) {

    IntIterator intIterator = benchmarkState.bitmap_c.getIntIterator();
    int result = 0;
    while (intIterator.hasNext()) {
      result = intIterator.next();
    }
    return result;
  }

  @Benchmark
  public int testFlyweight_c(BenchmarkState benchmarkState) {

    IntIteratorFlyweight intIterator = benchmarkState.flyweightIterator;

    intIterator.wrap(benchmarkState.bitmap_c);

    int result = 0;
    while (intIterator.hasNext()) {
      result = intIterator.next();
    }
    return result;
  }

  @Benchmark
  public int testReverseStandard_a(BenchmarkState benchmarkState) {

    IntIterator intIterator = benchmarkState.bitmap_a.getReverseIntIterator();
    int result = 0;
    while (intIterator.hasNext()) {
      result = intIterator.next();
    }
    return result;
  }

  @Benchmark
  public int testReverseFlyweight_a(BenchmarkState benchmarkState) {

    ReverseIntIteratorFlyweight intIterator = benchmarkState.flyweightReverseIterator;

    intIterator.wrap(benchmarkState.bitmap_a);

    int result = 0;
    while (intIterator.hasNext()) {
      result = intIterator.next();
    }
    return result;
  }

  @Benchmark
  public int testReverseStandard_b(BenchmarkState benchmarkState) {

    IntIterator intIterator = benchmarkState.bitmap_b.getReverseIntIterator();
    int result = 0;
    while (intIterator.hasNext()) {
      result = intIterator.next();
    }
    return result;
  }

  @Benchmark
  public int testReverseFlyweight_b(BenchmarkState benchmarkState) {

    ReverseIntIteratorFlyweight intIterator = benchmarkState.flyweightReverseIterator;

    intIterator.wrap(benchmarkState.bitmap_b);

    int result = 0;
    while (intIterator.hasNext()) {
      result = intIterator.next();
    }
    return result;
  }

  @Benchmark
  public int testReverseStandard_c(BenchmarkState benchmarkState) {

    IntIterator intIterator = benchmarkState.bitmap_c.getReverseIntIterator();
    int result = 0;
    while (intIterator.hasNext()) {
      result = intIterator.next();
    }
    return result;
  }

  @Benchmark
  public int testReverseFlyweight_c(BenchmarkState benchmarkState) {

    ReverseIntIteratorFlyweight intIterator = benchmarkState.flyweightReverseIterator;

    intIterator.wrap(benchmarkState.bitmap_c);

    int result = 0;
    while (intIterator.hasNext()) {
      result = intIterator.next();
    }
    return result;
  }

  @State(Scope.Benchmark)
  public static class BenchmarkState {

    final RoaringBitmap bitmap_a;

    final RoaringBitmap bitmap_b;

    final RoaringBitmap bitmap_c;

    final IntIteratorFlyweight flyweightIterator = new IntIteratorFlyweight();

    final ReverseIntIteratorFlyweight flyweightReverseIterator = new ReverseIntIteratorFlyweight();

    public BenchmarkState() {

      final int[] data = takeSortedAndDistinct(new Random(0xcb000a2b9b5bdfb6l), 100000);
      bitmap_a = RoaringBitmap.bitmapOf(data);

      bitmap_b = new RoaringBitmap();
      for (int k = 0; k < (1 << 30); k += 32) bitmap_b.add(k);

      bitmap_c = new RoaringBitmap();
      for (int k = 0; k < (1 << 30); k += 3) bitmap_c.add(k);
    }

    private int[] takeSortedAndDistinct(Random source, int count) {

      LinkedHashSet<Integer> ints = new LinkedHashSet<Integer>(count);

      for (int size = 0; size < count; size++) {
        int next;
        do {
          next = Math.abs(source.nextInt());
        } while (!ints.add(next));
      }

      int[] unboxed = toArray(ints);
      Arrays.sort(unboxed);
      return unboxed;
    }

    private int[] toArray(LinkedHashSet<Integer> integers) {
      int[] ints = new int[integers.size()];
      int i = 0;
      for (Integer n : integers) {
        ints[i++] = n;
      }
      return ints;
    }
  }
}
