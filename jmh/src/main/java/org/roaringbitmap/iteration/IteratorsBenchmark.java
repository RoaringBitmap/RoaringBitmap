package org.roaringbitmap.iteration;

import org.openjdk.jmh.annotations.*;
import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.IntIteratorFlyweight;
import org.roaringbitmap.ReverseIntIteratorFlyweight;
import org.roaringbitmap.RoaringBitmap;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.Iterator;
/**
 * Created by Borislav Ivanov on 4/2/15.
 */
@BenchmarkMode({Mode.SampleTime, Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class IteratorsBenchmark {

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
   public int testForeach_a(BenchmarkState benchmarkState) {
      LastConsumer c = new LastConsumer();
      benchmarkState.bitmap_a.forEach(c);
      return c.last;
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
   public int testForeach_b(BenchmarkState benchmarkState) {
      LastConsumer c = new LastConsumer();
      benchmarkState.bitmap_b.forEach(c);
      return c.last;
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
   public int testForeach_c(BenchmarkState benchmarkState) {
      LastConsumer c = new LastConsumer();
      benchmarkState.bitmap_c.forEach(c);
      return c.last;
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
   public int testBoxed_run(BenchmarkState benchmarkState) {
      Iterator<Integer> intIterator = benchmarkState.bitmap_run.iterator();
      int result = 0;
      while (intIterator.hasNext()) {
         result = intIterator.next();
      }
      return result;
   }

   @Benchmark
   public int testStandard_run(BenchmarkState benchmarkState) {
      IntIterator intIterator = benchmarkState.bitmap_run.getIntIterator();
      int result = 0;
      while (intIterator.hasNext()) {
         result = intIterator.next();
      }
      return result;
   }

   @Benchmark
   public int testForeach_run(BenchmarkState benchmarkState) {
      LastConsumer c = new LastConsumer();
      benchmarkState.bitmap_run.forEach(c);
      return c.last;
   }

   @Benchmark
   public int testFlyweight_run(BenchmarkState benchmarkState) {
      IntIteratorFlyweight intIterator = benchmarkState.flyweightIterator;
      intIterator.wrap(benchmarkState.bitmap_run);
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

      final RoaringBitmap bitmap_run;

      final IntIteratorFlyweight flyweightIterator = new IntIteratorFlyweight();

      final ReverseIntIteratorFlyweight flyweightReverseIterator = new ReverseIntIteratorFlyweight();

      public BenchmarkState() {

         final int[] data = takeSortedAndDistinct(new Random(0xcb000a2b9b5bdfb6l), 100000);
         bitmap_a = RoaringBitmap.bitmapOf(data);

         bitmap_b = new RoaringBitmap();
         for (int k = 0; k < (1 << 30); k += 32)
            bitmap_b.add(k);

         bitmap_c = new RoaringBitmap();
         for (int k = 0; k < (1 << 30); k += 3)
            bitmap_c.add(k);
         bitmap_run = new RoaringBitmap();
         for (long k = 0; k < (1 << 28); k += 2000)
            bitmap_run.add(k, k + 500);
         bitmap_run.runOptimize();

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

   private static class LastConsumer implements IntConsumer {
      int last = 0;
      @Override
      public void accept(int value) {
         last = value;
      }
   }

}
