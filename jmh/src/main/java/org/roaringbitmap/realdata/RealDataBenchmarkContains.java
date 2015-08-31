package org.roaringbitmap.realdata;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.roaringbitmap.realdata.wrapper.Bitmap;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class RealDataBenchmarkContains {

   @Benchmark
   public void get(BenchmarkState bs, Blackhole bh) {
      for (int k = 0; k < bs.bitmaps.size(); ++k) {
         Bitmap bitmap = bs.bitmaps.get(k);
         int range = bs.ranges.get(k);
         bh.consume(bitmap.contains((int) (range * 1. / 4)));
         bh.consume(bitmap.contains((int) (range * 2. / 4)));
         bh.consume(bitmap.contains((int) (range * 3. / 4)));
      }
   }

   @State(Scope.Benchmark)
   public static class BenchmarkState extends org.roaringbitmap.realdata.state.BenchmarkState {

      List<Integer> ranges;

      public BenchmarkState() {
      }

      @Setup
      public void setup() throws Exception {
         super.setup();

         ranges = new ArrayList<Integer>();
         for (Bitmap bitmap : bitmaps) {
            ranges.add(bitmap.last());
         }
      }
   }

}
