package org.roaringbitmap.realdata.state;

import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.roaringbitmap.ZipRealDataRetriever;
import org.roaringbitmap.realdata.wrapper.Bitmap;

import java.util.ArrayList;
import java.util.List;

import static org.roaringbitmap.realdata.wrapper.BitmapFactory.*;

@State(Scope.Benchmark)
public class BenchmarkState {

   @Param({// putting the data sets in alpha. order
           "census-income", "census1881",
           "dimension_008", "dimension_003",
           "dimension_033", "uscensus2000",
           "weather_sept_85", "wikileaks-noquotes",
           "census-income_srt", "census1881_srt",
           "weather_sept_85_srt", "wikileaks-noquotes_srt"
   })
   public String dataset;

   @Param({
           "concise",
           "wah",
           "ewah",
           "ewah32",
           "roaring",
           "roaring_with_run"
   })
   public String type;

   @Param({
           "false",
           "true"
   })
   public boolean immutable;


   public List<Bitmap> bitmaps;

   public BenchmarkState() {
   }

   @Setup
   public void setup() throws Exception {
      bitmaps = new ArrayList<Bitmap>();

      ZipRealDataRetriever dataRetriever = new ZipRealDataRetriever(dataset);

      for (int[] data : dataRetriever.fetchBitPositions()) {

         Bitmap bitmap = null;

         if ("concise".equals(type)) {
            if (!immutable) {
               bitmap = newConciseBitmap(data);
            }
         } else if ("wah".equals(type)) {
            if (!immutable) {
               bitmap = newWahBitmap(data);
            }
         } else if ("ewah".equals(type)) {
            if (!immutable) {
               bitmap = newEwahBitmap(data);
            } else {
               bitmap = newImmutableEwahBitmap(data);
            }
         } else if ("ewah32".equals(type)) {
            if (!immutable) {
               bitmap = newEwah32Bitmap(data);
            } else {
               bitmap = newImmutableEwah32Bitmap(data);
            }
         } else if ("roaring".equals(type)) {
            if (!immutable) {
               bitmap = newRoaringBitmap(data);
            } else {
               bitmap = newImmutableRoaringBitmap(data);
            }
         } else if ("roaring_with_run".equals(type)) {
            if (!immutable) {
               bitmap = newRoaringWithRunBitmap(data);
            } else {
               bitmap = newImmutableRoaringWithRunBitmap(data);
            }
         }

         if (bitmap == null) {
            throw new RuntimeException(String.format("Unsupported parameters: type=%s, immutable=%b", type, immutable));
         }

         bitmaps.add(bitmap);

      }
   }

}
