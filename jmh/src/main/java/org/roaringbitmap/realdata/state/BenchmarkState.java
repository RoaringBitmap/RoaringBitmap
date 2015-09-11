package org.roaringbitmap.realdata.state;

import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.roaringbitmap.ZipRealDataRetriever;
import org.roaringbitmap.realdata.wrapper.Bitmap;

import java.util.ArrayList;
import java.util.List;

import static org.roaringbitmap.RealDataset.*;
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.*;

@State(Scope.Benchmark)
public class BenchmarkState {

   @Param({// putting the data sets in alpha. order
           CENSUS_INCOME, CENSUS1881,
           DIMENSION_008, DIMENSION_003,
           DIMENSION_033, USCENSUS2000,
           WEATHER_SEPT_85, WIKILEAKS_NOQUOTES,
           CENSUS_INCOME_SRT, CENSUS1881_SRT,
           WEATHER_SEPT_85_SRT, WIKILEAKS_NOQUOTES_SRT
   })
   public String dataset;

   @Param({
           CONCISE,
           WAH,
           EWAH,
           EWAH32,
           ROARING,
           ROARING_WITH_RUN
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

           if (CONCISE.equals(type)) {
               if (!immutable) {
                   bitmap = newConciseBitmap(data);
               } else {
                   bitmap = newIImmutableConciseBitmap(data);
               }
           } else if (WAH.equals(type)) {
               if (!immutable) {
                   bitmap = newWahBitmap(data);
               } else {
                   bitmap = newImmutableWahBitmap(data);
               }
           } else if (EWAH.equals(type)) {
               if (!immutable) {
                   bitmap = newEwahBitmap(data);
               } else {
                   bitmap = newImmutableEwahBitmap(data);
               }
           } else if (EWAH32.equals(type)) {
               if (!immutable) {
                   bitmap = newEwah32Bitmap(data);
               } else {
                   bitmap = newImmutableEwah32Bitmap(data);
               }
           } else if (ROARING.equals(type)) {
               if (!immutable) {
                   bitmap = newRoaringBitmap(data);
               } else {
                   bitmap = newImmutableRoaringBitmap(data);
               }
           } else if (ROARING_WITH_RUN.equals(type)) {
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
