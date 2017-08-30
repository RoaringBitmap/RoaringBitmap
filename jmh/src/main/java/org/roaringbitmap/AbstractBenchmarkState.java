package org.roaringbitmap;

import static org.roaringbitmap.realdata.wrapper.BitmapFactory.BITMAP_TYPES;
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.CONCISE;
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.EWAH;
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.EWAH32;
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.ROARING;
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.ROARING_ONLY;
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.ROARING_WITH_RUN;
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.WAH;
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.cleanup;
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.newConciseBitmap;
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.newEwah32Bitmap;
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.newEwahBitmap;
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.newImmutableConciseBitmap;
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.newImmutableEwah32Bitmap;
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.newImmutableEwahBitmap;
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.newImmutableRoaringBitmap;
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.newImmutableRoaringWithRunBitmap;
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.newImmutableWahBitmap;
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.newRoaringBitmap;
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.newRoaringWithRunBitmap;
import static org.roaringbitmap.realdata.wrapper.BitmapFactory.newWahBitmap;

import java.util.ArrayList;
import java.util.List;

import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.roaringbitmap.realdata.wrapper.Bitmap;

@State(Scope.Benchmark)
public abstract class AbstractBenchmarkState {

  public List<Bitmap> bitmaps;

  public AbstractBenchmarkState() {}

  public void setup(String dataset, String type, boolean immutable) throws Exception {
    if (ROARING_ONLY.equals(System.getProperty(BITMAP_TYPES)) && !ROARING.equals(type)
        && !ROARING_WITH_RUN.equals(type)) {
      throw new RuntimeException(String.format("Skipping non Roaring type %s", type));
    }

    bitmaps = new ArrayList<Bitmap>();

    ZipRealDataRetriever dataRetriever = new ZipRealDataRetriever(dataset);

    for (int[] data : dataRetriever.fetchBitPositions()) {

      Bitmap bitmap = null;

      if (CONCISE.equals(type)) {
        if (!immutable) {
          bitmap = newConciseBitmap(data);
        } else {
          bitmap = newImmutableConciseBitmap(data);
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
        throw new RuntimeException(
            String.format("Unsupported parameters: type=%s, immutable=%b", type, immutable));
      }

      bitmaps.add(bitmap);

    }
  }

  @TearDown
  public void tearDown() {
    cleanup();
  }

}
