package org.roaringbitmap.realdata.wrapper;

import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.googlecode.javaewah32.EWAHCompressedBitmap32;
import io.druid.extendedset.intset.ConciseSet;
import io.druid.extendedset.intset.ImmutableConciseSet;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;

public final class BitmapFactory {

  public static final String CONCISE = "concise";
  public static final String WAH = "wah";
  public static final String EWAH = "ewah";
  public static final String EWAH32 = "ewah32";
  public static final String ROARING = "roaring";
  public static final String ROARING_WITH_RUN = "roaring_with_run";
  public static final String ROARING_ONLY = "ROARING_ONLY";
  public static final String BITMAP_TYPES = "BITMAP_TYPES";

  private BitmapFactory() {}

  public static Bitmap newConciseBitmap(int[] data) {
    return newConciseBitmap(data, false);
  }

  public static Bitmap newWahBitmap(int[] data) {
    return newConciseBitmap(data, true);
  }

  private static Bitmap newConciseBitmap(int[] data, boolean simulateWAH) {
    ConciseSet concise = newConciseSet(data, simulateWAH);
    return new ConciseSetWrapper(concise);
  }

  private static ConciseSet newConciseSet(int[] data, boolean simulateWAH) {
    ConciseSet concise = new ConciseSet(simulateWAH);
    for (int i : data) {
      concise.add(i);
    }
    return concise;
  }

  public static Bitmap newEwahBitmap(int[] data) {
    EWAHCompressedBitmap ewah = EWAHCompressedBitmap.bitmapOf(data);
    return new EwahBitmapWrapper(ewah);
  }

  public static Bitmap newEwah32Bitmap(int[] data) {
    EWAHCompressedBitmap32 ewah32 = EWAHCompressedBitmap32.bitmapOf(data);
    return new Ewah32BitmapWrapper(ewah32);
  }

  public static Bitmap newRoaringBitmap(int[] data) {
    return newRoaringBitmap(data, false);
  }

  public static Bitmap newRoaringWithRunBitmap(int[] data) {
    return newRoaringBitmap(data, true);
  }

  private static Bitmap newRoaringBitmap(int[] data, boolean optimize) {
    RoaringBitmap roaring = RoaringBitmap.bitmapOf(data);
    if (optimize) {
      roaring.runOptimize();
    }
    return new RoaringBitmapWrapper(roaring);
  }

  public static Bitmap newImmutableConciseBitmap(int[] data) {
    return newImmutableConciseBitmap(data, false);
  }

  public static Bitmap newImmutableWahBitmap(int[] data) {
    return newImmutableConciseBitmap(data, true);
  }

  private static Bitmap newImmutableConciseBitmap(int[] data, boolean simulateWAH) {
    ImmutableConciseSet concise =
        ImmutableConciseSet.newImmutableFromMutable(newConciseSet(data, simulateWAH));
    return new org.roaringbitmap.realdata.wrapper.ImmutableConciseSetWrapper(concise);
  }

  public static Bitmap newImmutableEwahBitmap(int[] data) throws Exception {
    ByteBuffer bb = toByteBuffer(newEwahBitmap(data));
    EWAHCompressedBitmap ewah = new EWAHCompressedBitmap(bb);
    return new EwahBitmapWrapper(ewah);
  }

  public static Bitmap newImmutableEwah32Bitmap(int[] data) throws Exception {
    ByteBuffer bb = toByteBuffer(newEwah32Bitmap(data));
    EWAHCompressedBitmap32 ewah = new EWAHCompressedBitmap32(bb);
    return new Ewah32BitmapWrapper(ewah);
  }

  public static Bitmap newImmutableRoaringBitmap(int[] data) throws Exception {
    return newImmutableRoaringBitmap(newRoaringBitmap(data));
  }

  public static Bitmap newImmutableRoaringWithRunBitmap(int[] data) throws Exception {
    return newImmutableRoaringBitmap(newRoaringWithRunBitmap(data));
  }

  private static Bitmap newImmutableRoaringBitmap(Bitmap bitmap) throws Exception {
    ByteBuffer bb = toByteBuffer(bitmap);
    ImmutableRoaringBitmap roaring = new ImmutableRoaringBitmap(bb);
    return new ImmutableRoaringBitmapWrapper(roaring);
  }

  private static ByteBuffer toByteBuffer(Bitmap bitmap) throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    bitmap.serialize(dos);
    return ByteBuffer.wrap(baos.toByteArray());
  }

  public static void cleanup() {}
}
