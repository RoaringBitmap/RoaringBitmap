package org.roaringbitmap.realdata.wrapper;


import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.googlecode.javaewah32.EWAHCompressedBitmap32;
import it.uniroma3.mat.extendedset.intset.ConciseSet;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public final class BitmapFactory {

   private BitmapFactory() {
   }

   public static Bitmap newConciseBitmap(int[] data) {
      return newConciseBitmap(data, false);
   }

   public static Bitmap newWahBitmap(int[] data) {
      return newConciseBitmap(data, true);
   }

   private static Bitmap newConciseBitmap(int[] data, boolean simulateWAH) {
      ConciseSet concise = new ConciseSet(simulateWAH);
      for (int i : data) {
         concise.add(i);
      }
      return new ConciseSetWrapper(concise);
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
      File file = File.createTempFile("bitmap", "bin");
      file.deleteOnExit();
      FileOutputStream fos = new FileOutputStream(file);
      DataOutputStream dos = new DataOutputStream(fos);
      bitmap.serialize(dos);
      long size = fos.getChannel().position();
      dos.close();
      final RandomAccessFile memoryMappedFile = new RandomAccessFile(file, "r");
      ByteBuffer bb = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, size);
      memoryMappedFile.close();
      return bb;
   }

}
