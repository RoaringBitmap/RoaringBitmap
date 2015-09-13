package org.roaringbitmap.realdata.wrapper;

import com.googlecode.javaewah32.EWAHCompressedBitmap32;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;

final class Ewah32BitmapWrapper implements Bitmap {

   private final EWAHCompressedBitmap32 bitmap;

   Ewah32BitmapWrapper(EWAHCompressedBitmap32 bitmap) {
      this.bitmap = bitmap;
   }

   @Override
   public boolean contains(int i) {
      return bitmap.get(i);
   }

   @Override
   public int last() {
      return bitmap.reverseIntIterator().next();
   }

   @Override
   public int cardinality() {
      return bitmap.cardinality();
   }

   @Override
   public BitmapIterator iterator() {
      return new EwahIteratorWrapper(bitmap.intIterator());
   }

   @Override
   public BitmapIterator reverseIterator() {
      return new EwahIteratorWrapper(bitmap.reverseIntIterator());
   }

   @Override
   public Bitmap and(Bitmap other) {
      return new Ewah32BitmapWrapper(bitmap.and(((Ewah32BitmapWrapper) other).bitmap));
   }

   @Override
   public Bitmap or(Bitmap other) {
      return new Ewah32BitmapWrapper(bitmap.or(((Ewah32BitmapWrapper) other).bitmap));
   }

   @Override
   public Bitmap xor(Bitmap other) {
      return new Ewah32BitmapWrapper(bitmap.xor(((Ewah32BitmapWrapper) other).bitmap));
   }

   @Override
   public Bitmap andNot(Bitmap other) {
      return new Ewah32BitmapWrapper(bitmap.andNot(((Ewah32BitmapWrapper) other).bitmap));
   }

   @Override
   public BitmapAggregator naiveOrAggregator() {
      return new BitmapAggregator() {
         @Override
         public Bitmap aggregate(Iterable<Bitmap> bitmaps) {
             final Iterator<Bitmap> i = bitmaps.iterator();
             EWAHCompressedBitmap32 bitmap = ((Ewah32BitmapWrapper) i.next()).bitmap;
             while(i.hasNext()) {
                 bitmap = bitmap.or(((Ewah32BitmapWrapper) i.next()).bitmap);
             }
             return new Ewah32BitmapWrapper(bitmap);
         }
      };
   }

   @Override
   public void serialize(DataOutputStream dos) throws IOException {
      bitmap.serialize(dos);
   }

}
