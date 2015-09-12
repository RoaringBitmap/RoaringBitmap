package org.roaringbitmap.realdata.wrapper;

import org.roaringbitmap.RoaringBitmap;

import java.io.DataOutputStream;
import java.io.IOException;

final class RoaringBitmapWrapper implements Bitmap {

   private final RoaringBitmap bitmap;

   RoaringBitmapWrapper(RoaringBitmap bitmap) {
      this.bitmap = bitmap;
   }

   @Override
   public boolean contains(int i) {
      return bitmap.contains(i);
   }

   @Override
   public int last() {
      return bitmap.getReverseIntIterator().next();
   }

   @Override
   public int cardinality() {
      return bitmap.getCardinality();
   }

   @Override
   public BitmapIterator iterator() {
      return new RoaringIteratorWrapper(bitmap.getIntIterator());
   }

   @Override
   public BitmapIterator reverseIterator() {
      return new RoaringIteratorWrapper(bitmap.getReverseIntIterator());
   }

   @Override
   public Bitmap and(Bitmap other) {
      return new RoaringBitmapWrapper(RoaringBitmap.and(bitmap, ((RoaringBitmapWrapper) other).bitmap));
   }

   @Override
   public Bitmap or(Bitmap other) {
      return new RoaringBitmapWrapper(RoaringBitmap.or(bitmap, ((RoaringBitmapWrapper) other).bitmap));
   }

   @Override
   public Bitmap xor(Bitmap other) {
      return new RoaringBitmapWrapper(RoaringBitmap.xor(bitmap, ((RoaringBitmapWrapper) other).bitmap));
   }

   @Override
   public Bitmap andNot(Bitmap other) {
      return new RoaringBitmapWrapper(RoaringBitmap.andNot(bitmap, ((RoaringBitmapWrapper) other).bitmap));
   }

   @Override
   public void serialize(DataOutputStream dos) throws IOException {
      bitmap.serialize(dos);
   }

}
