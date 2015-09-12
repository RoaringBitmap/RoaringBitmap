package org.roaringbitmap.realdata.wrapper;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import java.io.DataOutputStream;
import java.io.IOException;

final class ImmutableRoaringBitmapWrapper implements Bitmap {

   private final ImmutableRoaringBitmap bitmap;

   ImmutableRoaringBitmapWrapper(ImmutableRoaringBitmap bitmap) {
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
      return new ImmutableRoaringBitmapWrapper(ImmutableRoaringBitmap.and(bitmap, ((ImmutableRoaringBitmapWrapper) other).bitmap));
   }

   @Override
   public Bitmap or(Bitmap other) {
      return new ImmutableRoaringBitmapWrapper(ImmutableRoaringBitmap.or(bitmap, ((ImmutableRoaringBitmapWrapper) other).bitmap));
   }

   @Override
   public void serialize(DataOutputStream dos) throws IOException {
      bitmap.serialize(dos);
   }

}
