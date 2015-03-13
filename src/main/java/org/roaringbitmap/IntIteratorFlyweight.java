/*
 * (c) Daniel Lemire, Owen Kaser, Samy Chambi, Jon Alvarado, Rory Graves, Björn Sperber, Borislav Ivanov
 * Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap;

/**
 * Fast iterator minimizing the stress on the garbage collector.
 *
 * You can create one reusable instance of this class and then {@link #wrap(RoaringBitmap)}
 * 
 * @author  Borislav Ivanov
 **/
public class IntIteratorFlyweight implements IntIterator {

   private int hs;

   private ShortIterator iter;

   private ArrayContainerShortIteratorFlyweight arrIter = new ArrayContainerShortIteratorFlyweight();
   private BitmapContainerShortIteratorFlyweight bitmapIter = new BitmapContainerShortIteratorFlyweight();

   private int pos;

   private RoaringBitmap roaringBitmap = null;

   /**
    * Prepares a BitMap for iteration
    * @param r
    */
   public void wrap(RoaringBitmap r) {
      this.hs = 0;
      this.pos = 0;
      this.roaringBitmap = r;
      this.nextContainer();
   }

   @Override
   public boolean hasNext() {
      return pos < this.roaringBitmap.highLowContainer.size();
   }



   private void nextContainer() {
      if (pos < this.roaringBitmap.highLowContainer.size()) {

         Container container = this.roaringBitmap.highLowContainer.getContainerAtIndex(pos);

         if (container instanceof BitmapContainer) {
            bitmapIter.wrap((BitmapContainer) container);
            iter = bitmapIter;
         } else {
            arrIter.wrap((ArrayContainer) container);
            iter = arrIter;
         }

         hs = Util.toIntUnsigned(this.roaringBitmap.highLowContainer.getKeyAtIndex(pos)) << 16;
      }
   }

   @Override
   public int next() {
      int x = Util.toIntUnsigned(iter.next()) | hs;
      if (!iter.hasNext()) {
         ++pos;
         nextContainer();
      }
      return x;
   }

   @Override
   public IntIterator clone() {
      try {
         IntIteratorFlyweight x = (IntIteratorFlyweight) super.clone();
         x.iter = this.iter.clone();
         return x;
      } catch (CloneNotSupportedException e) {
         return null;// will not happen
      }
   }


   public static class ArrayContainerShortIteratorFlyweight implements ShortIterator {
      int pos;

      private ArrayContainer arrayContainer;

      public void wrap(ArrayContainer c) {
         arrayContainer = c;
         pos = 0;
      }

      @Override
      public boolean hasNext() {
         return pos < arrayContainer.cardinality;
      }

      @Override
      public short next() {
         return arrayContainer.content[pos++];
      }

      @Override
      public ShortIterator clone() {
         try {
            return (ShortIterator) super.clone();
         } catch (CloneNotSupportedException e) {
            return null;// will not happen
         }
      }

      @Override
      public void remove() {
         arrayContainer.remove((short) (pos - 1));
         pos--;
      }

   }

   public static class BitmapContainerShortIteratorFlyweight implements ShortIterator {

      int i;
      int j;
      int max;

      BitmapContainer bitmapContainer = null;

      public void wrap(BitmapContainer c) {
         bitmapContainer = c;
         i = bitmapContainer.nextSetBit(0);
         max = bitmapContainer.bitmap.length * 64 - 1;

      }

      @Override
      public boolean hasNext() {
         return i >= 0;
      }

      @Override
      public short next() {
         j = i;
         i = i < max ? bitmapContainer.nextSetBit(i + 1) : -1;
         return (short) j;
      }

      @Override
      public ShortIterator clone() {
         try {
            return (ShortIterator) super.clone();
         } catch (CloneNotSupportedException e) {
            return null;// will not happen
         }
      }

      @Override
      public void remove() {
         //TODO: implement
         throw new RuntimeException("unsupported operation: remove");
      }
   }


}

