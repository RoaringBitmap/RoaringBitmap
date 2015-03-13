package org.roaringbitmap;

/**
 * This Iterator will let you go green and make your garbage collector suffer less.
 *
 * You can create one reusable instance of this class and then {@link #wrap(RoaringBitmap)}
 *
 * I am aware of the code duplication and the VeryLongNamingOfTheIterators but in order
 * to fix this it will take to make a lot of changes to the other Iterators in this project as well.
 *
 */

public class ReverseIntIteratorFlyweight implements IntIterator {

   private int hs;

   private ShortIterator iter;

   private ArrayContainerShortReverseIteratorFlyweight arrIter = new ArrayContainerShortReverseIteratorFlyweight();
   private BitmapContainerShortReverseIteratorFlyweight bitmapIter = new BitmapContainerShortReverseIteratorFlyweight();

   private short pos;

   private RoaringBitmap roaringBitmap = null;

   /**
    * Prepares a BitMap for iteration
    * @param r
    */
   public void wrap(RoaringBitmap r) {
      this.roaringBitmap = r;
      this.hs = 0;
      this.pos = (short) (this.roaringBitmap.highLowContainer.size() - 1);
      this.nextContainer();
   }

   @Override
   public boolean hasNext() {
       return pos >= 0;
   }



   private void nextContainer() {



      if (pos >= 0) {

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
      final int x = Util.toIntUnsigned(iter.next()) | hs;
      if (!iter.hasNext()) {
         --pos;
         nextContainer();
      }
      return x;
   }

   @Override
   public IntIterator clone() {
      try {
         ReverseIntIteratorFlyweight x = (ReverseIntIteratorFlyweight) super.clone();
         x.iter = this.iter.clone();
         return x;
      } catch (CloneNotSupportedException e) {
         return null;// will not happen
      }
   }

   public static void main(String[] args) {
      RoaringBitmap b = new RoaringBitmap();

      for (int i = 0; i < 100000; i++) {
         b.add(i);
      }

      ReverseIntIteratorFlyweight iterator = new ReverseIntIteratorFlyweight();

      iterator.wrap(b);

      for (int i : b) {
         System.out.println(b);
      }

   }

   public static class ArrayContainerShortReverseIteratorFlyweight implements ShortIterator {


      int pos;

      private ArrayContainer arrayContainer;

      public void wrap(ArrayContainer c) {
         arrayContainer = c;
         pos = arrayContainer.cardinality-1;
      }


      @Override
      public boolean hasNext() {
         return pos >= 0;
      }

      @Override
      public short next() {
         return arrayContainer.content[pos--];
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
         arrayContainer.remove((short) (pos + 1));
         pos++;      }

   }

   public static class BitmapContainerShortReverseIteratorFlyweight implements ShortIterator {

      int i;



      BitmapContainer bitmapContainer = null;

      public void wrap(BitmapContainer c) {
         bitmapContainer = c;
         i = bitmapContainer.prevSetBit(64 * bitmapContainer.bitmap.length - 1);


      }

      @Override
      public boolean hasNext() {
         return i >= 0;
      }

      @Override
      public short next() {
         final int j = i;
         i = i > 0 ? bitmapContainer.prevSetBit(i - 1) : -1;
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

