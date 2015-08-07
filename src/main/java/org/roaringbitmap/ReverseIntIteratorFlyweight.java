/*
 * (c) the authors
 * Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap;

/**
 * Fast iterator minimizing the stress on the garbage collector.
 * You can create one reusable instance of this class and then {@link #wrap(RoaringBitmap)}
 * 
 * This iterator enumerates the stored values in reverse (starting from the end).
 * 
 * @author  Borislav Ivanov
 **/
public class ReverseIntIteratorFlyweight implements IntIterator {

   private int hs;

   private ShortIterator iter;

   private ReverseArrayContainerShortIterator arrIter = new ReverseArrayContainerShortIterator();
   
   private ReverseBitmapContainerShortIterator bitmapIter = new ReverseBitmapContainerShortIterator();

    private ReverseRunContainerShortIterator runIter = new ReverseRunContainerShortIterator();

   private short pos;

   private RoaringBitmap roaringBitmap = null;


   /**
    * Creates an instance that is not ready for iteration. You must first call
    * {@link #wrap(RoaringBitmap)}.
    */
   public ReverseIntIteratorFlyweight() {

   }

   /**
    * Creates an instance that is ready for iteration.
    * 
    * @param r
    *            bitmap to be iterated over
    */
   public ReverseIntIteratorFlyweight(RoaringBitmap r) {

   }  
 
   /**
    * Prepares a bitmap for iteration
    * @param r  bitmap to be iterated over
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
                bitmapIter.wrap(((BitmapContainer) container).bitmap);
                iter = bitmapIter;
            } else if (container instanceof ArrayContainer) {
                arrIter.wrap((ArrayContainer) container);
                iter = arrIter;
            } else {
                runIter.wrap((RunContainer) container);
                iter = runIter;
            }
            hs = Util.toIntUnsigned(this.roaringBitmap.highLowContainer.getKeyAtIndex(pos)) << 16;
      }
   }

   @Override
   public int next() {
      final int x = iter.nextAsInt() | hs;
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

}

