package org.roaringbitmap.realdata.wrapper;

import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.buffer.BufferFastAggregation;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;

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
    return new ImmutableRoaringBitmapWrapper(
        ImmutableRoaringBitmap.and(bitmap, ((ImmutableRoaringBitmapWrapper) other).bitmap));
  }

  @Override
  public Bitmap or(Bitmap other) {
    return new ImmutableRoaringBitmapWrapper(
        ImmutableRoaringBitmap.or(bitmap, ((ImmutableRoaringBitmapWrapper) other).bitmap));
  }

  @Override
  public Bitmap ior(Bitmap other) {
    throw new UnsupportedOperationException("Not implemented in ImmutableRoaringBitmap");
  }

  @Override
  public Bitmap xor(Bitmap other) {
    return new ImmutableRoaringBitmapWrapper(
        ImmutableRoaringBitmap.xor(bitmap, ((ImmutableRoaringBitmapWrapper) other).bitmap));
  }

  @Override
  public Bitmap flip(int rangeStart, int rangeEnd) {
    return new ImmutableRoaringBitmapWrapper(
        ImmutableRoaringBitmap.flip(bitmap, (long) rangeStart, (long) rangeEnd));
  }

  @Override
  public Bitmap andNot(Bitmap other) {
    return new ImmutableRoaringBitmapWrapper(
        ImmutableRoaringBitmap.andNot(bitmap, ((ImmutableRoaringBitmapWrapper) other).bitmap));
  }

  @Override
  public BitmapAggregator naiveAndAggregator() {
    return new BitmapAggregator() {
      @Override
      public Bitmap aggregate(Iterable<Bitmap> bitmaps) {
        Iterator<ImmutableRoaringBitmap> iterator = toImmutableRoaringBitmapIterator(bitmaps);
        return new ImmutableRoaringBitmapWrapper(BufferFastAggregation.naive_and(iterator));
      }
    };
  }

  @Override
  public BitmapAggregator naiveOrAggregator() {
    return new BitmapAggregator() {
      @Override
      public Bitmap aggregate(final Iterable<Bitmap> bitmaps) {
        Iterator<ImmutableRoaringBitmap> iterator = toImmutableRoaringBitmapIterator(bitmaps);
        return new ImmutableRoaringBitmapWrapper(BufferFastAggregation.naive_or(iterator));
      }
    };
  }

  @Override
  public BitmapAggregator priorityQueueOrAggregator() {
    return new BitmapAggregator() {
      @Override
      public Bitmap aggregate(final Iterable<Bitmap> bitmaps) {
        Iterator<ImmutableRoaringBitmap> iterator = toImmutableRoaringBitmapIterator(bitmaps);
        return new ImmutableRoaringBitmapWrapper(BufferFastAggregation.priorityqueue_or(iterator));
      }
    };
  }

  private Iterator<ImmutableRoaringBitmap> toImmutableRoaringBitmapIterator(
      final Iterable<Bitmap> bitmaps) {
    return new Iterator<ImmutableRoaringBitmap>() {
      final Iterator<Bitmap> i = bitmaps.iterator();

      @Override
      public boolean hasNext() {
        return i.hasNext();
      }

      @Override
      public ImmutableRoaringBitmap next() {
        return ((ImmutableRoaringBitmapWrapper) i.next()).bitmap;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Override
  public void forEach(IntConsumer ic) {
    bitmap.forEach(ic);
  }

  @Override
  public void serialize(DataOutputStream dos) throws IOException {
    bitmap.serialize(dos);
  }

  @Override
  public Bitmap clone() {
    return new ImmutableRoaringBitmapWrapper(bitmap.clone());
  }
}
