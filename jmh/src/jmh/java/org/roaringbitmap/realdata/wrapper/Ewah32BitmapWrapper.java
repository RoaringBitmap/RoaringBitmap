package org.roaringbitmap.realdata.wrapper;

import org.roaringbitmap.IntConsumer;

import com.googlecode.javaewah32.EWAHCompressedBitmap32;
import com.googlecode.javaewah32.FastAggregation32;

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
  public Bitmap ior(Bitmap other) {
    throw new UnsupportedOperationException("Not implemented in Ewah32");
  }

  @Override
  public Bitmap xor(Bitmap other) {
    return new Ewah32BitmapWrapper(bitmap.xor(((Ewah32BitmapWrapper) other).bitmap));
  }

  @Override
  public Bitmap flip(int rangeStart, int rangeEnd) {
    // synthesized with 2-upper-bounded NOTs
    int savedSize = bitmap.sizeInBits();
    EWAHCompressedBitmap32 temp = null;

    try {
      temp = (EWAHCompressedBitmap32) bitmap.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException("Can't happen");
    }

    temp.setSizeInBits(rangeEnd, false);
    temp.not();
    if (rangeStart != 0) {
      temp.setSizeInBits(rangeStart - 1, false);
      temp.not();
    }
    temp.setSizeInBits(savedSize, false);
    return new Ewah32BitmapWrapper(temp);
  }

  @Override
  public Bitmap andNot(Bitmap other) {
    return new Ewah32BitmapWrapper(bitmap.andNot(((Ewah32BitmapWrapper) other).bitmap));
  }

  @Override
  public BitmapAggregator naiveAndAggregator() {
    return new BitmapAggregator() {
      @Override
      public Bitmap aggregate(Iterable<Bitmap> bitmaps) {
        final Iterator<Bitmap> i = bitmaps.iterator();
        EWAHCompressedBitmap32 bitmap = ((Ewah32BitmapWrapper) i.next()).bitmap;
        while (i.hasNext()) {
          bitmap = bitmap.and(((Ewah32BitmapWrapper) i.next()).bitmap);
        }
        return new Ewah32BitmapWrapper(bitmap);
      }
    };
  }

  @Override
  public BitmapAggregator naiveOrAggregator() {
    return new BitmapAggregator() {
      @Override
      public Bitmap aggregate(Iterable<Bitmap> bitmaps) {
        final Iterator<Bitmap> i = bitmaps.iterator();
        EWAHCompressedBitmap32 bitmap = ((Ewah32BitmapWrapper) i.next()).bitmap;
        while (i.hasNext()) {
          bitmap = bitmap.or(((Ewah32BitmapWrapper) i.next()).bitmap);
        }
        return new Ewah32BitmapWrapper(bitmap);
      }
    };
  }

  @Override
  public BitmapAggregator priorityQueueOrAggregator() {
    return new BitmapAggregator() {
      @Override
      public Bitmap aggregate(final Iterable<Bitmap> bitmaps) {
        Iterator<EWAHCompressedBitmap32> iterator =
            new Iterator<EWAHCompressedBitmap32>() {
              final Iterator<Bitmap> i = bitmaps.iterator();

              @Override
              public boolean hasNext() {
                return i.hasNext();
              }

              @Override
              public EWAHCompressedBitmap32 next() {
                return ((Ewah32BitmapWrapper) i.next()).bitmap;
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }
            };
        return new Ewah32BitmapWrapper(FastAggregation32.or(iterator));
      }
    };
  }

  @Override
  public void forEach(IntConsumer ic) {
    throw new UnsupportedOperationException("Not implemented in Ewah32");
  }

  @Override
  public void serialize(DataOutputStream dos) throws IOException {
    bitmap.serialize(dos);
  }

  @Override
  public Bitmap clone() {
    try {
      return new Ewah32BitmapWrapper(bitmap.clone());
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }
}
