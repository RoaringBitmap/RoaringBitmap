package org.roaringbitmap.realdata.wrapper;

import org.roaringbitmap.IntConsumer;

import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.googlecode.javaewah.FastAggregation;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;

final class EwahBitmapWrapper implements Bitmap {

  private final EWAHCompressedBitmap bitmap;

  EwahBitmapWrapper(EWAHCompressedBitmap bitmap) {
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
    return new EwahBitmapWrapper(bitmap.and(((EwahBitmapWrapper) other).bitmap));
  }

  @Override
  public Bitmap or(Bitmap other) {
    return new EwahBitmapWrapper(bitmap.or(((EwahBitmapWrapper) other).bitmap));
  }

  @Override
  public Bitmap ior(Bitmap other) {
    throw new UnsupportedOperationException("Not implemented in Ewah");
  }

  @Override
  public Bitmap xor(Bitmap other) {
    return new EwahBitmapWrapper(bitmap.xor(((EwahBitmapWrapper) other).bitmap));
  }

  @Override
  public Bitmap flip(int rangeStart, int rangeEnd) {
    // synthesized with 2-upper-bounded NOTs
    // unfortunately, cannot be used with an immutable bitmap.
    // for that case, could synthesize from XOR and a mask for the range
    int savedSize = bitmap.sizeInBits();
    EWAHCompressedBitmap temp = null;
    try {
      temp = (EWAHCompressedBitmap) bitmap.clone();
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
    return new EwahBitmapWrapper(temp);
  }

  @Override
  public Bitmap andNot(Bitmap other) {
    return new EwahBitmapWrapper(bitmap.andNot(((EwahBitmapWrapper) other).bitmap));
  }

  @Override
  public BitmapAggregator naiveAndAggregator() {
    return new BitmapAggregator() {
      @Override
      public Bitmap aggregate(Iterable<Bitmap> bitmaps) {
        final Iterator<Bitmap> i = bitmaps.iterator();
        EWAHCompressedBitmap bitmap = ((EwahBitmapWrapper) i.next()).bitmap;
        while (i.hasNext()) {
          bitmap = bitmap.and(((EwahBitmapWrapper) i.next()).bitmap);
        }
        return new EwahBitmapWrapper(bitmap);
      }
    };
  }

  @Override
  public BitmapAggregator naiveOrAggregator() {
    return new BitmapAggregator() {
      @Override
      public Bitmap aggregate(Iterable<Bitmap> bitmaps) {
        final Iterator<Bitmap> i = bitmaps.iterator();
        EWAHCompressedBitmap bitmap = ((EwahBitmapWrapper) i.next()).bitmap;
        while (i.hasNext()) {
          bitmap = bitmap.or(((EwahBitmapWrapper) i.next()).bitmap);
        }
        return new EwahBitmapWrapper(bitmap);
      }
    };
  }

  @Override
  public BitmapAggregator priorityQueueOrAggregator() {
    return new BitmapAggregator() {
      @Override
      public Bitmap aggregate(final Iterable<Bitmap> bitmaps) {
        Iterator<EWAHCompressedBitmap> iterator =
            new Iterator<EWAHCompressedBitmap>() {
              final Iterator<Bitmap> i = bitmaps.iterator();

              @Override
              public boolean hasNext() {
                return i.hasNext();
              }

              @Override
              public EWAHCompressedBitmap next() {
                return ((EwahBitmapWrapper) i.next()).bitmap;
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }
            };
        return new EwahBitmapWrapper(FastAggregation.or(iterator));
      }
    };
  }

  @Override
  public void forEach(IntConsumer ic) {
    throw new UnsupportedOperationException("Not implemented in Ewah");
  }

  @Override
  public void serialize(DataOutputStream dos) throws IOException {
    bitmap.serialize(dos);
  }

  @Override
  public Bitmap clone() {
    try {
      return new EwahBitmapWrapper(bitmap.clone());
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }
}
