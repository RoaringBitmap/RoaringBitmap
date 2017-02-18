package org.roaringbitmap;

import java.util.NoSuchElementException;

class RoaringIterator implements IntIterator {
  final private RoaringBitmap parent;
  private ArrayContainer ac;
  private RunContainer rc;
  private int containerIndex;
  private short typecode = TypeCode.NONE;
  private int hb;
  private int le = 0;
  private int maxlength;
  private int base;
  private long w;
  private int pos;
  private long[] bitmap;

  RoaringIterator(RoaringBitmap parent) {
    this.parent = parent;
    this.hb = 0;
    this.containerIndex = 0;
    nextContainer();
  }

  public boolean hasNext() {
    return containerIndex <= parent.highLowContainer.size;
  }

  public int next() {
    int answer;
    switch (typecode) {
      case TypeCode.BITSET_CONTAINER:
        long t = w & -w;
        answer = hb | (pos * 64 + Long.bitCount(t - 1));
        w ^= t;
        while (w == 0) {
          ++pos;
          if (pos == bitmap.length) {
            nextContainer();
            break;
          }
          w = bitmap[pos];
        }
        return answer;
      case TypeCode.ARRAY_CONTAINER:
        answer = hb | Util.toIntUnsigned(ac.content[pos++]);
        if (pos == ac.cardinality) {
          nextContainer();
        }
        return answer;
      case TypeCode.RUN_CONTAINER:
        answer = base + le;
        le++;
        if (answer >= maxlength) {
          pos++;
          le = 0;
          if (pos < rc.nbrruns) {
            base = hb | Util.toIntUnsigned(rc.getValue(pos));
            maxlength = base + Util.toIntUnsigned(rc.getLength(pos));
          } else {
            nextContainer();
          }
        }
        return answer;
      default:
        throw new NoSuchElementException();
    }
  }


  private void nextContainer() {

    if (containerIndex >= parent.highLowContainer.size()) {
      containerIndex++;
      typecode = TypeCode.NONE;
      return;
    }
    Container container = parent.highLowContainer.values[containerIndex];
    typecode = TypeCode.of(container);
    hb = Util.toIntUnsigned(parent.highLowContainer.getKeyAtIndex(containerIndex)) << 16;
    containerIndex++;
    switch (typecode) {
      case TypeCode.BITSET_CONTAINER:
        bitmap = ((BitmapContainer) container).bitmap;
        for (pos = 0; pos < bitmap.length; ++pos) {
          if ((w = bitmap[pos]) != 0) {
            break;
          }
        }
        break;
      case TypeCode.ARRAY_CONTAINER:
        ac = (ArrayContainer) container;
        pos = 0;
        break;
      case TypeCode.RUN_CONTAINER:
        rc = (RunContainer) container;
        pos = 0;
        le = 0;
        if (pos < rc.nbrruns) {
          base = hb | Util.toIntUnsigned(rc.getValue(pos));
          maxlength = base + Util.toIntUnsigned(rc.getLength(pos));
        }
        break;
      default:
        throw new IllegalStateException("Unknow iteration error");
    }
  }

  boolean remove() {
    throw new UnsupportedOperationException();
  }

  @Override
  public IntIterator clone() {
    throw new UnsupportedOperationException();
  }
}

class TypeCode {
  static final short BITSET_CONTAINER = 0;
  static final short ARRAY_CONTAINER = 1;
  static final short RUN_CONTAINER = 2;
  static final short NONE = -1;

  static short of(Container c) {
    if (c instanceof BitmapContainer) {
      return BITSET_CONTAINER;
    } else if (c instanceof ArrayContainer) {
      return ARRAY_CONTAINER;
    } else if (c instanceof RunContainer) {
      return RUN_CONTAINER;
    } else {
      throw new IllegalArgumentException("Unknown container type");
    }
  }
}

