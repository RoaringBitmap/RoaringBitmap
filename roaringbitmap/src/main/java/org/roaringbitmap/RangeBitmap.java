package org.roaringbitmap;

import org.roaringbitmap.buffer.MappeableArrayContainer;
import org.roaringbitmap.buffer.MappeableBitmapContainer;
import org.roaringbitmap.buffer.MappeableContainer;
import org.roaringbitmap.buffer.MappeableRunContainer;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.LongBuffer;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.IntFunction;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.roaringbitmap.Util.cardinalityInBitmapRange;
import static org.roaringbitmap.Util.resetBitmapRange;
import static org.roaringbitmap.Util.setBitmapRange;

/**
 * A 2D bitmap which associates values with a row index and can perform range queries.
 */
public final class RangeBitmap {

  private static final int COOKIE = 0xF00D;
  private static final int BITMAP = 0;
  private static final int RUN = 1;
  private static final int ARRAY = 2;
  private static final int BITMAP_SIZE = 8192;

  /**
   * Append values to the RangeBitmap before sealing it.
   *
   * @param maxValue       the maximum value to be appended, values larger than this
   *                       value will be rejected.
   * @param bufferSupplier provides ByteBuffers.
   * @return an appender.
   */
  public static Appender appender(long maxValue,
                                  IntFunction<ByteBuffer> bufferSupplier,
                                  Consumer<ByteBuffer> cleaner) {
    return new Appender(maxValue, bufferSupplier, cleaner);
  }

  /**
   * Append values to the RangeBitmap before sealing it, defaults to on heap ByteBuffers.
   *
   * @param maxValue the maximum value to be appended, values larger than this
   *                 value will be rejected.
   * @return an appender.
   */
  public static Appender appender(long maxValue) {
    return appender(maxValue,
        capacity -> ByteBuffer.allocate(capacity).order(LITTLE_ENDIAN), b -> {
        });
  }

  /**
   * Maps the RangeBitmap from the buffer with minimal allocation.
   * The buffer must not be reused while the mapped RangeBitmap is live.
   *
   * @param buffer a buffer containing a serialized RangeBitmap.
   * @return a RangeBitmap backed by the buffer.
   */
  public static RangeBitmap map(ByteBuffer buffer) {
    ByteBuffer source = buffer.slice().order(LITTLE_ENDIAN);
    int cookie = source.getChar();
    if (cookie != COOKIE) {
      throw new InvalidRoaringFormat("invalid cookie for range bitmap (expected "
          + COOKIE + " but got " + cookie + ")");
    }
    int base = source.get() & 0xFF;
    if (base != 2) {
      throw new InvalidRoaringFormat("Unsupported base for range bitmap: " + cookie);
    }
    int sliceCount = source.get() & 0xFF;
    int maxKey = source.getChar();
    long mask = -1L >>> (64 - sliceCount);
    byte bytesPerMask = (byte) ((sliceCount + 7) >>> 3);
    long maxRid = source.getInt() & 0xFFFFFFFFL;
    int masksOffset = source.position();
    int containersOffset = masksOffset + maxKey * bytesPerMask;
    return new RangeBitmap(mask, maxRid, (ByteBuffer) source.position(buffer.position()),
        masksOffset, containersOffset, bytesPerMask);
  }

  private final ByteBuffer buffer;
  private final int masksOffset;
  private final int containersOffset;
  private final long mask;
  private final long max;
  private final byte bytesPerMask;

  RangeBitmap(long mask, long max, ByteBuffer buffer, int masksOffset, int containersOffset,
              byte bytesPerMask) {
    this.mask = mask;
    this.max = max;
    this.buffer = buffer;
    this.masksOffset = masksOffset;
    this.containersOffset = containersOffset;
    this.bytesPerMask = bytesPerMask;
  }

  /**
   * Returns a RoaringBitmap of rows which have a value in between the thresholds.
   *
   * @param min the inclusive minimum value.
   * @param max the inclusive maximum value.
   * @return a bitmap of matching rows.
   */
  public RoaringBitmap between(long min, long max) {
    if (min == 0 || Long.numberOfLeadingZeros(min) < Long.numberOfLeadingZeros(mask)) {
      return lte(max);
    }
    if (Long.numberOfLeadingZeros(max) < Long.numberOfLeadingZeros(mask)) {
      return gte(min);
    }
    return new DoubleEvaluation().compute(min - 1, max);
  }

  /**
   * Returns the number of rows which have a value in between the thresholds.
   *
   * @param min the inclusive minimum value.
   * @param max the inclusive maximum value.
   * @return the number of matching rows.
   */
  public long betweenCardinality(long min, long max) {
    if (min == 0 || Long.numberOfLeadingZeros(min) < Long.numberOfLeadingZeros(mask)) {
      return lteCardinality(max);
    }
    if (Long.numberOfLeadingZeros(max) < Long.numberOfLeadingZeros(mask)) {
      return gteCardinality(min);
    }
    return new DoubleEvaluation().count(min - 1, max);
  }

  /**
   * Returns the number of rows which have a value in between the thresholds.
   *
   * @param min the inclusive minimum value.
   * @param max the inclusive maximum value.
   * @param context to be intersected with.
   * @return the number of matching rows.
   */
  public long betweenCardinality(long min, long max, RoaringBitmap context) {
    if (min == 0 || Long.numberOfLeadingZeros(min) < Long.numberOfLeadingZeros(mask)) {
      return lteCardinality(max, context);
    }
    if (Long.numberOfLeadingZeros(max) < Long.numberOfLeadingZeros(mask)) {
      return gteCardinality(min, context);
    }
    return new DoubleEvaluation().count(min - 1, max, context);
  }

  /**
   * Returns a RoaringBitmap of rows which have a value less than or equal to the threshold.
   *
   * @param threshold the inclusive maximum value.
   * @return a bitmap of matching rows.
   */
  public RoaringBitmap lte(long threshold) {
    return new SingleEvaluation().computeRange(threshold, true);
  }

  /**
   * Returns a RoaringBitmap of rows which have a value less than or equal to the threshold,
   * and intersect with the context bitmap, which will not be modified.
   *
   * @param threshold the inclusive maximum value.
   * @param context   to be intersected with.
   * @return a bitmap of matching rows.
   */
  public RoaringBitmap lte(long threshold, RoaringBitmap context) {
    return new SingleEvaluation().computeRange(threshold, true, context);
  }

  /**
   * Returns the number of rows which have a value less than or equal to the threshold.
   *
   * @param threshold the inclusive maximum value.
   * @return the number of matching rows.
   */
  public long lteCardinality(long threshold) {
    return new SingleEvaluation().countRange(threshold, true);
  }

  /**
   * Returns the number of rows which have a value less than or equal to the threshold,
   * and intersect with the context bitmap, which will not be modified.
   *
   * @param threshold the inclusive maximum value.
   * @param context   to be intersected with.
   * @return the number of matching rows.
   */
  public long lteCardinality(long threshold, RoaringBitmap context) {
    return new SingleEvaluation().countRange(threshold, true, context);
  }

  /**
   * Returns a RoaringBitmap of rows which have a value less than the threshold.
   *
   * @param threshold the exclusive maximum value.
   * @return a bitmap of matching rows.
   */
  public RoaringBitmap lt(long threshold) {
    return threshold == 0 ? new RoaringBitmap() : lte(threshold - 1);
  }

  /**
   * Returns a RoaringBitmap of rows which have a value less than the threshold,
   * and intersect with the context bitmap, which will not be modified.
   *
   * @param threshold the exclusive maximum value.
   * @param context   to be intersected with.
   * @return a bitmap of matching rows which intersect .
   */
  public RoaringBitmap lt(long threshold, RoaringBitmap context) {
    return threshold == 0 ? new RoaringBitmap() : lte(threshold - 1, context);
  }

  /**
   * Returns the number of rows which have a value less than the threshold.
   *
   * @param threshold the exclusive maximum value.
   * @return the number of matching rows.
   */
  public long ltCardinality(long threshold) {
    return threshold == 0 ? 0L : lteCardinality(threshold - 1);
  }

  /**
   * Returns the number of rows which have a value less than the threshold,
   * and intersect with the context bitmap, which will not be modified.
   *
   * @param threshold the exclusive maximum value.
   * @param context   to be intersected with.
   * @return the number of matching rows which intersect .
   */
  public long ltCardinality(long threshold, RoaringBitmap context) {
    return threshold == 0 ? 0L : lteCardinality(threshold - 1, context);
  }

  /**
   * Returns a RoaringBitmap of rows which have a value greater than the threshold.
   *
   * @param threshold the exclusive minimum value.
   * @return a bitmap of matching rows.
   */
  public RoaringBitmap gt(long threshold) {
    return new SingleEvaluation().computeRange(threshold, false);
  }

  /**
   * Returns a RoaringBitmap of rows which have a value greater than the threshold,
   * and intersect with the context bitmap, which will not be modified.
   *
   * @param threshold the exclusive minimum value.
   * @param context   to be intersected with.
   * @return a bitmap of matching rows.
   */
  public RoaringBitmap gt(long threshold, RoaringBitmap context) {
    return new SingleEvaluation().computeRange(threshold, false, context);
  }

  /**
   * Returns the number of rows which have a value greater than the threshold.
   *
   * @param threshold the exclusive minimum value.
   * @return the number of matching rows.
   */
  public long gtCardinality(long threshold) {
    return new SingleEvaluation().countRange(threshold, false);
  }

  /**
   * Returns the number of rows which have a value greater than the threshold,
   * and intersect with the context bitmap, which will not be modified.
   *
   * @param threshold the exclusive minimum value.
   * @param context   to be intersected with.
   * @return the number of matching rows.
   */
  public long gtCardinality(long threshold, RoaringBitmap context) {
    return new SingleEvaluation().countRange(threshold, false, context);
  }

  /**
   * Returns a RoaringBitmap of rows which have a value greater than or equal to the threshold.
   *
   * @param threshold the inclusive minimum value.
   * @return a bitmap of matching rows.
   */
  public RoaringBitmap gte(long threshold) {
    return threshold == 0 ? RoaringBitmap.bitmapOfRange(0, max) : gt(threshold - 1);
  }

  /**
   * Returns a RoaringBitmap of rows which have a value greater than or equal to the threshold,
   * and intersect with the context bitmap, which will not be modified.
   *
   * @param threshold the inclusive minimum value.
   * @param context   to be intersected with.
   * @return a bitmap of matching rows.
   */
  public RoaringBitmap gte(long threshold, RoaringBitmap context) {
    return threshold == 0 ? context.clone() : gt(threshold - 1, context);
  }

  /**
   * Returns the number of rows which have a value greater than or equal to the threshold.
   *
   * @param threshold the inclusive minimum value.
   * @return the number of matching rows.
   */
  public long gteCardinality(long threshold) {
    return threshold == 0 ? max : gtCardinality(threshold - 1);
  }

  /**
   * Returns the number of rows which have a value greater than or equal to the threshold,
   * and intersect with the context bitmap, which will not be modified.
   *
   * @param threshold the inclusive minimum value.
   * @param context   to be intersected with.
   * @return the number of matching rows.
   */
  public long gteCardinality(long threshold, RoaringBitmap context) {
    return threshold == 0 ? context.getLongCardinality() : gtCardinality(threshold - 1, context);
  }

  /**
   * Returns a RoaringBitmap of rows which have a value equal to the value.
   *
   * @param value the value to filter by.
   * @return a bitmap of matching rows.
   */
  public RoaringBitmap eq(long value) {
    return new SingleEvaluation().computePoint(value, false);
  }

  /**
   * Returns a RoaringBitmap of rows which have a value equal to the value.
   *
   * @param value the value to filter by.
   * @param context to be intersected with.
   * @return a bitmap of matching rows.
   */
  public RoaringBitmap eq(long value, RoaringBitmap context) {
    return new SingleEvaluation().computePoint(value, false, context);
  }

  /**
   * Returns the number of rows which have a value equal to the value.
   *
   * @param value the inclusive minimum value.
   * @return the number of matching rows.
   */
  public long eqCardinality(long value) {
    return new SingleEvaluation().countPoint(value, false);
  }

  /**
   * Returns the number of rows which have a value equal to the value,
   * and intersect with the context bitmap, which will not be modified.
   *
   * @param value the inclusive minimum value.
   * @param context   to be intersected with.
   * @return the number of matching rows.
   */
  public long eqCardinality(long value, RoaringBitmap context) {
    return new SingleEvaluation().countPoint(value, false, context);
  }

  /**
   * Returns a RoaringBitmap of rows which have a value not equal to the value.
   *
   * @param value the value to filter by.
   * @return a bitmap of matching rows.
   */
  public RoaringBitmap neq(long value) {
    return new SingleEvaluation().computePoint(value, true);
  }

  /**
   * Returns a RoaringBitmap of rows which have a value not equal to the value.
   *
   * @param value the value to filter by.
   * @param context to be intersected with.
   * @return a bitmap of matching rows.
   */
  public RoaringBitmap neq(long value, RoaringBitmap context) {
    return new SingleEvaluation().computePoint(value, true, context);
  }

  /**
   * Returns the number of rows which have a value not equal to the value.
   *
   * @param value the inclusive minimum value.
   * @return the number of matching rows.
   */
  public long neqCardinality(long value) {
    return new SingleEvaluation().countPoint(value, true);
  }

  /**
   * Returns the number of rows which have a value not equal to the value,
   * and intersect with the context bitmap, which will not be modified.
   *
   * @param value the inclusive minimum value.
   * @param context   to be intersected with.
   * @return the number of matching rows.
   */
  public long neqCardinality(long value, RoaringBitmap context) {
    return new SingleEvaluation().countPoint(value, true, context);
  }

  private final class SingleEvaluation {

    private final long[] bits = new long[1024];
    private final ByteBuffer buffer = RangeBitmap.this.buffer.slice().order(LITTLE_ENDIAN);

    private int position = containersOffset;
    private boolean empty = true;

    public RoaringBitmap computePoint(long value, boolean negate) {
      if (Long.numberOfLeadingZeros(value) < Long.numberOfLeadingZeros(mask)) {
        return negate ? RoaringBitmap.bitmapOfRange(0, max) : new RoaringBitmap();
      }
      RoaringArray output = new RoaringArray();
      long remaining = max;
      int mPos = masksOffset;
      char key = 0;
      while (remaining > 0) {
        long containerMask = getContainerMask(buffer, mPos, mask, bytesPerMask);
        int limit = Math.min((int) remaining, 0x10000);
        evaluateHorizontalSlicePoint(limit, value, containerMask);
        if (negate) {
          Util.flipBitmapRange(bits, 0, limit);
          empty = false;
        }
        if (!empty) {
          Container toAppend = new BitmapContainer(bits, -1).repairAfterLazy().runOptimize();
          if (!toAppend.isEmpty()) {
            output.append(key, toAppend instanceof BitmapContainer ? toAppend.clone() : toAppend);
          }
        }
        key++;
        remaining -= 0x10000;
        mPos += bytesPerMask;
      }
      return new RoaringBitmap(output);
    }

    public RoaringBitmap computePoint(long value, boolean negate, RoaringBitmap context) {
      if (context.isEmpty()) {
        return new RoaringBitmap();
      }
      if (Long.numberOfLeadingZeros(value) < Long.numberOfLeadingZeros(mask)) {
        return negate ? context.clone() : new RoaringBitmap();
      }
      RoaringArray output = new RoaringArray();
      RoaringArray contextArray = context.highLowContainer;
      int contextPos = 0;
      int maxContextKey = contextArray.keys[contextArray.size - 1];
      long remaining = max;
      int mPos = masksOffset;
      for (int prefix = 0; prefix <= maxContextKey && remaining > 0; prefix++) {
        long containerMask = getContainerMask(buffer, mPos, mask, bytesPerMask);
        if (prefix < contextArray.keys[contextPos]) {
          skipContainers(containerMask);
        } else {
          int limit = Math.min((int) remaining, 0x10000);
          evaluateHorizontalSlicePoint(limit, value, containerMask);
          if (negate) {
            Util.flipBitmapRange(bits, 0, limit);
            empty = false;
          }
          if (!empty) {
            Container toAppend = new BitmapContainer(bits, -1)
                    .iand(contextArray.values[contextPos])
                    .repairAfterLazy()
                    .runOptimize();
            if (!toAppend.isEmpty()) {
              output.append((char) prefix,
                      toAppend instanceof BitmapContainer ? toAppend.clone() : toAppend);
            }
          }
          contextPos++;
        }
        remaining -= 0x10000;
        mPos += bytesPerMask;
      }
      return new RoaringBitmap(output);
    }

    public long countPoint(long value, boolean negate) {
      if (Long.numberOfLeadingZeros(value) < Long.numberOfLeadingZeros(mask)) {
        return negate ? max : 0L;
      }
      long count = 0;
      long remaining = max;
      int mPos = masksOffset;
      while (remaining > 0) {
        long containerMask = getContainerMask(buffer, mPos, mask, bytesPerMask);
        int limit = Math.min((int) remaining, 0x10000);
        evaluateHorizontalSlicePoint(limit, value, containerMask);
        int cardinality = cardinalityInBitmapRange(bits, 0, limit);
        count += negate ? (limit - cardinality) : cardinality;
        remaining -= 0x10000;
        mPos += bytesPerMask;
      }
      return count;
    }

    private long countPoint(long threshold, boolean negate, RoaringBitmap context) {
      if (context.isEmpty()) {
        return 0L;
      }
      if (Long.numberOfLeadingZeros(threshold) < Long.numberOfLeadingZeros(mask)) {
        return negate ? context.getLongCardinality() : 0L;
      }
      RoaringArray contextArray = context.highLowContainer;
      int contextPos = 0;
      int maxContextKey = contextArray.keys[contextArray.size - 1];
      long count = 0;
      long remaining = max;
      int mPos = masksOffset;
      for (int prefix = 0; prefix <= maxContextKey && remaining > 0; prefix++) {
        int limit = Math.min(0x10000, (int) remaining);
        long containerMask = getContainerMask(buffer, mPos, mask, bytesPerMask);
        if (prefix < contextArray.keys[contextPos]) {
          skipContainers(containerMask);
        } else {
          evaluateHorizontalSlicePoint(limit, threshold, containerMask);
          if (negate) {
            Util.flipBitmapRange(bits, 0, limit);
            empty = false;
          }
          Container container = contextArray.values[contextPos];
          int cardinality = container.andCardinality(new BitmapContainer(bits, -1));
          count += cardinality;
          contextPos++;
        }
        remaining -= 0x10000;
        mPos += bytesPerMask;
      }
      return count;
    }

    public RoaringBitmap computeRange(long threshold, boolean upper) {
      if (Long.numberOfLeadingZeros(threshold) < Long.numberOfLeadingZeros(mask)) {
        return upper ? RoaringBitmap.bitmapOfRange(0, max) : new RoaringBitmap();
      }
      RoaringArray output = new RoaringArray();
      long remaining = max;
      int mPos = masksOffset;
      char key = 0;
      while (remaining > 0) {
        long containerMask = getContainerMask(buffer, mPos, mask, bytesPerMask);
        evaluateHorizontalSliceRange(remaining, threshold, containerMask);
        if (!upper) {
          Util.flipBitmapRange(bits, 0, Math.min(0x10000, (int) remaining));
          empty = false;
        }
        if (!empty) {
          Container toAppend = new BitmapContainer(bits, -1).repairAfterLazy().runOptimize();
          if (!toAppend.isEmpty()) {
            output.append(key, toAppend instanceof BitmapContainer ? toAppend.clone() : toAppend);
          }
        }
        key++;
        remaining -= 0x10000;
        mPos += bytesPerMask;
      }
      return new RoaringBitmap(output);
    }

    private RoaringBitmap computeRange(long threshold, boolean upper, RoaringBitmap context) {
      if (context.isEmpty()) {
        return new RoaringBitmap();
      }
      if (Long.numberOfLeadingZeros(threshold) < Long.numberOfLeadingZeros(mask)) {
        return upper ? context.clone() : new RoaringBitmap();
      }
      RoaringArray contextArray = context.highLowContainer;
      int contextPos = 0;
      int maxContextKey = contextArray.keys[contextArray.size - 1];
      RoaringArray output = new RoaringArray();
      long remaining = max;
      int mPos = masksOffset;
      for (int prefix = 0; prefix <= maxContextKey && remaining > 0; prefix++) {
        long containerMask = getContainerMask(buffer, mPos, mask, bytesPerMask);
        if (prefix < contextArray.keys[contextPos]) {
          skipContainers(containerMask);
        } else {
          evaluateHorizontalSliceRange(remaining, threshold, containerMask);
          if (!upper) {
            Util.flipBitmapRange(bits, 0, Math.min(0x10000, (int) remaining));
            empty = false;
          }
          if (!empty) {
            Container toAppend = new BitmapContainer(bits, -1)
                .iand(contextArray.values[contextPos])
                .repairAfterLazy()
                .runOptimize();
            if (!toAppend.isEmpty()) {
              output.append((char) prefix,
                  toAppend instanceof BitmapContainer ? toAppend.clone() : toAppend);
            }
          }
          contextPos++;
        }
        remaining -= 0x10000;
        mPos += bytesPerMask;
      }
      return new RoaringBitmap(output);
    }

    public long countRange(long threshold, boolean upper) {
      if (Long.numberOfLeadingZeros(threshold) < Long.numberOfLeadingZeros(mask)) {
        return upper ? max : 0L;
      }
      long count = 0;
      long remaining = max;
      int mPos = masksOffset;
      while (remaining > 0) {
        long containerMask = getContainerMask(buffer, mPos, mask, bytesPerMask);
        evaluateHorizontalSliceRange(remaining, threshold, containerMask);
        int remainder = Math.min((int) remaining, 0x10000);
        int cardinality = cardinalityInBitmapRange(bits, 0, remainder);
        count += upper ? cardinality : (remainder - cardinality);
        remaining -= 0x10000;
        mPos += bytesPerMask;
      }
      return count;
    }

    private long countRange(long threshold, boolean upper, RoaringBitmap context) {
      if (context.isEmpty()) {
        return 0L;
      }
      if (Long.numberOfLeadingZeros(threshold) < Long.numberOfLeadingZeros(mask)) {
        return upper ? context.getLongCardinality() : 0L;
      }
      RoaringArray contextArray = context.highLowContainer;
      int contextPos = 0;
      int maxContextKey = contextArray.keys[contextArray.size - 1];
      long count = 0;
      long remaining = max;
      int mPos = masksOffset;
      for (int prefix = 0; prefix <= maxContextKey && remaining > 0; prefix++) {
        long containerMask = getContainerMask(buffer, mPos, mask, bytesPerMask);
        if (prefix < contextArray.keys[contextPos]) {
          skipContainers(containerMask);
        } else {
          evaluateHorizontalSliceRange(remaining, threshold, containerMask);
          Container container = contextArray.values[contextPos];
          int cardinality = upper
              ? container.andCardinality(new BitmapContainer(bits, -1))
              : container.andNot(new BitmapContainer(bits, -1).repairAfterLazy()).getCardinality();
          count += cardinality;
          contextPos++;
        }
        remaining -= 0x10000;
        mPos += bytesPerMask;
      }
      return count;
    }

    private void evaluateHorizontalSliceRange(long remaining, long threshold, long containerMask) {
      // most significant absent bit in the threshold for which there is no container;
      // everything before this is wasted work, so we just skip over the containers
      int skip = 64 - Long.numberOfLeadingZeros((~(threshold | containerMask) & mask));
      int slice = 0;
      if (skip > 0) {
        for (; slice < skip; ++slice) {
          if (((containerMask >>> slice) & 1) == 1) {
            skipContainer();
          }
        }
        if (!empty) {
          Arrays.fill(bits, 0L);
          empty = true;
        }
      } else {
        // the first slice is special: if the threshold includes this slice,
        // fill the buffer, otherwise copy the slice
        if ((threshold & 1) == 1) {
          if (remaining >= 0x10000) {
            Arrays.fill(bits, -1L);
          } else {
            setBitmapRange(bits, 0, (int) remaining);
            if (!empty) {
              resetBitmapRange(bits, (int) remaining, 0x10000);
            }
          }
          if ((containerMask & 1) == 1) {
            skipContainer();
          }
          empty = false;
        } else {
          if (!empty) {
            Arrays.fill(bits, 0L);
            empty = true;
          }
          if ((containerMask & 1) == 1) {
            if ((threshold & 1) == 0) {
              orNextIntoBits();
              empty = false;
            } else {
              skipContainer();
            }
          }
        }
        slice++;
      }
      for (; slice < Long.bitCount(mask); ++slice) {
        if ((containerMask >>> slice & 1) == 1) {
          if ((threshold >>> slice & 1) == 1) {
            // bit present in both both, include bits from slice
            orNextIntoBits();
            empty = false;
          } else {
            // bit present in container, absent from threshold, filter
            if (empty) {
              skipContainer();
            } else {
              andNextIntoBits();
            }
          }
        }
      }
    }

    private void evaluateHorizontalSlicePoint(int limit, long value, long containerMask) {
      // this could be skipped if the last slice resulted in a full bitset
      setBitmapRange(bits, 0, limit);
      resetBitmapRange(bits, limit, 0x10000);
      empty = false;
      // for each i
      //  - if the bit i is not set in the value, intersect the container with the bits
      //  - if the bit i is set in value, remove the container from the bits
      for (int slice = 0; slice < Long.bitCount(mask); slice++) {
        if (((value >>> slice) & 1) == 1) {
          // bit present, need to remove the container values from bits (bits &= ~container)
          if (((containerMask >>> slice) & 1) == 1) {
            if (empty) {
              skipContainer();
            } else {
              removeNextFromBits();
            }
          }
        } else {
          // bit not present, need to intersect the container bits (bits &= container)
          if (((containerMask >>> slice) & 1) == 1) {
            if (empty) {
              skipContainer();
            } else {
              andNextIntoBits();
            }
          } else if (!empty) {
            // there's no container so we can just clear the bits
            // but it can be skipped if the bits are already known to be empty
            resetBitmapRange(bits, 0, limit);
            empty = true;
          }
        }
      }
    }

    private void andNextIntoBits() {
      int type = buffer.get(position);
      position++;
      int size = buffer.getChar(position) & 0xFFFF;
      position += Character.BYTES;
      switch (type) {
        case ARRAY: {
          int skip = size << 1;
          CharBuffer cb = (CharBuffer) ((ByteBuffer) buffer.position(position)).asCharBuffer()
              .limit(skip >>> 1);
          MappeableArrayContainer array = new MappeableArrayContainer(cb, size);
          array.andInto(bits);
          position += skip;
        }
        break;
        case BITMAP: {
          LongBuffer lb = (LongBuffer) ((ByteBuffer) buffer.position(position)).asLongBuffer()
              .limit(1024);
          MappeableBitmapContainer bitmap = new MappeableBitmapContainer(lb, size);
          bitmap.andInto(bits);
          position += BITMAP_SIZE;
        }
        break;
        case RUN: {
          int skip = size << 2;
          CharBuffer cb = (CharBuffer) ((ByteBuffer) buffer.position(position)).asCharBuffer()
              .limit(skip >>> 1);
          MappeableRunContainer run = new MappeableRunContainer(cb, size);
          run.andInto(bits);
          position += skip;
        }
        break;
        default:
          throw new IllegalStateException("Unknown type " + type
              + " (this is a bug, please report it.)");
      }
    }

    private void orNextIntoBits() {
      int type = buffer.get(position);
      position++;
      int size = buffer.getChar(position) & 0xFFFF;
      position += Character.BYTES;
      switch (type) {
        case ARRAY: {
          int skip = size << 1;
          CharBuffer cb = (CharBuffer) ((ByteBuffer) buffer.position(position)).asCharBuffer()
              .limit(skip >>> 1);
          MappeableArrayContainer array = new MappeableArrayContainer(cb, size);
          array.orInto(bits);
          position += skip;
        }
        break;
        case BITMAP: {
          LongBuffer lb = (LongBuffer) ((ByteBuffer) buffer.position(position)).asLongBuffer()
              .limit(1024);
          MappeableBitmapContainer bitmap = new MappeableBitmapContainer(lb, size);
          bitmap.orInto(bits);
          position += BITMAP_SIZE;
        }
        break;
        case RUN: {
          int skip = size << 2;
          CharBuffer cb = (CharBuffer) ((ByteBuffer) buffer.position(position)).asCharBuffer()
              .limit(skip >>> 1);
          MappeableRunContainer run = new MappeableRunContainer(cb, size);
          run.orInto(bits);
          position += skip;
        }
        break;
        default:
          throw new IllegalStateException("Unknown type " + type
              + " (this is a bug, please report it.)");
      }
    }

    private void removeNextFromBits() {
      int type = buffer.get(position);
      position++;
      int size = buffer.getChar(position) & 0xFFFF;
      position += Character.BYTES;
      switch (type) {
        case ARRAY: {
          int skip = size << 1;
          CharBuffer cb = (CharBuffer) ((ByteBuffer) buffer.position(position)).asCharBuffer()
                  .limit(skip >>> 1);
          MappeableArrayContainer array = new MappeableArrayContainer(cb, size);
          array.removeFrom(bits);
          position += skip;
        }
        break;
        case BITMAP: {
          LongBuffer lb = (LongBuffer) ((ByteBuffer) buffer.position(position)).asLongBuffer()
                  .limit(1024);
          MappeableBitmapContainer bitmap = new MappeableBitmapContainer(lb, size);
          bitmap.removeFrom(bits);
          position += BITMAP_SIZE;
        }
        break;
        case RUN: {
          int skip = size << 2;
          CharBuffer cb = (CharBuffer) ((ByteBuffer) buffer.position(position)).asCharBuffer()
                  .limit(skip >>> 1);
          MappeableRunContainer run = new MappeableRunContainer(cb, size);
          run.removeFrom(bits);
          position += skip;
        }
        break;
        default:
          throw new IllegalStateException("Unknown type " + type
                  + " (this is a bug, please report it.)");
      }
    }

    private void skipContainer() {
      int type = buffer.get(position);
      int size = buffer.getChar(position + 1) & 0xFFFF;
      if (type == BITMAP) {
        position += 3 + BITMAP_SIZE;
      } else {
        position += 3 + (size << (type == RUN ? 2 : 1));
      }
    }

    private void skipContainers(long mask) {
      for (int i = 0; i < Long.bitCount(mask); i++) {
        skipContainer();
      }
    }
  }

  private final class DoubleEvaluation {

    private final ByteBuffer buffer = RangeBitmap.this.buffer.slice().order(LITTLE_ENDIAN);
    private final Bits low = new Bits();
    private final Bits high = new Bits();

    private int position = containersOffset;

    public RoaringBitmap compute(long lower, long upper) {
      RoaringArray output = new RoaringArray();
      long remaining = max;
      int mPos = masksOffset;
      char key = 0;
      while (remaining > 0) {
        long containerMask = getContainerMask(buffer, mPos, mask, bytesPerMask);
        evaluateHorizontalSlice(containerMask, remaining, lower, upper);
        if (!low.empty && !high.empty) {
          if (low.full && high.full) {
            output.append(key, RunContainer.full());
          } else {
            final long[] bits;
            if (low.full) {
              bits = high.bits;
            } else if (high.full) {
              bits = low.bits;
            } else {
              bits = low.bits;
              for (int i = 0; i < Math.min(bits.length, high.bits.length); i++) {
                bits[i] &= high.bits[i];
              }
            }
            Container toAppend = new BitmapContainer(bits, -1).repairAfterLazy().runOptimize();
            if (!toAppend.isEmpty()) {
              output.append(key, toAppend instanceof BitmapContainer ? toAppend.clone() : toAppend);
            }
          }
        }
        key++;
        remaining -= 0x10000;
        mPos += bytesPerMask;
      }
      return new RoaringBitmap(output);
    }

    public long count(long lower, long upper) {
      long count = 0;
      long remaining = max;
      int mPos = masksOffset;
      while (remaining > 0) {
        long containerMask = getContainerMask(buffer, mPos, mask, bytesPerMask);
        evaluateHorizontalSlice(containerMask, remaining, lower, upper);
        if (!low.empty && !high.empty) {
          int remainder = Math.min((int) remaining, 0x10000);
          if (low.full && high.full) {
            count += remainder;
          } else {
            if (low.full) {
              count += cardinalityInBitmapRange(high.bits, 0, remainder);
            } else if (high.full) {
              count += cardinalityInBitmapRange(low.bits, 0, remainder);
            } else {
              for (int i = 0; i < Math.min(low.bits.length, high.bits.length); i++) {
                high.bits[i] &= low.bits[i];
              }
              count += cardinalityInBitmapRange(high.bits, 0, remainder);
            }
          }
        }
        remaining -= 0x10000;
        mPos += bytesPerMask;
      }
      return count;
    }

    public long count(long lower, long upper, RoaringBitmap context) {
      long count = 0;
      long remaining = max;
      int mPos = masksOffset;
      RoaringArray contextArray = context.highLowContainer;
      int contextPos = 0;
      int maxContextKey = contextArray.keys[contextArray.size - 1];
      for (int prefix = 0; prefix <= maxContextKey && remaining > 0; prefix++) {
        long containerMask = getContainerMask(buffer, mPos, mask, bytesPerMask);
        if (prefix < contextArray.keys[contextPos]) {
          for (int i = 0; i < Long.bitCount(containerMask); i++) {
            skipContainer();
          }
        } else {
          evaluateHorizontalSlice(containerMask, remaining, lower, upper);
          if (!low.empty && !high.empty) {
            Container container = contextArray.values[contextPos];
            if (low.full && high.full) {
              count += container.getCardinality();
            } else {
              if (low.full) {
                count += new BitmapContainer(high.bits, -1).andCardinality(container);
              } else if (high.full) {
                count += new BitmapContainer(low.bits, -1).andCardinality(container);
              } else {
                for (int i = 0; i < Math.min(low.bits.length, high.bits.length); i++) {
                  high.bits[i] &= low.bits[i];
                }
                count += new BitmapContainer(high.bits, -1).andCardinality(container);
              }
            }
          }
        }
        remaining -= 0x10000;
        mPos += bytesPerMask;
      }
      return count;
    }

    private void evaluateHorizontalSlice(long containerMask, long remaining, long lower,
                                         long upper) {
      // most significant absent bit in the threshold for which there is no container;
      // everything before this is wasted work, so we just skip over the containers
      int skipLow = 64 - Long.numberOfLeadingZeros((~(lower | containerMask) & mask));
      if (skipLow == 64) {
        lower = 0L;
      } else if (skipLow > 0) {
        lower &= -(1L << skipLow);
      }
      int skipHigh = 64 - Long.numberOfLeadingZeros((~(upper | containerMask) & mask));
      if (skipHigh == 64) {
        upper = 0L;
      } else if (skipHigh > 0) {
        upper &= -(1L << skipHigh);
      }
      setupFirstSlice(upper, high, (int) remaining, skipHigh == 0);
      setupFirstSlice(lower, low, (int) remaining, skipLow == 0);
      if ((containerMask & 1) == 1) {
        skipContainer();
      }
      containerMask >>>= 1;
      upper >>>= 1;
      lower >>>= 1;
      for (; containerMask != 0; containerMask >>>= 1, upper >>>= 1, lower >>>= 1) {
        if ((containerMask & 1) == 1) {
          int flags = (int) ((upper & 1) | ((lower & 1) << 1));
          switch (flags) {
            case 0: // both absent
              andLowAndHigh();
              break;
            case 1: // upper present, lower absent
              andLowOrHigh();
              break;
            case 2: // lower present, upper absent
              orLowAndHigh();
              break;
            case 3: // both present
              orLowOrHigh();
              break;
            default:
          }
        }
      }
      low.flip(0, Math.min((int) remaining, 0x10000));
    }

    private void setupFirstSlice(long threshold, Bits bits, int remaining, boolean copy) {
      if ((threshold & 1) == 1) {
        if (remaining >= 0x10000) {
          bits.fill();
        } else {
          bits.reset(remaining);
        }
      } else {
        bits.clear();
        if (copy) {
          orNextIntoBits(bits);
        }
      }
    }

    private void orLowOrHigh() {
      int type = buffer.get(position);
      position++;
      int size = buffer.getChar(position) & 0xFFFF;
      position += Character.BYTES;
      switch (type) {
        case ARRAY: {
          int skip = size << 1;
          CharBuffer cb = (CharBuffer) ((ByteBuffer) buffer.position(position)).asCharBuffer()
              .limit(skip >>> 1);
          MappeableArrayContainer array = new MappeableArrayContainer(cb, size);
          low.or(array);
          high.or(array);
          position += skip;
        }
        break;
        case BITMAP: {
          LongBuffer lb = (LongBuffer) ((ByteBuffer) buffer.position(position)).asLongBuffer()
              .limit(1024);
          MappeableBitmapContainer bitmap = new MappeableBitmapContainer(lb, size);
          low.or(bitmap);
          high.or(bitmap);
          position += BITMAP_SIZE;
        }
        break;
        case RUN: {
          int skip = size << 2;
          CharBuffer cb = (CharBuffer) ((ByteBuffer) buffer.position(position)).asCharBuffer()
              .limit(skip >>> 1);
          MappeableRunContainer run = new MappeableRunContainer(cb, size);
          low.or(run);
          high.or(run);
          position += skip;
        }
        break;
        default:
          throw new IllegalStateException("Unknown type " + type
              + " (this is a bug, please report it.)");
      }
    }

    private void orLowAndHigh() {
      int type = buffer.get(position);
      position++;
      int size = buffer.getChar(position) & 0xFFFF;
      position += Character.BYTES;
      switch (type) {
        case ARRAY: {
          int skip = size << 1;
          CharBuffer cb = (CharBuffer) ((ByteBuffer) buffer.position(position)).asCharBuffer()
              .limit(skip >>> 1);
          MappeableArrayContainer array = new MappeableArrayContainer(cb, size);
          low.or(array);
          high.and(array);
          position += skip;
        }
        break;
        case BITMAP: {
          LongBuffer lb = (LongBuffer) ((ByteBuffer) buffer.position(position)).asLongBuffer()
              .limit(1024);
          MappeableBitmapContainer bitmap = new MappeableBitmapContainer(lb, size);
          low.or(bitmap);
          high.and(bitmap);
          position += BITMAP_SIZE;
        }
        break;
        case RUN: {
          int skip = size << 2;
          CharBuffer cb = (CharBuffer) ((ByteBuffer) buffer.position(position)).asCharBuffer()
              .limit(skip >>> 1);
          MappeableRunContainer run = new MappeableRunContainer(cb, size);
          low.or(run);
          high.and(run);
          position += skip;
        }
        break;
        default:
          throw new IllegalStateException("Unknown type " + type
              + " (this is a bug, please report it.)");
      }
    }

    private void andLowOrHigh() {
      int type = buffer.get(position);
      position++;
      int size = buffer.getChar(position) & 0xFFFF;
      position += Character.BYTES;
      switch (type) {
        case ARRAY: {
          int skip = size << 1;
          CharBuffer cb = (CharBuffer) ((ByteBuffer) buffer.position(position)).asCharBuffer()
              .limit(skip >>> 1);
          MappeableArrayContainer array = new MappeableArrayContainer(cb, size);
          low.and(array);
          high.or(array);
          position += skip;
        }
        break;
        case BITMAP: {
          LongBuffer lb = (LongBuffer) ((ByteBuffer) buffer.position(position)).asLongBuffer()
              .limit(1024);
          MappeableBitmapContainer bitmap = new MappeableBitmapContainer(lb, size);
          low.and(bitmap);
          high.or(bitmap);
          position += BITMAP_SIZE;
        }
        break;
        case RUN: {
          int skip = size << 2;
          CharBuffer cb = (CharBuffer) ((ByteBuffer) buffer.position(position)).asCharBuffer()
              .limit(skip >>> 1);
          MappeableRunContainer run = new MappeableRunContainer(cb, size);
          low.and(run);
          high.or(run);
          position += skip;
        }
        break;
        default:
          throw new IllegalStateException("Unknown type " + type
              + " (this is a bug, please report it.)");
      }
    }

    private void andLowAndHigh() {
      int type = buffer.get(position);
      position++;
      int size = buffer.getChar(position) & 0xFFFF;
      position += Character.BYTES;
      switch (type) {
        case ARRAY: {
          int skip = size << 1;
          CharBuffer cb = (CharBuffer) ((ByteBuffer) buffer.position(position)).asCharBuffer()
              .limit(skip >>> 1);
          MappeableArrayContainer array = new MappeableArrayContainer(cb, size);
          low.and(array);
          high.and(array);
          position += skip;
        }
        break;
        case BITMAP: {
          LongBuffer lb = (LongBuffer) ((ByteBuffer) buffer.position(position)).asLongBuffer()
              .limit(1024);
          MappeableBitmapContainer bitmap = new MappeableBitmapContainer(lb, size);
          low.and(bitmap);
          high.and(bitmap);
          position += BITMAP_SIZE;
        }
        break;
        case RUN: {
          int skip = size << 2;
          CharBuffer cb = (CharBuffer) ((ByteBuffer) buffer.position(position)).asCharBuffer()
              .limit(skip >>> 1);
          MappeableRunContainer run = new MappeableRunContainer(cb, size);
          low.and(run);
          high.and(run);
          position += skip;
        }
        break;
        default:
          throw new IllegalStateException("Unknown type " + type
              + " (this is a bug, please report it.)");
      }
    }

    private void orNextIntoBits(Bits bits) {
      int type = buffer.get(position);
      int size = buffer.getChar(position + 1) & 0xFFFF;
      switch (type) {
        case ARRAY: {
          int skip = size << 1;
          CharBuffer cb = (CharBuffer) ((ByteBuffer) buffer.position(position + 3)).asCharBuffer()
              .limit(skip >>> 1);
          bits.or(new MappeableArrayContainer(cb, size));
        }
        break;
        case BITMAP: {
          LongBuffer lb = (LongBuffer) ((ByteBuffer) buffer.position(position + 3)).asLongBuffer()
              .limit(1024);
          bits.or(new MappeableBitmapContainer(lb, size));
        }
        break;
        case RUN: {
          int skip = size << 2;
          CharBuffer cb = (CharBuffer) ((ByteBuffer) buffer.position(position + 3)).asCharBuffer()
              .limit(skip >>> 1);
          bits.or(new MappeableRunContainer(cb, size));
        }
        break;
        default:
          throw new IllegalStateException("Unknown type " + type
              + " (this is a bug, please report it.)");
      }
    }

    private void skipContainer() {
      int type = buffer.get(position);
      int size = buffer.getChar(position + 1) & 0xFFFF;
      if (type == BITMAP) {
        position += 3 + BITMAP_SIZE;
      } else {
        position += 3 + (size << (type == RUN ? 2 : 1));
      }
    }
  }

  private static final class Bits {
    private final long[] bits = new long[1024];
    private boolean empty = true;
    private boolean full = false;

    public void clear() {
      if (!empty) {
        Arrays.fill(bits, 0L);
        makeEmpty();
      }
    }

    public void fill() {
      if (!full) {
        Arrays.fill(bits, -1L);
        makeFull();
      }
    }

    public void reset(int boundary) {
      if (!full) {
        setBitmapRange(bits, 0, boundary);
      }
      if (!empty) {
        resetBitmapRange(bits, boundary, 0x10000);
      }
      makeNonEmpty();
      makeNonFull();
    }

    public void flip(int from, int to) {
      Util.flipBitmapRange(bits, from, to);
      if (!full) {
        if (empty) {
          makeFull();
        }
      } else {
        makeEmpty();
      }
    }

    public void or(MappeableContainer container) {
      if (container.isFull()) {
        fill();
      } else if (!full) {
        container.orInto(bits);
        makeNonEmpty();
      }
    }

    public void and(MappeableContainer container) {
      if (!empty && !container.isFull()) {
        container.andInto(bits);
        makeNonFull();
      }
    }

    private void makeEmpty() {
      this.empty = true;
      this.full = false;
    }

    private void makeNonEmpty() {
      this.empty = false;
    }

    private void makeFull() {
      this.full = true;
      this.empty = false;
    }

    private void makeNonFull() {
      this.full = false;
    }
  }

  private static long getContainerMask(ByteBuffer buffer, int position, long mask,
                                       int bytesPerMask) {
    switch (bytesPerMask) {
      case 0:
      case 1:
        return buffer.get(position) & mask;
      case 2:
        return buffer.getChar(position) & mask;
      case 3:
      case 4:
        return buffer.getInt(position) & mask;
      default:
        return buffer.getLong(position) & mask;
    }
  }

  /**
   * Builder for constructing immutable RangeBitmaps
   */
  public static final class Appender {

    private static final int GROWTH = 8;

    Appender(long maxValue, IntFunction<ByteBuffer> bufferSupplier, Consumer<ByteBuffer> cleaner) {
      this.bufferSupplier = bufferSupplier;
      this.bufferCleaner = cleaner;
      this.rangeMask = rangeMask(maxValue);
      this.bytesPerMask = bytesPerMask(maxValue);
      this.slice = new Container[Long.bitCount(rangeMask)];
      for (int i = 0; i < slice.length; ++i) {
        slice[i] = containerForSlice(i);
      }
      this.maskBuffer = bufferSupplier.apply(maskBufferGrowth());
      this.containers = bufferSupplier.apply(containerGrowth() * 1024);
    }

    private final IntFunction<ByteBuffer> bufferSupplier;
    private final Consumer<ByteBuffer> bufferCleaner;
    private final byte bytesPerMask;
    private final long rangeMask;
    private final Container[] slice;
    private ByteBuffer maskBuffer;
    private ByteBuffer containers;
    private int bufferPos;
    private long mask;
    private int rid;
    private int key = 0;
    private int serializedContainerSize;
    private boolean dirty;

    /**
     * Converts the appender into an immutable range index.
     *
     * @param supplier provides an appropriate ByteBuffer to store into
     * @return a queriable RangeBitmap
     */
    public RangeBitmap build(IntFunction<ByteBuffer> supplier) {
      flush();
      return build(supplier.apply(serializedSizeInBytes()));
    }

    /**
     * see build(IntFunction)
     */
    public RangeBitmap build() {
      return build(capacity -> ByteBuffer.allocate(capacity).order(LITTLE_ENDIAN));
    }

    /**
     * Converts the appender into an immutable range index, using the supplied ByteBuffer.
     *
     * @param buffer a little endian buffer which must have sufficient capacity for the appended
     *               values.
     * @return a queriable RangeBitmap
     */
    public RangeBitmap build(ByteBuffer buffer) {
      serialize(buffer);
      buffer.flip();
      return RangeBitmap.map(buffer);
    }

    /**
     * Call this to reuse the appender and its buffers
     */
    public void clear() {
      // no need to zero the buffers, we'll just write over them next time.
      containers.position(0);
      bufferPos = 0;
      mask = 0;
      rid = 0;
      key = 0;
      serializedContainerSize = 0;
    }

    /**
     * Returns the size of the RangeBitmap on disk.
     *
     * @return the serialized size in bytes.
     */
    public int serializedSizeInBytes() {
      flush();
      int cookieSize = 2;
      int baseSize = 1;
      int slicesSize = 1;
      int maxKeySize = 2;
      int maxRidSize = 4;
      int headerSize = cookieSize
          + baseSize
          + slicesSize
          + maxKeySize
          + maxRidSize;
      int keysSize = key * bytesPerMask;
      return headerSize + keysSize + serializedContainerSize;
    }

    /**
     * Serializes the bitmap to the buffer without materialising it.
     * The user should call serializedSizeInBytes to size the
     * buffer appropriately.
     * <p>
     * It is not guaranteed that all values will be written
     *
     * @param buffer expected to be large enough to contain the bitmap.
     */
    public void serialize(ByteBuffer buffer) {
      if (flush()) {
        throw new IllegalStateException(
            "Attempted to serialize without calling serializedSizeInBytes first");
      }
      ByteBuffer target = buffer.order() == LITTLE_ENDIAN
          ? buffer
          : buffer.slice().order(LITTLE_ENDIAN);
      target.putChar((char) COOKIE);
      target.put((byte) 2);
      target.put((byte) Long.bitCount(rangeMask));
      target.putChar((char) key);
      target.putInt(rid);
      int spaceForKeys = key * bytesPerMask;
      target.put(((ByteBuffer) maskBuffer.slice()
          .order(LITTLE_ENDIAN).limit(spaceForKeys)));
      target.put(((ByteBuffer) containers.slice()
          .order(LITTLE_ENDIAN).limit(serializedContainerSize)));
      if (buffer != target) {
        buffer.position(target.position());
      }
    }

    /**
     * Adds the value and associates it with the current row index.
     *
     * @param value the value, will be rejected if greater than max value.
     */
    public void add(long value) {
      if ((value & rangeMask) == value) {
        long bits = ~value & rangeMask;
        mask |= bits;
        while (bits != 0) {
          int index = Long.numberOfTrailingZeros(bits);
          bits &= (bits - 1);
          Container c = slice[index];
          Container updated = c.add((char) rid);
          // avoid useless write barrier
          if (updated != c) {
            slice[index] = updated;
          }
        }
      } else {
        throw new IllegalArgumentException(value + " too large");
      }
      rid++;
      dirty = true;
      if (rid >>> 16 > key) {
        append();
      }
    }

    private boolean flush() {
      if (dirty) {
        append();
        return true;
      }
      return false;
    }

    private void append() {
      if (maskBuffer.capacity() - bufferPos < 8) {
        maskBuffer = growBuffer(maskBuffer, maskBufferGrowth());
        maskBuffer.position(0);
      }
      maskBuffer.putLong(bufferPos, mask);
      bufferPos += bytesPerMask;
      for (Container container : slice) {
        if (!container.isEmpty()) {
          Container toSerialize = container.runOptimize();
          int serializedSize = toSerialize.serializedSizeInBytes();
          int type = (toSerialize instanceof BitmapContainer)
              ? BITMAP
              : (toSerialize instanceof RunContainer) ? RUN : ARRAY;
          int required = serializedSize + (type == BITMAP ? 3 : 1);
          if (containers.capacity() - serializedContainerSize < required) {
            int growthFactor = 8192 * slice.length;
            int newSize = Math.max(growthFactor, (required + 8191) & -8192);
            containers = growBuffer(containers, newSize);
          }
          containers.put(serializedContainerSize, (byte) type);
          if (type == BITMAP) {
            containers.putChar(serializedContainerSize + 1, (char) (container.getCardinality()));
            containers.position(serializedContainerSize + 3);
            toSerialize.writeArray(containers);
            containers.position(0);
            serializedContainerSize += required;
          } else if (type == RUN) {
            containers.position(serializedContainerSize + 1);
            toSerialize.writeArray(containers);
            containers.position(0);
            serializedContainerSize += required;
          } else {
            containers.putChar(serializedContainerSize + 1, (char) (container.getCardinality()));
            containers.position(serializedContainerSize + 3);
            toSerialize.writeArray(containers);
            containers.position(0);
            serializedContainerSize += required;
          }
          container.clear(); // for reuse
        }
      }
      mask = 0;
      key++;
      dirty = false;
    }

    private int maskBufferGrowth() {
      // 8 containers is space for ~0.5M rows, * the size in bytes of each mask
      return GROWTH * bytesPerMask;
    }

    private int containerGrowth() {
      return GROWTH * slice.length;
    }

    private ByteBuffer growBuffer(ByteBuffer buffer, int growth) {
      ByteBuffer newBuffer = bufferSupplier.apply(buffer.capacity() + growth);
      int pos = buffer.position();
      newBuffer.put(buffer);
      buffer.position(pos);
      bufferCleaner.accept(buffer);
      return newBuffer;
    }

    private Container containerForSlice(int sliceNumber) {
      if (sliceNumber >= 5) {
        return new RunContainer();
      }
      return new BitmapContainer();
    }

    /**
     * Produces a mask covering the smallest number of bytes required
     * to represent the sliced max value.
     *
     * @param maxValue the maximum value this bitmap should support.
     * @return a mask with a multiple of 8 contiguous bits set.
     */
    private static long rangeMask(long maxValue) {
      int lz = Long.numberOfLeadingZeros(maxValue | 1);
      return -1L >>> lz;
    }

    private static byte bytesPerMask(long maxValue) {
      int lz = Long.numberOfLeadingZeros(maxValue | 1);
      return (byte) ((64 - lz + 7) >>> 3);
    }
  }
}
