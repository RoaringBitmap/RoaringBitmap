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
   * @param maxValue       the maximum value to be appended, values larger than this
   *                       value will be rejected.
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
    long mask = sliceCount == 64 ? -1L : (1L << sliceCount) - 1;
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
   * Returns a RoaringBitmap of rows which have a value less than or equal to the threshold.
   *
   * @param threshold the inclusive maximum value.
   * @return a bitmap of matching rows.
   */
  public RoaringBitmap lte(long threshold) {
    return evaluateRange(threshold, true);
  }

  /**
   * Returns a RoaringBitmap of rows which have a value less than or equal to the threshold,
   * and intersect with the context bitmap, which will not be modified.
   *
   * @param threshold the inclusive maximum value.
   * @param context to be intersected with.
   * @return a bitmap of matching rows.
   */
  public RoaringBitmap lte(long threshold, RoaringBitmap context) {
    return evaluateRange(threshold, true, context);
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
   * @param context to be intersected with.
   * @return a bitmap of matching rows which intersect .
   */
  public RoaringBitmap lt(long threshold, RoaringBitmap context) {
    return threshold == 0 ? new RoaringBitmap() : lte(threshold - 1, context);
  }

  /**
   * Returns a RoaringBitmap of rows which have a value greater than the threshold.
   *
   * @param threshold the exclusive minimum value.
   * @return a bitmap of matching rows.
   */
  public RoaringBitmap gt(long threshold) {
    return evaluateRange(threshold, false);
  }

  /**
   * Returns a RoaringBitmap of rows which have a value greater than the threshold,
   * and intersect with the context bitmap, which will not be modified.
   *
   * @param threshold the exclusive minimum value.
   * @param context to be intersected with.
   * @return a bitmap of matching rows.
   */
  public RoaringBitmap gt(long threshold, RoaringBitmap context) {
    return evaluateRange(threshold, false, context);
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
   * @param context to be intersected with.
   * @return a bitmap of matching rows.
   */
  public RoaringBitmap gte(long threshold, RoaringBitmap context) {
    return threshold == 0 ? context.clone() : gt(threshold - 1, context);
  }

  private RoaringBitmap evaluateRange(long threshold, boolean upper) {
    if (Long.numberOfLeadingZeros(threshold) < Long.numberOfLeadingZeros(mask)) {
      return upper ? RoaringBitmap.bitmapOfRange(0, max) : new RoaringBitmap();
    }
    ByteBuffer containers = this.buffer.slice().order(LITTLE_ENDIAN);
    containers.position(containersOffset);
    RoaringArray output = new RoaringArray();
    long[] bits = new long[1024];
    long remaining = max;
    int mPos = masksOffset;
    char key = 0;
    boolean empty = true;
    while (remaining > 0) {
      long containerMask = this.buffer.getLong(mPos) & mask;
      empty = evaluateHorizontalSlice(containers, remaining, threshold, containerMask, empty, bits);
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

  private RoaringBitmap evaluateRange(long threshold, boolean upper, RoaringBitmap context) {
    if (context.isEmpty()) {
      return new RoaringBitmap();
    }
    if (Long.numberOfLeadingZeros(threshold) < Long.numberOfLeadingZeros(mask)) {
      return upper ? RoaringBitmap.bitmapOfRange(0, max) : new RoaringBitmap();
    }
    ByteBuffer containers = this.buffer.slice().order(LITTLE_ENDIAN);
    containers.position(containersOffset);
    RoaringArray contextArray = context.highLowContainer;
    int contextPos = 0;
    int maxContextKey = contextArray.keys[contextArray.size - 1];
    RoaringArray output = new RoaringArray();
    long[] bits = new long[1024];
    long remaining = max;
    int mPos = masksOffset;
    boolean empty = true;
    for (int prefix = 0; prefix <= maxContextKey && remaining > 0; prefix++) {
      long containerMask = this.buffer.getLong(mPos) & mask;
      if (prefix < contextArray.keys[contextPos]) {
        for (int i = 0; i < Long.bitCount(containerMask); i++) {
          skipContainer(containers);
        }
      } else {
        empty = evaluateHorizontalSlice(containers,
            remaining, threshold, containerMask, empty, bits);
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

  private boolean evaluateHorizontalSlice(ByteBuffer containers,
                                          long remaining,
                                          long threshold,
                                          long containerMask,
                                          boolean empty,
                                          long[] bits) {
    // most significant absent bit in the threshold for which there is no container;
    // everything before this is wasted work, so we just skip over the containers
    int skip = 64 - Long.numberOfLeadingZeros(((~threshold & ~containerMask) & mask));
    int slice = 0;
    if (skip > 0) {
      for (; slice < skip; ++slice) {
        if (((containerMask >>> slice) & 1) == 1) {
          skipContainer(containers);
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
          skipContainer(containers);
        }
        empty = false;
      } else {
        if (!empty) {
          Arrays.fill(bits, 0L);
          empty = true;
        }
        if ((containerMask & 1) == 1) {
          if ((threshold & 1) == 0) {
            nextContainer(containers).orInto(bits);
            empty = false;
          } else {
            skipContainer(containers);
          }
        }
      }
      slice++;
    }
    for (; slice < Long.bitCount(mask); ++slice) {
      if ((containerMask >>> slice & 1) == 1) {
        if ((threshold >>> slice & 1) == 1) {
          // bit present in both both, include bits from slice
          nextContainer(containers).orInto(bits);
          empty = false;
        } else {
          // bit present in container, absent from threshold, filter
          if (empty) {
            skipContainer(containers);
          } else {
            nextContainer(containers).andInto(bits);
          }
        }
      }
    }
    return empty;
  }

  private static MappeableContainer nextContainer(ByteBuffer buffer) {
    int type = buffer.get();
    int size = buffer.getChar() & 0xFFFF;
    if (type == BITMAP) {
      LongBuffer lb = ((ByteBuffer) buffer.slice()
          .order(LITTLE_ENDIAN).limit(BITMAP_SIZE)).asLongBuffer();
      buffer.position(buffer.position() + BITMAP_SIZE);
      return new MappeableBitmapContainer(lb, size);
    } else {
      int skip = size << (type == RUN ? 2 : 1);
      CharBuffer cb = ((ByteBuffer) buffer.slice().order(LITTLE_ENDIAN).limit(skip)).asCharBuffer();
      buffer.position(buffer.position() + skip);
      return type == RUN
          ? new MappeableRunContainer(cb, size)
          : new MappeableArrayContainer(cb, size);
    }
  }

  private static void skipContainer(ByteBuffer buffer) {
    int type = buffer.get();
    int size = buffer.getChar() & 0xFFFF;
    if (type == BITMAP) {
      buffer.position(buffer.position() + BITMAP_SIZE);
    } else {
      int skip = size << (type == RUN ? 2 : 1);
      buffer.position(buffer.position() + skip);
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
     * {@see #build(IntFunction)}
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
     * The user should call {@see serializedSizeInBytes} to size the
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
      if (rid >>> 16 > key) {
        append();
      }
    }

    private boolean flush() {
      if (mask != 0) {
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
            containers = growBuffer(containers, containerGrowth() * 1024);
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
      return lz == 0 ? -1L : (1L << (64 - lz)) - 1;
    }

    private static byte bytesPerMask(long maxValue) {
      int lz = Long.numberOfLeadingZeros(maxValue | 1);
      return (byte) ((64 - lz + 7) >>> 3);
    }
  }
}
