package org.roaringbitmap.bsi.longlong;

import org.roaringbitmap.bsi.BitmapSliceIndex;
import org.roaringbitmap.bsi.Pair;
import org.roaringbitmap.bsi.WritableUtils;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

public class Roaring64BitmapSliceIndex {
  /**
   * the maxValue of this bsi
   */
  private long maxValue;

  /**
   * the minValue of this bsi
   */
  private long minValue;

  /**
   * the bit component slice Array of this bsi
   */
  private Roaring64Bitmap[] bA;

  /**
   * the exist bitmap of this bsi which means the columnId have value in this bsi
   */
  private Roaring64Bitmap ebM;

  private Boolean runOptimized = false;

  /**
   * NewBSI constructs a new BSI.  Min/Max values are optional.  If set to 0
   * then the underlying BSI will be automatically sized.
   */
  public Roaring64BitmapSliceIndex(long minValue, long maxValue) {
    if (minValue < 0) {
      throw new IllegalArgumentException("Values should be non-negative");
    }

    this.bA = new Roaring64Bitmap[64 - Long.numberOfLeadingZeros(maxValue)];
    for (int i = 0; i < bA.length; i++) {
      this.bA[i] = new Roaring64Bitmap();
    }

    this.ebM = new Roaring64Bitmap();
  }

  /**
   * NewDefaultBSI constructs an auto-sized BSI
   */
  public Roaring64BitmapSliceIndex() {
    this(0L, 0L);
  }

  public void add(Roaring64BitmapSliceIndex otherBsi) {
    if (null == otherBsi || otherBsi.ebM.isEmpty()) {
      return;
    }

    this.ebM.or(otherBsi.ebM);
    if (otherBsi.bitCount() > this.bitCount()) {
      grow(otherBsi.bitCount());
    }

    for (int i = 0; i < otherBsi.bitCount(); i++) {
      this.addDigit(otherBsi.bA[i], i);
    }

    // update min and max after adding
    this.minValue = minValue();
    this.maxValue = maxValue();
  }

  private void addDigit(Roaring64Bitmap foundSet, int i) {
    Roaring64Bitmap carry = Roaring64Bitmap.and(this.bA[i], foundSet);
    this.bA[i].xor(foundSet);
    if (!carry.isEmpty()) {
      if (i + 1 >= this.bitCount()) {
        grow(this.bitCount() + 1);
      }
      this.addDigit(carry, i + 1);
    }
  }

  private long minValue() {
    if (ebM.isEmpty()) {
      return 0;
    }

    Roaring64Bitmap minValuesId = ebM;
    for (int i = bA.length - 1; i >= 0; i -= 1) {
      Roaring64Bitmap tmp = Roaring64Bitmap.andNot(minValuesId, bA[i]);
      if (!tmp.isEmpty()) {
        minValuesId = tmp;
      }
    }

    return valueAt(minValuesId.first());
  }

  private long maxValue() {
    if (ebM.isEmpty()) {
      return 0;
    }

    Roaring64Bitmap maxValuesId = ebM;
    for (int i = bA.length - 1; i >= 0; i -= 1) {
      Roaring64Bitmap tmp = Roaring64Bitmap.and(maxValuesId, bA[i]);
      if (!tmp.isEmpty()) {
        maxValuesId = tmp;
      }
    }

    return valueAt(maxValuesId.first());
  }

  private long valueAt(long columnId) {
    long value = 0;
    for (int i = 0; i < this.bitCount(); i += 1) {
      if (this.bA[i].contains(columnId)) {
        value |= (1L << i);
      }
    }

    return value;
  }

  /**
   * RunOptimize attempts to further compress the runs of consecutive values found in the bitmap
   */
  public void runOptimize() {
    this.ebM.runOptimize();

    for (Roaring64Bitmap integers : this.bA) {
      integers.runOptimize();
    }
    this.runOptimized = true;
  }

  /**
   * hasRunCompression returns true if the bitmap benefits from run compression
   */
  public boolean hasRunCompression() {
    return this.runOptimized;
  }

  /**
   * GetExistenceBitmap returns a pointer to the underlying existence bitmap of the BSI
   */
  public Roaring64Bitmap getExistenceBitmap() {
    return this.ebM;
  }

  public int bitCount() {
    return this.bA.length;
  }

  public long getLongCardinality() {
    return this.ebM.getLongCardinality();
  }

  /**
   * GetValue gets the value at the column ID.  Second param will be false for non-existence values.
   */
  public Pair<Long, Boolean> getValue(long columnId) {
    boolean exists = this.ebM.contains(columnId);
    if (!exists) {
      return Pair.newPair(0L, false);
    }

    return Pair.newPair(valueAt(columnId), true);
  }

  private void clear() {
    this.maxValue = 0;
    this.minValue = 0;
    this.ebM = null;
    this.bA = null;
  }

  public void serialize(DataOutput output) throws IOException {
    // write meta
    WritableUtils.writeVLong(output, minValue);
    WritableUtils.writeVLong(output, maxValue);
    output.writeBoolean(this.runOptimized);

    // write ebm
    this.ebM.serialize(output);

    // write ba
    WritableUtils.writeVInt(output, this.bA.length);
    for (Roaring64Bitmap rb : this.bA) {
      rb.serialize(output);
    }
  }

  public void deserialize(DataInput in) throws IOException {
    this.clear();

    // read meta
    this.minValue = WritableUtils.readVLong(in);
    this.maxValue = WritableUtils.readVLong(in);
    this.runOptimized = in.readBoolean();

    // read ebm
    Roaring64Bitmap ebm = new Roaring64Bitmap();
    ebm.deserialize(in);
    this.ebM = ebm;

    // read ba
    int bitDepth = WritableUtils.readVInt(in);
    Roaring64Bitmap[] ba = new Roaring64Bitmap[bitDepth];
    for (int i = 0; i < bitDepth; i++) {
      Roaring64Bitmap rb = new Roaring64Bitmap();
      rb.deserialize(in);
      ba[i] = rb;
    }
    this.bA = ba;
  }

  public void serialize(ByteBuffer buffer) throws IOException {
    // write meta
    buffer.putLong(this.minValue);
    buffer.putLong(this.maxValue);
    buffer.put(this.runOptimized ? (byte) 1 : (byte) 0);
    // write ebm
    this.ebM.serialize(buffer);

    // write ba
    buffer.putInt(this.bA.length);
    for (Roaring64Bitmap rb : this.bA) {
      rb.serialize(buffer);
    }
  }

  public void deserialize(ByteBuffer buffer) throws IOException {
    this.clear();
    // read meta
    this.minValue = buffer.getLong();
    this.maxValue = buffer.getLong();
    this.runOptimized = buffer.get() == (byte) 1;

    // read ebm
    Roaring64Bitmap ebm = new Roaring64Bitmap();
    ebm.deserialize(buffer);
    this.ebM = ebm;
    // read back
    buffer.position(buffer.position() + ebm.getSizeInBytes());
    int bitDepth = buffer.getInt();
    Roaring64Bitmap[] ba = new Roaring64Bitmap[bitDepth];
    for (int i = 0; i < bitDepth; i++) {
      Roaring64Bitmap rb = new Roaring64Bitmap();
      rb.deserialize(buffer);
      ba[i] = rb;
      buffer.position(buffer.position() + rb.getSizeInBytes());
    }
    this.bA = ba;
  }

  public int serializedSizeInBytes() {
    int size = 0;
    for (Roaring64Bitmap rb : this.bA) {
      size += rb.getSizeInBytes();
    }
    return 8 + 8 + 1 + 4 + this.ebM.getSizeInBytes() + size;
  }

  /**
   * valueExists tests whether the value exists.
   */
  public boolean valueExist(Long columnId) {
    return this.ebM.contains(columnId);
  }

  /**
   * SetValue sets a value for a given columnID.
   */
  public void setValue(long columnId, long value) {
    ensureCapacityInternal(value, value);
    setValueInternal(columnId, value);
  }

  private void setValueInternal(long columnId, long value) {
    for (int i = 0; i < this.bitCount(); i += 1) {
      if ((value & (1L << i)) > 0) {
        this.bA[i].add(columnId);
      } else {
        this.bA[i].remove(columnId);
      }
    }
    this.ebM.add(columnId);
  }

  private void ensureCapacityInternal(long minValue, long maxValue) {
    if (ebM.isEmpty()) {
      this.minValue = minValue;
      this.maxValue = maxValue;
      grow(Long.toBinaryString(maxValue).length());
    } else if (this.minValue > minValue) {
      this.minValue = minValue;
    } else if (this.maxValue < maxValue) {
      this.maxValue = maxValue;
      grow(Long.toBinaryString(maxValue).length());
    }
  }

  private void grow(int newBitDepth) {
    int oldBitDepth = this.bA.length;

    if (oldBitDepth >= newBitDepth) {
      return;
    }

    Roaring64Bitmap[] newBA = new Roaring64Bitmap[newBitDepth];
    if (oldBitDepth != 0) {
      System.arraycopy(this.bA, 0, newBA, 0, oldBitDepth);
    }

    for (int i = newBitDepth - 1; i >= oldBitDepth; i--) {
      newBA[i] = new Roaring64Bitmap();
      if (this.runOptimized) {
        newBA[i].runOptimize();
      }
    }
    this.bA = newBA;
  }

  public void setValues(List<Pair<Long, Long>> values) {
    long maxValue =
        values.stream().mapToLong(Pair::getRight).filter(Objects::nonNull).max().getAsLong();
    long minValue =
        values.stream().mapToLong(Pair::getRight).filter(Objects::nonNull).min().getAsLong();
    ensureCapacityInternal(minValue, maxValue);
    for (Pair<Long, Long> pair : values) {
      setValueInternal(pair.getKey(), pair.getValue());
    }
  }

  /**
   * merge will merge 2 bsi into current
   * merge API was designed for distributed computing
   * note: current and other bsi has no intersection
   *
   * @param otherBsi other bsi we need merge
   */
  public void merge(Roaring64BitmapSliceIndex otherBsi) {

    if (null == otherBsi || otherBsi.ebM.isEmpty()) {
      return;
    }

    // todo whether we need this
    if (Roaring64Bitmap.intersects(this.ebM, otherBsi.ebM)) {
      throw new IllegalArgumentException("merge can be used only in bsiA  bsiB  is null");
    }

    int bitDepth = Integer.max(this.bitCount(), otherBsi.bitCount());
    Roaring64Bitmap[] newBA = new Roaring64Bitmap[bitDepth];
    for (int i = 0; i < bitDepth; i++) {
      Roaring64Bitmap current = i < this.bA.length ? this.bA[i] : new Roaring64Bitmap();
      Roaring64Bitmap other = i < otherBsi.bA.length ? otherBsi.bA[i] : new Roaring64Bitmap();
      newBA[i] = Roaring64Bitmap.or(current, other);
      if (this.runOptimized || otherBsi.runOptimized) {
        newBA[i].runOptimize();
      }
    }
    this.bA = newBA;
    this.ebM.or(otherBsi.ebM);
    this.runOptimized = this.runOptimized || otherBsi.runOptimized;
    this.maxValue = Long.max(this.maxValue, otherBsi.maxValue);
    this.minValue = Long.min(this.minValue, otherBsi.minValue);
  }

  @Override
  public Roaring64BitmapSliceIndex clone() {
    Roaring64BitmapSliceIndex bitSliceIndex = new Roaring64BitmapSliceIndex();
    bitSliceIndex.minValue = this.minValue;
    bitSliceIndex.maxValue = this.maxValue;
    bitSliceIndex.ebM = this.ebM.clone();
    Roaring64Bitmap[] cloneBA = new Roaring64Bitmap[this.bitCount()];
    for (int i = 0; i < cloneBA.length; i++) {
      cloneBA[i] = this.bA[i].clone();
    }
    bitSliceIndex.bA = cloneBA;
    bitSliceIndex.runOptimized = this.runOptimized;

    return bitSliceIndex;
  }

  /**
   * O'Neil range using a bit-sliced index
   *
   * @param operation compare operation
   * @param predicate the value we found filter
   * @param foundSet  columnId set we want compare,using RoaringBitmap to express
   * @return columnId set we found in this bsi with giving conditions, using RoaringBitmap to express
   * see https://github.com/lemire/BitSliceIndex/blob/master/src/main/java/org/roaringbitmap/circuits/comparator/BasicComparator.java
   */
  private Roaring64Bitmap oNeilCompare(
      BitmapSliceIndex.Operation operation, long predicate, Roaring64Bitmap foundSet) {
    Roaring64Bitmap fixedFoundSet = foundSet == null ? this.ebM : foundSet;

    Roaring64Bitmap GT = new Roaring64Bitmap();
    Roaring64Bitmap LT = new Roaring64Bitmap();
    Roaring64Bitmap EQ = this.ebM;

    for (int i = this.bitCount() - 1; i >= 0; i--) {
      int bit = (int) ((predicate >> i) & 1);
      if (bit == 1) {
        LT = Roaring64Bitmap.or(LT, Roaring64Bitmap.andNot(EQ, this.bA[i]));
        EQ = Roaring64Bitmap.and(EQ, this.bA[i]);
      } else {
        GT = Roaring64Bitmap.or(GT, Roaring64Bitmap.and(EQ, this.bA[i]));
        EQ = Roaring64Bitmap.andNot(EQ, this.bA[i]);
      }
    }
    EQ = Roaring64Bitmap.and(fixedFoundSet, EQ);
    switch (operation) {
      case EQ:
        return EQ;
      case NEQ:
        return Roaring64Bitmap.andNot(fixedFoundSet, EQ);
      case GT:
        return Roaring64Bitmap.and(GT, fixedFoundSet);
      case LT:
        return Roaring64Bitmap.and(LT, fixedFoundSet);
      case LE:
        return Roaring64Bitmap.and(Roaring64Bitmap.or(LT, EQ), fixedFoundSet);
      case GE:
        return Roaring64Bitmap.and(Roaring64Bitmap.or(GT, EQ), fixedFoundSet);
      default:
        throw new IllegalArgumentException("");
    }
  }

  /**
   * BSI Compare using single thread
   * this Function compose algorithm from O'Neil and Owen Kaser
   * the GE algorithm is from Owen since the performance is better.  others are from O'Neil
   *
   * @param operation
   * @param startOrValue the start or value of comparison, when the comparison operation is range, it's start,
   *                     when others,it's value.
   * @param end          the end value of comparison. when the comparison operation is not range,the end = 0
   * @param foundSet     columnId set we want compare,using RoaringBitmap to express
   * @return columnId set we found in this bsi with giving conditions, using RoaringBitmap to express
   */
  public Roaring64Bitmap compare(
      BitmapSliceIndex.Operation operation, long startOrValue, long end, Roaring64Bitmap foundSet) {
    Roaring64Bitmap result = compareUsingMinMax(operation, startOrValue, end, foundSet);
    if (result != null) {
      return result;
    }

    switch (operation) {
      case EQ:
        return oNeilCompare(BitmapSliceIndex.Operation.EQ, startOrValue, foundSet);
      case NEQ:
        return oNeilCompare(BitmapSliceIndex.Operation.NEQ, startOrValue, foundSet);
      case GE:
        return oNeilCompare(BitmapSliceIndex.Operation.GE, startOrValue, foundSet);
      case GT:
        {
          return oNeilCompare(BitmapSliceIndex.Operation.GT, startOrValue, foundSet);
        }
      case LT:
        return oNeilCompare(BitmapSliceIndex.Operation.LT, startOrValue, foundSet);

      case LE:
        return oNeilCompare(BitmapSliceIndex.Operation.LE, startOrValue, foundSet);

      case RANGE:
        {
          if (startOrValue < minValue) {
            startOrValue = minValue;
          }
          if (end > maxValue) {
            end = maxValue;
          }
          Roaring64Bitmap left =
              oNeilCompare(BitmapSliceIndex.Operation.GE, startOrValue, foundSet);
          Roaring64Bitmap right = oNeilCompare(BitmapSliceIndex.Operation.LE, end, foundSet);

          return Roaring64Bitmap.and(left, right);
        }
      default:
        throw new IllegalArgumentException("not support operation!");
    }
  }

  private Roaring64Bitmap compareUsingMinMax(
      BitmapSliceIndex.Operation operation, long startOrValue, long end, Roaring64Bitmap foundSet) {
    Roaring64Bitmap all = foundSet == null ? ebM.clone() : Roaring64Bitmap.and(ebM, foundSet);
    Roaring64Bitmap empty = new Roaring64Bitmap();

    switch (operation) {
      case LT:
        if (startOrValue > maxValue) {
          return all;
        } else if (startOrValue <= minValue) {
          return empty;
        }

        break;
      case LE:
        if (startOrValue >= maxValue) {
          return all;
        } else if (startOrValue < minValue) {
          return empty;
        }

        break;
      case GT:
        if (startOrValue < minValue) {
          return all;
        } else if (startOrValue >= maxValue) {
          return empty;
        }

        break;
      case GE:
        if (startOrValue <= minValue) {
          return all;
        } else if (startOrValue > maxValue) {
          return empty;
        }

        break;
      case EQ:
        if (minValue == maxValue && minValue == startOrValue) {
          return all;
        } else if (startOrValue < minValue || startOrValue > maxValue) {
          return empty;
        }

        break;
      case NEQ:
        if (minValue == maxValue) {
          return minValue == startOrValue ? empty : all;
        }

        break;
      case RANGE:
        if (startOrValue <= minValue && end >= maxValue) {
          return all;
        } else if (startOrValue > maxValue || end < minValue) {
          return empty;
        }

        break;
      default:
        return null;
    }

    return null;
  }

  public Pair<Long, Long> sum(Roaring64Bitmap foundSet) {
    if (null == foundSet || foundSet.isEmpty()) {
      return Pair.newPair(0L, 0L);
    }
    long count = foundSet.getLongCardinality();

    Long sum =
        IntStream.range(0, this.bitCount())
            .mapToLong(x -> (1L << x) * Roaring64Bitmap.andCardinality(this.bA[x], foundSet))
            .sum();

    return Pair.newPair(sum, count);
  }

  public Roaring64Bitmap topK(Roaring64Bitmap foundSet, long k) {
    if (null == foundSet || foundSet.isEmpty()) {
      return new Roaring64Bitmap();
    }
    if (k >= foundSet.getLongCardinality()) {
      return foundSet;
    }
    Roaring64Bitmap re = new Roaring64Bitmap();
    Roaring64Bitmap candidates = foundSet.clone();

    for (int x = this.bitCount() - 1; x >= 0 && !candidates.isEmpty() && k > 0; x--) {
      long cardinality = Roaring64Bitmap.and(candidates, this.bA[x]).getLongCardinality();

      if (cardinality > k) {
        candidates.and(this.bA[x]);
      } else {
        re.or(Roaring64Bitmap.and(candidates, this.bA[x]));
        candidates.andNot(this.bA[x]);
        k -= cardinality;
      }
    }
    return re;
  }

  public Roaring64Bitmap transpose(Roaring64Bitmap foundSet) {
    Roaring64Bitmap re = new Roaring64Bitmap();
    Roaring64Bitmap fixedFoundSet =
        foundSet == null ? this.ebM : Roaring64Bitmap.and(foundSet, this.ebM);
    fixedFoundSet.forEach((long x) -> re.add(this.getValue(x).getKey()));
    return re;
  }

  public Roaring64BitmapSliceIndex transposeWithCount(Roaring64Bitmap foundSet) {
    Roaring64BitmapSliceIndex re = new Roaring64BitmapSliceIndex();
    Roaring64Bitmap fixedFoundSet =
        foundSet == null ? this.ebM : Roaring64Bitmap.and(foundSet, this.ebM);
    fixedFoundSet.forEach(
        (long x) -> {
          long nk = this.getValue(x).getKey();
          if (re.valueExist(nk)) {
            re.setValue(nk, re.getValue(nk).getKey() + 1);
          } else {
            re.setValue(nk, 1);
          }
        });
    return re;
  }
}
