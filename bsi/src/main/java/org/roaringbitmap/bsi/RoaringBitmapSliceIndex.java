package org.roaringbitmap.bsi;

import java.util.Objects;
import org.roaringbitmap.RoaringBitmap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.IntStream;

/**
 * RoaringBitmapSliceIndex
 */
public class RoaringBitmapSliceIndex implements BitmapSliceIndex {
  /**
   * the maxValue of this bsi
   */
  private int maxValue;

  /**
   * the minValue of this bsi
   */
  private int minValue;

  /**
   * the bit component slice Array of this bsi
   */
  private RoaringBitmap[] bA;

  /**
   * the exist bitmap of this bsi which means the columnId have value in this bsi
   */
  private RoaringBitmap ebM;


  private Boolean runOptimized = false;

  /**
   * NewBSI constructs a new BSI.  Min/Max values are optional.  If set to 0
   * then the underlying BSI will be automatically sized.
   */
  public RoaringBitmapSliceIndex(int minValue, int maxValue) {
    if (minValue < 0) {
      throw new IllegalArgumentException("Values should be non-negative");
    }

    this.bA = new RoaringBitmap[32 - Integer.numberOfLeadingZeros(maxValue)];
    for (int i = 0; i < bA.length; i++) {
      this.bA[i] = new RoaringBitmap();
    }

    this.ebM = new RoaringBitmap();
  }


  /**
   * NewDefaultBSI constructs an auto-sized BSI
   */
  public RoaringBitmapSliceIndex() {
    this(0, 0);
  }


  public void add(RoaringBitmapSliceIndex otherBsi) {
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

  private void addDigit(RoaringBitmap foundSet, int i) {
    RoaringBitmap carry = RoaringBitmap.and(this.bA[i], foundSet);
    this.bA[i].xor(foundSet);
    if (!carry.isEmpty()) {
      if (i + 1 >= this.bitCount()) {
        grow(this.bitCount() + 1);
      }
      this.addDigit(carry, i + 1);
    }

  }

  private int minValue() {
    if (ebM.isEmpty()) {
      return 0;
    }

    RoaringBitmap minValuesId = ebM;
    for (int i = bA.length - 1; i >= 0; i -= 1) {
      RoaringBitmap tmp = RoaringBitmap.andNot(minValuesId, bA[i]);
      if (!tmp.isEmpty()) {
        minValuesId = tmp;
      }
    }

    return valueAt(minValuesId.first());
  }

  private int maxValue() {
    if (ebM.isEmpty()) {
      return 0;
    }

    RoaringBitmap maxValuesId = ebM;
    for (int i = bA.length - 1; i >= 0; i -= 1) {
      RoaringBitmap tmp = RoaringBitmap.and(maxValuesId, bA[i]);
      if (!tmp.isEmpty()) {
        maxValuesId = tmp;
      }
    }

    return valueAt(maxValuesId.first());
  }

  private int valueAt(int columnId) {
    int value = 0;
    for (int i = 0; i < this.bitCount(); i += 1) {
      if (this.bA[i].contains(columnId)) {
        value |= (1 << i);
      }
    }

    return value;
  }

  /**
   * RunOptimize attempts to further compress the runs of consecutive values found in the bitmap
   */
  public void runOptimize() {
    this.ebM.runOptimize();

    for (RoaringBitmap integers : this.bA) {
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

  public RoaringBitmap getExistenceBitmap() {
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
  public Pair<Integer, Boolean> getValue(int columnId) {
    boolean exists = this.ebM.contains(columnId);
    if (!exists) {
      return Pair.newPair(0, false);
    }

    return Pair.newPair(valueAt(columnId), true);
  }


  private void clear() {
    this.maxValue = 0;
    this.minValue = 0;
    this.ebM = null;
    this.bA = null;
  }

  @Override
  public void serialize(DataOutput output) throws IOException {
    // write meta
    WritableUtils.writeVInt(output, minValue);
    WritableUtils.writeVInt(output, maxValue);
    output.writeBoolean(this.runOptimized);

    // write ebm
    this.ebM.serialize(output);

    // write ba
    WritableUtils.writeVInt(output, this.bA.length);
    for (RoaringBitmap rb : this.bA) {
      rb.serialize(output);
    }
  }

  public void deserialize(DataInput in) throws IOException {
    this.clear();

    // read meta
    this.minValue = WritableUtils.readVInt(in);
    this.maxValue = WritableUtils.readVInt(in);
    this.runOptimized = in.readBoolean();

    // read ebm
    RoaringBitmap ebm = new RoaringBitmap();
    ebm.deserialize(in);
    this.ebM = ebm;

    // read ba
    int bitDepth = WritableUtils.readVInt(in);
    RoaringBitmap[] ba = new RoaringBitmap[bitDepth];
    for (int i = 0; i < bitDepth; i++) {
      RoaringBitmap rb = new RoaringBitmap();
      rb.deserialize(in);
      ba[i] = rb;
    }
    this.bA = ba;
  }

  public void serialize(ByteBuffer buffer) {
    // write meta
    buffer.putInt(this.minValue);
    buffer.putInt(this.maxValue);
    buffer.put(this.runOptimized ? (byte) 1 : (byte) 0);
    // write ebm
    this.ebM.serialize(buffer);

    // write ba
    buffer.putInt(this.bA.length);
    for (RoaringBitmap rb : this.bA) {
      rb.serialize(buffer);
    }

  }

  public void deserialize(ByteBuffer buffer) throws IOException {
    this.clear();
    // read meta
    this.minValue = buffer.getInt();
    this.maxValue = buffer.getInt();
    this.runOptimized = buffer.get() == (byte) 1;

    // read ebm
    RoaringBitmap ebm = new RoaringBitmap();
    ebm.deserialize(buffer);
    this.ebM = ebm;
    // read ba
    buffer.position(buffer.position() + ebm.serializedSizeInBytes());
    int bitDepth = buffer.getInt();
    RoaringBitmap[] ba = new RoaringBitmap[bitDepth];
    for (int i = 0; i < bitDepth; i++) {
      RoaringBitmap rb = new RoaringBitmap();
      rb.deserialize(buffer);
      ba[i] = rb;
      buffer.position(buffer.position() + rb.serializedSizeInBytes());
    }
    this.bA = ba;
  }

  @Override
  public int serializedSizeInBytes() {
    int size = 0;
    for (RoaringBitmap rb : this.bA) {
      size += rb.serializedSizeInBytes();
    }
    return 4 + 4 + 1 + 4 + this.ebM.serializedSizeInBytes() + size;
  }


  /**
   * valueExists tests whether the value exists.
   */
  public boolean valueExist(Long columnId) {
    return this.ebM.contains(columnId.intValue());
  }

  /**
   * SetValue sets a value for a given columnID.
   */
  public void setValue(int columnId, int value) {
    ensureCapacityInternal(value, value);
    setValueInternal(columnId, value);
  }

  private void setValueInternal(int columnId, int value) {
    for (int i = 0; i < this.bitCount(); i += 1) {
      if ((value & (1 << i)) > 0) {
        this.bA[i].add(columnId);
      } else {
        this.bA[i].remove(columnId);
      }
    }
    this.ebM.add(columnId);
  }

  private void ensureCapacityInternal(int minValue, int maxValue) {
    if (ebM.isEmpty()) {
      this.minValue = minValue;
      this.maxValue = maxValue;
      grow(Integer.toBinaryString(maxValue).length());
    } else if (this.minValue > minValue) {
      this.minValue = minValue;
    } else if (this.maxValue < maxValue) {
      this.maxValue = maxValue;
      grow(Integer.toBinaryString(maxValue).length());
    }
  }

  private void grow(int newBitDepth) {
    int oldBitDepth = this.bA.length;

    if (oldBitDepth >= newBitDepth) {
      return;
    }

    RoaringBitmap[] newBA = new RoaringBitmap[newBitDepth];
    if (oldBitDepth != 0) {
      System.arraycopy(this.bA, 0, newBA, 0, oldBitDepth);
    }

    for (int i = newBitDepth - 1; i >= oldBitDepth; i--) {
      newBA[i] = new RoaringBitmap();
      if (this.runOptimized) {
        newBA[i].runOptimize();
      }
    }
    this.bA = newBA;
  }

  @Override
  public void setValues(List<Pair<Integer, Integer>> values) {
    int maxValue = values.stream().mapToInt(Pair::getRight).filter(Objects::nonNull).max().getAsInt();
    int minValue = values.stream().mapToInt(Pair::getRight).filter(Objects::nonNull).min().getAsInt();
    ensureCapacityInternal(minValue, maxValue);
    for (Pair<Integer, Integer> pair : values) {
      setValueInternal(pair.getKey(), pair.getValue());
    }
  }

  /**
   * Replaced by {@code setValues(values)}
   */
  @Deprecated
  public void setValues(List<Pair<Integer, Integer>> values, Integer currentMaxValue, Integer currentMinValue) {
    int maxValue = currentMaxValue != null ? currentMaxValue : values.stream().mapToInt(Pair::getRight).max().getAsInt();
    int minValue = currentMinValue != null ? currentMinValue : values.stream().mapToInt(Pair::getRight).min().getAsInt();
    ensureCapacityInternal(minValue, maxValue);
    for (Pair<Integer, Integer> pair : values) {
      this.setValue(pair.getKey(), pair.getValue());
    }
  }

  /**
   * merge will merge 2 bsi into current
   * merge API was designed for distributed computing
   * note: current and other bsi has no intersection
   *
   * @param otherBsi other bsi we need merge
   */
  public void merge(RoaringBitmapSliceIndex otherBsi) {

    if (null == otherBsi || otherBsi.ebM.isEmpty()) {
      return;
    }

    // todo whether we need this
    if (RoaringBitmap.intersects(this.ebM, otherBsi.ebM)) {
      throw new IllegalArgumentException("merge can be used only in bsiA  bsiB  is null");
    }

    int bitDepth = Integer.max(this.bitCount(), otherBsi.bitCount());
    RoaringBitmap[] newBA = new RoaringBitmap[bitDepth];
    for (int i = 0; i < bitDepth; i++) {
      RoaringBitmap current = i < this.bA.length ? this.bA[i] : new RoaringBitmap();
      RoaringBitmap other = i < otherBsi.bA.length ? otherBsi.bA[i] : new RoaringBitmap();
      newBA[i] = RoaringBitmap.or(current, other);
      if (this.runOptimized || otherBsi.runOptimized) {
        newBA[i].runOptimize();
      }
    }
    this.bA = newBA;
    this.ebM.or(otherBsi.ebM);
    this.runOptimized = this.runOptimized || otherBsi.runOptimized;
    this.maxValue = Integer.max(this.maxValue, otherBsi.maxValue);
    this.minValue = Integer.min(this.minValue, otherBsi.minValue);
  }


  public RoaringBitmapSliceIndex clone() {
    RoaringBitmapSliceIndex bitSliceIndex = new RoaringBitmapSliceIndex();
    bitSliceIndex.minValue = this.minValue;
    bitSliceIndex.maxValue = this.maxValue;
    bitSliceIndex.ebM = this.ebM.clone();
    RoaringBitmap[] cloneBA = new RoaringBitmap[this.bitCount()];
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
  private RoaringBitmap oNeilCompare(BitmapSliceIndex.Operation operation, int predicate, RoaringBitmap foundSet) {
    RoaringBitmap fixedFoundSet = foundSet == null ? this.ebM : foundSet;

    RoaringBitmap GT = new RoaringBitmap();
    RoaringBitmap LT = new RoaringBitmap();
    RoaringBitmap EQ = this.ebM;


    for (int i = this.bitCount() - 1; i >= 0; i--) {
      int bit = (predicate >> i) & 1;
      if (bit == 1) {
        LT = RoaringBitmap.or(LT, RoaringBitmap.andNot(EQ, this.bA[i]));
        EQ = RoaringBitmap.and(EQ, this.bA[i]);
      } else {
        GT = RoaringBitmap.or(GT, RoaringBitmap.and(EQ, this.bA[i]));
        EQ = RoaringBitmap.andNot(EQ, this.bA[i]);
      }

    }
    EQ = RoaringBitmap.and(fixedFoundSet, EQ);
    switch (operation) {
      case EQ:
        return EQ;
      case NEQ:
        return RoaringBitmap.andNot(fixedFoundSet, EQ);
      case GT:
        return RoaringBitmap.and(GT, fixedFoundSet);
      case LT:
        return RoaringBitmap.and(LT, fixedFoundSet);
      case LE:
        return RoaringBitmap.or(LT, EQ);
      case GE:
        return RoaringBitmap.or(GT, EQ);
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
   * @param startOrValue  the start or value of comparison, when the comparison operation is range, it's start,
   *            when others,it's value.
   * @param end       the end value of comparison. when the comparison operation is not range,the end = 0
   * @param foundSet    columnId set we want compare,using RoaringBitmap to express
   * @return columnId set we found in this bsi with giving conditions, using RoaringBitmap to express
   */
  public RoaringBitmap compare(BitmapSliceIndex.Operation operation, int startOrValue, int end, RoaringBitmap foundSet) {
    RoaringBitmap result = compareUsingMinMax(operation, startOrValue, end, foundSet);
    if (result != null) {
      return result;
    }

    switch (operation) {
      case EQ:
        return oNeilCompare(Operation.EQ, startOrValue, foundSet);
      case NEQ:
        return oNeilCompare(Operation.NEQ, startOrValue, foundSet);
      case GE:
        return oNeilCompare(Operation.GE, startOrValue, foundSet);
      case GT: {
        return oNeilCompare(BitmapSliceIndex.Operation.GT, startOrValue, foundSet);
      }
      case LT:
        return oNeilCompare(BitmapSliceIndex.Operation.LT, startOrValue, foundSet);

      case LE:
        return oNeilCompare(BitmapSliceIndex.Operation.LE, startOrValue, foundSet);

      case RANGE: {
        RoaringBitmap left = oNeilCompare(Operation.GE, startOrValue, foundSet);
        RoaringBitmap right = oNeilCompare(BitmapSliceIndex.Operation.LE, end, foundSet);

        return RoaringBitmap.and(left, right);
      }
      default:
        throw new IllegalArgumentException("not support operation!");
    }
  }

  private RoaringBitmap compareUsingMinMax(BitmapSliceIndex.Operation operation, int startOrValue, int end, RoaringBitmap foundSet) {
    RoaringBitmap all = foundSet == null ? ebM.clone() : RoaringBitmap.and(ebM, foundSet);
    RoaringBitmap empty = new RoaringBitmap();

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

  public Pair<Long, Long> sum(RoaringBitmap foundSet) {
    if (null == foundSet || foundSet.isEmpty()) {
      return Pair.newPair(0L, 0L);
    }
    long count = foundSet.getLongCardinality();

    Long sum = IntStream.range(0, this.bitCount())
        .mapToLong(x -> (long) (1 << x) * RoaringBitmap.andCardinality(this.bA[x], foundSet))
        .sum();

    return Pair.newPair(sum, count);
  }

}

