package org.roaringbitmap.bsi.buffer;

import java.util.Objects;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.bsi.BitmapSliceIndex;
import org.roaringbitmap.bsi.Pair;
import org.roaringbitmap.bsi.WritableUtils;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.OptionalInt;

/**
 * MutableBSI
 */
public class MutableBitSliceIndex extends BitSliceIndexBase implements BitmapSliceIndex {

  private boolean runOptimized;

  /**
   * construct a new ImmutableBitSliceIndex from raw slice
   *
   * @param maxValue max value for this bsi
   * @param minValue min value for this bsi
   * @param bA  bit slices for this bsi.using MutableRoaringBitmap array express
   * @param ebM exits value bitmap,use MutableRoaringBitmap express
   */
  public MutableBitSliceIndex(int maxValue, int minValue, 
      MutableRoaringBitmap[] bA, MutableRoaringBitmap ebM) {
    this.maxValue = maxValue;
    this.minValue = minValue;
    this.bA = bA;
    this.ebM = ebM;
  }


  /**
   * construct a new MutableBitSliceIndex.
   * Min/Max values are optional.  If set to 0 then the underlying BSI will be automatically sized.
   */
  public MutableBitSliceIndex(int minValue, int maxValue) {
    if (minValue < 0) {
      throw new IllegalArgumentException("Values should be non-negative");
    }

    this.bA = new MutableRoaringBitmap[32 - Integer.numberOfLeadingZeros(maxValue)];
    for (int i = 0; i < bA.length; i++) {
      this.bA[i] = new MutableRoaringBitmap();
    }
    this.ebM = new MutableRoaringBitmap();
  }

  /**
   * constructs an auto-sized BSI
   */
  public MutableBitSliceIndex() {
    this(0, 0);
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

  /**
   * auto expend the bA length.
   *
   * @param newBitDepth new bit depth
   */
  private void grow(int newBitDepth) {
    int oldBitDepth = this.bA.length;

    if (oldBitDepth >= newBitDepth) {
      return;
    }

    MutableRoaringBitmap[] newBA = new MutableRoaringBitmap[newBitDepth];

    if (oldBitDepth != 0) {
      System.arraycopy(this.bA, 0, newBA, 0, oldBitDepth);
    }

    for (int i = newBitDepth - 1; i >= oldBitDepth; i--) {
      newBA[i] = new MutableRoaringBitmap();
      if (this.runOptimized) {
        newBA[i].runOptimize();
      }
    }
    this.bA = newBA;
  }


  /**
   * RunOptimize attempts to further compress the runs of consecutive values found in the bitmap
   */
  public void runOptimize() {
    this.ebM.toMutableRoaringBitmap().runOptimize();

    for (int i = 0; i < this.bA.length; i++) {
      this.getMutableSlice(i).runOptimize();
    }
    this.runOptimized = true;
  }

  public boolean hasRunCompression() {
    return this.runOptimized;
  }

  public void addDigit(MutableRoaringBitmap foundSet, int i) {
    MutableRoaringBitmap carry = MutableRoaringBitmap.and(this.bA[i], foundSet);
    this.getMutableSlice(i).xor(foundSet);
    if (carry.getCardinality() > 0) {
      if (i + 1 >= this.bitCount()) {
        grow(this.bitCount() + 1);
      }
      this.addDigit(carry, i + 1);
    }
  }

  public MutableRoaringBitmap getExistenceBitmap() {
    return (MutableRoaringBitmap) this.ebM;
  }

  public MutableRoaringBitmap getMutableSlice(int i) {
    return (MutableRoaringBitmap) this.bA[i];
  }

  /**
   * SetValue sets a value for a given columnID.
   *
   * @param columnId columnID
   * @param value the value for columnID
   */
  public void setValue(int columnId, int value) {
    ensureCapacityInternal(value, value);
    setValueInternal(columnId, value);
  }

  private void setValueInternal(int columnId, int value) {
    for (int i = 0; i < this.bitCount(); i += 1) {
      if ((value & (1 << i)) > 0) {
        this.getMutableSlice(i).add(columnId);
      } else {
        this.getMutableSlice(i).remove(columnId);
      }
    }

    this.getExistenceBitmap().add(columnId);
  }


  public void setValues(List<Pair<Integer, Integer>> values, 
      Integer currentMaxValue, Integer currentMinValue) {
    OptionalInt maxValue = currentMaxValue != null 
        ? OptionalInt.of(currentMaxValue) : values.stream().mapToInt(Pair::getRight).max();
    OptionalInt minValue = currentMinValue != null 
        ? OptionalInt.of(currentMinValue) : values.stream().mapToInt(Pair::getRight).min();

    if (!maxValue.isPresent() || !minValue.isPresent()) {
      throw new IllegalArgumentException("wrong input values list");
    }

    ensureCapacityInternal(minValue.getAsInt(), maxValue.getAsInt());
    for (Pair<Integer, Integer> pair : values) {
      this.setValue(pair.getKey(), pair.getValue());
    }

  }

  /**
   * Set a batch of values.
   *
   * @param values
   */
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
   * add tow bsi index
   *
   */
  public void add(MutableBitSliceIndex otherBsi) {

    if (null == otherBsi || otherBsi.ebM.isEmpty()) {
      return;
    }

    this.getExistenceBitmap().or(otherBsi.getExistenceBitmap());
    if (otherBsi.bitCount() > this.bitCount()) {
      grow(otherBsi.bitCount());
    }

    for (int i = 0; i < otherBsi.bitCount(); i++) {
      this.addDigit(otherBsi.getMutableSlice(i), i);
    }

    // update min and max after adding
    this.minValue = minValue();
    this.maxValue = maxValue();
  }

  private int minValue() {
    if (ebM.isEmpty()) {
      return 0;
    }

    MutableRoaringBitmap minValuesId = getExistenceBitmap();
    for (int i = bA.length - 1; i >= 0; i -= 1) {
      MutableRoaringBitmap tmp = MutableRoaringBitmap.andNot(minValuesId, bA[i]);
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

    MutableRoaringBitmap maxValuesId = getExistenceBitmap();
    for (int i = bA.length - 1; i >= 0; i -= 1) {
      MutableRoaringBitmap tmp = MutableRoaringBitmap.and(maxValuesId, bA[i]);
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
   * merge will merge 2 bsi into current
   * merge API was designed for distributed computing
   * NOTE: current and other bsi have no intersection
   *
   */
  public void merge(MutableBitSliceIndex otherBsi) {

    if (null == otherBsi || otherBsi.ebM.isEmpty()) {
      return;
    }

    // todo whether we need this
    if (MutableRoaringBitmap.intersects(this.ebM, otherBsi.ebM)) {
      throw new IllegalArgumentException("merge can be used only in bsiA  bsiB  is null");
    }

    int bitDepth = Integer.max(this.bitCount(), otherBsi.bitCount());
    MutableRoaringBitmap[] newBA = new MutableRoaringBitmap[bitDepth];
    for (int i = 0; i < bitDepth; i++) {
      MutableRoaringBitmap current = i < this.bA.length 
          ? this.getMutableSlice(i) : new MutableRoaringBitmap();
      MutableRoaringBitmap other = i < otherBsi.bA.length 
          ? otherBsi.getMutableSlice(i) : new MutableRoaringBitmap();
      newBA[i] = MutableRoaringBitmap.or(current, other);
      if (this.runOptimized || otherBsi.runOptimized) {
        newBA[i].runOptimize();
      }
    }
    this.bA = newBA;
    this.getExistenceBitmap().or(otherBsi.getExistenceBitmap());
    this.runOptimized = this.runOptimized || otherBsi.runOptimized;
    this.maxValue = Integer.max(this.maxValue, otherBsi.maxValue);
    this.minValue = Integer.min(this.minValue, otherBsi.minValue);
  }

  public MutableBitSliceIndex clone() {
    MutableBitSliceIndex bitSliceIndex = new MutableBitSliceIndex();
    bitSliceIndex.minValue = this.minValue;
    bitSliceIndex.maxValue = this.maxValue;
    bitSliceIndex.ebM = this.ebM.clone();
    MutableRoaringBitmap[] cloneBA = new MutableRoaringBitmap[this.bitCount()];
    for (int i = 0; i < cloneBA.length; i++) {
      cloneBA[i] = this.getMutableSlice(i).clone();
    }
    bitSliceIndex.bA = cloneBA;
    bitSliceIndex.runOptimized = this.runOptimized;

    return bitSliceIndex;

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
    for (ImmutableRoaringBitmap rb : this.bA) {
      rb.serialize(buffer);
    }
  }

  public void serialize(DataOutput output) throws IOException {
    // write meta
    WritableUtils.writeVInt(output, minValue);
    WritableUtils.writeVInt(output, maxValue);
    output.writeBoolean(this.runOptimized);

    // write ebm
    this.ebM.serialize(output);

    // write ba
    WritableUtils.writeVInt(output, this.bA.length);
    for (ImmutableRoaringBitmap rb : this.bA) {
      rb.serialize(output);
    }
  }

  private void clear() {
    this.maxValue = 0;
    this.minValue = 0;
    this.ebM = null;
    this.bA = null;
  }


  public void deserialize(ByteBuffer buffer) throws IOException {
    this.clear();
    // read meta
    this.minValue = buffer.getInt();
    this.maxValue = buffer.getInt();
    this.runOptimized = buffer.get() == (byte) 1;

    // read ebm
    MutableRoaringBitmap ebm = new MutableRoaringBitmap();
    ebm.deserialize(buffer);
    this.ebM = ebm;
    // read ba
    buffer.position(buffer.position() + ebm.serializedSizeInBytes());
    int bitDepth = buffer.getInt();
    MutableRoaringBitmap[] ba = new MutableRoaringBitmap[bitDepth];
    for (int i = 0; i < bitDepth; i++) {
      MutableRoaringBitmap rb = new MutableRoaringBitmap();
      rb.deserialize(buffer);
      ba[i] = rb;
      buffer.position(buffer.position() + rb.serializedSizeInBytes());
    }
    this.bA = ba;
  }

  public void deserialize(DataInput in) throws IOException {
    this.clear();

    // read meta
    this.minValue = WritableUtils.readVInt(in);
    this.maxValue = WritableUtils.readVInt(in);
    this.runOptimized = in.readBoolean();

    // read ebm
    MutableRoaringBitmap ebm = new MutableRoaringBitmap();
    ebm.deserialize(in);
    this.ebM = ebm;

    // read ba
    int bitDepth = WritableUtils.readVInt(in);
    MutableRoaringBitmap[] ba = new MutableRoaringBitmap[bitDepth];
    for (int i = 0; i < bitDepth; i++) {
      MutableRoaringBitmap rb = new MutableRoaringBitmap();
      rb.deserialize(in);
      ba[i] = rb;
    }
    this.bA = ba;
  }

  public int serializedSizeInBytes() {
    int size = 0;
    for (ImmutableRoaringBitmap rb : this.bA) {
      size += rb.serializedSizeInBytes();
    }
    return 4 + 4 + 1 + 4 + this.ebM.serializedSizeInBytes() + size;
   }

  public ImmutableBitSliceIndex toImmutableBitSliceIndex() {
    ImmutableRoaringBitmap[] ibA = new ImmutableRoaringBitmap[this.bA.length];
    for (int i = 0; i < this.bA.length; i++) {
      ibA[i] = this.bA[i];
    }

    ImmutableBitSliceIndex bsi = new ImmutableBitSliceIndex(
        this.maxValue, this.minValue, ibA, this.ebM);
    return bsi;
  }
}

