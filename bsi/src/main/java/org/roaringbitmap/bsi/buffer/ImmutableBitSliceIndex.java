package org.roaringbitmap.bsi.buffer;

import org.roaringbitmap.bsi.BitmapSliceIndex;
import org.roaringbitmap.bsi.Pair;
import org.roaringbitmap.bsi.WritableUtils;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * ImmutableBSI
 */
public class ImmutableBitSliceIndex extends BitSliceIndexBase implements BitmapSliceIndex {

  /**
   * constructs a BSI use slice and ebM
   * notes: the max size of BSI might be:
   * 33 * 120MB( 1 Billion cardinality) = 3960MB which might lead to OOM.
   * so don't use this function if you have no idea of the mechanism
   * you'd better split your data and make every shard suitable for memory.
   *
   * @param bA
   * @param ebM
   * @param maxValue
   * @param minValue
   */

  public ImmutableBitSliceIndex(int maxValue, int minValue, ImmutableRoaringBitmap[] bA, ImmutableRoaringBitmap ebM) {
    this.maxValue = maxValue;
    this.minValue = minValue;
    this.bA = bA;
    this.ebM = ebM;
  }

  public ImmutableBitSliceIndex() {
  }

  /**
   * constructs a BSI from byteBuffer
   * notes: the max size of BSI might be:
   * 33 * 120MB( 1 Billion cardinality) = 3960MB which might lead to OOM.
   * so don't use this function if you have no idea of the mechanism
   * you'd better split your data and make every shard suitable for memory.
   *
   * @param buffer
   * @throws IOException
   */
  public ImmutableBitSliceIndex(ByteBuffer buffer) throws IOException {
    this.clear();
    // read meta
    this.minValue = buffer.getInt();
    this.maxValue = buffer.getInt();

    // read ebm
    ImmutableRoaringBitmap ebm = new ImmutableRoaringBitmap(buffer);
    this.ebM = ebm;
    // read ba
    buffer.position(buffer.position() + ebm.serializedSizeInBytes());
    int bitDepth = buffer.getInt();
    ImmutableRoaringBitmap[] ba = new ImmutableRoaringBitmap[bitDepth];
    for (int i = 0; i < bitDepth; i++) {
      ImmutableRoaringBitmap rb = new ImmutableRoaringBitmap(buffer);
      ba[i] = rb;
      buffer.position(buffer.position() + rb.serializedSizeInBytes());
    }
    this.bA = ba;
  }


  public void addDigit(ImmutableRoaringBitmap foundSet, int i) {
    throw new UnsupportedOperationException("ImmutableBSI don't support setValue");
  }


  public ImmutableRoaringBitmap getExistenceBitmap() {
    return this.ebM;
  }

  public void setValue(int cid, int value) {
    throw new UnsupportedOperationException("ImmutableBSI don't support setValue");
  }


  @Override
  public void setValues(List<Pair<Integer, Integer>> values, 
      Integer currentMaxValue, Integer currentMinValue) {
    throw new UnsupportedOperationException("ImmutableBSI don't support setValues");
  }

  @Override
  public void setValues(List<Pair<Integer, Integer>> values) {
    throw new UnsupportedOperationException("ImmutableBSI does not support setting values");
  }


  public void add(BitmapSliceIndex otherBitmapSliceIndex) {
    throw new UnsupportedOperationException("ImmutableBSI don't support add");
  }

  public void merge(BitmapSliceIndex otherBitmapSliceIndex) {
    throw new UnsupportedOperationException("ImmutableBSI don't support merge");
  }

  private void clear() {
    this.maxValue = 0;
    this.minValue = 0;
    this.ebM = null;
    this.bA = null;
  }

  public void serialize(ByteBuffer buffer) {
    // write meta
    buffer.putInt(this.minValue);
    buffer.putInt(this.maxValue);
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

    // write ebm
    this.ebM.serialize(output);

    // write ba
    WritableUtils.writeVInt(output, this.bA.length);
    for (ImmutableRoaringBitmap rb : this.bA) {
      rb.serialize(output);
    }
  }


  public int serializedSizeInBytes() {
    int size = 0;
    for (ImmutableRoaringBitmap rb : this.bA) {
      size += rb.serializedSizeInBytes();
    }
    return 4 + 4 + 1 + 4 + this.ebM.serializedSizeInBytes() + size;
  }


  public MutableBitSliceIndex toMutableBitSliceIndex() {
    MutableRoaringBitmap[] ibA = new MutableRoaringBitmap[this.bA.length];
    for (int i = 0; i < this.bA.length; i++) {
      ibA[i] = this.bA[i].toMutableRoaringBitmap();
    }

    MutableBitSliceIndex bsi = new MutableBitSliceIndex(
        this.maxValue, this.minValue, ibA, this.ebM.toMutableRoaringBitmap());
    return bsi;
  }

  public ImmutableBitSliceIndex clone() {
    ImmutableBitSliceIndex bsi = new ImmutableBitSliceIndex();
    bsi.minValue = this.minValue;
    bsi.maxValue = this.maxValue;
    bsi.ebM = this.ebM.clone();
    ImmutableRoaringBitmap[] cloneBA = new ImmutableRoaringBitmap[this.bitCount()];
    for (int i = 0; i < cloneBA.length; i++) {
      cloneBA[i] = this.bA[i].clone();
    }
    bsi.bA = cloneBA;

    return bsi;
  }


}

