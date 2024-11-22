package org.roaringbitmap.bsi.longlong;

import org.roaringbitmap.bsi.BitmapSliceIndex;
import org.roaringbitmap.bsi.Pair;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

/**
 * Roaring64NavigableMap based bsi implementation.</br>
 *
 * See {@link #serialize} for more information for serialization format}
 */
public class Roaring64NavigableMapSliceIndex {
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
  private Roaring64NavigableMap[] bA;

  /**
   * the exist bitmap of this bsi which means the columnId have value in this bsi
   */
  private Roaring64NavigableMap ebM;

  private Boolean runOptimized = false;

  /**
   *  bsi serialization mode compatible with roaring(go)
   */
  public static int BSI64_SERIALIZATION_MODE_COMPATIBLE_WITH_GO = 0;

  /**
   * The serialization format for Roaring64NavigableMapSliceIndex.</br>
   * Currently, {@link #BSI64_SERIALIZATION_MODE_COMPATIBLE_WITH_GO} is supported.</br>
   * Other serialization formats maybe added in the future, and the serialization formats may change.
   * See {@link #serialize} for more information for serialization format}
   */
  public static int DEFAULT_BSI64_NAVIGABLE_SERIALIZATION_MODE =
      BSI64_SERIALIZATION_MODE_COMPATIBLE_WITH_GO;

  /**
   * NewDefaultBSI constructs an auto-sized BSI.
   */
  public Roaring64NavigableMapSliceIndex() {
    this(0L, 0L);
  }

  /**
   * NewBSI constructs a new BSI.  Min/Max values are optional.  If set to 0
   * then the underlying BSI will be automatically sized.
   */
  public Roaring64NavigableMapSliceIndex(long minValue, long maxValue) {
    if (minValue < 0) {
      throw new IllegalArgumentException("Values should be non-negative");
    }
    if (maxValue < minValue) {
      throw new IllegalArgumentException("maxValue should GE minValue");
    }

    this.bA = new Roaring64NavigableMap[64 - Long.numberOfLeadingZeros(maxValue)];
    for (int i = 0; i < bA.length; i++) {
      this.bA[i] = new Roaring64NavigableMap();
    }

    this.ebM = new Roaring64NavigableMap();
  }

  /**
   * add all content from another BSI instance, current bsi is modified
   */
  public void add(Roaring64NavigableMapSliceIndex otherBsi) {
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

  /**
   * add the specify slice to current 'bA[]' at index 'i', and 'carry' will be processed automatically
   */
  private void addDigit(Roaring64NavigableMap foundSet, int i) {
    Roaring64NavigableMap carry = Roaring64NavigableMap.and(this.bA[i], foundSet);
    this.bA[i].xor(foundSet);
    if (!carry.isEmpty()) {
      if (i + 1 >= this.bitCount()) {
        grow(this.bitCount() + 1);
      }
      this.addDigit(carry, i + 1);
    }
  }

  /**
   * get the min value from the bsi
   */
  public long minValue() {
    if (ebM.isEmpty()) {
      return 0;
    }

    Roaring64NavigableMap minValuesId = ebM;
    for (int i = bA.length - 1; i >= 0; i -= 1) {
      Roaring64NavigableMap tmp = Roaring64NavigableMap.andNot(minValuesId, bA[i]);
      if (!tmp.isEmpty()) {
        minValuesId = tmp;
      }
    }

    return valueAt(minValuesId.first());
  }

  /**
   * get the max value from the bsi
   */
  public long maxValue() {
    if (ebM.isEmpty()) {
      return 0;
    }

    Roaring64NavigableMap maxValuesId = ebM;
    for (int i = bA.length - 1; i >= 0; i -= 1) {
      Roaring64NavigableMap tmp = Roaring64NavigableMap.and(maxValuesId, bA[i]);
      if (!tmp.isEmpty()) {
        maxValuesId = tmp;
      }
    }

    return valueAt(maxValuesId.first());
  }

  /**
   * return the value associated with the input columnId
   * @param columnId the column id
   * @return the value associated with the input columnId
   */
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

    for (Roaring64NavigableMap integers : this.bA) {
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
  public Roaring64NavigableMap getExistenceBitmap() {
    return this.ebM;
  }

  /**
   * the number of slice
   */
  public int bitCount() {
    return this.bA.length;
  }

  /**
   * get the cardinality of the bsi (the number of distinct values added to the ebM)
   */
  public long getLongCardinality() {
    return this.ebM.getLongCardinality();
  }

  /**
   * get the value at the column ID.
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

  /**
   * Serialize the BSI.
   * <p>
   * Currently, only {@link #BSI64_SERIALIZATION_MODE_COMPATIBLE_WITH_GO} is supported.</br>
   * *NOTE* Other serialization formats maybe added in the future, and the serialization formats may change.
   * <p>
   * The serialization format for underlying Roaring64NavigableMap depends on
   * {@link Roaring64NavigableMap#SERIALIZATION_MODE}.</br>
   * NOTE: if {@link Roaring64NavigableMap#SERIALIZATION_MODE} = {@link Roaring64NavigableMap#SERIALIZATION_MODE_PORTABLE}
   * and {@link #DEFAULT_BSI64_NAVIGABLE_SERIALIZATION_MODE} = {@link #BSI64_SERIALIZATION_MODE_COMPATIBLE_WITH_GO},
   * then the bsi serialization is compatible with bsi implementation of roaring(Go).
   * <p>
   * Current serialization format:<br/>
   * ---
   * <pre>
   * <b>ebM</b> as standard Roaring64NavigableMap format (with specify legacy/portable format)
   * <b>loop over bA[]</b>  as standard Roaring64NavigableMap format (with specify legacy/portable format)
   * </pre>
   * ---
   * <br/>
   * Consider calling {@link #runOptimize} before serialization to improve compression.
   * The current bsi is not modified.
   * </p>
   * @see <a href="https://github.com/RoaringBitmap/roaring/blob/b32ae1a8cc386adbf7753251b8a580ed37f270a9/roaring64/bsi64.go#L882">bsi64.WriteTo</a>
   * @param output the DataOutput stream
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public void serialize(DataOutput output) throws IOException {
    if (DEFAULT_BSI64_NAVIGABLE_SERIALIZATION_MODE == BSI64_SERIALIZATION_MODE_COMPATIBLE_WITH_GO) {
      // write ebm
      this.ebM.serialize(output);
      for (Roaring64NavigableMap rb : this.bA) {
        rb.serialize(output);
      }
    } else {
      throw new UnsupportedOperationException(
          "unsupported serialization mode " + DEFAULT_BSI64_NAVIGABLE_SERIALIZATION_MODE);
    }
  }

  /**
   * Deserialize (retrieve) the BSI.</br>
   * See {@link #serialize(DataOutput)} for the serialization format.
   * The current bsi is overwritten.
   * <p>
   * *NOTE*</br>
   * 1. multiple bsi objects in one {@code DataInput} not supported for current serialization format.</br>
   * 2. for proper deserialization, {@link Roaring64NavigableMap#SERIALIZATION_MODE} should be set
   * to the value as serialization</br>
   * @param in the DataInput stream
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public void deserialize(DataInput in) throws IOException {
    if (DEFAULT_BSI64_NAVIGABLE_SERIALIZATION_MODE == BSI64_SERIALIZATION_MODE_COMPATIBLE_WITH_GO) {
      this.clear();
      // read ebm
      Roaring64NavigableMap ebm = new Roaring64NavigableMap();
      ebm.deserialize(in);
      this.ebM = ebm;

      List<Roaring64NavigableMap> baList = new ArrayList<>();
      try (DataInputStream is = new DataInputStream((InputStream) in); ) {
        while (is.available() > 0) {
          Roaring64NavigableMap rb = new Roaring64NavigableMap();
          rb.deserialize(in);
          baList.add(rb);
        }
      }
      this.bA = baList.toArray(new Roaring64NavigableMap[0]);
      this.minValue = minValue();
      this.maxValue = maxValue();
    } else {
      throw new UnsupportedOperationException(
          "unsupported serialization mode " + DEFAULT_BSI64_NAVIGABLE_SERIALIZATION_MODE);
    }
  }

  /*
   * get the serialized size in bytes
   */
  public long serializedSizeInBytes() {
    long size = 0;
    for (Roaring64NavigableMap rb : this.bA) {
      size += rb.serializedSizeInBytes();
    }
    return this.ebM.serializedSizeInBytes() + size;
  }

  /**
   * valueExists tests whether the value exists.
   */
  public boolean valueExist(long columnId) {
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
        this.bA[i].removeLong(columnId);
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

    Roaring64NavigableMap[] newBA = new Roaring64NavigableMap[newBitDepth];
    if (oldBitDepth != 0) {
      System.arraycopy(this.bA, 0, newBA, 0, oldBitDepth);
    }

    for (int i = newBitDepth - 1; i >= oldBitDepth; i--) {
      newBA[i] = new Roaring64NavigableMap();
      if (this.runOptimized) {
        newBA[i].runOptimize();
      }
    }
    this.bA = newBA;
  }

  /**
   * set list of values
   * @param values list of value pairs
   */
  public void setValues(List<Pair<Long, Long>> values) {
    if (values == null || values.isEmpty()) return;
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
  public void merge(Roaring64NavigableMapSliceIndex otherBsi) {

    if (null == otherBsi || otherBsi.ebM.isEmpty()) {
      return;
    }

    // todo whether we need this
    if (Roaring64NavigableMap.intersects(this.ebM, otherBsi.ebM)) {
      throw new IllegalArgumentException("merge can be used only in bsiA  bsiB  is null");
    }

    int bitDepth = Integer.max(this.bitCount(), otherBsi.bitCount());
    Roaring64NavigableMap[] newBA = new Roaring64NavigableMap[bitDepth];
    for (int i = 0; i < bitDepth; i++) {
      Roaring64NavigableMap current = i < this.bA.length ? this.bA[i] : new Roaring64NavigableMap();
      Roaring64NavigableMap other =
          i < otherBsi.bA.length ? otherBsi.bA[i] : new Roaring64NavigableMap();
      newBA[i] = Roaring64NavigableMap.or(current, other);
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

  /**
   * clone a bsi instance
   */
  @Override
  public Roaring64NavigableMapSliceIndex clone() {
    Roaring64NavigableMapSliceIndex bitSliceIndex = new Roaring64NavigableMapSliceIndex();
    bitSliceIndex.minValue = this.minValue;
    bitSliceIndex.maxValue = this.maxValue;
    bitSliceIndex.ebM = this.ebM.clone();
    Roaring64NavigableMap[] cloneBA = new Roaring64NavigableMap[this.bitCount()];
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
  private Roaring64NavigableMap oNeilCompare(
      BitmapSliceIndex.Operation operation, long predicate, Roaring64NavigableMap foundSet) {
    Roaring64NavigableMap fixedFoundSet = foundSet == null ? this.ebM : foundSet;

    Roaring64NavigableMap GT = new Roaring64NavigableMap();
    Roaring64NavigableMap LT = new Roaring64NavigableMap();
    Roaring64NavigableMap EQ = this.ebM;

    for (int i = this.bitCount() - 1; i >= 0; i--) {
      int bit = (int) ((predicate >> i) & 1);
      if (bit == 1) {
        LT = Roaring64NavigableMap.or(LT, Roaring64NavigableMap.andNot(EQ, this.bA[i]));
        EQ = Roaring64NavigableMap.and(EQ, this.bA[i]);
      } else {
        GT = Roaring64NavigableMap.or(GT, Roaring64NavigableMap.and(EQ, this.bA[i]));
        EQ = Roaring64NavigableMap.andNot(EQ, this.bA[i]);
      }
    }
    EQ = Roaring64NavigableMap.and(fixedFoundSet, EQ);
    switch (operation) {
      case EQ:
        return EQ;
      case NEQ:
        return Roaring64NavigableMap.andNot(fixedFoundSet, EQ);
      case GT:
        return Roaring64NavigableMap.and(GT, fixedFoundSet);
      case LT:
        return Roaring64NavigableMap.and(LT, fixedFoundSet);
      case LE:
        return Roaring64NavigableMap.and(Roaring64NavigableMap.or(LT, EQ), fixedFoundSet);
      case GE:
        return Roaring64NavigableMap.and(Roaring64NavigableMap.or(GT, EQ), fixedFoundSet);
      default:
        throw new IllegalArgumentException("");
    }
  }

  /**
   * BSI Compare using single thread
   * this Function compose algorithm from O'Neil and Owen Kaser
   * the GE algorithm is from Owen since the performance is better.  others are from O'Neil
   *
   * @param operation    the operation of BitmapSliceIndex.Operation
   * @param startOrValue the start or value of comparison, when the comparison operation is range, it's start,
   *                     when others,it's value.
   * @param end          the end value of comparison. when the comparison operation is not range,the end = 0
   * @param foundSet     columnId set we want compare,using RoaringBitmap to express
   * @return columnId set we found in this bsi with giving conditions, using Roaring64NavigableMap to express
   */
  public Roaring64NavigableMap compare(
      BitmapSliceIndex.Operation operation,
      long startOrValue,
      long end,
      Roaring64NavigableMap foundSet) {
    Roaring64NavigableMap result = compareUsingMinMax(operation, startOrValue, end, foundSet);
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
          Roaring64NavigableMap left =
              oNeilCompare(BitmapSliceIndex.Operation.GE, startOrValue, foundSet);
          Roaring64NavigableMap right = oNeilCompare(BitmapSliceIndex.Operation.LE, end, foundSet);

          return Roaring64NavigableMap.and(left, right);
        }
      default:
        throw new IllegalArgumentException("not support operation!");
    }
  }

  private Roaring64NavigableMap compareUsingMinMax(
      BitmapSliceIndex.Operation operation,
      long startOrValue,
      long end,
      Roaring64NavigableMap foundSet) {
    Roaring64NavigableMap all =
        foundSet == null ? ebM.clone() : Roaring64NavigableMap.and(ebM, foundSet);
    Roaring64NavigableMap empty = new Roaring64NavigableMap();

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

  /**
   * given columns in foundSet, return the count and sum of values found in BSI
   * @param foundSet given columnIds (represent as Roaring64NavigableMap)
   * @return the &lt;sum, count&gt; pair
   */
  public Pair<Long, Long> sum(Roaring64NavigableMap foundSet) {
    if (null == foundSet || foundSet.isEmpty()) {
      return Pair.newPair(0L, 0L);
    }
    long count = foundSet.getLongCardinality();

    long sum =
        IntStream.range(0, this.bitCount())
            .mapToLong(x -> (1L << x) * Roaring64NavigableMap.andCardinality(this.bA[x], foundSet))
            .sum();

    return Pair.newPair(sum, count);
  }

  /**
   * given columns in foundSet, return the top k columnId found in BSI
   * @param foundSet the columnIds needs to be checked (represent as Roaring64NavigableMap)
   * @param k top k
   * @return the top k columnIds found in BSI
   */
  public Roaring64NavigableMap topK(Roaring64NavigableMap foundSet, long k) {
    if (null == foundSet || foundSet.isEmpty()) {
      return new Roaring64NavigableMap();
    }
    if (k >= foundSet.getLongCardinality()) {
      return foundSet;
    }
    Roaring64NavigableMap re = new Roaring64NavigableMap();
    Roaring64NavigableMap candidates = foundSet.clone();
    // keep target K value
    long originalTargetTopK = k;

    for (int x = this.bitCount() - 1; x >= 0 && !candidates.isEmpty() && k > 0; x--) {
      Roaring64NavigableMap X = Roaring64NavigableMap.and(candidates, this.bA[x]);
      long cardinality = X.getLongCardinality();
      if (cardinality > k) {
        candidates.and(this.bA[x]);
      } else {
        re.or(X);
        candidates.andNot(this.bA[x]);
        k -= cardinality;
      }
    }

    // check whether we get enough items in 're'
    long reCardinality = re.getLongCardinality();
    long candidatesCardinality = candidates.getLongCardinality();
    if (reCardinality < originalTargetTopK) {
      // 're' has not enough items, pick some items from candidates, then merge re&candidates
      long dismissCandidatesCnt = candidatesCardinality + reCardinality - originalTargetTopK;
      Iterator<Long> iterator = candidates.iterator();
      while (iterator.hasNext() && dismissCandidatesCnt > 0) {
        // remove some items from candidates
        candidates.removeLong(iterator.next());
        dismissCandidatesCnt--;
      }
    } else {
      candidates = null;
    }
    return Roaring64NavigableMap.or(re, candidates);
  }

  /**
   * given columns in foundSet,
   * 1. if one columnId of foundSet is in the bsi, then get the associated value
   * 2. then return all the values as a Roaring64NavigableMap.
   * @param foundSet the columnIds needs to be checked, null if all columnIds needs to be checked
   */
  public Roaring64NavigableMap transpose(Roaring64NavigableMap foundSet) {
    Roaring64NavigableMap re = new Roaring64NavigableMap();
    Roaring64NavigableMap fixedFoundSet =
        foundSet == null ? this.ebM : Roaring64NavigableMap.and(foundSet, this.ebM);
    fixedFoundSet.forEach((long x) -> re.add(this.getValue(x).getKey()));
    return re;
  }

  /**
   * given columns in foundSet,
   * 1. if one columnId of foundSet is in the bsi, then get the associated value in the bsi
   * 2. then return all the &lt;value, # of columnIds&gt; as a bsi.
   * @param foundSet the columnIds needs to be checked, null if all columnIds needs to be checked
   */
  public Roaring64NavigableMapSliceIndex transposeWithCount(Roaring64NavigableMap foundSet) {
    Roaring64NavigableMapSliceIndex re = new Roaring64NavigableMapSliceIndex();
    Roaring64NavigableMap fixedFoundSet =
        foundSet == null ? this.ebM : Roaring64NavigableMap.and(foundSet, this.ebM);
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
