package org.roaringbitmap.bsi;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * BitSliceIndex
 * bit slice index can be used to
 *    1. store high cardinality dim column for OLAP system.
 *    2. high compression ratio for number
 * Given that,we have a table T(c1,c2,c3....Cn). As we know,most database has rowId for each row.
 * then table T is actually  T(rowId,c1,c2,c3,Cn).
 * 1. if column c1 is string, we can encode c1 using dictionary. By bsi, 
 * we can only use 33 bit slice express 2^32 cardinality dim.
 * 2. if column c2 is int32(that is 4Byte), for 1_000_000 rows, the size 
 * of c2 is more than 3.81MB. however,
 * by bsi, the size might be less than 1MB.
 *
 */
public interface BitmapSliceIndex {
  enum Operation {
    // EQ equal
    EQ,
    // NEQ not equal
    NEQ,
    // LE less than or equal
    LE,
    // LT less than
    LT,
    // GE greater than or equal
    GE,
    // GT greater than
    GT,
    // RANGE range
    RANGE
  }

  int bitCount();

  long getLongCardinality();


  /**
   * set value for bsi, setValue will set each bit slice according to the input value
   * given that we have bsi as follow
   * ebm:  RB[1 1 1 1]
   * slice0: RB[0 1 0 1]   -
   * slice1: RB[1 0 1 0]  |
   * slice2: RB[1 0 0 1]  |--bA:bit slice Array
   * slice3: RB[0 0 0 1]   -
   *      1 2 3 4
   *      |
   *      ---------------- columnId or rowId
   *  for columnId 1, the value is 110 that is :6
   *  for columnId 2, the value is 1 that is :1
   *  for columnId 3, the value is 10 that is :2
   *  for columnId 4, the value is 1101 that is :11
   *
   * @param columnId   columnId or rowId
   * @param value    value for this columnId or rowId
   */
  void setValue(int columnId, int value);

  /**
   *
   * @param columnId columnId or rowId
   * @return the value of this columnId
   */
  Pair<Integer, Boolean> getValue(int columnId);

  /**
   * setValues will batch set value for this bsi.
   * currentMaxValue/currentMinValue are optional,it's can be compute from input value list.
   * and avoiding bsi expend slice array capacity.
   */
  @Deprecated
  void setValues(List<Pair<Integer, Integer>> values, Integer currentMaxValue, Integer currentMinValue);

  /**
   * Set a batch of values.
   * @param values
   */
  void setValues(List<Pair<Integer, Integer>> values);

  void serialize(ByteBuffer buffer) throws IOException;

  void serialize(DataOutput output) throws IOException;

  int serializedSizeInBytes();


}

