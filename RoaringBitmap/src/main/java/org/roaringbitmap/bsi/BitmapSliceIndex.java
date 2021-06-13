package org.roaringbitmap.bsi;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * BitSliceIndex
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


    void setValue(int cid, int value);

    Pair<Integer, Boolean> getValue(int cid);

    void setValues(List<Pair<Integer, Integer>> values, Integer currentMaxValue, Integer currentMinValue);


    void serialize(ByteBuffer buffer) throws IOException;

    void serialize(DataOutput output) throws IOException;

    int serializedSizeInBytes();


}

