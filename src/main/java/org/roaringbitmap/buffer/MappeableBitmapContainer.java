/*
 * (c) the authors
 * Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap.buffer;

import org.roaringbitmap.BitmapContainer;
import org.roaringbitmap.ShortIterator;
import org.roaringbitmap.Util;

import java.io.*;
import java.nio.LongBuffer;
import java.util.Iterator;

/**
 * Simple bitset-like container. Unlike org.roaringbitmap.BitmapContainer, this
 * class uses a LongBuffer to store data.
 */
public final class MappeableBitmapContainer extends MappeableContainer
        implements Cloneable {
    protected static final int MAX_CAPACITY = 1 << 16;

    private static boolean USE_IN_PLACE = true; // optimization flag

    LongBuffer bitmap;
    
    private static final long serialVersionUID = 2L;

    int cardinality;

    /**
     * Create a bitmap container with all bits set to false
     */
    public MappeableBitmapContainer() {
        this.cardinality = 0;
        this.bitmap = LongBuffer.allocate(MAX_CAPACITY / 64);
    }

    /**
     * Create a bitmap container with a run of ones from firstOfRun to
     * lastOfRun, inclusive caller must ensure that the range isn't so small
     * that an ArrayContainer should have been created instead
     * 
     * @param firstOfRun
     *            first index
     * @param lastOfRun
     *            last index (range is exclusive)
     */
    public MappeableBitmapContainer(final int firstOfRun, final int lastOfRun) {
        // TODO: this can be optimized for performance
        this.cardinality = lastOfRun - firstOfRun ;
        this.bitmap = LongBuffer.allocate(MAX_CAPACITY / 64);
        if (this.cardinality == MAX_CAPACITY) {// perhaps a common case
            int len = bitmap.limit();
            for (int k = 0; k < len; ++k)
                bitmap.put(k, -1L);
        } else {
            final int firstWord = firstOfRun / 64;
            final int lastWord = (lastOfRun - 1) / 64;
            final int zeroPrefixLength = firstOfRun & 63;
            final int zeroSuffixLength = 63 - ((lastOfRun - 1) & 63);
            for (int k = firstWord; k < lastWord + 1; ++k)
                bitmap.put(k, -1L);
            bitmap.put(firstWord, bitmap.get(firstWord)
                    ^ ((1L << zeroPrefixLength) - 1));
            final long blockOfOnes = (1L << zeroSuffixLength) - 1;
            final long maskOnLeft = blockOfOnes << (64 - zeroSuffixLength);
            bitmap.put(lastWord, bitmap.get(lastWord) ^ maskOnLeft);
        }
    }

    MappeableBitmapContainer(int newCardinality, LongBuffer newBitmap) {
        this.cardinality = newCardinality;
        LongBuffer tmp = newBitmap.duplicate(); // for thread safety
        this.bitmap = LongBuffer.allocate(tmp.limit());
        tmp.rewind();
        this.bitmap.put(tmp);
    }

    /**
     * Construct a new BitmapContainer backed by the provided LongBuffer.
     * 
     * @param array
     *            LongBuffer where the data is stored
     * @param initCardinality
     *            cardinality (number of values stored)
     */
    public MappeableBitmapContainer(final LongBuffer array,
            final int initCardinality) {
        if (array.limit() != MAX_CAPACITY / 64)
            throw new RuntimeException(
                    "Mismatch between buffer and storage requirements: "
                            + array.limit() + " vs. " + MAX_CAPACITY / 64);
        this.cardinality = initCardinality;
        this.bitmap = array;
    }

    @Override
    int numberOfRuns() {
        if (BufferUtil.isBackedBySimpleArray(this.bitmap)) {
            long[] src = this.bitmap.array();
            int numRuns = 0;
            long nextWord = src[0];

            for (int i = 0; i < src.length - 1; i++) {
                long word = nextWord;
                nextWord = src[i + 1];
                numRuns += Long.bitCount((~word) & (word << 1))
                        + ((word >>> 63) & ~nextWord);
            }

            long word = nextWord;
            numRuns += Long.bitCount((~word) & (word << 1));
            if ((word & 0x8000000000000000L) != 0)
                numRuns++;

            return numRuns;
        } else {
            int numRuns = 0;
            long nextWord = bitmap.get(0);
            int len = bitmap.limit();

            for (int i = 0; i < len - 1; i++) {
                long word = nextWord;
                nextWord = bitmap.get(i + 1);
                numRuns += Long.bitCount((~word) & (word << 1))
                        + ((word >>> 63) & ~nextWord);
            }

            long word = nextWord;
            numRuns += Long.bitCount((~word) & (word << 1));
            if ((word & 0x8000000000000000L) != 0)
                numRuns++;

            return numRuns;

        }
    }
    
    /**
   * Computes the number of runs
   * @return the number of runs
   */
  public int numberOfRunsAdjustment() {
      int ans = 0;
        if (BufferUtil.isBackedBySimpleArray(bitmap)) {
            long[] b = bitmap.array();
            long nextWord = b[0];
            for (int i = 0; i < b.length - 1; i++) {
                final long word = nextWord;

                nextWord = b[i + 1];
                ans += ((word >>> 63) & ~nextWord);
            }

            final long word = nextWord;

            if ((word & 0x8000000000000000L) != 0)
                ans++;

        } else {
            long nextWord = bitmap.get(0);
            int len = bitmap.limit();
            for (int i = 0; i < len - 1; i++) {
                final long word = nextWord;

                nextWord = bitmap.get(i + 1);
                ans += ((word >>> 63) & ~nextWord);
            }

            final long word = nextWord;

            if ((word & 0x8000000000000000L) != 0)
                ans++;
        }
        return ans;
  }



  // bail out early when the number of runs is excessive, without
  // an exact count (just a decent lower bound)
  private static final int BLOCKSIZE = 128;
  // 64 words can have max 32 runs per word, max 2k runs

  /**
   * Counts how many runs there is in the bitmap, up to a maximum
   * @param mustNotExceed maximum of runs beyond which counting is pointless
   * @return estimated number of courses
   */
    public int numberOfRunsLowerBound(int mustNotExceed) {
        int numRuns = 0;
        if (BufferUtil.isBackedBySimpleArray(bitmap)) {
            long[] b = bitmap.array();

            for (int blockOffset = 0; blockOffset < b.length; blockOffset += BLOCKSIZE) {

                for (int i = blockOffset; i < blockOffset + BLOCKSIZE; i++) {
                    long word = b[i];
                    numRuns += Long.bitCount((~word) & (word << 1));
                }
                if (numRuns > mustNotExceed)
                    return numRuns;
            }
        } else {
            int len = bitmap.limit();
            for (int blockOffset = 0; blockOffset < len; blockOffset += BLOCKSIZE) {

                for (int i = blockOffset; i < blockOffset + BLOCKSIZE; i++) {
                    long word = bitmap.get(i);
                    numRuns += Long.bitCount((~word) & (word << 1));
                }
                if (numRuns > mustNotExceed)
                    return numRuns;
            }
        }
        return numRuns;
    }

    // nruns value for which RunContainer.serializedSizeInBytes == BitmapContainer.getArraySizeInBytes()
    private final int MAXRUNS = (getArraySizeInBytes() - 2) / 4;
    

    @Override
    public MappeableContainer runOptimize() {
        int numRuns = numberOfRunsLowerBound(MAXRUNS); // decent choice

        int sizeAsRunContainerLowerBound = MappeableRunContainer.serializedSizeInBytes(numRuns);

        if (sizeAsRunContainerLowerBound >= getArraySizeInBytes())
            return this;
        // else numRuns is a relatively tight bound that needs to be exact
        // in some cases (or if we need to make the runContainer the right
        // size)
        numRuns += numberOfRunsAdjustment();
        int sizeAsRunContainer = MappeableRunContainer.serializedSizeInBytes(numRuns);
        
        if (getArraySizeInBytes() > sizeAsRunContainer) {
            return new MappeableRunContainer(this,  numRuns); 
        } 
        else 
            return this;
    }

    @Override
    public MappeableContainer add(final short i) {
        final int x = BufferUtil.toIntUnsigned(i);
        final long previous = bitmap.get(x / 64);
        final long newv = previous | (1l << x);
        bitmap.put(x / 64, newv);
        if(USE_BRANCHLESS)
            cardinality += (previous ^ newv) >>> x;
        else 
            if(previous != newv) cardinality++;
        return this;
    }

    @Override
    public MappeableArrayContainer and(final MappeableArrayContainer value2) {

        final MappeableArrayContainer answer = new MappeableArrayContainer(
                value2.content.limit());
        if(!BufferUtil.isBackedBySimpleArray(answer.content))
            throw new RuntimeException("Should not happen. Internal bug.");
        short[] sarray = answer.content.array();
        if (BufferUtil.isBackedBySimpleArray(value2.content)) {
            short[] c = value2.content.array();
            int ca = value2.cardinality;
            for (int k = 0; k < ca; ++k) {
                short v = c[k];
                if (this.contains(v))
                    sarray[answer.cardinality++] = v;
            }

        } else {
            int ca = value2.cardinality;
            for (int k = 0; k < ca; ++k) {
                short v = value2.content.get(k);
                if (this.contains(v))
                    sarray[answer.cardinality++] = v;
            }
        }
        return answer;
    }

    @Override
    public MappeableContainer and(final MappeableBitmapContainer value2) {
        int newCardinality = 0;
        if (BufferUtil.isBackedBySimpleArray(this.bitmap) && BufferUtil.isBackedBySimpleArray(value2.bitmap)) {
            long[] tb = this.bitmap.array();
            long[] v2b = value2.bitmap.array();
            int len = this.bitmap.limit();
            for (int k = 0; k < len; ++k) {
                newCardinality += Long.bitCount(tb[k] & v2b[k]);
            }
        } else {
            int len = this.bitmap.limit();
            for (int k = 0; k < len; ++k) {
                newCardinality += Long.bitCount(this.bitmap.get(k)
                        & value2.bitmap.get(k));
            }
        }
        if (newCardinality > MappeableArrayContainer.DEFAULT_MAX_SIZE) {
            final MappeableBitmapContainer answer = new MappeableBitmapContainer();
            if(!BufferUtil.isBackedBySimpleArray(answer.bitmap))
                throw new RuntimeException("Should not happen. Internal bug.");
            long[] bitArray = answer.bitmap.array();
            if (BufferUtil.isBackedBySimpleArray(this.bitmap) && BufferUtil.isBackedBySimpleArray(value2.bitmap)) {
                long[] tb = this.bitmap.array();
                long[] v2b = value2.bitmap.array();
                int len = this.bitmap.limit();
                for (int k = 0; k < len; ++k) {
                    bitArray[k] = tb[k] & v2b[k];
                }
            } else {
                int len = this.bitmap.limit();
                for (int k = 0; k < len; ++k) {
                    bitArray[k] = this.bitmap.get(k) & value2.bitmap.get(k);
                }
            }
            answer.cardinality = newCardinality;
            return answer;
        }
        final MappeableArrayContainer ac = new MappeableArrayContainer(
                newCardinality);
        if (BufferUtil.isBackedBySimpleArray(this.bitmap) && BufferUtil.isBackedBySimpleArray(value2.bitmap))
            org.roaringbitmap.Util.fillArrayAND(ac.content.array(),
                    this.bitmap.array(), value2.bitmap.array());
        else
            BufferUtil.fillArrayAND(ac.content.array(), this.bitmap,
                    value2.bitmap);
        ac.cardinality = newCardinality;
        return ac;
    }


   
    @Override
    public MappeableContainer and(final MappeableRunContainer value2) {
        return value2.and(this);
    }



    @Override
    public MappeableContainer andNot(final MappeableArrayContainer value2) {
        final MappeableBitmapContainer answer = clone();
        if(!BufferUtil.isBackedBySimpleArray(answer.bitmap))
            throw new RuntimeException("Should not happen. Internal bug.");
        long[] bitArray = answer.bitmap.array();
        if (BufferUtil.isBackedBySimpleArray(value2.content) && BufferUtil.isBackedBySimpleArray(this.bitmap)) {
            short[] v2 = value2.content.array();
            int c = value2.cardinality;
            for (int k = 0; k < c; ++k) {
                short v = v2[k];
                final int i = BufferUtil.toIntUnsigned(v) >>> 6;
                long w = bitArray[i];
                long aft = w & (~(1l << v));
                bitArray[i] = aft;
                answer.cardinality -= (w ^ aft) >>> v;
            }
        } else {
            int c = value2.cardinality;
            for (int k = 0; k < c; ++k) {
                short v2 = value2.content.get(k);
                final int i = BufferUtil.toIntUnsigned(v2) >>> 6;
                long w = bitArray[i];
                long aft = bitArray[i]  & (~(1l << v2));
                bitArray[i] = aft;
                answer.cardinality -= (w ^ aft) >>> v2;
            }
        }
        if (answer.cardinality <= MappeableArrayContainer.DEFAULT_MAX_SIZE)
            return answer.toArrayContainer();
        return answer;
    }

    @Override
    public MappeableContainer andNot(final MappeableBitmapContainer value2) {

        int newCardinality = 0;
        if (BufferUtil.isBackedBySimpleArray(this.bitmap) && BufferUtil.isBackedBySimpleArray(value2.bitmap)) {
            long[] b = this.bitmap.array();
            long[] v2 = value2.bitmap.array();
            int len = this.bitmap.limit();
            for (int k = 0; k < len; ++k) {
                newCardinality += Long.bitCount(b[k] & (~v2[k]));
            }

        } else { 
            int len = this.bitmap.limit();
            for (int k = 0; k < len; ++k) {
                newCardinality += Long.bitCount(this.bitmap.get(k)
                        & (~value2.bitmap.get(k)));
            }
        }
        if (newCardinality > MappeableArrayContainer.DEFAULT_MAX_SIZE) {
            final MappeableBitmapContainer answer = new MappeableBitmapContainer();
            if(!BufferUtil.isBackedBySimpleArray(answer.bitmap))
                throw new RuntimeException("Should not happen. Internal bug.");
            long[] bitArray = answer.bitmap.array();
            if (BufferUtil.isBackedBySimpleArray(this.bitmap) && BufferUtil.isBackedBySimpleArray(value2.bitmap)) {
                long[] b = this.bitmap.array();
                long[] v2 = value2.bitmap.array();
                int len = answer.bitmap.limit();
                for (int k = 0; k < len; ++k) {
                    bitArray[k] = b[k] & (~v2[k]);
                }
            } else {
                int len = answer.bitmap.limit();
                for (int k = 0; k < len; ++k) {
                    bitArray[k] = this.bitmap.get(k) & (~value2.bitmap.get(k));
                }
            }
            answer.cardinality = newCardinality;
            return answer;
        }
        final MappeableArrayContainer ac = new MappeableArrayContainer(
                newCardinality);
        if (BufferUtil.isBackedBySimpleArray(this.bitmap) && BufferUtil.isBackedBySimpleArray(value2.bitmap))
            org.roaringbitmap.Util.fillArrayANDNOT(ac.content.array(),
                    this.bitmap.array(), value2.bitmap.array());
        else
            BufferUtil.fillArrayANDNOT(ac.content.array(), this.bitmap,
                    value2.bitmap);
        ac.cardinality = newCardinality;
        return ac;
    }

    @Override
    public MappeableContainer andNot(final MappeableRunContainer value2) {
        MappeableBitmapContainer answer = this.clone();
        if (BufferUtil.isBackedBySimpleArray(answer.bitmap)) {
            long[] b = answer.bitmap.array();
            for (int rlepos = 0; rlepos < value2.nbrruns; ++rlepos) {
                int start = BufferUtil.toIntUnsigned(value2.getValue(rlepos));
                int end = BufferUtil.toIntUnsigned(value2.getValue(rlepos))
                        + BufferUtil.toIntUnsigned(value2.getLength(rlepos))
                        + 1;
                Util.resetBitmapRange(b, start, end);
            }
        } else {
            for (int rlepos = 0; rlepos < value2.nbrruns; ++rlepos) {
                int start = BufferUtil.toIntUnsigned(value2.getValue(rlepos));
                int end = BufferUtil.toIntUnsigned(value2.getValue(rlepos))
                        + BufferUtil.toIntUnsigned(value2.getLength(rlepos))
                        + 1;
                BufferUtil.resetBitmapRange(answer.bitmap, start, end);
            }
        }
        answer.computeCardinality();
        if (answer.getCardinality() > MappeableArrayContainer.DEFAULT_MAX_SIZE)
            return answer;
        else
            return answer.toArrayContainer();
    }



    @Override
    public void clear() {
        if (cardinality != 0) {
            cardinality = 0;
            int len = this.bitmap.limit();
            for (int k = 0; k < len; ++k)
                bitmap.put(k, 0);
        }
    }

    @Override
    public MappeableBitmapContainer clone() {
        return new MappeableBitmapContainer(this.cardinality, this.bitmap);
    }

    @Override
    public boolean contains(final short i) {
        final int x = BufferUtil.toIntUnsigned(i);
        return (bitmap.get(x / 64) & (1l << x)) != 0;
    }

    @Override

    public boolean equals(Object o) {
        if (o instanceof MappeableBitmapContainer) {
            final MappeableBitmapContainer srb = (MappeableBitmapContainer) o;
            if (srb.cardinality != this.cardinality)
                return false;
            if (BufferUtil.isBackedBySimpleArray(this.bitmap) && BufferUtil.isBackedBySimpleArray(srb.bitmap)) {
                long[] b = this.bitmap.array();
                long[] s = srb.bitmap.array();
                int len = this.bitmap.limit();
                for (int k = 0; k < len; ++k)
                    if (b[k] != s[k])
                        return false;
            } else {
                int len = this.bitmap.limit();
                for (int k = 0; k < len; ++k)
                    if (this.bitmap.get(k) != srb.bitmap.get(k))
                        return false;
            }
            return true;

        } else if (o instanceof MappeableRunContainer) {
            return o.equals(this);
        }
        return false;
    }

    /**
     * Fill the array with set bits
     * 
     * @param array
     *            container (should be sufficiently large)
     */
    protected void fillArray(final short[] array) {
        int pos = 0;
        if (BufferUtil.isBackedBySimpleArray(bitmap)) {
            long[] b = bitmap.array();
            for (int k = 0; k < b.length; ++k) {
                long bitset = b[k];
                while (bitset != 0) {
                    final long t = bitset & -bitset;
                    array[pos++] = (short) (k * 64 + Long.bitCount(t - 1));
                    bitset ^= t;
                }
            }

        } else {
            int len = this.bitmap.limit();
            for (int k = 0; k < len; ++k) {
                long bitset = bitmap.get(k);
                while (bitset != 0) {
                    final long t = bitset & -bitset;
                    array[pos++] = (short) (k * 64 + Long.bitCount(t - 1));
                    bitset ^= t;
                }
            }
        }
    }

    @Override
    public void fillLeastSignificant16bits(int[] x, int i, int mask) {
        int pos = i;
        if (BufferUtil.isBackedBySimpleArray(bitmap)) {
            long[] b = bitmap.array();
            int len = this.bitmap.limit();
            for (int k = 0; k < len; ++k) {
                long bitset = b[k];
                while (bitset != 0) {
                    final long t = bitset & -bitset;
                    x[pos++] = (k * 64 + Long.bitCount(t - 1)) | mask;
                    bitset ^= t;
                }
            }

        } else {
            int len = this.bitmap.limit();
            for (int k = 0; k < len; ++k) {
                long bitset = bitmap.get(k);
                while (bitset != 0) {
                    final long t = bitset & -bitset;
                    x[pos++] = (k * 64 + Long.bitCount(t - 1)) | mask;
                    bitset ^= t;
                }
            }
        }
    }

    @Override
    protected int getArraySizeInBytes() {
        return MAX_CAPACITY / 8;
    }


    protected static int getArraySizeInBytes(int unusedCardinality) {
        return MAX_CAPACITY / 8;
    }



    @Override
    public int getCardinality() {
        return cardinality;
    }

    @Override
    public ShortIterator getShortIterator() {
        if(this.isArrayBacked())
            return BitmapContainer.getShortIterator(bitmap.array()); 
        return new MappeableBitmapContainerShortIterator(this);
    }

    @Override
    public ShortIterator getReverseShortIterator() {
        if(this.isArrayBacked())
            return BitmapContainer.getReverseShortIterator(bitmap.array());
        return new ReverseMappeableBitmapContainerShortIterator(this);
    }

    @Override
    public int getSizeInBytes() {
        return this.bitmap.limit() * 8;
    }

    @Override
    public int hashCode() {
        long hash = 0;
        int len = this.bitmap.limit();
        for (int k = 0; k < len; ++k)
            hash += 31 * hash + this.bitmap.get(k);
        return (int) hash;
    }

    @Override
    public MappeableContainer iand(final MappeableArrayContainer b2) {
        return b2.and(this);// no inplace possible
    }

    @Override
    public MappeableContainer iand(final MappeableBitmapContainer b2) {
        if(BufferUtil.isBackedBySimpleArray(this.bitmap) && BufferUtil.isBackedBySimpleArray(b2.bitmap)) {
            int newCardinality = 0;
            long[] tb = this.bitmap.array();
            long[] tb2 = b2.bitmap.array();
            int len = this.bitmap.limit();
            for (int k = 0; k < len; ++k) {
                newCardinality += Long.bitCount(tb[k]
                        & tb2[k]);
            }
            if (newCardinality > MappeableArrayContainer.DEFAULT_MAX_SIZE) {
                for (int k = 0; k < len; ++k) {
                    tb[k] &= tb2[k];
                }
                this.cardinality = newCardinality;
                return this;
            }
            final MappeableArrayContainer ac = new MappeableArrayContainer(
                    newCardinality);
            BufferUtil.fillArrayAND(ac.content.array(), this.bitmap, b2.bitmap);
            ac.cardinality = newCardinality;
            return ac;    
        } else {
            int newCardinality = 0;
            int len = this.bitmap.limit();
            for (int k = 0; k < len; ++k) {
                newCardinality += Long.bitCount(this.bitmap.get(k)
                        & b2.bitmap.get(k));
            }
            if (newCardinality > MappeableArrayContainer.DEFAULT_MAX_SIZE) {
                for (int k = 0; k < len; ++k) {
                    this.bitmap.put(k, this.bitmap.get(k) & b2.bitmap.get(k));
                }
                this.cardinality = newCardinality;
                return this;
            }
            final MappeableArrayContainer ac = new MappeableArrayContainer(
                    newCardinality);
            BufferUtil.fillArrayAND(ac.content.array(), this.bitmap, b2.bitmap);
            ac.cardinality = newCardinality;
            return ac;
        }
    }


    @Override
    public MappeableContainer iand(final MappeableRunContainer x) {
         final int card = x.getCardinality();
        if(card <= MappeableArrayContainer.DEFAULT_MAX_SIZE) {
            // no point in doing it in-place
            MappeableArrayContainer answer = new MappeableArrayContainer(card);
            answer.cardinality=0;
            for (int rlepos=0; rlepos < x.nbrruns; ++rlepos) {
                int runStart = BufferUtil.toIntUnsigned(x.getValue(rlepos));
                int runEnd = runStart + BufferUtil.toIntUnsigned(x.getLength(rlepos));
                for (int runValue = runStart; runValue <= runEnd; ++runValue) {
                    if ( this.contains((short) runValue)) {
                        answer.content.put(answer.cardinality++,(short) runValue);
                    }
                }
            }
            return answer;
        }
        int start = 0;
        for(int rlepos = 0; rlepos < x.nbrruns; ++rlepos ) {
            int end = BufferUtil.toIntUnsigned(x.getValue(rlepos));
            BufferUtil.resetBitmapRange(this.bitmap, start, end);
            start = end + BufferUtil.toIntUnsigned(x.getLength(rlepos)) + 1;
        }
        BufferUtil.resetBitmapRange(this.bitmap, start, BufferUtil.maxLowBitAsInteger() + 1);
        computeCardinality();
        if(getCardinality() > MappeableArrayContainer.DEFAULT_MAX_SIZE)
            return this;
        else return toArrayContainer();
    }

    @Override
    public MappeableContainer iandNot(final MappeableArrayContainer b2) {
        for (int k = 0; k < b2.cardinality; ++k) {
            this.remove(b2.content.get(k));
        }
        if (cardinality <= MappeableArrayContainer.DEFAULT_MAX_SIZE)
            return this.toArrayContainer();
        return this;
    }

    @Override
    public MappeableContainer iandNot(final MappeableBitmapContainer b2) {
        int newCardinality = 0;
        if(!BufferUtil.isBackedBySimpleArray(bitmap))
            throw new RuntimeException("Should not happen. Internal bug.");
        long[] b = this.bitmap.array();
        if (BufferUtil.isBackedBySimpleArray(b2.bitmap)) {
            long[] b2Arr = b2.bitmap.array();
            int len = this.bitmap.limit();
            for (int k = 0; k < len; ++k) {
                newCardinality += Long.bitCount(b[k] & (~b2Arr[k]));
            }
            if (newCardinality > MappeableArrayContainer.DEFAULT_MAX_SIZE) {
                for (int k = 0; k < len; ++k) {
                    this.bitmap.put(k, b[k] & (~b2Arr[k]));
                }
                this.cardinality = newCardinality;
                return this;
            }
            final MappeableArrayContainer ac = new MappeableArrayContainer(
                    newCardinality);
            org.roaringbitmap.Util
                    .fillArrayANDNOT(ac.content.array(), b, b2Arr);
            ac.cardinality = newCardinality;
            return ac;

        }
        int len = this.bitmap.limit();
        for (int k = 0; k < len; ++k) {
            newCardinality += Long.bitCount(b[k] & (~b2.bitmap.get(k)));
        }
        if (newCardinality > MappeableArrayContainer.DEFAULT_MAX_SIZE) {
            for (int k = 0; k < len; ++k) {
                b[k] &= (~b2.bitmap.get(k));
            }
            this.cardinality = newCardinality;
            return this;
        }
        final MappeableArrayContainer ac = new MappeableArrayContainer(
                newCardinality);

        BufferUtil.fillArrayANDNOT(ac.content.array(), this.bitmap, b2.bitmap);
        ac.cardinality = newCardinality;
        return ac;
    }



    @Override
    public MappeableContainer iandNot(final MappeableRunContainer x) {
        if(BufferUtil.isBackedBySimpleArray(this.bitmap)) {
            long[] b = this.bitmap.array();
            for (int rlepos = 0; rlepos < x.nbrruns; ++rlepos) {
                int start = BufferUtil.toIntUnsigned(x.getValue(rlepos));
                int end = start
                        + BufferUtil.toIntUnsigned(x.getLength(rlepos)) + 1;
                Util.resetBitmapRange(b, start, end);
            }
            computeCardinality();
            if (getCardinality() > MappeableArrayContainer.DEFAULT_MAX_SIZE)
                return this;
            else
                return toArrayContainer();            
        }
        for (int rlepos = 0; rlepos < x.nbrruns; ++rlepos) {
            int start = BufferUtil.toIntUnsigned(x.getValue(rlepos));
            int end = start
                    + BufferUtil.toIntUnsigned(x.getLength(rlepos)) + 1;
            BufferUtil.resetBitmapRange(this.bitmap, start, end);
        }
        computeCardinality();
        if (getCardinality() > MappeableArrayContainer.DEFAULT_MAX_SIZE)
            return this;
        else
            return toArrayContainer();
    
    }

    @Override
    public MappeableContainer inot(final int firstOfRange, final int lastOfRange) {
        return not(this, firstOfRange, lastOfRange); 
    }

    @Override
    public MappeableBitmapContainer ior(final MappeableArrayContainer value2) {
        if (!BufferUtil.isBackedBySimpleArray(this.bitmap))
            throw new RuntimeException("Should not happen. Internal bug.");
        long[] b = this.bitmap.array();
        if (BufferUtil.isBackedBySimpleArray(value2.content)) {
            short[] v2 = value2.content.array();
            int c = value2.cardinality;
            for (int k = 0; k < c; ++k) {
                final int i = BufferUtil.toIntUnsigned(v2[k]) >>> 6;
                long bef = b[i];
                long aft = bef | (1l << v2[k]);
                b[i] = aft;
                if (USE_BRANCHLESS) {
                    cardinality += (bef - aft) >>> 63;
                } else {
                    if (aft != bef)
                        cardinality++;
                }
            }
            return this;
        }
        int c = value2.cardinality;
        for (int k = 0; k < c; ++k) {
            short v2 = value2.content.get(k);
            final int i = BufferUtil.toIntUnsigned(v2) >>> 6;
            long bef = b[i];
            long aft = bef | (1l << v2);
            b[i] = aft;
            if (USE_BRANCHLESS) {
                cardinality += (bef - aft) >>> 63;
            } else {
                if (aft != bef)
                    cardinality++;
            }
        }
        return this;
    }

    @Override
    public MappeableContainer ior(final MappeableBitmapContainer b2) {
        if(!BufferUtil.isBackedBySimpleArray(bitmap))
            throw new RuntimeException("Should not happen. Internal bug.");
        long[] b = this.bitmap.array();
        this.cardinality = 0;
        if (BufferUtil.isBackedBySimpleArray(b2.bitmap)) {
            long[] b2Arr = b2.bitmap.array();
            int len = this.bitmap.limit();
            for (int k = 0; k < len; k++) {
                long w = b[k] | b2Arr[k];
                b[k] = w;
                this.cardinality += Long.bitCount(w);
            }
            return this;
        }
        int len = this.bitmap.limit();
        for (int k = 0; k < len; k++) {
            long w = b[k] | b2.bitmap.get(k);
            b[k] = w;
            this.cardinality += Long.bitCount(w);
        }
        return this;
    }


    @Override
    protected boolean isArrayBacked() {
        return BufferUtil.isBackedBySimpleArray(this.bitmap);
    }
    
    @Override
    public MappeableContainer ior(final MappeableRunContainer x) {
        if (BufferUtil.isBackedBySimpleArray(this.bitmap)) {
            long[] b = this.bitmap.array();
            for (int rlepos = 0; rlepos < x.nbrruns; ++rlepos) {
                int start = BufferUtil.toIntUnsigned(x.getValue(rlepos));
                int end = start + BufferUtil.toIntUnsigned(x.getLength(rlepos))
                        + 1;
                Util.setBitmapRange(b, start, end);
            }
        } else {
            for (int rlepos = 0; rlepos < x.nbrruns; ++rlepos) {
                int start = BufferUtil.toIntUnsigned(x.getValue(rlepos));
                int end = start + BufferUtil.toIntUnsigned(x.getLength(rlepos))
                        + 1;
                BufferUtil.setBitmapRange(this.bitmap, start, end);
            }
        }
        computeCardinality();
        return this;
    }


    @Override
    public Iterator<Short> iterator() {
        return new Iterator<Short>() {
            final ShortIterator si = MappeableBitmapContainer.this.getShortIterator();

            @Override
            public boolean hasNext() {
                return si.hasNext();
            }

            @Override
            public Short next() {
                return si.next();
            }

            @Override
            public void remove() {
                //TODO: implement
                throw new RuntimeException("unsupported operation: remove");
            }
        };
    }

    @Override
    public MappeableContainer ixor(final MappeableArrayContainer value2) {
        if (!BufferUtil.isBackedBySimpleArray(bitmap))
            throw new RuntimeException("Should not happen. Internal bug.");
        long[] b = bitmap.array();
        if (BufferUtil.isBackedBySimpleArray(value2.content)) {
            short[] v2 = value2.content.array();
            int c = value2.cardinality;
            for (int k = 0; k < c; ++k) {
                short vc = v2[k];
                long mask = (1l << v2[k]);
                final int index = BufferUtil.toIntUnsigned(vc) >>> 6;
                long ba = b[index];
                // TODO: check whether a branchy version could be faster
                this.cardinality += 1 - 2 * ((ba & mask) >>> vc);
                b[index] = ba ^  mask;
            }

        } else {
            int c = value2.cardinality;
            for (int k = 0; k < c; ++k) {
                short v2 = value2.content.get(k);
                long mask = (1l << v2);
                final int index = BufferUtil.toIntUnsigned(v2) >>> 6;
                long ba = b[index];
                // TODO: check whether a branchy version could be faster
                this.cardinality += 1 - 2 * ((ba & mask) >>> v2);
                b[index] = ba ^ mask;
            }
        }
        if (this.cardinality <= MappeableArrayContainer.DEFAULT_MAX_SIZE) {
            return this.toArrayContainer();
        }
        return this;
    }

    @Override
    public MappeableContainer ixor(MappeableBitmapContainer b2) {
        if(!BufferUtil.isBackedBySimpleArray(bitmap))
            throw new RuntimeException("Should not happen. Internal bug.");
        long[] b = bitmap.array();
        if (BufferUtil.isBackedBySimpleArray(b2.bitmap)) {
            long[] b2Arr = b2.bitmap.array();

            int newCardinality = 0;
            int len = this.bitmap.limit();
            for (int k = 0; k < len; ++k) {
                newCardinality += Long.bitCount(b[k] ^ b2Arr[k]);
            }
            if (newCardinality > MappeableArrayContainer.DEFAULT_MAX_SIZE) {
                for (int k = 0; k < len; ++k) {
                    b[k] ^= b2Arr[k];
                }
                this.cardinality = newCardinality;
                return this;
            }
            final MappeableArrayContainer ac = new MappeableArrayContainer(
                    newCardinality);

            org.roaringbitmap.Util.fillArrayXOR(ac.content.array(), b, b2Arr);
            ac.cardinality = newCardinality;
            return ac;

        }
        int newCardinality = 0;
        int len = this.bitmap.limit();
        for (int k = 0; k < len; ++k) {
            newCardinality += Long.bitCount(b[k] ^ b2.bitmap.get(k));
        }
        if (newCardinality > MappeableArrayContainer.DEFAULT_MAX_SIZE) {
            for (int k = 0; k < len; ++k) {
                b[k] ^= b2.bitmap.get(k);
            }
            this.cardinality = newCardinality;
            return this;
        }
        final MappeableArrayContainer ac = new MappeableArrayContainer(
                newCardinality);

        BufferUtil.fillArrayXOR(ac.content.array(), this.bitmap, b2.bitmap);
        ac.cardinality = newCardinality;
        return ac;
    }



    @Override
    public MappeableContainer ixor(final MappeableRunContainer x) {
        if (BufferUtil.isBackedBySimpleArray(this.bitmap)) {
            long[] b = this.bitmap.array();
            for (int rlepos = 0; rlepos < x.nbrruns; ++rlepos) {
                int start = BufferUtil.toIntUnsigned(x.getValue(rlepos));
                int end = start + BufferUtil.toIntUnsigned(x.getLength(rlepos))
                        + 1;
                Util.flipBitmapRange(b, start, end);
            }
        } else {
            for (int rlepos = 0; rlepos < x.nbrruns; ++rlepos) {
                int start = BufferUtil.toIntUnsigned(x.getValue(rlepos));
                int end = start + BufferUtil.toIntUnsigned(x.getLength(rlepos))
                        + 1;
                BufferUtil.flipBitmapRange(this.bitmap, start, end);
            }
        }
        computeCardinality();
        if (this.getCardinality() > MappeableArrayContainer.DEFAULT_MAX_SIZE)
            return this;
        else
            return toArrayContainer();
    }

    protected void loadData(final MappeableArrayContainer arrayContainer) {
        this.cardinality = arrayContainer.cardinality;
        if(!BufferUtil.isBackedBySimpleArray(bitmap))
            throw new RuntimeException("Should not happen. Internal bug.");
        long[] bitArray = bitmap.array();
        if (BufferUtil.isBackedBySimpleArray(bitmap) && BufferUtil.isBackedBySimpleArray(arrayContainer.content)) {
            long[] b = bitmap.array();
            short[] ac = arrayContainer.content.array();
            for (int k = 0; k < arrayContainer.cardinality; ++k) {
                final short x = ac[k];
                bitArray[BufferUtil.toIntUnsigned(x) / 64] = b[BufferUtil
                        .toIntUnsigned(x) / 64] | (1l << x);
            }

        } else
            for (int k = 0; k < arrayContainer.cardinality; ++k) {
                final short x = arrayContainer.content.get(k);
                bitArray[BufferUtil.toIntUnsigned(x) / 64] = bitmap
                        .get(BufferUtil.toIntUnsigned(x) / 64) | (1l << x);
            }
    }

    /**
     * Find the index of the next set bit greater or equal to i, returns -1 if
     * none found.
     * 
     * @param i
     *            starting index
     * @return index of the next set bit
     */
    public int nextSetBit(final int i) {
        int x = i >> 6; // signed i / 64
        long w = bitmap.get(x);
        w >>>= i;
        if (w != 0) {
            return i + Long.numberOfTrailingZeros(w);
        }
        // for speed, replaced bitmap.limit() with hardcoded MAX_CAPACITY / 64
        for (++x; x < MAX_CAPACITY / 64; ++x) {
            long X = bitmap.get(x);
            if (X != 0) {
                return x * 64 + Long.numberOfTrailingZeros(X);
            }
        }
        return -1;
    }

    /**
     * Find the index of the previous set bit less than or equal to i, returns -1
     * if none found.
     *
     * @param i starting index
     * @return index of the previous set bit
     */
    public int prevSetBit(final int i) {
        int x = i >> 6; // signed i / 64
        long w = bitmap.get(x);
        w <<= 64 - i - 1;
        if (w != 0) {
            return i - Long.numberOfLeadingZeros(w);
        }
        for (--x; x >= 0; --x) {
            long X = bitmap.get(x);
            if (X != 0) {
                return x * 64 + 63 - Long.numberOfLeadingZeros(X);
            }
        }
        return -1;
    }

    /**
     * Find the index of the next unset bit greater or equal to i, returns -1 if
     * none found.
     * 
     * @param i
     *            starting index
     * @return index of the next unset bit
     */
    public short nextUnsetBit(final int i) {
        int x = i / 64;
        long w = ~bitmap.get(x);
        w >>>= i;
        if (w != 0) {
            return (short) (i + Long.numberOfTrailingZeros(w));
        }
        ++x;
        // for speed, replaced bitmap.limit() with hardcoded MAX_CAPACITY / 64
        for (; x < MAX_CAPACITY / 64; ++x) {
            long X = bitmap.get(x);
            if (X != ~0L) {
                return (short) (x * 64 + Long.numberOfTrailingZeros(~X));
            }
        }
        return -1;
    }

    @Override
    public MappeableContainer not(final int firstOfRange, final int lastOfRange) {
        return not(new MappeableBitmapContainer(), firstOfRange, lastOfRange);
    }

    // answer could be a new BitmapContainer, or (for inplace) it can be
    // "this"
    private MappeableContainer not(MappeableBitmapContainer answer,
            final int firstOfRange, final int lastOfRange) {
        // TODO: this can be optimized for performance
        assert bitmap.limit() == MAX_CAPACITY / 64; // checking
        // assumption
        // that partial
        // bitmaps are not
        // allowed
        // an easy case for full range, should be common
        if (lastOfRange - firstOfRange  == MAX_CAPACITY) {
            final int newCardinality = MAX_CAPACITY - cardinality;
            int len = this.bitmap.limit();
            for (int k = 0; k < len; ++k)
                answer.bitmap.put(k, ~this.bitmap.get(k));
            answer.cardinality = newCardinality;
            if (newCardinality <= MappeableArrayContainer.DEFAULT_MAX_SIZE)
                return answer.toArrayContainer();
            return answer;
        }

        // could be optimized to first determine the answer cardinality,
        // rather than update/create bitmap and then possibly convert

        int cardinalityChange = 0;
        final int rangeFirstWord = firstOfRange / 64;
        final int rangeFirstBitPos = firstOfRange & 63;
        final int rangeLastWord = (lastOfRange - 1) / 64;
        final long rangeLastBitPos = (lastOfRange - 1) & 63;

        // if not in place, we need to duplicate stuff before
        // rangeFirstWord and after rangeLastWord
        if (answer != this) {
            for (int i = 0; i < rangeFirstWord; ++i)
                answer.bitmap.put(i, bitmap.get(i));
            int len = this.bitmap.limit();
            for (int i = rangeLastWord + 1; i < len; ++i)
                answer.bitmap.put(i, bitmap.get(i));
        }

        // unfortunately, the simple expression gives the wrong mask for
        // rangeLastBitPos==63
        // no branchless way comes to mind
        final long maskOnLeft = (rangeLastBitPos == 63) ? -1L
                : (1L << (rangeLastBitPos + 1)) - 1;

        long mask = -1L; // now zero out stuff in the prefix
        mask ^= ((1L << rangeFirstBitPos) - 1);

        if (rangeFirstWord == rangeLastWord) {
            // range starts and ends in same word (may have
            // unchanged bits on both left and right)
            mask &= maskOnLeft;
            cardinalityChange = -Long.bitCount(bitmap.get(rangeFirstWord));
            answer.bitmap
                    .put(rangeFirstWord, bitmap.get(rangeFirstWord) ^ mask);
            cardinalityChange += Long.bitCount(answer.bitmap
                    .get(rangeFirstWord));
            answer.cardinality = cardinality + cardinalityChange;

            if (answer.cardinality <= MappeableArrayContainer.DEFAULT_MAX_SIZE)
                return answer.toArrayContainer();
            return answer;
        }

        // range spans words
        cardinalityChange += -Long.bitCount(bitmap.get(rangeFirstWord));
        answer.bitmap.put(rangeFirstWord, bitmap.get(rangeFirstWord) ^ mask);
        cardinalityChange += Long.bitCount(answer.bitmap.get(rangeFirstWord));

        cardinalityChange += -Long.bitCount(bitmap.get(rangeLastWord));
        answer.bitmap
                .put(rangeLastWord, bitmap.get(rangeLastWord) ^ maskOnLeft);
        cardinalityChange += Long.bitCount(answer.bitmap.get(rangeLastWord));

        // negate the words, if any, strictly between first and last
        for (int i = rangeFirstWord + 1; i < rangeLastWord; ++i) {
            cardinalityChange += (64 - 2 * Long.bitCount(bitmap.get(i)));
            answer.bitmap.put(i, ~bitmap.get(i));
        }
        answer.cardinality = cardinality + cardinalityChange;

        if (answer.cardinality <= MappeableArrayContainer.DEFAULT_MAX_SIZE)
            return answer.toArrayContainer();
        return answer;
    }

    @Override
    public MappeableBitmapContainer or(final MappeableArrayContainer value2) {
        final MappeableBitmapContainer answer = clone();
        if (!BufferUtil.isBackedBySimpleArray(answer.bitmap))
            throw new RuntimeException("Should not happen. Internal bug.");
        long[] bitArray = answer.bitmap.array();
        if (BufferUtil.isBackedBySimpleArray(answer.bitmap)
                && BufferUtil.isBackedBySimpleArray(value2.content)) {
            long[] ab = answer.bitmap.array();
            short[] v2 = value2.content.array();
            int c = value2.cardinality;
            for (int k = 0; k < c; ++k) {
                short v = v2[k];
                final int i = BufferUtil.toIntUnsigned(v) >>> 6;
                long w = ab[i];
                long aft = w | (1l << v);
                bitArray[i] = aft;
                if (USE_BRANCHLESS) {
                    answer.cardinality += (w - aft) >>> 63;
                } else {
                    if (w != aft)
                        answer.cardinality++;
                }
            }
        } else {
            int c = value2.cardinality;
            for (int k = 0; k < c; ++k) {
                short v2 = value2.content.get(k);
                final int i = BufferUtil.toIntUnsigned(v2) >>> 6;
                long w = answer.bitmap.get(i);
                long aft = w | (1l << v2);
                bitArray[i] = aft;
                if (USE_BRANCHLESS) {
                    answer.cardinality += (w - aft) >>> 63;
                } else {
                    if (w != aft)
                        answer.cardinality++;
                }
            }
        }
        return answer;
    }

    @Override
    public MappeableContainer or(final MappeableBitmapContainer value2) {
        if (USE_IN_PLACE) {
            final MappeableBitmapContainer value1 = this.clone();
            return value1.ior(value2);
        }
        final MappeableBitmapContainer answer = new MappeableBitmapContainer();
        if(!BufferUtil.isBackedBySimpleArray(answer.bitmap))
            throw new RuntimeException("Should not happen. Internal bug.");
        long[] bitArray = answer.bitmap.array();
        answer.cardinality = 0;
        if (BufferUtil.isBackedBySimpleArray(this.bitmap) && BufferUtil.isBackedBySimpleArray(value2.bitmap)) {
            long[] b = this.bitmap.array();
            long[] v2 = value2.bitmap.array();
            int len = this.bitmap.limit();
            for (int k = 0; k < len; ++k) {
                long w = b[k] | v2[k];
                bitArray[k] = w;
                answer.cardinality += Long.bitCount(w);
            }
        } else {
            int len = this.bitmap.limit();
            for (int k = 0; k < len; ++k) {
                long w = this.bitmap.get(k) | value2.bitmap.get(k);
                bitArray[k] = w;
                answer.cardinality += Long.bitCount(w);
            }
        }
        return answer;
    }

    @Override
    public MappeableContainer or(final MappeableRunContainer value2) {
        return value2.or(this);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        // little endian
        this.cardinality = 0;
        int len = this.bitmap.limit();
        if(BufferUtil.isBackedBySimpleArray(bitmap)) {
            long[] b = this.bitmap.array();
            for (int k = 0; k < len; ++k) {
                long w = Long.reverseBytes(in.readLong());
                b[k] = w;
                this.cardinality += Long.bitCount(w);
            }
        } else {
            for (int k = 0; k < len; ++k) {
                long w = Long.reverseBytes(in.readLong());
                bitmap.put(k,w);
                this.cardinality += Long.bitCount(w);
            }
        }
    }

    @Override
    public MappeableContainer remove(final short i) {
        final int x = BufferUtil.toIntUnsigned(i);
        long X = bitmap.get(x / 64);
        long mask = 1l << x;

        if (cardinality == MappeableArrayContainer.DEFAULT_MAX_SIZE + 1) {// this is
            // the
            // uncommon
            // path
            if ((X & mask) != 0) {
                --cardinality;
                bitmap.put(x / 64, X & (~mask));
                return this.toArrayContainer();
            }
        }
        long aft = X & ~(mask);
        cardinality -= (aft - X) >>> 63;
        bitmap.put(x / 64, aft);
        return this;
    }
    
    @Override
    public int serializedSizeInBytes() {
        return serializedSizeInBytes(0);
    }
    
    // the parameter is for overloading and symmetry with ArrayContainer
    protected static int serializedSizeInBytes(int unusedCardinality) {
        return MAX_CAPACITY / 8;
    }
    
    /**
     * Copies the data to an array container
     * 
     * @return the array container
     */
    public MappeableArrayContainer toArrayContainer() {
        final MappeableArrayContainer ac = new MappeableArrayContainer(
                cardinality);
        ac.loadData(this);
        return ac;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        final ShortIterator i = this.getShortIterator();
        sb.append("{");
        while (i.hasNext()) {
            sb.append(i);
            if (i.hasNext())
                sb.append(",");
        }
        sb.append("}");
        return sb.toString();
    }

    @Override
    public void trim() {
    }

    @Override
    protected void writeArray(DataOutput out) throws IOException {
        // little endian
        int len = this.bitmap.limit();
        if(BufferUtil.isBackedBySimpleArray(bitmap)) {
            long[] b = bitmap.array();
            for (int k = 0; k < len; ++k) {
                out.writeLong(Long.reverseBytes(b[k]));
            }    
        } else {
            for (int k = 0; k < len; ++k) {
                final long w = bitmap.get(k);
                out.writeLong(Long.reverseBytes(w));
            }
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        writeArray(out);
    }

    @Override
    public MappeableContainer xor(final MappeableArrayContainer value2) {
        final MappeableBitmapContainer answer = clone();
        if (!BufferUtil.isBackedBySimpleArray(answer.bitmap))
            throw new RuntimeException("Should not happen. Internal bug.");
        long[] bitArray = answer.bitmap.array();
        if (BufferUtil.isBackedBySimpleArray(value2.content)) {
            short[] v2 = value2.content.array();
            int c = value2.cardinality;
            for (int k = 0; k < c; ++k) {
                short vc = v2[k];
                long mask = 1l << vc;
                final int index = BufferUtil.toIntUnsigned(vc) >>> 6;
                long ba = bitArray[index];
                // TODO: check whether a branchy version could be faster
                answer.cardinality += 1 - 2 * ((ba & mask) >>> vc);
                bitArray[index] = ba ^ mask;
            }
        } else {
            int c = value2.cardinality;
            for (int k = 0; k < c; ++k) {
                short v2 = value2.content.get(k);
                long mask = 1l << v2;
                final int index = BufferUtil.toIntUnsigned(v2) >>> 6;
                long ba = bitArray[index];
                // TODO: check whether a branchy version could be faster
                answer.cardinality += 1 - 2 * ((ba & mask) >>> v2);
                bitArray[index] = ba ^ mask;
            }
        }
        if (answer.cardinality <= MappeableArrayContainer.DEFAULT_MAX_SIZE)
            return answer.toArrayContainer();
        return answer;
    }

    @Override
    public MappeableContainer xor(MappeableBitmapContainer value2) {
        int newCardinality = 0;
        if (BufferUtil.isBackedBySimpleArray(this.bitmap) && BufferUtil.isBackedBySimpleArray(value2.bitmap)) {
            long[] b = this.bitmap.array();
            long[] v2 = value2.bitmap.array();
            int len = this.bitmap.limit();
            for (int k = 0; k < len; ++k) {
                newCardinality += Long.bitCount(b[k] ^ v2[k]);
            }
        } else {
            int len = this.bitmap.limit();
            for (int k = 0; k < len; ++k) {
                newCardinality += Long.bitCount(this.bitmap.get(k)
                        ^ value2.bitmap.get(k));
            }
        }
        if (newCardinality > MappeableArrayContainer.DEFAULT_MAX_SIZE) {
            final MappeableBitmapContainer answer = new MappeableBitmapContainer();
            long[] bitArray = answer.bitmap.array();
            if (BufferUtil.isBackedBySimpleArray(this.bitmap) && BufferUtil.isBackedBySimpleArray(value2.bitmap)) {
                long[] b = this.bitmap.array();
                long[] v2 = value2.bitmap.array();
                int len = answer.bitmap.limit();
                for (int k = 0; k < len; ++k) {
                    bitArray[k] = b[k] ^ v2[k];
                }
            } else {
                int len = answer.bitmap.limit();
                for (int k = 0; k < len; ++k) {
                    bitArray[k] = this.bitmap.get(k) ^ value2.bitmap.get(k);
                }
            }
            answer.cardinality = newCardinality;
            return answer;
        }
        final MappeableArrayContainer ac = new MappeableArrayContainer(
                newCardinality);
        if (BufferUtil.isBackedBySimpleArray(this.bitmap) && BufferUtil.isBackedBySimpleArray(value2.bitmap))
            org.roaringbitmap.Util.fillArrayXOR(ac.content.array(),
                    this.bitmap.array(), value2.bitmap.array());
        else

            BufferUtil.fillArrayXOR(ac.content.array(), this.bitmap,
                    value2.bitmap);
        ac.cardinality = newCardinality;
        return ac;
    }
 


    @Override
    public MappeableContainer xor(final MappeableRunContainer value2) {
        return value2.xor(this);
    }



    protected MappeableContainer ilazyor(MappeableArrayContainer value2) {
        this.cardinality = -1;// invalid
        if(!BufferUtil.isBackedBySimpleArray(bitmap))
            throw new RuntimeException("Should not happen. Internal bug.");
        long[] b = this.bitmap.array();
        int c = value2.cardinality;
        for (int k = 0; k < c; ++k) {
            short v2 = value2.content.get(k);
            final int i = BufferUtil.toIntUnsigned(v2) >>> 6;
            b[i] |= (1l << v2);
        }
        return this;
    }

    protected MappeableContainer ilazyor(MappeableBitmapContainer x) {
        if (BufferUtil.isBackedBySimpleArray(x.bitmap)) {
            long[] b = this.bitmap.array();
            long[] b2 = x.bitmap.array();
            for (int k = 0; k < b.length; k++) {
                b[k] |= b2[k];
            }
        } else {
            final int m = this.bitmap.limit();
            for (int k = 0; k < m; k++) {
                this.bitmap.put(k, this.bitmap.get(k) | x.bitmap.get(k));
            }
        }
        this.cardinality = -1;// invalid
        return this;
    }
    

  



    protected MappeableContainer lazyor(MappeableArrayContainer value2) {
        MappeableBitmapContainer answer = clone();
        answer.cardinality = -1;// invalid
        if(!BufferUtil.isBackedBySimpleArray(answer.bitmap))
            throw new RuntimeException("Should not happen. Internal bug.");
        long[] b = answer.bitmap.array();
        int c = value2.cardinality;
        for (int k = 0; k < c; ++k) {
            short v2 = value2.content.get(k);
            final int i = BufferUtil.toIntUnsigned(v2) >>> 6;
            b[i] |=  1l << v2;
        }
        return answer;
    }

    protected MappeableContainer lazyor(MappeableBitmapContainer x) {
        MappeableBitmapContainer answer = new MappeableBitmapContainer();
        answer.cardinality = -1;// invalid
        if(!BufferUtil.isBackedBySimpleArray(answer.bitmap))
            throw new RuntimeException("Should not happen. Internal bug.");
        long[] b = answer.bitmap.array();
        for (int k = 0; k < b.length; k++) {
            b[k] = this.bitmap.get(k) | x.bitmap.get(k);
        }
        return answer;
    }    


    protected MappeableContainer lazyor(MappeableRunContainer x) {
        MappeableBitmapContainer bc = clone();
        bc.cardinality = -1;
        if(BufferUtil.isBackedBySimpleArray(bc.bitmap)) {
            long[] b = bc.bitmap.array();
            for(int rlepos = 0; rlepos < x.nbrruns; ++rlepos ) {
                int start = BufferUtil.toIntUnsigned(x.getValue(rlepos));
                int end = start + BufferUtil.toIntUnsigned(x.getLength(rlepos)) + 1;
                Util.setBitmapRange(b, start, end);
            }
            return bc;

        }
        for(int rlepos = 0; rlepos < x.nbrruns; ++rlepos ) {
            int start = BufferUtil.toIntUnsigned(x.getValue(rlepos));
            int end = start + BufferUtil.toIntUnsigned(x.getLength(rlepos)) + 1;
            BufferUtil.setBitmapRange(bc.bitmap, start, end);
        }
        return bc;
    }

    protected MappeableContainer ilazyor(MappeableRunContainer x) {
        for(int rlepos = 0; rlepos < x.nbrruns; ++rlepos ) {
            int start = BufferUtil.toIntUnsigned(x.getValue(rlepos));
            int end = start + BufferUtil.toIntUnsigned(x.getLength(rlepos)) + 1;
            BufferUtil.setBitmapRange(this.bitmap, start, end);
        }
        this.cardinality = -1;
        return this;
    }



    /**
     * Recomputes the cardinality of the bitmap.
     */    
    protected void computeCardinality() {
        this.cardinality = 0;
        if (BufferUtil.isBackedBySimpleArray(bitmap)) {
            long[] b = this.bitmap.array();
            for (int k = 0; k < b.length; k++) {
                this.cardinality += Long.bitCount(b[k]);
            }
        } else {
            int m = this.bitmap.limit();
            for (int k = 0; k < m; k++) {
                this.cardinality += Long.bitCount(this.bitmap.get(k));
            }
        }
    }


    @Override
    public int rank(short lowbits) {
        int x = BufferUtil.toIntUnsigned(lowbits);
        int leftover = (x + 1) & 63;
        int answer = 0;
        if (BufferUtil.isBackedBySimpleArray(this.bitmap)) {
            long[] b = this.bitmap.array();
            for (int k = 0; k < (x + 1) / 64; ++k)
                answer += Long.bitCount(b[k]);
            if (leftover != 0) {
                answer += Long.bitCount(b[(x + 1) / 64] << (64 - leftover));
            }
        } else {
            for (int k = 0; k < (x + 1) / 64; ++k)
                answer += Long.bitCount(bitmap.get(k));
            if (leftover != 0) {
                answer += Long
                        .bitCount(bitmap.get((x + 1) / 64) << (64 - leftover));
            }
        }
        return answer;
    }

    @Override
    public short select(int j) {
        int leftover = j;
        if (BufferUtil.isBackedBySimpleArray(this.bitmap)) {
            long[] b = this.bitmap.array();

            for (int k = 0; k < b.length; ++k) {
                int w = Long.bitCount(b[k]);
                if (w > leftover) {
                    return (short) (k * 64 + Util.select(b[k], leftover));
                }
                leftover -= w;
            }
        } else {
            int len = this.bitmap.limit();
            for (int k = 0; k < len; ++k) {
                long X = bitmap.get(k);
                int w = Long.bitCount(X);
                if (w > leftover) {
                    return (short) (k * 64 + Util.select(X,
                            leftover));
                }
                leftover -= w;
            }
        }
        throw new IllegalArgumentException("Insufficient cardinality.");
    }

    @Override
    public MappeableContainer limit(int maxcardinality) {
        if(maxcardinality >= this.cardinality) {
            return clone();
        } 
        if(maxcardinality <= MappeableArrayContainer.DEFAULT_MAX_SIZE) {
            MappeableArrayContainer ac = new MappeableArrayContainer(maxcardinality);
            int pos = 0;
            if(!BufferUtil.isBackedBySimpleArray(ac.content))
                throw new RuntimeException("Should not happen. Internal bug.");
            short[] cont = ac.content.array();
            int len = this.bitmap.limit();
            for (int k = 0; (ac.cardinality <maxcardinality) && (k < len); ++k) {
                long bitset = bitmap.get(k);
                while ((ac.cardinality <maxcardinality) && ( bitset != 0)) {
                    long t = bitset & -bitset;
                    cont[pos++] = (short) (k * 64 + Long
                            .bitCount(t - 1));
                    ac.cardinality++;
                    bitset ^= t;
                }
            }
            return ac;
        } 
        MappeableBitmapContainer bc = new MappeableBitmapContainer(maxcardinality,this.bitmap);
        int s = BufferUtil.toIntUnsigned(select(maxcardinality));
        int usedwords = (s+63)/64;
        int len = this.bitmap.limit();
        int todelete = len - usedwords;
        for(int k = 0; k<todelete; ++k)
            bc.bitmap.put(len-1-k, 0);
        int lastword = s % 64; 
        if(lastword != 0) {
            bc.bitmap.put(s/64,  (bc.bitmap.get(s/64) << (64-lastword)) >> (64-lastword));
        }
        return bc;
    }

    @Override
    public MappeableContainer flip(short i) {
        final int x = BufferUtil.toIntUnsigned(i);
        final long bef = bitmap.get(x / 64);
        final long mask =  (1l << x);
        if (cardinality == MappeableArrayContainer.DEFAULT_MAX_SIZE + 1) {// this
                                                                          // is
            // the
            // uncommon
            // path
            if ((bef & mask) != 0) {
                --cardinality;
                bitmap.put(x / 64, bef & ~mask);
                return this.toArrayContainer();
            }
        }
        long aft = bef ^ mask;;
        //  TODO: check whether a branchy version could be faster
        cardinality += 1 - 2 * ((bef & mask) >>> x);
        bitmap.put(x / 64, aft);
        return this;
    }

    @Override
    public MappeableContainer iadd(int begin, int end) {
        // TODO: may need to convert to a RunContainer
        BufferUtil.setBitmapRange(bitmap, begin, end);
        computeCardinality();
        return this;
    }

    @Override
    public MappeableContainer iremove(int begin, int end) {
        BufferUtil.resetBitmapRange(bitmap, begin, end);
        computeCardinality();
        if (getCardinality() < MappeableArrayContainer.DEFAULT_MAX_SIZE)
            return toArrayContainer();
        return this;
    }

    @Override
    public MappeableContainer add(int begin, int end) {
        // TODO: may need to convert to a RunContainer
        MappeableBitmapContainer answer = clone();
        BufferUtil.setBitmapRange(answer.bitmap, begin, end);
        answer.computeCardinality();
        return answer;
    }

    @Override
    public MappeableContainer remove(int begin, int end) {
        MappeableBitmapContainer answer = clone();
        BufferUtil.resetBitmapRange(answer.bitmap, begin, end);
        answer.computeCardinality();
        if (answer.getCardinality() < MappeableArrayContainer.DEFAULT_MAX_SIZE)
            return answer.toArrayContainer();
        return answer;
    }

    @Override
    public MappeableContainer repairAfterLazy() {
        if(getCardinality() < 0)
            computeCardinality();
        return this;
    }

    @Override
    public boolean intersects(MappeableArrayContainer value2) {
        if (BufferUtil.isBackedBySimpleArray(value2.content)) {
            short[] c = value2.content.array();
            int ca = value2.cardinality;
            for (int k = 0; k < ca; ++k)
                if (this.contains(c[k]))
                    return true;

        } else {
            int ca = value2.cardinality;
            for (int k = 0; k < ca; ++k) {
                short v = value2.content.get(k);
                if (this.contains(v))
                    return true;
            }
        }
        return false;
    }

    @Override
    public boolean intersects(MappeableBitmapContainer value2) {
        if (BufferUtil.isBackedBySimpleArray(this.bitmap)
                && BufferUtil.isBackedBySimpleArray(value2.bitmap)) {
            long[] tb = this.bitmap.array();
            long[] v2b = value2.bitmap.array();
            int len = this.bitmap.limit();
            for (int k = 0; k < len; ++k) {
                if ((tb[k] & v2b[k]) != 0)
                    return true;
            }
        } else {
            int len = this.bitmap.limit();
            for (int k = 0; k < len; ++k) {
                if ((this.bitmap.get(k) & value2.bitmap.get(k)) != 0)
                    return true;
            }
        }
        return false;
    }

    @Override
    public boolean intersects(MappeableRunContainer x) {
        return x.intersects(this);
    }
    
    // optimization flag: whether the cardinality of the bitmaps is maintained through branchless operations
    public static final boolean USE_BRANCHLESS = true;

}

final class MappeableBitmapContainerShortIterator implements ShortIterator {
    long w; 
    int x; 
    final static int len = MappeableBitmapContainer.MAX_CAPACITY / 64;//hard coded for speed
    
    
    MappeableBitmapContainer parent;

    MappeableBitmapContainerShortIterator() {
    }

    MappeableBitmapContainerShortIterator(MappeableBitmapContainer p) {
        wrap(p);
    }

    void wrap(MappeableBitmapContainer p) {
        parent = p;
        for(x=0; x < len;++x)
            if((w=parent.bitmap.get(x))!=0) break;
    }

    @Override
    public boolean hasNext() {
        return x < len;
    }

    @Override
    public short next() {
        long t = w & -w;
        short answer  = (short) (x * 64 + Long
                .bitCount(t - 1));
        w ^= t;
        while (w == 0) {
            ++x;
            if(x == len) break;
            w = parent.bitmap.get(x);
        }
        return answer;
    }

    @Override
    public int nextAsInt() {
        long t = w & -w;
        int answer  = x * 64 + Long
                .bitCount(t - 1);
        w ^= t;
        while (w == 0) {
            ++x;
            if(x == len) break;
            w = parent.bitmap.get(x);
        }
        return answer;
    }
    
    @Override
    public ShortIterator clone() {
        try {
            return (ShortIterator) super.clone();
        } catch (CloneNotSupportedException e) {
            return null;// will not happen
        }
    }

    @Override
    public void remove() {
        //TODO: implement
        throw new RuntimeException("unsupported operation: remove");
    }
}

final class ReverseMappeableBitmapContainerShortIterator implements ShortIterator {

    long w;
    int x;
    final static int len = MappeableBitmapContainer.MAX_CAPACITY / 64;//hard coded for speed

    MappeableBitmapContainer parent;
    
    ReverseMappeableBitmapContainerShortIterator() {
    }
    
    ReverseMappeableBitmapContainerShortIterator(MappeableBitmapContainer p) {
        wrap(p);
    }

    public void wrap(MappeableBitmapContainer p) {
        parent = p;
        for(x=len-1; x>=0; --x)
            if((w=Long.reverse(parent.bitmap.get(x)))!=0) break;
    }

    @Override
    public boolean hasNext() {
        return x >= 0;
    }

    @Override
    public short next() {
        long t = w & -w;
        short answer  = (short) ((x+1) * 64 - 1 - Long.bitCount(t - 1));
        w ^= t;
        while (w == 0) {
            --x;
            if(x < 0) break;
            w = Long.reverse(parent.bitmap.get(x));
        }
        return answer;
    }

    @Override
    public int nextAsInt() {
        long t = w & -w;
        int answer  = (x+1) * 64 - 1 - Long.bitCount(t - 1);
        w ^= t;
        while (w == 0) {
            --x;
            if(x < 0) break;
            w = Long.reverse(parent.bitmap.get(x));
        }
        return answer;
    }
    
    @Override
    public ShortIterator clone() {
        try {
            return (ShortIterator) super.clone();
        } catch (CloneNotSupportedException e) {
            return null;
        }
    }

    @Override
    public void remove() {
        //TODO: implement
        throw new RuntimeException("unsupported operation: remove");
    }
}
