/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */
package org.roaringbitmap.buffer;


import org.roaringbitmap.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ShortBuffer;
import java.util.Arrays;
import java.util.Iterator;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.roaringbitmap.buffer.BufferUtil.toIntUnsigned;
import static org.roaringbitmap.buffer.MappeableBitmapContainer.MAX_CAPACITY;

/**
 * This container takes the form of runs of consecutive values (effectively, run-length encoding).
 * Uses a ShortBuffer to store data, unlike org.roaringbitmap.RunContainer. Otherwise similar.
 *
 *
 * Adding and removing content from this container might make it wasteful so regular calls to
 * "runOptimize" might be warranted.
 */
public final class MappeableRunContainer extends MappeableContainer implements Cloneable {
  private static final int DEFAULT_INIT_SIZE = 4;
  private static final long serialVersionUID = 1L;

  private static int branchyBufferedUnsignedInterleavedBinarySearch(final ShortBuffer sb,
      final int begin, final int end, final short k) {
    int ikey = toIntUnsigned(k);
    int low = begin;
    int high = end - 1;
    while (low <= high) {
      final int middleIndex = (low + high) >>> 1;
      final int middleValue = toIntUnsigned(sb.get(2 * middleIndex));
      if (middleValue < ikey) {
        low = middleIndex + 1;
      } else if (middleValue > ikey) {
        high = middleIndex - 1;
      } else {
        return middleIndex;
      }
    }
    return -(low + 1);
  }

  private static int branchyBufferedUnsignedInterleavedBinarySearch(final ByteBuffer sb,
      int position, final int begin, final int end, final short k) {
    int ikey = toIntUnsigned(k);
    int low = begin;
    int high = end - 1;
    while (low <= high) {
      final int middleIndex = (low + high) >>> 1;
      final int middleValue = toIntUnsigned(sb.getShort(position + 2 * middleIndex * 2));
      if (middleValue < ikey) {
        low = middleIndex + 1;
      } else if (middleValue > ikey) {
        high = middleIndex - 1;
      } else {
        return middleIndex;
      }
    }
    return -(low + 1);
  }

  private static int bufferedUnsignedInterleavedBinarySearch(final ShortBuffer sb, final int begin,
      final int end, final short k) {
    return branchyBufferedUnsignedInterleavedBinarySearch(sb, begin, end, k);
  }


  private static int bufferedUnsignedInterleavedBinarySearch(final ByteBuffer sb, int position,
      final int begin, final int end, final short k) {
    return branchyBufferedUnsignedInterleavedBinarySearch(sb, position, begin, end, k);
  }

  protected static int getArraySizeInBytes(int nbrruns) {
    return 2 + 4 * nbrruns;
  }

  static short getLength(short[] vl, int index) {
    return vl[2 * index + 1];
  }


  static short getValue(short[] vl, int index) {
    return vl[2 * index];
  }

  protected static int serializedSizeInBytes(int numberOfRuns) {
    return 2 + 2 * 2 * numberOfRuns; // each run requires 2 2-byte entries.
  }

  protected ShortBuffer valueslength;

  protected int nbrruns = 0;// how many runs, this number should fit in 16 bits.


  /**
   * Create a container with default capacity
   */
  public MappeableRunContainer() {
    this(DEFAULT_INIT_SIZE);
  }

  /**
   * Create an array container with specified capacity
   *
   * @param capacity The capacity of the container
   */
  public MappeableRunContainer(final int capacity) {
    valueslength = ShortBuffer.allocate(2 * capacity);
  }


  private MappeableRunContainer(int nbrruns, final ShortBuffer valueslength) {
    this.nbrruns = nbrruns;
    ShortBuffer tmp = valueslength.duplicate();// for thread safety
    this.valueslength = ShortBuffer.allocate(Math.max(2 * nbrruns, tmp.limit()));
    tmp.rewind();
    this.valueslength.put(tmp); // may copy more than it needs to??
  }


  protected MappeableRunContainer(MappeableArrayContainer arr, int nbrRuns) {
    this.nbrruns = nbrRuns;
    valueslength = ShortBuffer.allocate(2 * nbrRuns);
    short[] vl = valueslength.array();
    if (nbrRuns == 0) {
      return;
    }

    int prevVal = -2;
    int runLen = 0;
    int runCount = 0;
    if (BufferUtil.isBackedBySimpleArray(arr.content)) {
      short[] a = arr.content.array();
      for (int i = 0; i < arr.cardinality; i++) {
        int curVal = toIntUnsigned(a[i]);
        if (curVal == prevVal + 1) {
          ++runLen;
        } else {
          if (runCount > 0) {
            vl[2 * (runCount - 1) + 1] = (short) runLen;
          }
          // setLength(runCount - 1, (short) runLen);
          vl[2 * runCount] = (short) curVal;
          // setValue(runCount, (short) curVal);
          runLen = 0;
          ++runCount;
        }
        prevVal = curVal;
      }

    } else {
      for (int i = 0; i < arr.cardinality; i++) {
        int curVal = toIntUnsigned(arr.content.get(i));
        if (curVal == prevVal + 1) {
          ++runLen;
        } else {
          if (runCount > 0) {
            vl[2 * (runCount - 1) + 1] = (short) runLen;
          }
          // setLength(runCount - 1, (short) runLen);
          vl[2 * runCount] = (short) curVal;
          // setValue(runCount, (short) curVal);
          runLen = 0;
          ++runCount;
        }
        prevVal = curVal;
      }
    }
    // setLength(runCount-1, (short) runLen);
    vl[2 * (runCount - 1) + 1] = (short) runLen;
  }

  /**
   * Create an run container with a run of ones from firstOfRun to lastOfRun.
   *
   * @param firstOfRun first index
   * @param lastOfRun last index (range is exclusive)
   */
  public MappeableRunContainer(final int firstOfRun, final int lastOfRun) {
    this.nbrruns = 1;
    short[] vl = {(short) firstOfRun, (short) (lastOfRun - 1 - firstOfRun)};
    this.valueslength = ShortBuffer.wrap(vl);
  }

  // convert a bitmap container to a run container somewhat efficiently.
  protected MappeableRunContainer(MappeableBitmapContainer bc, int nbrRuns) {
    this.nbrruns = nbrRuns;
    valueslength = ShortBuffer.allocate(2 * nbrRuns);
    if (!BufferUtil.isBackedBySimpleArray(valueslength)) {
      throw new RuntimeException("Unexpected internal error.");
    }
    short[] vl = valueslength.array();
    if (nbrRuns == 0) {
      return;
    }
    if (bc.isArrayBacked()) {
      long[] b = bc.bitmap.array();
      int longCtr = 0; // index of current long in bitmap
      long curWord = b[0]; // its value
      int runCount = 0;
      final int len = bc.bitmap.limit();
      while (true) {
        // potentially multiword advance to first 1 bit
        while (curWord == 0L && longCtr < len - 1) {
          curWord = b[++longCtr];
        }

        if (curWord == 0L) {
          // wrap up, no more runs
          return;
        }
        int localRunStart = Long.numberOfTrailingZeros(curWord);
        int runStart = localRunStart + 64 * longCtr;
        // stuff 1s into number's LSBs
        long curWordWith1s = curWord | (curWord - 1);

        // find the next 0, potentially in a later word
        int runEnd = 0;
        while (curWordWith1s == -1L && longCtr < len - 1) {
          curWordWith1s = b[++longCtr];
        }

        if (curWordWith1s == -1L) {
          // a final unterminated run of 1s (32 of them)
          runEnd = 64 + longCtr * 64;
          // setValue(runCount, (short) runStart);
          vl[2 * runCount] = (short) runStart;
          // setLength(runCount, (short) (runEnd-runStart-1));
          vl[2 * runCount + 1] = (short) (runEnd - runStart - 1);
          return;
        }
        int localRunEnd = Long.numberOfTrailingZeros(~curWordWith1s);
        runEnd = localRunEnd + longCtr * 64;
        // setValue(runCount, (short) runStart);
        vl[2 * runCount] = (short) runStart;
        // setLength(runCount, (short) (runEnd-runStart-1));
        vl[2 * runCount + 1] = (short) (runEnd - runStart - 1);
        runCount++;
        // now, zero out everything right of runEnd.
        curWord = curWordWith1s & (curWordWith1s + 1);
        // We've lathered and rinsed, so repeat...
      }
    } else {
      int longCtr = 0; // index of current long in bitmap
      long curWord = bc.bitmap.get(0); // its value
      int runCount = 0;
      final int len = bc.bitmap.limit();
      while (true) {
        // potentially multiword advance to first 1 bit
        while (curWord == 0L && longCtr < len - 1) {
          curWord = bc.bitmap.get(++longCtr);
        }

        if (curWord == 0L) {
          // wrap up, no more runs
          return;
        }
        int localRunStart = Long.numberOfTrailingZeros(curWord);
        int runStart = localRunStart + 64 * longCtr;
        // stuff 1s into number's LSBs
        long curWordWith1s = curWord | (curWord - 1);

        // find the next 0, potentially in a later word
        int runEnd = 0;
        while (curWordWith1s == -1L && longCtr < len - 1) {
          curWordWith1s = bc.bitmap.get(++longCtr);
        }

        if (curWordWith1s == -1L) {
          // a final unterminated run of 1s (32 of them)
          runEnd = 64 + longCtr * 64;
          // setValue(runCount, (short) runStart);
          vl[2 * runCount] = (short) runStart;
          // setLength(runCount, (short) (runEnd-runStart-1));
          vl[2 * runCount + 1] = (short) (runEnd - runStart - 1);
          return;
        }
        int localRunEnd = Long.numberOfTrailingZeros(~curWordWith1s);
        runEnd = localRunEnd + longCtr * 64;
        // setValue(runCount, (short) runStart);
        vl[2 * runCount] = (short) runStart;
        // setLength(runCount, (short) (runEnd-runStart-1));
        vl[2 * runCount + 1] = (short) (runEnd - runStart - 1);
        runCount++;
        // now, zero out everything right of runEnd.

        curWord = curWordWith1s & (curWordWith1s + 1);
        // We've lathered and rinsed, so repeat...
      }

    }
  }

  /**
   * Creates a new container from a non-mappeable one. This copies the data.
   *
   * @param bc the original container
   */
  public MappeableRunContainer(RunContainer bc) {
    this.nbrruns = bc.numberOfRuns();
    this.valueslength = bc.toShortBuffer();
  }

  /**
   * Construct a new RunContainer backed by the provided ShortBuffer. Note that if you modify the
   * RunContainer a new ShortBuffer may be produced.
   *
   * @param array ShortBuffer where the data is stored
   * @param numRuns number of runs (each using 2 shorts in the buffer)
   *
   */
  public MappeableRunContainer(final ShortBuffer array, final int numRuns) {
    if (array.limit() < 2 * numRuns) {
      throw new RuntimeException("Mismatch between buffer and numRuns");
    }
    this.nbrruns = numRuns;
    this.valueslength = array;
  }

  @Override
  public MappeableContainer add(int begin, int end) {
    MappeableRunContainer rc = (MappeableRunContainer) clone();
    return rc.iadd(begin, end);
  }

  @Override
  // not thread-safe
  public MappeableContainer add(short k) {
    // TODO: it might be better and simpler to do return
    // toBitmapOrArrayContainer(getCardinality()).add(k)
    int index = bufferedUnsignedInterleavedBinarySearch(valueslength, 0, nbrruns, k);
    if (index >= 0) {
      return this;// already there
    }
    index = -index - 2;// points to preceding value, possibly -1
    if (index >= 0) {// possible match
      int offset = toIntUnsigned(k) - toIntUnsigned(getValue(index));
      int le = toIntUnsigned(getLength(index));
      if (offset <= le) {
        return this;
      }
      if (offset == le + 1) {
        // we may need to fuse
        if (index + 1 < nbrruns) {
          if (toIntUnsigned(getValue(index + 1)) == toIntUnsigned(k) + 1) {
            // indeed fusion is needed
            setLength(index,
                (short) (getValue(index + 1) + getLength(index + 1) - getValue(index)));
            recoverRoomAtIndex(index + 1);
            return this;
          }
        }
        incrementLength(index);
        return this;
      }
      if (index + 1 < nbrruns) {
        // we may need to fuse
        if (toIntUnsigned(getValue(index + 1)) == toIntUnsigned(k) + 1) {
          // indeed fusion is needed
          setValue(index + 1, k);
          setLength(index + 1, (short) (getLength(index + 1) + 1));
          return this;
        }
      }
    }
    if (index == -1) {
      // we may need to extend the first run
      if (0 < nbrruns) {
        if (getValue(0) == k + 1) {
          incrementLength(0);
          decrementValue(0);
          return this;
        }
      }
    }
    makeRoomAtIndex(index + 1);
    setValue(index + 1, k);
    setLength(index + 1, (short) 0);
    return this;
  }

  @Override
  public boolean isEmpty() {
    return nbrruns == 0;
  }


  @Override
  public MappeableContainer and(MappeableArrayContainer x) {
    MappeableArrayContainer ac = new MappeableArrayContainer(x.cardinality);
    if (this.nbrruns == 0) {
      return ac;
    }
    int rlepos = 0;
    int arraypos = 0;

    int rleval = toIntUnsigned(this.getValue(rlepos));
    int rlelength = toIntUnsigned(this.getLength(rlepos));
    while (arraypos < x.cardinality) {
      int arrayval = toIntUnsigned(x.content.get(arraypos));
      while (rleval + rlelength < arrayval) {// this will frequently be false
        ++rlepos;
        if (rlepos == this.nbrruns) {
          return ac;// we are done
        }
        rleval = toIntUnsigned(this.getValue(rlepos));
        rlelength = toIntUnsigned(this.getLength(rlepos));
      }
      if (rleval > arrayval) {
        arraypos =
            BufferUtil.advanceUntil(x.content, arraypos, x.cardinality, (short)rleval);
      } else {
        ac.content.put(ac.cardinality, (short) arrayval);
        ac.cardinality++;
        arraypos++;
      }
    }
    return ac;
  }

  @Override
  public MappeableContainer and(MappeableBitmapContainer x) {
    int card = this.getCardinality();
    if (card <= MappeableArrayContainer.DEFAULT_MAX_SIZE) {
      // result can only be an array (assuming that we never make a RunContainer)
      if (card > x.cardinality) {
        card = x.cardinality;
      }
      MappeableArrayContainer answer = new MappeableArrayContainer(card);
      answer.cardinality = 0;
      for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
        int runStart = toIntUnsigned(this.getValue(rlepos));
        int runEnd = runStart + toIntUnsigned(this.getLength(rlepos));
        for (int runValue = runStart; runValue <= runEnd; ++runValue) {
          if (x.contains((short) runValue)) {
            answer.content.put(answer.cardinality++, (short) runValue);
          }
        }
      }
      return answer;
    }
    // we expect the answer to be a bitmap (if we are lucky)

    MappeableBitmapContainer answer = x.clone();
    int start = 0;
    for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
      int end = toIntUnsigned(this.getValue(rlepos));
      int prevOnes = answer.cardinalityInRange(start, end);
      BufferUtil.resetBitmapRange(answer.bitmap, start, end);
      answer.updateCardinality(prevOnes, 0);
      start = end + toIntUnsigned(this.getLength(rlepos)) + 1;
    }
    int ones = answer.cardinalityInRange(start, MAX_CAPACITY);
    BufferUtil.resetBitmapRange(answer.bitmap, start, MAX_CAPACITY);
    answer.updateCardinality(ones, 0);
    if (answer.getCardinality() > MappeableArrayContainer.DEFAULT_MAX_SIZE) {
      return answer;
    } else {
      return answer.toArrayContainer();
    }

  }

  @Override
  public MappeableContainer and(MappeableRunContainer x) {
    MappeableRunContainer answer =
        new MappeableRunContainer(ShortBuffer.allocate(2 * (this.nbrruns + x.nbrruns)), 0);
    short[] vl = answer.valueslength.array();
    int rlepos = 0;
    int xrlepos = 0;
    int start = toIntUnsigned(this.getValue(rlepos));
    int end = start + toIntUnsigned(this.getLength(rlepos)) + 1;
    int xstart = toIntUnsigned(x.getValue(xrlepos));
    int xend = xstart + toIntUnsigned(x.getLength(xrlepos)) + 1;
    while ((rlepos < this.nbrruns) && (xrlepos < x.nbrruns)) {
      if (end <= xstart) {
        // exit the first run
        rlepos++;
        if (rlepos < this.nbrruns) {
          start = toIntUnsigned(this.getValue(rlepos));
          end = start + toIntUnsigned(this.getLength(rlepos)) + 1;
        }
      } else if (xend <= start) {
        // exit the second run
        xrlepos++;
        if (xrlepos < x.nbrruns) {
          xstart = toIntUnsigned(x.getValue(xrlepos));
          xend = xstart + toIntUnsigned(x.getLength(xrlepos)) + 1;
        }
      } else {// they overlap
        final int lateststart = start > xstart ? start : xstart;
        int earliestend;
        if (end == xend) {// improbable
          earliestend = end;
          rlepos++;
          xrlepos++;
          if (rlepos < this.nbrruns) {
            start = toIntUnsigned(this.getValue(rlepos));
            end = start + toIntUnsigned(this.getLength(rlepos)) + 1;
          }
          if (xrlepos < x.nbrruns) {
            xstart = toIntUnsigned(x.getValue(xrlepos));
            xend = xstart + toIntUnsigned(x.getLength(xrlepos)) + 1;
          }
        } else if (end < xend) {
          earliestend = end;
          rlepos++;
          if (rlepos < this.nbrruns) {
            start = toIntUnsigned(this.getValue(rlepos));
            end = start + toIntUnsigned(this.getLength(rlepos)) + 1;
          }

        } else {// end > xend
          earliestend = xend;
          xrlepos++;
          if (xrlepos < x.nbrruns) {
            xstart = toIntUnsigned(x.getValue(xrlepos));
            xend = xstart + toIntUnsigned(x.getLength(xrlepos)) + 1;
          }
        }
        vl[2 * answer.nbrruns] = (short) lateststart;
        vl[2 * answer.nbrruns + 1] = (short) (earliestend - lateststart - 1);
        answer.nbrruns++;
      }
    }
    return answer;
  }

  @Override
  public MappeableContainer andNot(MappeableArrayContainer x) {
    // when x is small, we guess that the result will still be a run container
    final int arbitrary_threshold = 32; // this is arbitrary
    if (x.getCardinality() < arbitrary_threshold) {
      return lazyandNot(x).toEfficientContainer();
    }
    // otherwise we generate either an array or bitmap container
    final int card = getCardinality();
    if (card <= MappeableArrayContainer.DEFAULT_MAX_SIZE) {
      // if the cardinality is small, we construct the solution in place
      MappeableArrayContainer ac = new MappeableArrayContainer(card);
      ac.cardinality = org.roaringbitmap.Util.unsignedDifference(this.getShortIterator(),
          x.getShortIterator(), ac.content.array());
      return ac;
    }
    // otherwise, we generate a bitmap
    return toBitmapOrArrayContainer(card).iandNot(x);
  }

  @Override
  public MappeableContainer andNot(MappeableBitmapContainer x) {
    int card = this.getCardinality();
    if (card <= MappeableArrayContainer.DEFAULT_MAX_SIZE) {
      // result can only be an array (assuming that we never make a RunContainer)
      MappeableArrayContainer answer = new MappeableArrayContainer(card);
      answer.cardinality = 0;
      for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
        int runStart = toIntUnsigned(this.getValue(rlepos));
        int runEnd = runStart + toIntUnsigned(this.getLength(rlepos));
        for (int runValue = runStart; runValue <= runEnd; ++runValue) {
          if (!x.contains((short) runValue)) {
            answer.content.put(answer.cardinality++, (short) runValue);
          }
        }
      }
      return answer;
    }
    // we expect the answer to be a bitmap (if we are lucky)
    MappeableBitmapContainer answer = x.clone();
    int lastPos = 0;
    for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
      int start = toIntUnsigned(this.getValue(rlepos));
      int end = start + toIntUnsigned(this.getLength(rlepos)) + 1;
      int prevOnes = answer.cardinalityInRange(lastPos, start);
      int flippedOnes = answer.cardinalityInRange(start, end);
      BufferUtil.resetBitmapRange(answer.bitmap, lastPos, start);
      BufferUtil.flipBitmapRange(answer.bitmap, start, end);
      answer.updateCardinality(prevOnes + flippedOnes, end - start - flippedOnes);
      lastPos = end;
    }
    int ones = answer.cardinalityInRange(lastPos, MAX_CAPACITY);
    BufferUtil.resetBitmapRange(answer.bitmap, lastPos, answer.bitmap.capacity() * 64);
    answer.updateCardinality(ones, 0);
    if (answer.getCardinality() > MappeableArrayContainer.DEFAULT_MAX_SIZE) {
      return answer;
    } else {
      return answer.toArrayContainer();
    }
  }

  @Override
  public MappeableContainer andNot(MappeableRunContainer x) {
    MappeableRunContainer answer =
        new MappeableRunContainer(ShortBuffer.allocate(2 * (this.nbrruns + x.nbrruns)), 0);
    short[] vl = answer.valueslength.array();
    int rlepos = 0;
    int xrlepos = 0;
    int start = toIntUnsigned(this.getValue(rlepos));
    int end = start + toIntUnsigned(this.getLength(rlepos)) + 1;
    int xstart = toIntUnsigned(x.getValue(xrlepos));
    int xend = xstart + toIntUnsigned(x.getLength(xrlepos)) + 1;
    while ((rlepos < this.nbrruns) && (xrlepos < x.nbrruns)) {
      if (end <= xstart) {
        // output the first run
        vl[2 * answer.nbrruns] = (short) start;
        vl[2 * answer.nbrruns + 1] = (short) (end - start - 1);
        answer.nbrruns++;
        rlepos++;
        if (rlepos < this.nbrruns) {
          start = toIntUnsigned(this.getValue(rlepos));
          end = start + toIntUnsigned(this.getLength(rlepos)) + 1;
        }
      } else if (xend <= start) {
        // exit the second run
        xrlepos++;
        if (xrlepos < x.nbrruns) {
          xstart = toIntUnsigned(x.getValue(xrlepos));
          xend = xstart + toIntUnsigned(x.getLength(xrlepos)) + 1;
        }
      } else {
        if (start < xstart) {
          vl[2 * answer.nbrruns] = (short) start;
          vl[2 * answer.nbrruns + 1] = (short) (xstart - start - 1);
          answer.nbrruns++;
        }
        if (xend < end) {
          start = xend;
        } else {
          rlepos++;
          if (rlepos < this.nbrruns) {
            start = toIntUnsigned(this.getValue(rlepos));
            end = start + toIntUnsigned(this.getLength(rlepos)) + 1;
          }
        }
      }
    }
    if (rlepos < this.nbrruns) {
      vl[2 * answer.nbrruns] = (short) start;
      vl[2 * answer.nbrruns + 1] = (short) (end - start - 1);
      answer.nbrruns++;
      rlepos++;
      for (; rlepos < this.nbrruns; ++rlepos) {
        vl[2 * answer.nbrruns] = this.valueslength.get(2 * rlepos);
        vl[2 * answer.nbrruns + 1] = this.valueslength.get(2 * rlepos + 1);
        answer.nbrruns++;
      }
      // next bit would be faster but not thread-safe because of the "position"
      // if(rlepos < this.nbrruns) {
      // this.valueslength.position(2 * rlepos);
      // this.valueslength.get(vl, 2 * answer.nbrruns, 2*(this.nbrruns-rlepos ));
      // answer.nbrruns = answer.nbrruns + this.nbrruns - rlepos;
      // }
    }
    return answer;
  }

  // Append a value length with all values until a given value
  private void appendValueLength(int value, int index) {
    int previousValue = toIntUnsigned(getValue(index));
    int length = toIntUnsigned(getLength(index));
    int offset = value - previousValue;
    if (offset > length) {
      setLength(index, (short) offset);
    }
  }


  // To check if a value length can be prepended with a given value
  private boolean canPrependValueLength(int value, int index) {
    if (index < this.nbrruns) {
      int nextValue = toIntUnsigned(getValue(index));
      if (nextValue == value + 1) {
        return true;
      }
    }
    return false;
  }



  @Override
  public void clear() {
    nbrruns = 0;
  }


  @Override
  public MappeableContainer clone() {
    return new MappeableRunContainer(nbrruns, valueslength);
  }



  // To set the last value of a value length
  private void closeValueLength(int value, int index) {
    int initialValue = toIntUnsigned(getValue(index));
    setLength(index, (short) (value - initialValue));
  }

  @Override
  public boolean contains(short x) {
    int index = bufferedUnsignedInterleavedBinarySearch(valueslength, 0, nbrruns, x);
    if (index >= 0) {
      return true;
    }
    index = -index - 2; // points to preceding value, possibly -1
    if (index != -1) {// possible match
      int offset = toIntUnsigned(x) - toIntUnsigned(getValue(index));
      int le = toIntUnsigned(getLength(index));
      if (offset <= le) {
        return true;
      }
    }
    return false;
  }

  /**
   * Checks whether the run container contains x.
   *
   * @param buf underlying ByteBuffer
   * @param position starting position of the container in the ByteBuffer
   * @param x target 16-bit value
   * @param numRuns number of runs
   * @return whether the run container contains x
   */
  public static boolean contains(ByteBuffer buf, int position, short x, final int numRuns) {
    int index = bufferedUnsignedInterleavedBinarySearch(buf, position, 0, numRuns, x);
    if (index >= 0) {
      return true;
    }
    index = -index - 2; // points to preceding value, possibly -1
    if (index != -1) {// possible match
      int offset = toIntUnsigned(x)
          - toIntUnsigned(buf.getShort(position + index * 2 * 2));
      int le = toIntUnsigned(buf.getShort(position + index * 2 * 2 + 2));
      if (offset <= le) {
        return true;
      }
    }
    return false;

  }

  // a very cheap check... if you have more than 4096, then you should use a bitmap container.
  // this function avoids computing the cardinality
  private MappeableContainer convertToLazyBitmapIfNeeded() {
    // when nbrruns exceed MappeableArrayContainer.DEFAULT_MAX_SIZE, then we know it should be
    // stored as a bitmap, always
    if (this.nbrruns > MappeableArrayContainer.DEFAULT_MAX_SIZE) {
      MappeableBitmapContainer answer = new MappeableBitmapContainer();
      for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
        int start = toIntUnsigned(this.getValue(rlepos));
        int end = start + toIntUnsigned(this.getLength(rlepos)) + 1;
        BufferUtil.setBitmapRange(answer.bitmap, start, end);
      }
      answer.cardinality = -1;
      return answer;
    }
    return this;
  }

  // Push all values length to the end of the array (resize array if needed)
  private void copyToOffset(int offset) {
    final int minCapacity = 2 * (offset + nbrruns);
    if (valueslength.capacity() < minCapacity) {
      // expensive case where we need to reallocate
      int newCapacity = valueslength.capacity();
      while (newCapacity < minCapacity) {
        newCapacity = (newCapacity == 0) ? DEFAULT_INIT_SIZE
            : newCapacity < 64 ? newCapacity * 2
                : newCapacity < 1024 ? newCapacity * 3 / 2 : newCapacity * 5 / 4;
      }
      ShortBuffer newvalueslength = ShortBuffer.allocate(newCapacity);
      copyValuesLength(this.valueslength, 0, newvalueslength, offset, nbrruns);
      this.valueslength = newvalueslength;
    } else {
      // efficient case where we just copy
      copyValuesLength(this.valueslength, 0, this.valueslength, offset, nbrruns);
    }
  }

  private void copyValuesLength(ShortBuffer src, int srcIndex, ShortBuffer dst, int dstIndex,
      int length) {
    if (BufferUtil.isBackedBySimpleArray(src) && BufferUtil.isBackedBySimpleArray(dst)) {
      // common case.
      System.arraycopy(src.array(), 2 * srcIndex, dst.array(), 2 * dstIndex, 2 * length);
      return;
    }
    // source and destination may overlap
    // consider specialized code for various cases, rather than using a second buffer
    ShortBuffer temp = ShortBuffer.allocate(2 * length);
    for (int i = 0; i < 2 * length; ++i) {
      temp.put(src.get(2 * srcIndex + i));
    }
    temp.flip();
    for (int i = 0; i < 2 * length; ++i) {
      dst.put(2 * dstIndex + i, temp.get());
    }
  }

  private void decrementLength(int index) {
    // caller is responsible to ensure that value is non-zero
    valueslength.put(2 * index + 1, (short) (valueslength.get(2 * index + 1) - 1));
  }


  private void decrementValue(int index) {
    valueslength.put(2 * index, (short) (valueslength.get(2 * index) - 1));
  }

  // not thread safe!
  // not actually used anywhere, but potentially useful
  protected void ensureCapacity(int minNbRuns) {
    final int minCapacity = 2 * minNbRuns;
    if (valueslength.capacity() < minCapacity) {
      int newCapacity = valueslength.capacity();
      while (newCapacity < minCapacity) {
        newCapacity = (newCapacity == 0) ? DEFAULT_INIT_SIZE
            : newCapacity < 64 ? newCapacity * 2
                : newCapacity < 1024 ? newCapacity * 3 / 2 : newCapacity * 5 / 4;
      }
      final ShortBuffer nv = ShortBuffer.allocate(newCapacity);
      valueslength.rewind();
      nv.put(valueslength);
      valueslength = nv;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof MappeableRunContainer) {
      return equals((MappeableRunContainer) o);
    } else if (o instanceof MappeableArrayContainer) {
      return equals((MappeableArrayContainer) o);
    } else if (o instanceof MappeableContainer) {
      if (((MappeableContainer) o).getCardinality() != this.getCardinality()) {
        return false; // should be a frequent branch if they differ
      }
      // next bit could be optimized if needed:
      ShortIterator me = this.getShortIterator();
      ShortIterator you = ((MappeableContainer) o).getShortIterator();
      while (me.hasNext()) {
        if (me.next() != you.next()) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  private boolean equals(MappeableRunContainer runContainer) {
    if (runContainer.nbrruns != this.nbrruns) {
      return false;
    }
    for (int i = 0; i < nbrruns; ++i) {
      if (this.getValue(i) != runContainer.getValue(i)) {
        return false;
      }
      if (this.getLength(i) != runContainer.getLength(i)) {
        return false;
      }
    }
    return true;
  }

  private boolean equals(MappeableArrayContainer arrayContainer) {
    int pos = 0;
    for (short i = 0; i < nbrruns; ++i) {
      short runStart = getValue(i);
      int length = toIntUnsigned(getLength(i));
      if (pos + length >= arrayContainer.getCardinality()) {
        return false;
      }
      if (arrayContainer.select(pos) != runStart) {
        return false;
      }
      if (arrayContainer.select(pos + length) != (short)(toIntUnsigned(runStart) + length)) {
        return false;
      }
      pos += length + 1;
    }
    return pos == arrayContainer.getCardinality();
  }

  @Override
  public void fillLeastSignificant16bits(int[] x, int i, int mask) {
    int pos = i;
    for (int k = 0; k < this.nbrruns; ++k) {
      final int limit = toIntUnsigned(this.getLength(k));
      final int base = toIntUnsigned(this.getValue(k));
      for (int le = 0; le <= limit; ++le) {
        x[pos++] = (base + le) | mask;
      }
    }
  }


  @Override
  public MappeableContainer flip(short x) {
    if (this.contains(x)) {
      return this.remove(x);
    } else {
      return this.add(x);
    }
  }

  @Override
  protected int getArraySizeInBytes() {
    return 2 + 4 * this.nbrruns; // "array" includes its size
  }

  @Override
  public int getCardinality() {
    int sum = nbrruns; // lengths are stored -1
    if (isArrayBacked()) {
      short[] vl = valueslength.array();
      for (int k = 0; k < nbrruns; ++k) {
        sum = sum + toIntUnsigned(vl[2 * k + 1])/* + 1 */;
      }
    } else {
      for (int k = 0; k < nbrruns; ++k) {
        sum = sum + toIntUnsigned(getLength(k))/* + 1 */;
      }
    }
    return sum;
  }

  /**
   * Gets the length of the run at the index.
   * @param index the index of the run.
   * @return the length of the run at the index.
   * @throws ArrayIndexOutOfBoundsException if index is negative or larger than the index of the
   *     last run.
   */
  public short getLength(int index) {
    return valueslength.get(2 * index + 1);
  }

  @Override
  public ShortIterator getReverseShortIterator() {
    if (isArrayBacked()) {
      return new RawReverseMappeableRunContainerShortIterator(this);
    }
    return new ReverseMappeableRunContainerShortIterator(this);
  }

  @Override
  public PeekableShortIterator getShortIterator() {
    if (isArrayBacked()) {
      return new RawMappeableRunContainerShortIterator(this);
    }
    return new MappeableRunContainerShortIterator(this);
  }

  @Override
  public ContainerBatchIterator getBatchIterator() {
    return new RunBatchIterator(this);
  }

  @Override
  public int getSizeInBytes() {
    return this.nbrruns * 4 + 4; // not sure about how exact it will be
  }

  /**
   * Gets the value of the first element of the run at the index.
   * @param index the index of the run.
   * @return the value of the first element of the run at the index.
   * @throws ArrayIndexOutOfBoundsException if index is negative or larger than the index of the
   *     last run.
   */
  public short getValue(int index) {
    return valueslength.get(2 * index);
  }

  @Override
  public int hashCode() {
    int hash = 0;
    for (int k = 0; k < nbrruns * 2; ++k) {
      hash += 31 * hash + valueslength.get(k);
    }
    return hash;
  }

  @Override
  // not thread-safe
  public MappeableContainer iadd(int begin, int end) {
    // TODO: it might be better and simpler to do return
    // toBitmapOrArrayContainer(getCardinality()).iadd(begin,end)
    if(end == begin) {
      return this;
    }
    if ((begin > end) || (end > (1 << 16))) {
      throw new IllegalArgumentException("Invalid range [" + begin + "," + end + ")");
    }
    if (begin == end - 1) {
      add((short) begin);
      return this;
    }

    int bIndex =
        bufferedUnsignedInterleavedBinarySearch(this.valueslength, 0, this.nbrruns, (short) begin);
    int eIndex = bufferedUnsignedInterleavedBinarySearch(this.valueslength, 0, this.nbrruns,
        (short) (end - 1));

    if (bIndex >= 0 && eIndex >= 0) {
      mergeValuesLength(bIndex, eIndex);
      return this;

    } else if (bIndex >= 0 && eIndex < 0) {
      eIndex = -eIndex - 2;

      if (canPrependValueLength(end - 1, eIndex + 1)) {
        mergeValuesLength(bIndex, eIndex + 1);
        return this;
      }

      appendValueLength(end - 1, eIndex);
      mergeValuesLength(bIndex, eIndex);
      return this;

    } else if (bIndex < 0 && eIndex >= 0) {
      bIndex = -bIndex - 2;

      if (bIndex >= 0) {
        if (valueLengthContains(begin - 1, bIndex)) {
          mergeValuesLength(bIndex, eIndex);
          return this;
        }
      }
      prependValueLength(begin, bIndex + 1);
      mergeValuesLength(bIndex + 1, eIndex);
      return this;

    } else {
      bIndex = -bIndex - 2;
      eIndex = -eIndex - 2;

      if (eIndex >= 0) {
        if (bIndex >= 0) {
          if (!valueLengthContains(begin - 1, bIndex)) {
            if (bIndex == eIndex) {
              if (canPrependValueLength(end - 1, eIndex + 1)) {
                prependValueLength(begin, eIndex + 1);
                return this;
              }
              makeRoomAtIndex(eIndex + 1);
              setValue(eIndex + 1, (short) begin);
              setLength(eIndex + 1, (short) (end - 1 - begin));
              return this;

            } else {
              bIndex++;
              prependValueLength(begin, bIndex);
            }
          }
        } else {
          bIndex = 0;
          prependValueLength(begin, bIndex);
        }

        if (canPrependValueLength(end - 1, eIndex + 1)) {
          mergeValuesLength(bIndex, eIndex + 1);
          return this;
        }

        appendValueLength(end - 1, eIndex);
        mergeValuesLength(bIndex, eIndex);
        return this;

      } else {
        if (canPrependValueLength(end - 1, 0)) {
          prependValueLength(begin, 0);
        } else {
          makeRoomAtIndex(0);
          setValue(0, (short) begin);
          setLength(0, (short) (end - 1 - begin));
        }
        return this;
      }
    }
  }



  @Override
  public MappeableContainer iand(MappeableArrayContainer x) {
    return and(x);
  }

  @Override
  public MappeableContainer iand(MappeableBitmapContainer x) {
    return and(x);
  }


  @Override
  public MappeableContainer iand(MappeableRunContainer x) {
    return and(x);
  }

  @Override
  public MappeableContainer iandNot(MappeableArrayContainer x) {
    return andNot(x);
  }

  @Override
  public MappeableContainer iandNot(MappeableBitmapContainer x) {
    return andNot(x);
  }

  @Override
  public MappeableContainer iandNot(MappeableRunContainer x) {
    return andNot(x);
  }

  protected MappeableContainer ilazyor(MappeableArrayContainer x) {
    if (isFull()) {
      return this; // this can sometimes solve a lot of computation!
    }
    return ilazyorToRun(x);
  }


  private MappeableContainer ilazyorToRun(MappeableArrayContainer x) {
    if (isFull()) {
      return full();
    }
    final int nbrruns = this.nbrruns;
    final int offset = Math.max(nbrruns, x.getCardinality());
    copyToOffset(offset);
    short[] vl = valueslength.array();
    int rlepos = 0;
    this.nbrruns = 0;
    PeekableShortIterator i = x.getShortIterator();
    while (i.hasNext() && (rlepos < nbrruns)) {
      if (BufferUtil.compareUnsigned(getValue(vl, rlepos + offset), i.peekNext()) <= 0) {
        smartAppend(vl, getValue(vl, rlepos + offset), getLength(vl, rlepos + offset));
        rlepos++;
      } else {
        smartAppend(vl, i.next());
      }
    }
    if (i.hasNext()) {
      /*
       * if(this.nbrruns>0) { // this might be useful if the run container has just one very large
       * run int lastval = BufferUtil.toIntUnsigned(getValue(vl,nbrruns + offset - 1)) +
       * BufferUtil.toIntUnsigned(getLength(vl,nbrruns + offset - 1)) + 1; i.advanceIfNeeded((short)
       * lastval); }
       */
      while (i.hasNext()) {
        smartAppend(vl, i.next());
      }
    } else {
      while (rlepos < nbrruns) {
        smartAppend(vl, getValue(vl, rlepos + offset), getLength(vl, rlepos + offset));
        rlepos++;
      }
    }
    return convertToLazyBitmapIfNeeded();
  }

  // not thread safe!
  private void increaseCapacity() {
    int newCapacity = (valueslength.capacity() == 0) ? DEFAULT_INIT_SIZE
        : valueslength.capacity() < 64 ? valueslength.capacity() * 2
            : valueslength.capacity() < 1024 ? valueslength.capacity() * 3 / 2
                : valueslength.capacity() * 5 / 4;

    final ShortBuffer nv = ShortBuffer.allocate(newCapacity);
    valueslength.rewind();
    nv.put(valueslength);
    valueslength = nv;
  }

  private void incrementLength(int index) {
    valueslength.put(2 * index + 1, (short) (1 + valueslength.get(2 * index + 1)));
  }

  private void incrementValue(int index) {
    valueslength.put(2 * index, (short) (1 + valueslength.get(2 * index)));
  }

  // To set the first value of a value length
  private void initValueLength(int value, int index) {
    int initialValue = toIntUnsigned(getValue(index));
    int length = toIntUnsigned(getLength(index));
    setValue(index, (short) (value));
    setLength(index, (short) (length - (value - initialValue)));
  }


  @Override
  public MappeableContainer inot(int rangeStart, int rangeEnd) {
    if (rangeEnd <= rangeStart) {
      return this;
    }
    short[] vl = this.valueslength.array();

    // TODO: write special case code for rangeStart=0; rangeEnd=65535
    // a "sliding" effect where each range records the gap adjacent it
    // can probably be quite fast. Probably have 2 cases: start with a
    // 0 run vs start with a 1 run. If you both start and end with 0s,
    // you will require room for expansion.

    // the +1 below is needed in case the valueslength.length is odd
    if (vl.length <= 2 * nbrruns + 1) {
      // no room for expansion
      // analyze whether this is a case that will require expansion (that we cannot do)
      // this is a bit costly now (4 "contains" checks)

      boolean lastValueBeforeRange = false;
      boolean firstValueInRange = false;
      boolean lastValueInRange = false;
      boolean firstValuePastRange = false;

      // contains is based on a binary search and is hopefully fairly fast.
      // however, one binary search could *usually* suffice to find both
      // lastValueBeforeRange AND firstValueInRange. ditto for
      // lastVaueInRange and firstValuePastRange

      // find the start of the range
      if (rangeStart > 0) {
        lastValueBeforeRange = contains((short) (rangeStart - 1));
      }
      firstValueInRange = contains((short) rangeStart);

      if (lastValueBeforeRange == firstValueInRange) {
        // expansion is required if also lastValueInRange==firstValuePastRange

        // tougher to optimize out, but possible.
        lastValueInRange = contains((short) (rangeEnd - 1));
        if (rangeEnd != 65536) {
          firstValuePastRange = contains((short) rangeEnd);
        }

        // there is definitely one more run after the operation.
        if (lastValueInRange == firstValuePastRange) {
          return not(rangeStart, rangeEnd); // can't do in-place: true space limit
        }
      }
    }
    // either no expansion required, or we have room to handle any required expansion for it.

    // remaining code is just a minor variation on not()
    int myNbrRuns = nbrruns;

    MappeableRunContainer ans = this; // copy on top of self.
    int k = 0;
    ans.nbrruns = 0; // losing this.nbrruns, which is stashed in myNbrRuns.

    // could try using unsignedInterleavedBinarySearch(valueslength, 0, nbrruns, rangeStart) instead
    // of sequential scan
    // to find the starting location

    for (; k < myNbrRuns && toIntUnsigned(this.getValue(k)) < rangeStart; ++k) {
      // since it is atop self, there is no copying needed
      // ans.valueslength[2 * k] = this.valueslength[2 * k];
      // ans.valueslength[2 * k + 1] = this.valueslength[2 * k + 1];
      ans.nbrruns++;
    }
    // We will work left to right, with a read pointer that always stays
    // left of the write pointer. However, we need to give the read pointer a head start.
    // use local variables so we are always reading 1 location ahead.

    short bufferedValue = 0, bufferedLength = 0; // 65535 start and 65535 length would be illegal,
                                                 // could use as sentinel
    short nextValue = 0, nextLength = 0;
    if (k < myNbrRuns) { // prime the readahead variables
      bufferedValue = vl[2 * k];// getValue(k);
      bufferedLength = vl[2 * k + 1];// getLength(k);
    }

    ans.smartAppendExclusive(vl, (short) rangeStart, (short) (rangeEnd - rangeStart - 1));

    for (; k < myNbrRuns; ++k) {
      if (ans.nbrruns > k + 1) {
        throw new RuntimeException(
            "internal error in inot, writer has overtaken reader!! " + k + " " + ans.nbrruns);
      }
      if (k + 1 < myNbrRuns) {
        nextValue = vl[2 * (k + 1)];// getValue(k+1); // readahead for next iteration
        nextLength = vl[2 * (k + 1) + 1];// getLength(k+1);
      }
      ans.smartAppendExclusive(vl, bufferedValue, bufferedLength);
      bufferedValue = nextValue;
      bufferedLength = nextLength;
    }
    // the number of runs can increase by one, meaning (rarely) a bitmap will become better
    // or the cardinality can decrease by a lot, making an array better
    return ans.toEfficientContainer();
  }

  @Override
  public boolean intersects(MappeableArrayContainer x) {
    if (this.nbrruns == 0) {
      return false;
    }
    int rlepos = 0;
    int arraypos = 0;

    int rleval = toIntUnsigned(this.getValue(rlepos));
    int rlelength = toIntUnsigned(this.getLength(rlepos));
    while (arraypos < x.cardinality) {
      int arrayval = toIntUnsigned(x.content.get(arraypos));
      while (rleval + rlelength < arrayval) {// this will frequently be false
        ++rlepos;
        if (rlepos == this.nbrruns) {
          return false;
        }
        rleval = toIntUnsigned(this.getValue(rlepos));
        rlelength = toIntUnsigned(this.getLength(rlepos));
      }
      if (rleval > arrayval) {
        arraypos =
            BufferUtil.advanceUntil(x.content, arraypos, x.cardinality, this.getValue(rlepos));
      } else {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean intersects(MappeableBitmapContainer x) {
    // possibly inefficient
    for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
      int runStart = toIntUnsigned(this.getValue(rlepos));
      int runEnd = runStart + toIntUnsigned(this.getLength(rlepos));
      for (int runValue = runStart; runValue <= runEnd; ++runValue) {
        if (x.contains((short) runValue)) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public boolean intersects(MappeableRunContainer x) {
    int rlepos = 0;
    int xrlepos = 0;
    int start = toIntUnsigned(this.getValue(rlepos));
    int end = start + toIntUnsigned(this.getLength(rlepos)) + 1;
    int xstart = toIntUnsigned(x.getValue(xrlepos));
    int xend = xstart + toIntUnsigned(x.getLength(xrlepos)) + 1;
    while ((rlepos < this.nbrruns) && (xrlepos < x.nbrruns)) {
      if (end <= xstart) {
        // exit the first run
        rlepos++;
        if (rlepos < this.nbrruns) {
          start = toIntUnsigned(this.getValue(rlepos));
          end = start + toIntUnsigned(this.getLength(rlepos)) + 1;
        }
      } else if (xend <= start) {
        // exit the second run
        xrlepos++;
        if (xrlepos < x.nbrruns) {
          xstart = toIntUnsigned(x.getValue(xrlepos));
          xend = xstart + toIntUnsigned(x.getLength(xrlepos)) + 1;
        }
      } else {// they overlap
        return true;
      }
    }
    return false;
  }

  @Override
  public MappeableContainer ior(MappeableArrayContainer x) {
    if (isFull()) {
      return this;
    }
    final int nbrruns = this.nbrruns;
    final int offset = Math.max(nbrruns, x.getCardinality());
    copyToOffset(offset);
    short[] vl = this.valueslength.array();
    int rlepos = 0;
    this.nbrruns = 0;
    PeekableShortIterator i = x.getShortIterator();
    while (i.hasNext() && (rlepos < nbrruns)) {
      if (BufferUtil.compareUnsigned(getValue(vl, rlepos + offset), i.peekNext()) <= 0) {
        smartAppend(vl, getValue(vl, rlepos + offset), getLength(vl, rlepos + offset));
        rlepos++;
      } else {
        smartAppend(vl, i.next());
      }
    }
    if (i.hasNext()) {
      /*
       * if(this.nbrruns>0) { // this might be useful if the run container has just one very large
       * run int lastval = BufferUtil.toIntUnsigned(getValue(nbrruns + offset - 1)) +
       * BufferUtil.toIntUnsigned(getLength(nbrruns + offset - 1)) + 1; i.advanceIfNeeded((short)
       * lastval); }
       */
      while (i.hasNext()) {
        smartAppend(vl, i.next());
      }
    } else {
      while (rlepos < nbrruns) {
        smartAppend(vl, getValue(vl, rlepos + offset), getLength(vl, rlepos + offset));
        rlepos++;
      }
    }
    return toEfficientContainer();
  }

  @Override
  public MappeableContainer ior(MappeableBitmapContainer x) {
    if (isFull()) {
      return this;
    }
    return or(x);
  }

  @Override
  public MappeableContainer ior(MappeableRunContainer x) {
    if (isFull()) {
      return this;
    }

    final int nbrruns = this.nbrruns;
    final int xnbrruns = x.nbrruns;
    final int offset = Math.max(nbrruns, xnbrruns);

    // Push all values length to the end of the array (resize array if needed)
    copyToOffset(offset);

    // Aggregate and store the result at the beginning of the array
    this.nbrruns = 0;
    int rlepos = 0;
    int xrlepos = 0;
    short[] vl = this.valueslength.array();

    // Add values length (smaller first)
    while ((rlepos < nbrruns) && (xrlepos < xnbrruns)) {
      final short value = getValue(vl, offset + rlepos);
      final short xvalue = x.getValue(xrlepos);
      final short length = getLength(vl, offset + rlepos);
      final short xlength = x.getLength(xrlepos);

      if (BufferUtil.compareUnsigned(value, xvalue) <= 0) {
        this.smartAppend(vl, value, length);
        ++rlepos;
      } else {
        this.smartAppend(vl, xvalue, xlength);
        ++xrlepos;
      }
    }
    while (rlepos < nbrruns) {
      this.smartAppend(vl, getValue(vl, offset + rlepos), getLength(vl, offset + rlepos));
      ++rlepos;
    }
    while (xrlepos < xnbrruns) {
      this.smartAppend(vl, x.getValue(xrlepos), x.getLength(xrlepos));
      ++xrlepos;
    }
    return this.toBitmapIfNeeded();
  }

  @Override
  // not thread-safe
  public MappeableContainer iremove(int begin, int end) {
    // TODO: it might be better and simpler to do return
    // toBitmapOrArrayContainer(getCardinality()).iremove(begin,end)
    if(end == begin) {
      return this;
    }
    if ((begin > end) || (end > (1 << 16))) {
      throw new IllegalArgumentException("Invalid range [" + begin + "," + end + ")");
    }
    if (begin == end - 1) {
      remove((short) begin);
      return this;
    }

    int bIndex =
        bufferedUnsignedInterleavedBinarySearch(this.valueslength, 0, this.nbrruns, (short) begin);
    int eIndex = bufferedUnsignedInterleavedBinarySearch(this.valueslength, 0, this.nbrruns,
        (short) (end - 1));

    if (bIndex >= 0) {
      if (eIndex < 0) {
        eIndex = -eIndex - 2;
      }

      if (valueLengthContains(end, eIndex)) {
        initValueLength(end, eIndex);
        recoverRoomsInRange(bIndex - 1, eIndex - 1);
      } else {
        recoverRoomsInRange(bIndex - 1, eIndex);
      }

    } else if (bIndex < 0 && eIndex >= 0) {
      bIndex = -bIndex - 2;

      if (bIndex >= 0) {
        if (valueLengthContains(begin, bIndex)) {
          closeValueLength(begin - 1, bIndex);
        }
      }
      // last run is one shorter
      if (getLength(eIndex) == 0) {// special case where we remove last run
        recoverRoomsInRange(eIndex, eIndex + 1);
      } else {
        incrementValue(eIndex);
        decrementLength(eIndex);
      }
      recoverRoomsInRange(bIndex, eIndex - 1);

    } else {
      bIndex = -bIndex - 2;
      eIndex = -eIndex - 2;

      if (eIndex >= 0) {
        if (bIndex >= 0) {
          if (bIndex == eIndex) {
            if (valueLengthContains(begin, bIndex)) {
              if (valueLengthContains(end, eIndex)) {
                makeRoomAtIndex(bIndex);
                closeValueLength(begin - 1, bIndex);
                initValueLength(end, bIndex + 1);
                return this;
              }
              closeValueLength(begin - 1, bIndex);
            }
          } else {
            if (valueLengthContains(begin, bIndex)) {
              closeValueLength(begin - 1, bIndex);
            }
            if (valueLengthContains(end, eIndex)) {
              initValueLength(end, eIndex);
              eIndex--;
            }
            recoverRoomsInRange(bIndex, eIndex);
          }

        } else {
          if (valueLengthContains(end, eIndex)) { // was end-1
            initValueLength(end, eIndex);
            recoverRoomsInRange(bIndex, eIndex - 1);
          } else {
            recoverRoomsInRange(bIndex, eIndex);
          }
        }

      }

    }
    return this;
  }

  @Override
  protected boolean isArrayBacked() {
    return BufferUtil.isBackedBySimpleArray(this.valueslength);
  }

  protected boolean isFull() {
    return (this.nbrruns == 1) && (this.getValue(0) == 0) && (this.getLength(0) == -1);
  }

  public static MappeableRunContainer full() {
    return new MappeableRunContainer(0, 1 << 16);
  }

  @Override
  public Iterator<Short> iterator() {
    final ShortIterator i = getShortIterator();
    return new Iterator<Short>() {

      @Override
      public boolean hasNext() {
        return i.hasNext();
      }

      @Override
      public Short next() {
        return i.next();
      }

      @Override
      public void remove() {
        i.remove();
      }
    };

  }

  @Override
  public MappeableContainer ixor(MappeableArrayContainer x) {
    return xor(x);
  }


  @Override
  public MappeableContainer ixor(MappeableBitmapContainer x) {
    return xor(x);
  }

  @Override
  public MappeableContainer ixor(MappeableRunContainer x) {
    return xor(x);
  }

  private MappeableRunContainer lazyandNot(MappeableArrayContainer x) {
    if (x.isEmpty()) {
      return this;
    }
    MappeableRunContainer answer =
        new MappeableRunContainer(ShortBuffer.allocate(2 * (this.nbrruns + x.cardinality)), 0);
    short[] vl = answer.valueslength.array();
    int rlepos = 0;
    int xrlepos = 0;
    int start = toIntUnsigned(this.getValue(rlepos));
    int end = start + toIntUnsigned(this.getLength(rlepos)) + 1;
    int xstart = toIntUnsigned(x.content.get(xrlepos));
    while ((rlepos < this.nbrruns) && (xrlepos < x.cardinality)) {
      if (end <= xstart) {
        // output the first run
        vl[2 * answer.nbrruns] = (short) start;
        vl[2 * answer.nbrruns + 1] = (short) (end - start - 1);
        answer.nbrruns++;
        rlepos++;
        if (rlepos < this.nbrruns) {
          start = toIntUnsigned(this.getValue(rlepos));
          end = start + toIntUnsigned(this.getLength(rlepos)) + 1;
        }
      } else if (xstart + 1 <= start) {
        // exit the second run
        xrlepos++;
        if (xrlepos < x.cardinality) {
          xstart = toIntUnsigned(x.content.get(xrlepos));
        }
      } else {
        if (start < xstart) {
          vl[2 * answer.nbrruns] = (short) start;
          vl[2 * answer.nbrruns + 1] = (short) (xstart - start - 1);
          answer.nbrruns++;
        }
        if (xstart + 1 < end) {
          start = xstart + 1;
        } else {
          rlepos++;
          if (rlepos < this.nbrruns) {
            start = toIntUnsigned(this.getValue(rlepos));
            end = start + toIntUnsigned(this.getLength(rlepos)) + 1;
          }
        }
      }
    }
    if (rlepos < this.nbrruns) {
      vl[2 * answer.nbrruns] = (short) start;
      vl[2 * answer.nbrruns + 1] = (short) (end - start - 1);
      answer.nbrruns++;
      rlepos++;
      for (; rlepos < this.nbrruns; ++rlepos) {
        vl[2 * answer.nbrruns] = this.valueslength.get(2 * rlepos);
        vl[2 * answer.nbrruns + 1] = this.valueslength.get(2 * rlepos + 1);
        answer.nbrruns++;
      }
      // next bit would be faster, but not thread-safe because of the "position"
      // if(rlepos < this.nbrruns) {
      // this.valueslength.position(2 * rlepos);
      // this.valueslength.get(vl, 2 * answer.nbrruns, 2*(this.nbrruns-rlepos ));
      // answer.nbrruns = answer.nbrruns + this.nbrruns - rlepos;
      // }
    }
    return answer;
  }



  protected MappeableContainer lazyor(MappeableArrayContainer x) {
    return lazyorToRun(x);
  }

  private MappeableContainer lazyorToRun(MappeableArrayContainer x) {
    if (isFull()) {
      return full();
    }
    // TODO: should optimize for the frequent case where we have a single run
    MappeableRunContainer answer =
        new MappeableRunContainer(ShortBuffer.allocate(2 * (this.nbrruns + x.getCardinality())), 0);
    short[] vl = answer.valueslength.array();
    int rlepos = 0;
    PeekableShortIterator i = x.getShortIterator();

    while ((rlepos < this.nbrruns) && i.hasNext()) {
      if (BufferUtil.compareUnsigned(getValue(rlepos), i.peekNext()) <= 0) {
        answer.smartAppend(vl, getValue(rlepos), getLength(rlepos));
        // could call i.advanceIfNeeded(minval);
        rlepos++;
      } else {
        answer.smartAppend(vl, i.next());
      }
    }
    if (i.hasNext()) {
      /*
       * if(answer.nbrruns>0) { // this might be useful if the run container has just one very large
       * run int lastval = BufferUtil.toIntUnsigned(answer.getValue(answer.nbrruns - 1)) +
       * BufferUtil.toIntUnsigned(answer.getLength(answer.nbrruns - 1)) + 1;
       * i.advanceIfNeeded((short) lastval); }
       */
      while (i.hasNext()) {
        answer.smartAppend(vl, i.next());
      }
    } else {

      while (rlepos < this.nbrruns) {
        answer.smartAppend(vl, getValue(rlepos), getLength(rlepos));
        rlepos++;
      }
    }
    if (answer.isFull()) {
      return full();
    }
    return answer.convertToLazyBitmapIfNeeded();
  }

  private MappeableContainer lazyxor(MappeableArrayContainer x) {
    if (x.isEmpty()) {
      return this;
    }
    if (this.nbrruns == 0) {
      return x;
    }
    MappeableRunContainer answer =
        new MappeableRunContainer(ShortBuffer.allocate(2 * (this.nbrruns + x.getCardinality())), 0);
    short[] vl = answer.valueslength.array();
    int rlepos = 0;
    ShortIterator i = x.getShortIterator();
    short cv = i.next();
    while (true) {
      if (BufferUtil.compareUnsigned(getValue(rlepos), cv) < 0) {
        answer.smartAppendExclusive(vl, getValue(rlepos), getLength(rlepos));
        rlepos++;
        if (rlepos == this.nbrruns) {
          answer.smartAppendExclusive(vl, cv);
          while (i.hasNext()) {
            answer.smartAppendExclusive(vl, i.next());
          }
          break;
        }
      } else {
        answer.smartAppendExclusive(vl, cv);
        if (!i.hasNext()) {
          while (rlepos < this.nbrruns) {
            answer.smartAppendExclusive(vl, getValue(rlepos), getLength(rlepos));
            rlepos++;
          }
          break;
        } else {
          cv = i.next();
        }
      }
    }
    return answer;
  }

  @Override
  public MappeableContainer limit(int maxcardinality) {
    if (maxcardinality >= getCardinality()) {
      return clone();
    }

    int r;
    int cardinality = 0;
    for (r = 0; r < this.nbrruns; ++r) {
      cardinality += toIntUnsigned(getLength(r)) + 1;
      if (maxcardinality <= cardinality) {
        break;
      }
    }

    ShortBuffer newBuf = ShortBuffer.allocate(2 * (r + 1));
    for (int i = 0; i < 2 * (r + 1); ++i) {
      newBuf.put(valueslength.get(i)); // could be optimized
    }
    MappeableRunContainer rc = new MappeableRunContainer(newBuf, r + 1);

    rc.setLength(r,
        (short) (toIntUnsigned(rc.getLength(r)) - cardinality + maxcardinality));
    return rc;
  }

  // not thread-safe
  private void makeRoomAtIndex(int index) {
    if (2 * (nbrruns + 1) > valueslength.capacity()) {
      increaseCapacity();
    }
    copyValuesLength(valueslength, index, valueslength, index + 1, nbrruns - index);
    nbrruns++;
  }

  // To merge values length from begin(inclusive) to end(inclusive)
  private void mergeValuesLength(int begin, int end) {
    if (begin < end) {
      int bValue = toIntUnsigned(getValue(begin));
      int eValue = toIntUnsigned(getValue(end));
      int eLength = toIntUnsigned(getLength(end));
      int newLength = eValue - bValue + eLength;
      setLength(begin, (short) newLength);
      recoverRoomsInRange(begin, end);
    }
  }

  @Override
  public MappeableContainer not(int rangeStart, int rangeEnd) {
    if (rangeEnd <= rangeStart) {
      return this.clone();
    }
    MappeableRunContainer ans = new MappeableRunContainer(nbrruns + 1);
    if (!ans.isArrayBacked()) {
      throw new RuntimeException("internal bug");
    }
    short[] vl = ans.valueslength.array();
    int k = 0;

    if (isArrayBacked()) {
      short[] myVl = valueslength.array();
      for (; k < this.nbrruns && toIntUnsigned(getValue(myVl, k)) < rangeStart; ++k) {
        vl[2 * k] = myVl[2 * k];
        vl[2 * k + 1] = myVl[2 * k + 1];
        ans.nbrruns++;
      }
      ans.smartAppendExclusive(vl, (short) rangeStart, (short) (rangeEnd - rangeStart - 1));
      for (; k < this.nbrruns; ++k) {
        ans.smartAppendExclusive(vl, getValue(myVl, k), getLength(myVl, k));
      }
    } else { // not array backed

      for (; k < this.nbrruns && toIntUnsigned(this.getValue(k)) < rangeStart; ++k) {
        vl[2 * k] = getValue(k);
        vl[2 * k + 1] = getLength(k);
        ans.nbrruns++;
      }
      ans.smartAppendExclusive(vl, (short) rangeStart, (short) (rangeEnd - rangeStart - 1));
      for (; k < this.nbrruns; ++k) {
        ans.smartAppendExclusive(vl, getValue(k), getLength(k));
      }
    }
    return ans.toEfficientContainer();
  }

  @Override
  public int numberOfRuns() {
    return this.nbrruns;
  }

  @Override
  public MappeableContainer or(MappeableArrayContainer x) {
    // we guess that, often, the result will still be efficiently expressed as a run container
    return lazyorToRun(x).repairAfterLazy();
  }

  @Override
  public MappeableContainer or(MappeableBitmapContainer x) {
    if (isFull()) {
      return full();
    }
    MappeableBitmapContainer answer = x.clone();
    for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
      int start = toIntUnsigned(this.getValue(rlepos));
      int end = start + toIntUnsigned(this.getLength(rlepos)) + 1;
      int prevOnesInRange = answer.cardinalityInRange(start, end);
      BufferUtil.setBitmapRange(answer.bitmap, start, end);
      answer.updateCardinality(prevOnesInRange, end - start);
    }
    if (answer.isFull()) {
      return full();
    }
    return answer;
  }

  @Override
  public MappeableContainer or(MappeableRunContainer x) {
    if (isFull() || x.isFull()) {
      return full(); // cheap case that can save a lot of computation
    }
    // we really ought to optimize the rest of the code for the frequent case where there is a
    // single run
    MappeableRunContainer answer =
        new MappeableRunContainer(ShortBuffer.allocate(2 * (this.nbrruns + x.nbrruns)), 0);
    short[] vl = answer.valueslength.array();
    int rlepos = 0;
    int xrlepos = 0;

    while ((rlepos < this.nbrruns) && (xrlepos < x.nbrruns)) {
      if (BufferUtil.compareUnsigned(getValue(rlepos), x.getValue(xrlepos)) <= 0) {
        answer.smartAppend(vl, getValue(rlepos), getLength(rlepos));
        rlepos++;
      } else {
        answer.smartAppend(vl, x.getValue(xrlepos), x.getLength(xrlepos));
        xrlepos++;
      }
    }
    while (xrlepos < x.nbrruns) {
      answer.smartAppend(vl, x.getValue(xrlepos), x.getLength(xrlepos));
      xrlepos++;
    }
    while (rlepos < this.nbrruns) {
      answer.smartAppend(vl, getValue(rlepos), getLength(rlepos));
      rlepos++;
    }
    if (answer.isFull()) {
      return full();
    }
    return answer.toBitmapIfNeeded();
  }

  // Prepend a value length with all values starting from a given value
  private void prependValueLength(int value, int index) {
    int initialValue = toIntUnsigned(getValue(index));
    int length = toIntUnsigned(getLength(index));
    setValue(index, (short) value);
    setLength(index, (short) (initialValue - value + length));
  }

  @Override
  public int rank(short lowbits) {
    int x = toIntUnsigned(lowbits);
    int answer = 0;
    for (int k = 0; k < this.nbrruns; ++k) {
      int value = toIntUnsigned(getValue(k));
      int length = toIntUnsigned(getLength(k));
      if (x < value) {
        return answer;
      } else if (value + length + 1 > x) {
        return answer + x - value + 1;
      }
      answer += length + 1;
    }
    return answer;
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    // little endian
    this.nbrruns = 0xFFFF & Short.reverseBytes(in.readShort());
    if (this.valueslength.capacity() < 2 * this.nbrruns) {
      this.valueslength = ShortBuffer.allocate(2 * this.nbrruns);
    }
    for (int k = 0; k < 2 * this.nbrruns; ++k) {
      this.valueslength.put(k, Short.reverseBytes(in.readShort()));
    }
  }

  private void recoverRoomAtIndex(int index) {
    copyValuesLength(valueslength, index + 1, valueslength, index, nbrruns - index - 1);
    nbrruns--;
  }

  // To recover rooms between begin(exclusive) and end(inclusive)
  private void recoverRoomsInRange(int begin, int end) {
    if (end + 1 < nbrruns) {
      copyValuesLength(valueslength, end + 1, valueslength, begin + 1, nbrruns - 1 - end);
    }
    nbrruns -= end - begin;
  }

  @Override
  public MappeableContainer remove(int begin, int end) {
    MappeableRunContainer rc = (MappeableRunContainer) clone();
    return rc.iremove(begin, end);
  }

  @Override
  // not thread-safe
  public MappeableContainer remove(short x) {
    int index = bufferedUnsignedInterleavedBinarySearch(valueslength, 0, nbrruns, x);
    if (index >= 0) {
      if (getLength(index) == 0) {
        recoverRoomAtIndex(index);
      } else {
        incrementValue(index);
        decrementLength(index);
      }
      return this;// already there
    }
    index = -index - 2;// points to preceding value, possibly -1
    if (index >= 0) {// possible match
      int offset = toIntUnsigned(x) - toIntUnsigned(getValue(index));
      int le = toIntUnsigned(getLength(index));
      if (offset < le) {
        // need to break in two
        this.setLength(index, (short) (offset - 1));
        // need to insert
        int newvalue = toIntUnsigned(x) + 1;
        int newlength = le - offset - 1;
        makeRoomAtIndex(index + 1);
        this.setValue(index + 1, (short) newvalue);
        this.setLength(index + 1, (short) newlength);
        return this;
      } else if (offset == le) {
        decrementLength(index);
      }
    }
    // no match
    return this;
  }

  @Override
  public MappeableContainer repairAfterLazy() {
    return toEfficientContainer();
  }

  /**
   * Convert to Array or Bitmap container if the serialized form would be shorter
   */

  @Override
  public MappeableContainer runOptimize() {
    return toEfficientContainer(); // which had the same functionality.
  }

  @Override
  public short select(int j) {
    int offset = 0;
    for (int k = 0; k < this.nbrruns; ++k) {
      int nextOffset = offset + toIntUnsigned(getLength(k)) + 1;
      if (nextOffset > j) {
        return (short) (getValue(k) + (j - offset));
      }
      offset = nextOffset;
    }
    throw new IllegalArgumentException(
        "Cannot select " + j + " since cardinality is " + getCardinality());
  }

  @Override
  public int serializedSizeInBytes() {
    return serializedSizeInBytes(nbrruns);
  }



  private void setLength(int index, short v) {
    setLength(valueslength, index, v);
  }



  private void setLength(ShortBuffer valueslength, int index, short v) {
    valueslength.put(2 * index + 1, v);
  }


  private void setValue(int index, short v) {
    setValue(valueslength, index, v);
  }

  private void setValue(ShortBuffer valueslength, int index, short v) {
    valueslength.put(2 * index, v);
  }



  // assume that the (maybe) inplace operations
  // will never actually *be* in place if they are
  // to return ArrayContainer or BitmapContainer

  private void smartAppend(short[] vl, short val) {
    int oldend;
    if ((nbrruns == 0) || (
            toIntUnsigned(val) > (oldend = toIntUnsigned(vl[2 * (nbrruns - 1)])
            + toIntUnsigned(vl[2 * (nbrruns - 1) + 1])) + 1)) { // we add a new one
      vl[2 * nbrruns] = val;
      vl[2 * nbrruns + 1] = 0;
      nbrruns++;
      return;
    }
    if (val == (short) (oldend + 1)) { // we merge
      vl[2 * (nbrruns - 1) + 1]++;
    }
  }

  void smartAppend(short start, short length) {
    int oldend;
    if ((nbrruns == 0) || (toIntUnsigned(start) > (oldend =
          toIntUnsigned(getValue(nbrruns - 1)) + toIntUnsigned(getLength(nbrruns - 1)))
          + 1)) { // we add a new one
      ensureCapacity(nbrruns + 1);
      valueslength.put(2 * nbrruns, start);
      valueslength.put(2 * nbrruns + 1, length);
      nbrruns++;
      return;
    }
    int newend = toIntUnsigned(start) + toIntUnsigned(length) + 1;
    if (newend > oldend) { // we merge
      setLength(nbrruns - 1, (short) (newend - 1 - toIntUnsigned(getValue(nbrruns - 1))));
    }
  }

  private void smartAppend(short[] vl, short start, short length) {
    int oldend;
    if ((nbrruns == 0) || (
            toIntUnsigned(start) > (oldend = toIntUnsigned(vl[2 * (nbrruns - 1)])
            + toIntUnsigned(vl[2 * (nbrruns - 1) + 1])) + 1)) { // we add a new one
      vl[2 * nbrruns] = start;
      vl[2 * nbrruns + 1] = length;
      nbrruns++;
      return;
    }
    int newend = toIntUnsigned(start) + toIntUnsigned(length) + 1;
    if (newend > oldend) { // we merge
      vl[2 * (nbrruns - 1) + 1] =
          (short) (newend - 1 - toIntUnsigned(vl[2 * (nbrruns - 1)]));
    }
  }

  private void smartAppendExclusive(short[] vl, short val) {
    int oldend;
    if ((nbrruns == 0) || (
            toIntUnsigned(val) > (oldend = toIntUnsigned(getValue(nbrruns - 1))
            + toIntUnsigned(getLength(nbrruns - 1)) + 1))) { // we add a new one
      vl[2 * nbrruns] = val;
      vl[2 * nbrruns + 1] = 0;
      nbrruns++;
      return;
    }
    if (oldend == toIntUnsigned(val)) {
      // we merge
      vl[2 * (nbrruns - 1) + 1]++;
      return;
    }


    int newend = toIntUnsigned(val) + 1;

    if (toIntUnsigned(val) == toIntUnsigned(getValue(nbrruns - 1))) {
      // we wipe out previous
      if (newend != oldend) {
        setValue(nbrruns - 1, (short) newend);
        setLength(nbrruns - 1, (short) (oldend - newend - 1));
        return;
      } else { // they cancel out
        nbrruns--;
        return;
      }
    }
    setLength(nbrruns - 1, (short) (val - toIntUnsigned(getValue(nbrruns - 1)) - 1));

    if (newend < oldend) {
      setValue(nbrruns, (short) newend);
      setLength(nbrruns, (short) (oldend - newend - 1));
      nbrruns++;
    } else if (oldend < newend) {
      setValue(nbrruns, (short) oldend);
      setLength(nbrruns, (short) (newend - oldend - 1));
      nbrruns++;
    }
  }

  private void smartAppendExclusive(short[] vl, short start, short length) {
    int oldend;
    if ((nbrruns == 0) || (
            toIntUnsigned(start) > (oldend = toIntUnsigned(getValue(nbrruns - 1))
            + toIntUnsigned(getLength(nbrruns - 1)) + 1))) { // we add a new one
      vl[2 * nbrruns] = start;
      vl[2 * nbrruns + 1] = length;
      nbrruns++;
      return;
    }
    if (oldend == toIntUnsigned(start)) {
      // we merge
      vl[2 * (nbrruns - 1) + 1] += length + 1;
      return;
    }


    int newend = toIntUnsigned(start) + toIntUnsigned(length) + 1;

    if (toIntUnsigned(start) == toIntUnsigned(getValue(nbrruns - 1))) {
      // we wipe out previous
      if (newend < oldend) {
        setValue(nbrruns - 1, (short) newend);
        setLength(nbrruns - 1, (short) (oldend - newend - 1));
        return;
      } else if (newend > oldend) {
        setValue(nbrruns - 1, (short) oldend);
        setLength(nbrruns - 1, (short) (newend - oldend - 1));
        return;
      } else { // they cancel out
        nbrruns--;
        return;
      }
    }
    setLength(nbrruns - 1, (short) (start - toIntUnsigned(getValue(nbrruns - 1)) - 1));

    if (newend < oldend) {
      setValue(nbrruns, (short) newend);
      setLength(nbrruns, (short) (oldend - newend - 1));
      nbrruns++;
    } else if (newend > oldend) {
      setValue(nbrruns, (short) oldend);
      setLength(nbrruns, (short) (newend - oldend - 1));
      nbrruns++;
    }
  }


  // convert to bitmap *if needed* (useful if you know it can't be an array)
  private MappeableContainer toBitmapIfNeeded() {
    int sizeAsRunContainer = MappeableRunContainer.serializedSizeInBytes(this.nbrruns);
    int sizeAsBitmapContainer = MappeableBitmapContainer.serializedSizeInBytes(0);
    if (sizeAsBitmapContainer > sizeAsRunContainer) {
      return this;
    }
    return toBitmapContainer();
  }

  /**
   * Convert the container to either a Bitmap or an Array Container, depending on the cardinality.
   *
   * @param card the current cardinality
   * @return new container
   */
  MappeableContainer toBitmapOrArrayContainer(int card) {
    // int card = this.getCardinality();
    if (card <= MappeableArrayContainer.DEFAULT_MAX_SIZE) {
      MappeableArrayContainer answer = new MappeableArrayContainer(card);
      answer.cardinality = 0;
      for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
        int runStart = toIntUnsigned(this.getValue(rlepos));
        int runEnd = runStart + toIntUnsigned(this.getLength(rlepos));

        for (int runValue = runStart; runValue <= runEnd; ++runValue) {
          answer.content.put(answer.cardinality++, (short) runValue);
        }
      }
      return answer;
    }
    MappeableBitmapContainer answer = new MappeableBitmapContainer();
    for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
      int start = toIntUnsigned(this.getValue(rlepos));
      int end = start + toIntUnsigned(this.getLength(rlepos)) + 1;
      BufferUtil.setBitmapRange(answer.bitmap, start, end);
    }
    answer.cardinality = card;
    return answer;
  }

  @Override
  public Container toContainer() {
    return new RunContainer(this);
  }


  // convert to bitmap or array *if needed*
  private MappeableContainer toEfficientContainer() {
    int sizeAsRunContainer = MappeableRunContainer.serializedSizeInBytes(this.nbrruns);
    int sizeAsBitmapContainer = MappeableBitmapContainer.serializedSizeInBytes(0);
    int card = this.getCardinality();
    int sizeAsArrayContainer = MappeableArrayContainer.serializedSizeInBytes(card);
    if (sizeAsRunContainer <= Math.min(sizeAsBitmapContainer, sizeAsArrayContainer)) {
      return this;
    }
    if (card <= MappeableArrayContainer.DEFAULT_MAX_SIZE) {
      MappeableArrayContainer answer = new MappeableArrayContainer(card);
      answer.cardinality = 0;
      for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
        int runStart = toIntUnsigned(this.getValue(rlepos));
        int runEnd = runStart + toIntUnsigned(this.getLength(rlepos));
        // next bit could potentially be faster, test
        if (BufferUtil.isBackedBySimpleArray(answer.content)) {
          short[] ba = answer.content.array();
          for (int runValue = runStart; runValue <= runEnd; ++runValue) {
            ba[answer.cardinality++] = (short) runValue;
          }
        } else {
          for (int runValue = runStart; runValue <= runEnd; ++runValue) {
            answer.content.put(answer.cardinality++, (short) runValue);
          }
        }
      }
      return answer;
    }
    MappeableBitmapContainer answer = new MappeableBitmapContainer();
    for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
      int start = toIntUnsigned(this.getValue(rlepos));
      int end = start + toIntUnsigned(this.getLength(rlepos)) + 1;
      BufferUtil.setBitmapRange(answer.bitmap, start, end);
    }
    answer.cardinality = card;
    return answer;
  }

  /**
   * Create a copy of the content of this container as a short array. This creates a copy.
   *
   * @return copy of the content as a short array
   */
  public short[] toShortArray() {
    short[] answer = new short[2 * nbrruns];
    valueslength.rewind();
    valueslength.get(answer);
    return answer;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (int k = 0; k < this.nbrruns; ++k) {
      sb.append("[");
      sb.append(toIntUnsigned(this.getValue(k)));
      sb.append(",");
      sb.append(toIntUnsigned(this.getValue(k))
          + toIntUnsigned(this.getLength(k)));
      sb.append("]");
    }
    return sb.toString();
  }

  @Override
  public void trim() {
    if (valueslength.limit() == 2 * nbrruns) {
      return;
    }
    if (BufferUtil.isBackedBySimpleArray(valueslength)) {
      this.valueslength = ShortBuffer.wrap(Arrays.copyOf(valueslength.array(), 2 * nbrruns));
    } else {

      final ShortBuffer co = ShortBuffer.allocate(2 * nbrruns);
      short[] a = co.array();
      for (int k = 0; k < 2 * nbrruns; ++k) {
        a[k] = this.valueslength.get(k);
      }
      this.valueslength = co;
    }
  }

  // To check if a value length contains a given value
  private boolean valueLengthContains(int value, int index) {
    int initialValue = toIntUnsigned(getValue(index));
    int length = toIntUnsigned(getLength(index));

    return value <= initialValue + length;
  }

  @Override
  protected void writeArray(DataOutput out) throws IOException {
    out.writeShort(Short.reverseBytes((short) this.nbrruns));
    for (int k = 0; k < 2 * this.nbrruns; ++k) {
      out.writeShort(Short.reverseBytes(this.valueslength.get(k)));
    }
  }

  @Override
  protected void writeArray(ByteBuffer buffer) {
    assert buffer.order() == LITTLE_ENDIAN;
    ShortBuffer source = valueslength.duplicate();
    source.position(0);
    source.limit(nbrruns * 2);
    ShortBuffer target = buffer.asShortBuffer();
    target.put((short)nbrruns);
    target.put(source);
    int bytesWritten = (nbrruns * 2 + 1) * 2;
    buffer.position(buffer.position() + bytesWritten);
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeShort(Short.reverseBytes((short) this.nbrruns));
    for (int k = 0; k < 2 * this.nbrruns; ++k) {
      out.writeShort(Short.reverseBytes(this.valueslength.get(k)));
    }
  }

  @Override
  public MappeableContainer xor(MappeableArrayContainer x) {
    // if the cardinality of the array is small, guess that the output will still be a run container
    final int arbitrary_threshold = 32; // 32 is arbitrary here
    if (x.getCardinality() < arbitrary_threshold) {
      return lazyxor(x).repairAfterLazy();
    }
    // otherwise, we expect the output to be either an array or bitmap
    final int card = getCardinality();
    if (card <= MappeableArrayContainer.DEFAULT_MAX_SIZE) {
      // if the cardinality is small, we construct the solution in place
      return x.xor(this.getShortIterator());
    }
    // otherwise, we generate a bitmap
    return toBitmapOrArrayContainer(card).ixor(x);
  }

  @Override
  public MappeableContainer xor(MappeableBitmapContainer x) {
    MappeableBitmapContainer answer = x.clone();
    for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
      int start = toIntUnsigned(this.getValue(rlepos));
      int end = start + toIntUnsigned(this.getLength(rlepos)) + 1;
      int prevOnes = answer.cardinalityInRange(start, end);
      BufferUtil.flipBitmapRange(answer.bitmap, start, end);
      answer.updateCardinality(prevOnes, end - start - prevOnes);
    }
    if (answer.getCardinality() > MappeableArrayContainer.DEFAULT_MAX_SIZE) {
      return answer;
    } else {
      return answer.toArrayContainer();
    }
  }

  @Override
  public MappeableContainer xor(MappeableRunContainer x) {
    if (x.nbrruns == 0) {
      return this.clone();
    }
    if (this.nbrruns == 0) {
      return x.clone();
    }
    MappeableRunContainer answer =
        new MappeableRunContainer(ShortBuffer.allocate(2 * (this.nbrruns + x.nbrruns)), 0);
    short[] vl = answer.valueslength.array();
    int rlepos = 0;
    int xrlepos = 0;

    while (true) {
      if (BufferUtil.compareUnsigned(getValue(rlepos), x.getValue(xrlepos)) < 0) {
        answer.smartAppendExclusive(vl, getValue(rlepos), getLength(rlepos));
        rlepos++;
        if (rlepos == this.nbrruns) {
          while (xrlepos < x.nbrruns) {
            answer.smartAppendExclusive(vl, x.getValue(xrlepos), x.getLength(xrlepos));
            xrlepos++;
          }
          break;
        }
      } else {
        answer.smartAppendExclusive(vl, x.getValue(xrlepos), x.getLength(xrlepos));
        xrlepos++;
        if (xrlepos == x.nbrruns) {
          while (rlepos < this.nbrruns) {
            answer.smartAppendExclusive(vl, getValue(rlepos), getLength(rlepos));
            rlepos++;
          }
          break;
        }
      }
    }
    return answer.toEfficientContainer();
  }


  @Override
  public void forEach(short msb, IntConsumer ic) {
    int high = ((int)msb) << 16;
    for(int k = 0; k < this.nbrruns; ++k) {
      int base = (this.getValue(k) & 0xFFFF) | high;
      int le = this.getLength(k) & 0xFFFF;
      for(int l = base; l <= base + le; ++l ) {
        ic.accept(l);
      }
    }
  }



  @Override
  public int andCardinality(MappeableArrayContainer x) {
    if (this.nbrruns == 0) {
      return 0;
    }
    int rlepos = 0;
    int arraypos = 0;
    int andCardinality = 0;
    int rleval = toIntUnsigned(this.getValue(rlepos));
    int rlelength = toIntUnsigned(this.getLength(rlepos));
    while (arraypos < x.cardinality) {
      int arrayval = toIntUnsigned(x.content.get(arraypos));
      while (rleval + rlelength < arrayval) {// this will frequently be false
        ++rlepos;
        if (rlepos == this.nbrruns) {
          return andCardinality;// we are done
        }
        rleval = toIntUnsigned(this.getValue(rlepos));
        rlelength = toIntUnsigned(this.getLength(rlepos));
      }
      if (rleval > arrayval) {
        arraypos = BufferUtil.advanceUntil(x.content, arraypos,
            x.cardinality, this.getValue(rlepos));
      } else {
        andCardinality++;
        arraypos++;
      }
    }
    return andCardinality;
  }


  @Override
  public int andCardinality(MappeableBitmapContainer x) {
    // could be implemented as return toBitmapOrArrayContainer().iand(x);
    int cardinality = 0;
    for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
      int runStart = toIntUnsigned(this.getValue(rlepos));
      int runEnd = runStart + toIntUnsigned(this.getLength(rlepos));
      for (int runValue = runStart; runValue <= runEnd; ++runValue) {
        if (x.contains((short) runValue)) {// it looks like contains() should be cheap enough if
                                           // accessed sequentially
          cardinality++;
        }
      }
    }
    return cardinality;
  }

  @Override
  public int andCardinality(MappeableRunContainer x) {
    int cardinality = 0;
    int rlepos = 0;
    int xrlepos = 0;
    int start = toIntUnsigned(this.getValue(rlepos));
    int end = start + toIntUnsigned(this.getLength(rlepos)) + 1;
    int xstart = toIntUnsigned(x.getValue(xrlepos));
    int xend = xstart + toIntUnsigned(x.getLength(xrlepos)) + 1;
    while ((rlepos < this.nbrruns) && (xrlepos < x.nbrruns)) {
      if (end <= xstart) {
        ++rlepos;
        if (rlepos < this.nbrruns) {
          start = toIntUnsigned(this.getValue(rlepos));
          end = start + toIntUnsigned(this.getLength(rlepos)) + 1;
        }
      } else if (xend <= start) {
        ++xrlepos;

        if (xrlepos < x.nbrruns) {
          xstart = toIntUnsigned(x.getValue(xrlepos));
          xend = xstart + toIntUnsigned(x.getLength(xrlepos)) + 1;
        }
      } else {// they overlap
        final int lateststart = start > xstart ? start : xstart;
        int earliestend;
        if (end == xend) {// improbable
          earliestend = end;
          rlepos++;
          xrlepos++;
          if (rlepos < this.nbrruns) {
            start = toIntUnsigned(this.getValue(rlepos));
            end = start + toIntUnsigned(this.getLength(rlepos)) + 1;
          }
          if (xrlepos < x.nbrruns) {
            xstart = toIntUnsigned(x.getValue(xrlepos));
            xend = xstart + toIntUnsigned(x.getLength(xrlepos)) + 1;
          }
        } else if (end < xend) {
          earliestend = end;
          rlepos++;
          if (rlepos < this.nbrruns) {
            start = toIntUnsigned(this.getValue(rlepos));
            end = start + toIntUnsigned(this.getLength(rlepos)) + 1;
          }

        } else {// end > xend
          earliestend = xend;
          xrlepos++;
          if (xrlepos < x.nbrruns) {
            xstart = toIntUnsigned(x.getValue(xrlepos));
            xend = xstart + toIntUnsigned(x.getLength(xrlepos)) + 1;
          }
        }
        // earliestend - lateststart are all values that are true.
        cardinality += earliestend - lateststart;
      }
    }
    return cardinality;
  }


  @Override
  public MappeableBitmapContainer toBitmapContainer() {
    int card = this.getCardinality();
    MappeableBitmapContainer answer = new MappeableBitmapContainer();
    for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
      int start = toIntUnsigned(this.getValue(rlepos));
      int end = start + toIntUnsigned(this.getLength(rlepos)) + 1;
      BufferUtil.setBitmapRange(answer.bitmap, start, end);
    }
    answer.cardinality = card;
    return answer;
  }

  @Override
  public int first() {
    assertNonEmpty(numberOfRuns() == 0);
    return toIntUnsigned(getValue(0));
  }

  @Override
  public int last() {
    assertNonEmpty(numberOfRuns() == 0);
    int index = numberOfRuns() - 1;
    int start = toIntUnsigned(getValue(index));
    int length = toIntUnsigned(getLength(index));
    return start + length;
  }

  @Override
  public int nextValue(short fromValue) {
    int index = bufferedUnsignedInterleavedBinarySearch(valueslength, 0, nbrruns, fromValue);
    int effectiveIndex = index >= 0 ? index : -index - 2;
    if (effectiveIndex == -1) {
      return first();
    }
    int startValue = toIntUnsigned(getValue(effectiveIndex));
    int value = toIntUnsigned(fromValue);
    int offset = value - startValue;
    int le = toIntUnsigned(getLength(effectiveIndex));
    if (offset <= le) {
      return value;
    }
    if (effectiveIndex + 1 < numberOfRuns()) {
      return toIntUnsigned(getValue(effectiveIndex + 1));
    }
    return -1;
  }

  @Override
  public int previousValue(short fromValue) {
    int index = bufferedUnsignedInterleavedBinarySearch(valueslength, 0, nbrruns, fromValue);
    int effectiveIndex = index >= 0 ? index : -index - 2;
    if (effectiveIndex == -1) {
      return -1;
    }
    int startValue = toIntUnsigned(getValue(effectiveIndex));
    int value = toIntUnsigned(fromValue);
    int offset = value - startValue;
    int le = toIntUnsigned(getLength(effectiveIndex));
    if (offset >= 0 && offset <= le) {
      return value;
    }
    return startValue + le;
  }

  @Override
  public int nextAbsentValue(short fromValue) {
    int index = bufferedUnsignedInterleavedBinarySearch(valueslength, 0, nbrruns, fromValue);
    int effectiveIndex = index >= 0 ? index : -index - 2;
    if (effectiveIndex == -1) {
      return toIntUnsigned(fromValue);
    }
    int startValue = toIntUnsigned(getValue(effectiveIndex));
    int value = toIntUnsigned(fromValue);
    int offset = value - startValue;
    int le = toIntUnsigned(getLength(effectiveIndex));
    return offset <= le ? startValue + le + 1 : value;
  }

  @Override
  public int previousAbsentValue(short fromValue) {
    int index = bufferedUnsignedInterleavedBinarySearch(valueslength, 0, nbrruns, fromValue);
    int effectiveIndex = index >= 0 ? index : -index - 2;
    if (effectiveIndex == -1) {
      return toIntUnsigned(fromValue);
    }
    int startValue = toIntUnsigned(getValue(effectiveIndex));
    int value = toIntUnsigned(fromValue);
    int offset = value - startValue;
    int le = toIntUnsigned(getLength(effectiveIndex));
    return offset <= le ? startValue - 1 : value;
  }

  @Override
  protected boolean contains(MappeableRunContainer runContainer) {
    int i1 = 0, i2 = 0;
    while(i1 < numberOfRuns() && i2 < runContainer.numberOfRuns()) {
      int start1 = toIntUnsigned(getValue(i1));
      int stop1 = start1 + toIntUnsigned(getLength(i1));
      int start2 = toIntUnsigned(runContainer.getValue(i2));
      int stop2 = start2 + toIntUnsigned(runContainer.getLength(i2));
      if(start1 > start2) {
        return false;
      } else {
        if(stop1 > stop2) {
          i2++;
        } else if(stop1 == stop2) {
          i1++;
          i2++;
        } else {
          i1++;
        }
      }
    }
    return i2 == runContainer.numberOfRuns();
  }

  @Override
  protected boolean contains(MappeableArrayContainer arrayContainer) {
    final int cardinality = getCardinality();
    final int runCount = numberOfRuns();
    if (arrayContainer.getCardinality() > cardinality) {
      return false;
    }
    int ia = 0, ir = 0;
    while(ia < arrayContainer.getCardinality() && ir < runCount) {
      int start = toIntUnsigned(getValue(ir));
      int stop = start + toIntUnsigned(getLength(ir));
      int value = toIntUnsigned(arrayContainer.content.get(ia));
      if(value < start) {
        return false;
      } else if (value > stop) {
        ++ir;
      } else {
        ++ia;
      }
    }
    return ia == arrayContainer.getCardinality();
  }

  @Override
  protected boolean contains(MappeableBitmapContainer bitmapContainer) {
    final int cardinality = getCardinality();
    if (bitmapContainer.getCardinality() != -1 && bitmapContainer.getCardinality() > cardinality) {
      return false;
    }
    final int runCount = numberOfRuns();
    short ib = 0, ir = 0;
    while(ib < MappeableBitmapContainer.MAX_CAPACITY / 64 && ir < runCount) {
      long w = bitmapContainer.bitmap.get(ib);
      while (w != 0 && ir < runCount) {
        int start = toIntUnsigned(getValue(ir));
        int stop = start+ toIntUnsigned(getLength(ir));
        long t = w & -w;
        long r = ib * 64 + Long.numberOfTrailingZeros(w);
        if (r < start) {
          return false;
        } else if(r > stop) {
          ++ir;
        } else {
          w ^= t;
        }
      }
      if(w == 0) {
        ++ib;
      } else {
        return false;
      }
    }
    if(ib < MappeableBitmapContainer.MAX_CAPACITY / 64) {
      for(; ib < MappeableBitmapContainer.MAX_CAPACITY / 64 ; ib++) {
        if(bitmapContainer.bitmap.get(ib) != 0) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public boolean intersects(int minimum, int supremum) {
    if((minimum < 0) || (supremum < minimum) || (supremum > (1<<16))) {
      throw new RuntimeException("This should never happen (bug).");
    }
    for (int i = 0; i < numberOfRuns(); ++i) {
      short runFirstValue = getValue(i);
      short runLastValue = (short) (runFirstValue + getLength(i));

      if (BufferUtil.toIntUnsigned(runFirstValue) < supremum
          && BufferUtil.compareUnsigned(runLastValue, (short)minimum) >= 0){
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean contains(int minimum, int supremum) {
    int count = 0;
    for (int i = 0; i < numberOfRuns(); ++i) {
      int start = toIntUnsigned(getValue(i));
      int length = toIntUnsigned(getLength(i));
      int stop = start + length;
      if (start >= supremum) {
        break;
      }
      if (stop >= supremum) {
        count += Math.max(0, supremum - start);
        break;
      }
      count += Math.min(Math.max(0, stop - minimum), length);
    }
    return count >= supremum - minimum - 1;
  }


}


final class MappeableRunContainerShortIterator implements PeekableShortIterator {
  int pos;
  int le = 0;
  int maxlength;
  int base;

  MappeableRunContainer parent;

  MappeableRunContainerShortIterator() {}

  MappeableRunContainerShortIterator(MappeableRunContainer p) {
    wrap(p);
  }

  @Override
  public PeekableShortIterator clone() {
    try {
      return (PeekableShortIterator) super.clone();
    } catch (CloneNotSupportedException e) {
      return null;// will not happen
    }
  }

  @Override
  public boolean hasNext() {
    return pos < parent.nbrruns;
  }

  @Override
  public short next() {
    short ans = (short) (base + le);
    le++;
    if (le > maxlength) {
      pos++;
      le = 0;
      if (pos < parent.nbrruns) {
        maxlength = toIntUnsigned(parent.getLength(pos));
        base = toIntUnsigned(parent.getValue(pos));
      }
    }
    return ans;
  }

  @Override
  public int nextAsInt() {
    int ans = base + le;
    le++;
    if (le > maxlength) {
      pos++;
      le = 0;
      if (pos < parent.nbrruns) {
        maxlength = toIntUnsigned(parent.getLength(pos));
        base = toIntUnsigned(parent.getValue(pos));
      }
    }
    return ans;
  }

  @Override
  public void remove() {
    throw new RuntimeException("Not implemented");// TODO
  }

  void wrap(MappeableRunContainer p) {
    parent = p;
    pos = 0;
    le = 0;
    if (pos < parent.nbrruns) {
      maxlength = toIntUnsigned(parent.getLength(pos));
      base = toIntUnsigned(parent.getValue(pos));
    }
  }

  @Override
  public void advanceIfNeeded(short minval) {
    while (base + maxlength < toIntUnsigned(minval)) {
      pos++;
      le = 0;
      if (pos < parent.nbrruns) {
        maxlength = toIntUnsigned(parent.getLength(pos));
        base = toIntUnsigned(parent.getValue(pos));
      } else {
        return;
      }
    }
    if (base > toIntUnsigned(minval)) {
      return;
    }
    le = toIntUnsigned(minval) - base;
  }

  @Override
  public short peekNext() {
    return (short) (base + le);
  }

}


final class RawMappeableRunContainerShortIterator implements PeekableShortIterator {
  int pos;
  int le = 0;
  int maxlength;
  int base;

  MappeableRunContainer parent;
  short[] vl;


  RawMappeableRunContainerShortIterator(MappeableRunContainer p) {
    wrap(p);
  }

  @Override
  public PeekableShortIterator clone() {
    try {
      return (PeekableShortIterator) super.clone();
    } catch (CloneNotSupportedException e) {
      return null;// will not happen
    }
  }

  short getLength(int index) {
    return vl[2 * index + 1];
  }

  short getValue(int index) {
    return vl[2 * index];
  }

  @Override
  public boolean hasNext() {
    return pos < parent.nbrruns;
  }

  @Override
  public short next() {
    short ans = (short) (base + le);
    le++;
    if (le > maxlength) {
      pos++;
      le = 0;
      if (pos < parent.nbrruns) {
        maxlength = toIntUnsigned(getLength(pos));
        base = toIntUnsigned(getValue(pos));
      }
    }
    return ans;
  }


  @Override
  public int nextAsInt() {
    int ans = base + le;
    le++;
    if (le > maxlength) {
      pos++;
      le = 0;
      if (pos < parent.nbrruns) {
        maxlength = toIntUnsigned(getLength(pos));
        base = toIntUnsigned(getValue(pos));
      }
    }
    return ans;
  }

  @Override
  public void remove() {
    throw new RuntimeException("Not implemented");// TODO
  }

  void wrap(MappeableRunContainer p) {
    parent = p;
    if (!parent.isArrayBacked()) {
      throw new RuntimeException("internal error");
    }
    vl = parent.valueslength.array();
    pos = 0;
    le = 0;
    if (pos < parent.nbrruns) {
      maxlength = toIntUnsigned(getLength(pos));
      base = toIntUnsigned(getValue(pos));
    }
  }

  @Override
  public void advanceIfNeeded(short minval) {
    while (base + maxlength < toIntUnsigned(minval)) {
      pos++;
      le = 0;
      if (pos < parent.nbrruns) {
        maxlength = toIntUnsigned(parent.getLength(pos));
        base = toIntUnsigned(parent.getValue(pos));
      } else {
        return;
      }
    }
    if (base > toIntUnsigned(minval)) {
      return;
    }
    le = toIntUnsigned(minval) - base;
  }

  @Override
  public short peekNext() {
    return (short) (base + le);
  }

}


final class RawReverseMappeableRunContainerShortIterator implements ShortIterator {
  int pos;
  int le;
  int maxlength;
  int base;
  MappeableRunContainer parent;
  short[] vl;



  RawReverseMappeableRunContainerShortIterator(MappeableRunContainer p) {
    wrap(p);
  }

  @Override
  public ShortIterator clone() {
    try {
      return (ShortIterator) super.clone();
    } catch (CloneNotSupportedException e) {
      return null;// will not happen
    }
  }

  short getLength(int index) {
    return vl[2 * index + 1];
  }

  short getValue(int index) {
    return vl[2 * index];
  }

  @Override
  public boolean hasNext() {
    return pos >= 0;
  }

  @Override
  public short next() {
    short ans = (short) (base + maxlength - le);
    le++;
    if (le > maxlength) {
      pos--;
      le = 0;
      if (pos >= 0) {
        maxlength = toIntUnsigned(getLength(pos));
        base = toIntUnsigned(getValue(pos));
      }
    }
    return ans;
  }

  @Override
  public int nextAsInt() {
    int ans = base + maxlength - le;
    le++;
    if (le > maxlength) {
      pos--;
      le = 0;
      if (pos >= 0) {
        maxlength = toIntUnsigned(getLength(pos));
        base = toIntUnsigned(getValue(pos));
      }
    }
    return ans;
  }

  @Override
  public void remove() {
    throw new RuntimeException("Not implemented");// TODO
  }

  void wrap(MappeableRunContainer p) {
    parent = p;
    if (!parent.isArrayBacked()) {
      throw new RuntimeException("internal error");
    }
    vl = parent.valueslength.array();
    pos = parent.nbrruns - 1;
    le = 0;
    if (pos >= 0) {
      maxlength = toIntUnsigned(getLength(pos));
      base = toIntUnsigned(getValue(pos));
    }
  }

}


final class ReverseMappeableRunContainerShortIterator implements ShortIterator {
  int pos;
  int le;
  int maxlength;
  int base;
  MappeableRunContainer parent;


  ReverseMappeableRunContainerShortIterator() {}

  ReverseMappeableRunContainerShortIterator(MappeableRunContainer p) {
    wrap(p);
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
  public boolean hasNext() {
    return pos >= 0;
  }

  @Override
  public short next() {
    short ans = (short) (base + maxlength - le);
    le++;
    if (le > maxlength) {
      pos--;
      le = 0;
      if (pos >= 0) {
        maxlength = toIntUnsigned(parent.getLength(pos));
        base = toIntUnsigned(parent.getValue(pos));
      }
    }
    return ans;
  }

  @Override
  public int nextAsInt() {
    int ans = base + maxlength - le;
    le++;
    if (le > maxlength) {
      pos--;
      le = 0;
      if (pos >= 0) {
        maxlength = toIntUnsigned(parent.getLength(pos));
        base = toIntUnsigned(parent.getValue(pos));
      }
    }
    return ans;
  }

  @Override
  public void remove() {
    throw new RuntimeException("Not implemented");// TODO
  }

  void wrap(MappeableRunContainer p) {
    parent = p;
    pos = parent.nbrruns - 1;
    le = 0;
    if (pos >= 0) {
      maxlength = toIntUnsigned(parent.getLength(pos));
      base = toIntUnsigned(parent.getValue(pos));
    }
  }

}
