/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */
package org.roaringbitmap;

import org.roaringbitmap.buffer.MappeableContainer;
import org.roaringbitmap.buffer.MappeableRunContainer;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.util.Arrays;
import java.util.Iterator;


/**
 * This container takes the form of runs of consecutive values (effectively, run-length encoding).
 *
 * Adding and removing content from this container might make it wasteful so regular calls to
 * "runOptimize" might be warranted.
 */
public final class RunContainer extends Container implements Cloneable {
  private static final int DEFAULT_INIT_SIZE = 4;
  private static final boolean ENABLE_GALLOPING_AND = false;

  private static final long serialVersionUID = 1L;

  private static int branchyUnsignedInterleavedBinarySearch(final char[] array, final int begin,
      final int end, final char k) {
    int low = begin;
    int high = end - 1;
    while (low <= high) {
      final int middleIndex = (low + high) >>> 1;
      final int middleValue = (array[2 * middleIndex]);
      if (middleValue < (int) (k)) {
        low = middleIndex + 1;
      } else if (middleValue > (int) (k)) {
        high = middleIndex - 1;
      } else {
        return middleIndex;
      }
    }
    return -(low + 1);
  }

  // starts with binary search and finishes with a sequential search
  private static int hybridUnsignedInterleavedBinarySearch(final char[] array, final int begin,
      final int end, final char k) {
    int low = begin;
    int high = end - 1;
    // 16 in the next line matches the size of a cache line
    while (low + 16 <= high) {
      final int middleIndex = (low + high) >>> 1;
      final int middleValue = (array[2 * middleIndex]);
      if (middleValue < (int) (k)) {
        low = middleIndex + 1;
      } else if (middleValue > (int) (k)) {
        high = middleIndex - 1;
      } else {
        return middleIndex;
      }
    }
    // we finish the job with a sequential search
    int x = low;
    for (; x <= high; ++x) {
      final int val = (array[2 * x]);
      if (val >= (int) (k)) {
        if (val == (int) (k)) {
          return x;
        }
        break;
      }
    }
    return -(x + 1);
  }

  protected static int serializedSizeInBytes(int numberOfRuns) {
    return 2 + 2 * 2 * numberOfRuns; // each run requires 2 2-byte entries.
  }

  private static int unsignedInterleavedBinarySearch(final char[] array, final int begin,
      final int end, final char k) {
    if (Util.USE_HYBRID_BINSEARCH) {
      return hybridUnsignedInterleavedBinarySearch(array, begin, end, k);
    } else {
      return branchyUnsignedInterleavedBinarySearch(array, begin, end, k);
    }

  }

  private char[] valueslength;// we interleave values and lengths, so
  // that if you have the values 11,12,13,14,15, you store that as 11,4 where 4 means that beyond 11
  // itself, there are
  // 4 contiguous values that follows.
  // Other example: e.g., 1, 10, 20,0, 31,2 would be a concise representation of 1, 2, ..., 11, 20,
  // 31, 32, 33

  int nbrruns = 0;// how many runs, this number should fit in 16 bits.


  /**
   * Create a container with default capacity
   */
  public RunContainer() {
    this(DEFAULT_INIT_SIZE);
  }


  protected RunContainer(ArrayContainer arr, int nbrRuns) {
    this.nbrruns = nbrRuns;
    valueslength = new char[2 * nbrRuns];
    if (nbrRuns == 0) {
      return;
    }

    int prevVal = -2;
    int runLen = 0;
    int runCount = 0;

    for (int i = 0; i < arr.cardinality; i++) {
      int curVal = arr.content[i];
      if (curVal == prevVal + 1) {
        ++runLen;
      } else {
        if (runCount > 0) {
          setLength(runCount - 1, (char) runLen);
        }
        setValue(runCount, (char) curVal);
        runLen = 0;
        ++runCount;
      }
      prevVal = curVal;
    }
    setLength(runCount - 1, (char) runLen);
  }

  /**
   * Create an run container with a run of ones from firstOfRun to lastOfRun.
   *
   * @param firstOfRun first index
   * @param lastOfRun last index (range is exclusive)
   */
  public RunContainer(final int firstOfRun, final int lastOfRun) {
    this.nbrruns = 1;
    this.valueslength = new char[]{(char) firstOfRun, (char) (lastOfRun - 1 - firstOfRun)};
  }

  // convert a bitmap container to a run container somewhat efficiently.
  protected RunContainer(BitmapContainer bc, int nbrRuns) {
    this.nbrruns = nbrRuns;
    valueslength = new char[2 * nbrRuns];
    if (nbrRuns == 0) {
      return;
    }

    int longCtr = 0; // index of current long in bitmap
    long curWord = bc.bitmap[0]; // its value
    int runCount = 0;
    while (true) {
      // potentially multiword advance to first 1 bit
      while (curWord == 0L && longCtr < bc.bitmap.length - 1) {
        curWord = bc.bitmap[++longCtr];
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
      int runEnd;
      while (curWordWith1s == -1L && longCtr < bc.bitmap.length - 1) {
        curWordWith1s = bc.bitmap[++longCtr];
      }

      if (curWordWith1s == -1L) {
        // a final unterminated run of 1s (32 of them)
        runEnd = 64 + longCtr * 64;
        setValue(runCount, (char) runStart);
        setLength(runCount, (char) (runEnd - runStart - 1));
        return;
      }
      int localRunEnd = Long.numberOfTrailingZeros(~curWordWith1s);
      runEnd = localRunEnd + longCtr * 64;
      setValue(runCount, (char) runStart);
      setLength(runCount, (char) (runEnd - runStart - 1));
      runCount++;
      // now, zero out everything right of runEnd.
      curWord = curWordWith1s & (curWordWith1s + 1);
      // We've lathered and rinsed, so repeat...
    }
  }

  /**
   * Create an array container with specified capacity
   *
   * @param capacity The capacity of the container
   */
  public RunContainer(final int capacity) {
    valueslength = new char[2 * capacity];
  }


  private RunContainer(int nbrruns, char[] valueslength) {
    this.nbrruns = nbrruns;
    this.valueslength = Arrays.copyOf(valueslength, valueslength.length);
  }


  /**
   * Creates a new non-mappeable container from a mappeable one. This copies the data.
   *
   * @param bc the original container
   */
  public RunContainer(MappeableRunContainer bc) {
    this.nbrruns = bc.numberOfRuns();
    this.valueslength = bc.toCharArray();
  }

  /**
   * Construct a new RunContainer backed by the provided array. Note that if you modify the
   * RunContainer a new array may be produced.
   *
   * @param array array where the data is stored
   * @param numRuns number of runs (each using 2 shorts in the buffer)
   *
   */
  public RunContainer(final char[] array, final int numRuns) {
    if (array.length < 2 * numRuns) {
      throw new RuntimeException("Mismatch between buffer and numRuns");
    }
    this.nbrruns = numRuns;
    this.valueslength = array;
  }

  @Override
  public Container add(int begin, int end) {
    RunContainer rc = (RunContainer) clone();
    return rc.iadd(begin, end);
  }

  @Override
  public Container add(char k) {
    // TODO: it might be better and simpler to do return
    // toBitmapOrArrayContainer(getCardinality()).add(k)
    // but note that some unit tests use this method to build up test runcontainers without calling
    // runOptimize
    int index = unsignedInterleavedBinarySearch(valueslength, 0, nbrruns, k);
    if (index >= 0) {
      return this;// already there
    }
    index = -index - 2;// points to preceding value, possibly -1
    if (index >= 0) {// possible match
      int offset = (k) - (getValue(index));
      int le = (getLength(index));
      if (offset <= le) {
        return this;
      }
      if (offset == le + 1) {
        // we may need to fuse
        if (index + 1 < nbrruns) {
          if ((getValue(index + 1)) == (k) + 1) {
            // indeed fusion is needed
            setLength(index,
                (char) (getValue(index + 1) + getLength(index + 1) - getValue(index)));
            recoverRoomAtIndex(index + 1);
            return this;
          }
        }
        incrementLength(index);
        return this;
      }
      if (index + 1 < nbrruns) {
        // we may need to fuse
        if ((getValue(index + 1)) == (k) + 1) {
          // indeed fusion is needed
          setValue(index + 1, k);
          setLength(index + 1, (char) (getLength(index + 1) + 1));
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
    setLength(index + 1, (char) 0);
    return this;
  }

  @Override
  public Container and(ArrayContainer x) {
    ArrayContainer ac = new ArrayContainer(x.cardinality);
    if (this.nbrruns == 0) {
      return ac;
    }
    int rlepos = 0;
    int arraypos = 0;

    int rleval = (this.getValue(rlepos));
    int rlelength = (this.getLength(rlepos));
    while (arraypos < x.cardinality) {
      int arrayval = (x.content[arraypos]);
      while (rleval + rlelength < arrayval) {// this will frequently be false
        ++rlepos;
        if (rlepos == this.nbrruns) {
          return ac;// we are done
        }
        rleval = (this.getValue(rlepos));
        rlelength = (this.getLength(rlepos));
      }
      if (rleval > arrayval) {
        arraypos = Util.advanceUntil(x.content, arraypos, x.cardinality, (char)rleval);
      } else {
        ac.content[ac.cardinality] = (char) arrayval;
        ac.cardinality++;
        arraypos++;
      }
    }
    return ac;
  }


  @Override
  public Container and(BitmapContainer x) {
    // could be implemented as return toBitmapOrArrayContainer().iand(x);
    int card = this.getCardinality();
    if (card <= ArrayContainer.DEFAULT_MAX_SIZE) {
      // result can only be an array (assuming that we never make a RunContainer)
      if (card > x.cardinality) {
        card = x.cardinality;
      }
      ArrayContainer answer = new ArrayContainer(card);
      answer.cardinality = 0;
      for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
        int runStart = (this.getValue(rlepos));
        int runEnd = runStart + (this.getLength(rlepos));
        for (int runValue = runStart; runValue <= runEnd; ++runValue) {
          if (x.contains((char) runValue)) {// it looks like contains() should be cheap enough if
                                             // accessed sequentially
            answer.content[answer.cardinality++] = (char) runValue;
          }
        }
      }
      return answer;
    }
    // we expect the answer to be a bitmap (if we are lucky)
    BitmapContainer answer = x.clone();
    int start = 0;
    for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
      int end = (this.getValue(rlepos));
      int prevOnes = answer.cardinalityInRange(start, end);
      Util.resetBitmapRange(answer.bitmap, start, end); // had been x.bitmap
      answer.updateCardinality(prevOnes, 0);
      start = end + (this.getLength(rlepos)) + 1;
    }
    int ones = answer.cardinalityInRange(start, BitmapContainer.MAX_CAPACITY);
    Util.resetBitmapRange(answer.bitmap, start, BitmapContainer.MAX_CAPACITY); // had been x.bitmap
    answer.updateCardinality(ones, 0);
    if (answer.getCardinality() > ArrayContainer.DEFAULT_MAX_SIZE) {
      return answer;
    } else {
      return answer.toArrayContainer();
    }
  }

  @Override
  public Container and(RunContainer x) {
    int maxRunsAfterIntersection = nbrruns + x.nbrruns;
    RunContainer answer = new RunContainer(new char[2 * maxRunsAfterIntersection], 0);
    if (isEmpty()) {
      return answer;
    }
    int rlepos = 0;
    int xrlepos = 0;
    int start = this.getValue(rlepos);
    int end = start + this.getLength(rlepos) + 1;
    int xstart = x.getValue(xrlepos);
    int xend = xstart + x.getLength(xrlepos) + 1;
    while (rlepos < this.nbrruns && xrlepos < x.nbrruns) {
      if (end <= xstart) {
        if (ENABLE_GALLOPING_AND) {
          rlepos = skipAhead(this, rlepos, xstart); // skip over runs until we have end > xstart (or
                                                    // rlepos is advanced beyond end)
        } else {
          ++rlepos;
        }

        if (rlepos < this.nbrruns) {
          start = this.getValue(rlepos);
          end = start + this.getLength(rlepos) + 1;
        }
      } else if (xend <= start) {
        // exit the second run
        if (ENABLE_GALLOPING_AND) {
          xrlepos = skipAhead(x, xrlepos, start);
        } else {
          ++xrlepos;
        }

        if (xrlepos < x.nbrruns) {
          xstart = x.getValue(xrlepos);
          xend = xstart + x.getLength(xrlepos) + 1;
        }
      } else {// they overlap
        final int lateststart = Math.max(start, xstart);
        int earliestend;
        if (end == xend) {// improbable
          earliestend = end;
          rlepos++;
          xrlepos++;
          if (rlepos < this.nbrruns) {
            start = this.getValue(rlepos);
            end = start + this.getLength(rlepos) + 1;
          }
          if (xrlepos < x.nbrruns) {
            xstart = x.getValue(xrlepos);
            xend = xstart + x.getLength(xrlepos) + 1;
          }
        } else if (end < xend) {
          earliestend = end;
          rlepos++;
          if (rlepos < this.nbrruns) {
            start = this.getValue(rlepos);
            end = start + this.getLength(rlepos) + 1;
          }

        } else {// end > xend
          earliestend = xend;
          xrlepos++;
          if (xrlepos < x.nbrruns) {
            xstart = x.getValue(xrlepos);
            xend = xstart + x.getLength(xrlepos) + 1;
          }
        }
        answer.valueslength[2 * answer.nbrruns] = (char) lateststart;
        answer.valueslength[2 * answer.nbrruns + 1] = (char) (earliestend - lateststart - 1);
        answer.nbrruns++;
      }
    }
    return answer.toEfficientContainer(); // subsequent trim() may be required to avoid wasted
                                          // space.
  }

  @Override
  public int andCardinality(ArrayContainer x) {
    if (this.nbrruns == 0) {
      return x.cardinality;
    }
    int rlepos = 0;
    int arraypos = 0;
    int andCardinality = 0;
    int rleval = (this.getValue(rlepos));
    int rlelength = (this.getLength(rlepos));
    while (arraypos < x.cardinality) {
      int arrayval = (x.content[arraypos]);
      while (rleval + rlelength < arrayval) {// this will frequently be false
        ++rlepos;
        if (rlepos == this.nbrruns) {
          return andCardinality;// we are done
        }
        rleval = (this.getValue(rlepos));
        rlelength = (this.getLength(rlepos));
      }
      if (rleval > arrayval) {
        arraypos = Util.advanceUntil(x.content, arraypos, x.cardinality, this.getValue(rlepos));
      } else {
        andCardinality++;
        arraypos++;
      }
    }
    return andCardinality;
  }


  @Override
  public int andCardinality(BitmapContainer x) {
    // could be implemented as return toBitmapOrArrayContainer().iand(x);
    int cardinality = 0;
    for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
      int runStart = this.getValue(rlepos);
      int runEnd = runStart + this.getLength(rlepos);
      cardinality += x.cardinalityInRange(runStart, runEnd + 1);
    }
    return cardinality;
  }

  @Override
  public int andCardinality(RunContainer x) {
    int cardinality = 0;
    int rlepos = 0;
    int xrlepos = 0;
    int start = (this.getValue(rlepos));
    int end = start + (this.getLength(rlepos)) + 1;
    int xstart = (x.getValue(xrlepos));
    int xend = xstart + (x.getLength(xrlepos)) + 1;
    while ((rlepos < this.nbrruns) && (xrlepos < x.nbrruns)) {
      if (end <= xstart) {
        if (ENABLE_GALLOPING_AND) {
          rlepos = skipAhead(this, rlepos, xstart); // skip over runs until we have end > xstart (or
                                                    // rlepos is advanced beyond end)
        } else {
          ++rlepos;
        }

        if (rlepos < this.nbrruns) {
          start = (this.getValue(rlepos));
          end = start + (this.getLength(rlepos)) + 1;
        }
      } else if (xend <= start) {
        // exit the second run
        if (ENABLE_GALLOPING_AND) {
          xrlepos = skipAhead(x, xrlepos, start);
        } else {
          ++xrlepos;
        }

        if (xrlepos < x.nbrruns) {
          xstart = (x.getValue(xrlepos));
          xend = xstart + (x.getLength(xrlepos)) + 1;
        }
      } else {// they overlap
        final int lateststart = Math.max(start, xstart);
        int earliestend;
        if (end == xend) {// improbable
          earliestend = end;
          rlepos++;
          xrlepos++;
          if (rlepos < this.nbrruns) {
            start = (this.getValue(rlepos));
            end = start + (this.getLength(rlepos)) + 1;
          }
          if (xrlepos < x.nbrruns) {
            xstart = (x.getValue(xrlepos));
            xend = xstart + (x.getLength(xrlepos)) + 1;
          }
        } else if (end < xend) {
          earliestend = end;
          rlepos++;
          if (rlepos < this.nbrruns) {
            start = (this.getValue(rlepos));
            end = start + (this.getLength(rlepos)) + 1;
          }

        } else {// end > xend
          earliestend = xend;
          xrlepos++;
          if (xrlepos < x.nbrruns) {
            xstart = (x.getValue(xrlepos));
            xend = xstart + (x.getLength(xrlepos)) + 1;
          }
        }
        // earliestend - lateststart are all values that are true.
        cardinality += earliestend - lateststart;
      }
    }
    return cardinality;
  }

  @Override
  public Container andNot(ArrayContainer x) {
    // when x is small, we guess that the result will still be a run container
    final int arbitrary_threshold = 32; // this is arbitrary
    if (x.getCardinality() < arbitrary_threshold) {
      return lazyandNot(x).toEfficientContainer();
    }
    // otherwise we generate either an array or bitmap container
    final int card = getCardinality();
    if (card <= ArrayContainer.DEFAULT_MAX_SIZE) {
      // if the cardinality is small, we construct the solution in place
      ArrayContainer ac = new ArrayContainer(card);
      ac.cardinality =
          Util.unsignedDifference(this.getCharIterator(), x.getCharIterator(), ac.content);
      return ac;
    }
    // otherwise, we generate a bitmap
    return toBitmapOrArrayContainer(card).iandNot(x);
  }

  @Override
  public Container andNot(BitmapContainer x) {
    // could be implemented as toTemporaryBitmap().iandNot(x);
    int card = this.getCardinality();
    if (card <= ArrayContainer.DEFAULT_MAX_SIZE) {
      // result can only be an array (assuming that we never make a RunContainer)
      ArrayContainer answer = new ArrayContainer(card);
      answer.cardinality = 0;
      for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
        int runStart = (this.getValue(rlepos));
        int runEnd = runStart + (this.getLength(rlepos));
        for (int runValue = runStart; runValue <= runEnd; ++runValue) {
          if (!x.contains((char) runValue)) {// it looks like contains() should be cheap enough if
                                              // accessed sequentially
            answer.content[answer.cardinality++] = (char) runValue;
          }
        }
      }
      return answer;
    }
    // we expect the answer to be a bitmap (if we are lucky)
    BitmapContainer answer = x.clone();
    int lastPos = 0;
    for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
      int start = (this.getValue(rlepos));
      int end = start + (this.getLength(rlepos)) + 1;
      int prevOnes = answer.cardinalityInRange(lastPos, start);
      int flippedOnes = answer.cardinalityInRange(start, end);
      Util.resetBitmapRange(answer.bitmap, lastPos, start);
      Util.flipBitmapRange(answer.bitmap, start, end);
      answer.updateCardinality(prevOnes + flippedOnes, end - start - flippedOnes);
      lastPos = end;
    }
    int ones = answer.cardinalityInRange(lastPos, BitmapContainer.MAX_CAPACITY);
    Util.resetBitmapRange(answer.bitmap, lastPos, BitmapContainer.MAX_CAPACITY);
    answer.updateCardinality(ones, 0);
    if (answer.getCardinality() > ArrayContainer.DEFAULT_MAX_SIZE) {
      return answer;
    } else {
      return answer.toArrayContainer();
    }
  }

  @Override
  public Container andNot(RunContainer x) {
    RunContainer answer = new RunContainer(new char[2 * (this.nbrruns + x.nbrruns)], 0);
    int rlepos = 0;
    int xrlepos = 0;
    int start = (this.getValue(rlepos));
    int end = start + (this.getLength(rlepos)) + 1;
    int xstart = (x.getValue(xrlepos));
    int xend = xstart + (x.getLength(xrlepos)) + 1;
    while ((rlepos < this.nbrruns) && (xrlepos < x.nbrruns)) {
      if (end <= xstart) {
        // output the first run
        answer.valueslength[2 * answer.nbrruns] = (char) start;
        answer.valueslength[2 * answer.nbrruns + 1] = (char) (end - start - 1);
        answer.nbrruns++;
        rlepos++;
        if (rlepos < this.nbrruns) {
          start = (this.getValue(rlepos));
          end = start + (this.getLength(rlepos)) + 1;
        }
      } else if (xend <= start) {
        // exit the second run
        xrlepos++;
        if (xrlepos < x.nbrruns) {
          xstart = (x.getValue(xrlepos));
          xend = xstart + (x.getLength(xrlepos)) + 1;
        }
      } else {
        if (start < xstart) {
          answer.valueslength[2 * answer.nbrruns] = (char) start;
          answer.valueslength[2 * answer.nbrruns + 1] = (char) (xstart - start - 1);
          answer.nbrruns++;
        }
        if (xend < end) {
          start = xend;
        } else {
          rlepos++;
          if (rlepos < this.nbrruns) {
            start = (this.getValue(rlepos));
            end = start + (this.getLength(rlepos)) + 1;
          }
        }
      }
    }
    if (rlepos < this.nbrruns) {
      answer.valueslength[2 * answer.nbrruns] = (char) start;
      answer.valueslength[2 * answer.nbrruns + 1] = (char) (end - start - 1);
      answer.nbrruns++;
      rlepos++;
      if (rlepos < this.nbrruns) {
        System.arraycopy(this.valueslength, 2 * rlepos, answer.valueslength, 2 * answer.nbrruns,
            2 * (this.nbrruns - rlepos));
        answer.nbrruns = answer.nbrruns + this.nbrruns - rlepos;
      }
    }
    return answer.toEfficientContainer();
  }

  // Append a value length with all values until a given value
  private void appendValueLength(int value, int index) {
    int previousValue = (getValue(index));
    int length = (getLength(index));
    int offset = value - previousValue;
    if (offset > length) {
      setLength(index, (char) offset);
    }
  }

  // To check if a value length can be prepended with a given value
  private boolean canPrependValueLength(int value, int index) {
    if (index < this.nbrruns) {
      int nextValue = (getValue(index));
      return nextValue == value + 1;
    }
    return false;
  }

  @Override
  public void clear() {
    nbrruns = 0;
  }

  @Override
  public Container clone() {
    return new RunContainer(nbrruns, valueslength);
  }

  @Override
  public boolean isEmpty() {
    return nbrruns == 0;
  }

  // To set the last value of a value length
  private void closeValueLength(int value, int index) {
    int initialValue = (getValue(index));
    setLength(index, (char) (value - initialValue));
  }

  @Override
  public boolean contains(char x) {
    int index = unsignedInterleavedBinarySearch(valueslength, 0, nbrruns, x);
    if (index >= 0) {
      return true;
    }
    index = -index - 2; // points to preceding value, possibly -1
    if (index != -1) {// possible match
      int offset = x - getValue(index);
      int le = getLength(index);
      return offset <= le;
    }
    return false;
  }

  @Override
  public boolean contains(int minimum, int supremum) {
    for (int i = 0; i < numberOfRuns(); ++i) {
      int start = getValue(i);
      int length = getLength(i);
      int stop = start + length + 1;
      if (start >= supremum) {
        break;
      }
      if (minimum >= start && supremum <= stop) {
        return true;
      }
    }
    return false;
  }

  @Override
  protected boolean contains(RunContainer runContainer) {
    int i1 = 0, i2 = 0;
    while(i1 < numberOfRuns() && i2 < runContainer.numberOfRuns()) {
      int start1 = (getValue(i1));
      int stop1 = start1 + (getLength(i1));
      int start2 = (runContainer.getValue(i2));
      int stop2 = start2 + (runContainer.getLength(i2));
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
  protected boolean contains(ArrayContainer arrayContainer) {
    final int cardinality = getCardinality();
    final int runCount = numberOfRuns();
    if (arrayContainer.getCardinality() > cardinality) {
      return false;
    }
    int ia = 0, ir = 0;
    while(ia < arrayContainer.getCardinality() && ir < runCount) {
      int start = (this.getValue(ir));
      int stop = start + (getLength(ir));
      int ac = (arrayContainer.content[ia]);
      if(ac < start) {
        return false;
      } else if (ac > stop) {
        ++ir;
      } else {
        ++ia;
      }
    }
    return ia == arrayContainer.getCardinality();
  }

  @Override
  protected boolean contains(BitmapContainer bitmapContainer) {
    final int cardinality = getCardinality();
    if (bitmapContainer.getCardinality() != -1 && bitmapContainer.getCardinality() > cardinality) {
      return false;
    }
    final int runCount = numberOfRuns();
    char ib = 0, ir = 0;
    while(ib < bitmapContainer.bitmap.length && ir < runCount) {
      long w = bitmapContainer.bitmap[ib];
      while (w != 0 && ir < runCount) {
        int start = (getValue(ir));
        int stop = start+ (getLength(ir));
        long t = w & -w;
        long r = ib * 64L + Long.numberOfTrailingZeros(w);
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
    if(ib < bitmapContainer.bitmap.length) {
      for(; ib < bitmapContainer.bitmap.length ; ib++) {
        if(bitmapContainer.bitmap[ib] != 0) {
          return false;
        }
      }
    }
    return true;
  }


  // a very cheap check... if you have more than 4096, then you should use a bitmap container.
  // this function avoids computing the cardinality
  private Container convertToLazyBitmapIfNeeded() {
    // when nbrruns exceed ArrayContainer.DEFAULT_MAX_SIZE, then we know it should be stored as a
    // bitmap, always
    if (this.nbrruns > ArrayContainer.DEFAULT_MAX_SIZE) {
      BitmapContainer answer = new BitmapContainer();
      for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
        int start = (this.getValue(rlepos));
        int end = start + (this.getLength(rlepos)) + 1;
        Util.setBitmapRange(answer.bitmap, start, end);
      }
      answer.cardinality = -1;
      return answer;
    }
    return this;
  }



  // Push all values length to the end of the array (resize array if needed)
  private void copyToOffset(int offset) {
    final int minCapacity = 2 * (offset + nbrruns);
    if (valueslength.length < minCapacity) {
      // expensive case where we need to reallocate
      int newCapacity = valueslength.length;
      while (newCapacity < minCapacity) {
        newCapacity = (newCapacity == 0) ? DEFAULT_INIT_SIZE
            : newCapacity < 64 ? newCapacity * 2
                : newCapacity < 1024 ? newCapacity * 3 / 2 : newCapacity * 5 / 4;
      }
      char[] newvalueslength = new char[newCapacity];
      copyValuesLength(this.valueslength, 0, newvalueslength, offset, nbrruns);
      this.valueslength = newvalueslength;
    } else {
      // efficient case where we just copy
      copyValuesLength(this.valueslength, 0, this.valueslength, offset, nbrruns);
    }
  }

  private void copyValuesLength(char[] src, int srcIndex, char[] dst, int dstIndex, int length) {
    System.arraycopy(src, 2 * srcIndex, dst, 2 * dstIndex, 2 * length);
  }

  private void decrementLength(int index) {
    valueslength[2 * index + 1]--;// caller is responsible to ensure that value is non-zero
  }



  private void decrementValue(int index) {
    valueslength[2 * index]--;
  }

  @Override
  public void deserialize(DataInput in) throws IOException {
    nbrruns = Character.reverseBytes(in.readChar());
    if (valueslength.length < 2 * nbrruns) {
      valueslength = new char[2 * nbrruns];
    }
    for (int k = 0; k < 2 * nbrruns; ++k) {
      this.valueslength[k] = Character.reverseBytes(in.readChar());
    }
  }

  // not actually used anywhere, but potentially useful
  void ensureCapacity(int minNbRuns) {
    final int minCapacity = 2 * minNbRuns;
    if (valueslength.length < minCapacity) {
      int newCapacity = valueslength.length;
      while (newCapacity < minCapacity) {
        newCapacity = (newCapacity == 0) ? DEFAULT_INIT_SIZE
            : newCapacity < 64 ? newCapacity * 2
                : newCapacity < 1024 ? newCapacity * 3 / 2 : newCapacity * 5 / 4;
      }
      char[] nv = new char[newCapacity];
      copyValuesLength(valueslength, 0, nv, 0, nbrruns);
      valueslength = nv;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof RunContainer) {
      return equals((RunContainer) o);
    } else if (o instanceof ArrayContainer) {
      return equals((ArrayContainer) o);
    } else if (o instanceof Container) {
      if (((Container) o).getCardinality() != this.getCardinality()) {
        return false; // should be a frequent branch if they differ
      }
      // next bit could be optimized if needed:
      CharIterator me = this.getCharIterator();
      CharIterator you = ((Container) o).getCharIterator();
      while (me.hasNext()) {
        if (me.next() != you.next()) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  private boolean equals(RunContainer rc) {
    return ArraysShim.equals(valueslength, 0, 2 * nbrruns,
            rc.valueslength, 0, 2 * rc.nbrruns);
  }

  private boolean equals(ArrayContainer arrayContainer) {
    int pos = 0;
    for (char i = 0; i < nbrruns; ++i) {
      char runStart = getValue(i);
      int length = (getLength(i));
      if (pos + length >= arrayContainer.getCardinality()) {
        return false;
      }
      if (arrayContainer.content[pos] != runStart) {
        return false;
      }
      if (arrayContainer.content[pos + length] != (char)((runStart) + length)) {
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
      final int limit = (this.getLength(k));
      final int base = (this.getValue(k));
      for (int le = 0; le <= limit; ++le) {
        x[pos++] = (base + le) | mask;
      }
    }
  }

  @Override
  public Container flip(char x) {
    if (this.contains(x)) {
      return this.remove(x);
    } else {
      return this.add(x);
    }
  }

  @Override
  public int getArraySizeInBytes() {
    return 2 + 4 * this.nbrruns; // "array" includes its size
  }


  @Override
  public int getCardinality() {
    int sum = nbrruns;// lengths are returned -1
    for (int k = 0; k < nbrruns; ++k) {
      sum = sum + (getLength(k))/* + 1 */;
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
  public char getLength(int index) {
    return valueslength[2 * index + 1];
  }

  @Override
  public PeekableCharIterator getReverseCharIterator() {
    return new ReverseRunContainerCharIterator(this);
  }

  @Override
  public PeekableCharIterator getCharIterator() {
    return new RunContainerCharIterator(this);
  }

  @Override
  public PeekableCharRankIterator getCharRankIterator() {
    return new RunContainerCharRankIterator(this);
  }

  @Override
  public ContainerBatchIterator getBatchIterator() {
    return new RunBatchIterator(this);
  }

  @Override
  public int getSizeInBytes() {
    return this.nbrruns * 4 + 4;
  }

  /**
   * Gets the value of the first element of the run at the index.
   * @param index the index of the run.
   * @return the value of the first element of the run at the index.
   * @throws ArrayIndexOutOfBoundsException if index is negative or larger than the index of the
   *     last run.
   */
  public char getValue(int index) {
    return valueslength[2 * index];
  }

  @Override
  public int hashCode() {
    int hash = 0;
    for (int k = 0; k < nbrruns * 2; ++k) {
      hash += 31 * hash + valueslength[k];
    }
    return hash;
  }

  @Override
  public Container iadd(int begin, int end) {
    // TODO: it might be better and simpler to do return
    // toBitmapOrArrayContainer(getCardinality()).iadd(begin,end)
    if(end == begin) {
      return this;
    }
    if ((begin > end) || (end > (1 << 16))) {
      throw new IllegalArgumentException("Invalid range [" + begin + "," + end + ")");
    }

    if (begin == end - 1) {
      add((char) begin);
      return this;
    }

    int bIndex = unsignedInterleavedBinarySearch(this.valueslength, 0, this.nbrruns, (char) begin);
    int eIndex =
        unsignedInterleavedBinarySearch(this.valueslength, 0, this.nbrruns, (char) (end - 1));

    if (bIndex >= 0 && eIndex >= 0) {
      mergeValuesLength(bIndex, eIndex);
      return this;

    } else if (bIndex >= 0) {
      eIndex = -eIndex - 2;

      if (canPrependValueLength(end - 1, eIndex + 1)) {
        mergeValuesLength(bIndex, eIndex + 1);
        return this;
      }

      appendValueLength(end - 1, eIndex);
      mergeValuesLength(bIndex, eIndex);
      return this;

    } else if (eIndex >= 0) {
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
              setValue(eIndex + 1, (char) begin);
              setLength(eIndex + 1, (char) (end - 1 - begin));
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
          setValue(0, (char) begin);
          setLength(0, (char) (end - 1 - begin));
        }
        return this;
      }
    }
  }

  @Override
  public Container iand(ArrayContainer x) {
    return and(x);
  }

  @Override
  public Container iand(BitmapContainer x) {
    return and(x);
  }



  @Override
  public Container iand(RunContainer x) {
    return and(x);
  }


  @Override
  public Container iandNot(ArrayContainer x) {
    return andNot(x);
  }

  @Override
  public Container iandNot(BitmapContainer x) {
    return andNot(x);
  }

  @Override
  public Container iandNot(RunContainer x) {
    return andNot(x);
  }

  Container ilazyor(ArrayContainer x) {
    if (isFull()) {
      return this; // this can sometimes solve a lot of computation!
    }
    return ilazyorToRun(x);
  }

  private Container ilazyorToRun(ArrayContainer x) {
    if (isFull()) {
      return full();
    }
    final int nbrruns = this.nbrruns;
    final int offset = Math.max(nbrruns, x.getCardinality());
    copyToOffset(offset);
    int rlepos = 0;
    this.nbrruns = 0;
    PeekableCharIterator i = x.getCharIterator();
    while (i.hasNext() && (rlepos < nbrruns)) {
      if (getValue(rlepos + offset) - i.peekNext() <= 0) {
        smartAppend(getValue(rlepos + offset), getLength(rlepos + offset));
        rlepos++;
      } else {
        smartAppend(i.next());
      }
    }
    if (i.hasNext()) {
      /*
       * if(this.nbrruns>0) { // this might be useful if the run container has just one very large
       * run int lastval = (getValue(nbrruns + offset - 1)) +
       * (getLength(nbrruns + offset - 1)) + 1; i.advanceIfNeeded((char)
       * lastval); }
       */
      while (i.hasNext()) {
        smartAppend(i.next());
      }
    } else {
      while (rlepos < nbrruns) {
        smartAppend(getValue(rlepos + offset), getLength(rlepos + offset));
        rlepos++;
      }
    }
    return convertToLazyBitmapIfNeeded();
  }

  private void increaseCapacity() {
    int newCapacity = (valueslength.length == 0) ? DEFAULT_INIT_SIZE
        : valueslength.length < 64 ? valueslength.length * 2
            : valueslength.length < 1024 ? valueslength.length * 3 / 2
                : valueslength.length * 5 / 4;
    char[] nv = new char[newCapacity];
    System.arraycopy(valueslength, 0, nv, 0, 2 * nbrruns);
    valueslength = nv;
  }


  private void incrementLength(int index) {
    valueslength[2 * index + 1]++;
  }


  private void incrementValue(int index) {
    valueslength[2 * index]++;
  }

  // To set the first value of a value length
  private void initValueLength(int value, int index) {
    int initialValue = (getValue(index));
    int length = (getLength(index));
    setValue(index, (char) (value));
    setLength(index, (char) (length - (value - initialValue)));
  }

  @Override
  public Container inot(int rangeStart, int rangeEnd) {
    if (rangeEnd <= rangeStart) {
      return this;
    }

    // TODO: write special case code for rangeStart=0; rangeEnd=65535
    // a "sliding" effect where each range records the gap adjacent it
    // can probably be quite fast. Probably have 2 cases: start with a
    // 0 run vs start with a 1 run. If you both start and end with 0s,
    // you will require room for expansion.

    // the +1 below is needed in case the valueslength.length is odd
    if (valueslength.length <= 2 * nbrruns + 1) {
      // no room for expansion
      // analyze whether this is a case that will require expansion (that we cannot do)
      // this is a bit costly now (4 "contains" checks)

      boolean lastValueBeforeRange = false;
      boolean firstValueInRange;
      boolean lastValueInRange;
      boolean firstValuePastRange = false;

      // contains is based on a binary search and is hopefully fairly fast.
      // however, one binary search could *usually* suffice to find both
      // lastValueBeforeRange AND firstValueInRange. ditto for
      // lastVaueInRange and firstValuePastRange

      // find the start of the range
      if (rangeStart > 0) {
        lastValueBeforeRange = contains((char) (rangeStart - 1));
      }
      firstValueInRange = contains((char) rangeStart);

      if (lastValueBeforeRange == firstValueInRange) {
        // expansion is required if also lastValueInRange==firstValuePastRange

        // tougher to optimize out, but possible.
        lastValueInRange = contains((char) (rangeEnd - 1));
        if (rangeEnd != 65536) {
          firstValuePastRange = contains((char) rangeEnd);
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

    RunContainer ans = this; // copy on top of self.
    int k = 0;
    ans.nbrruns = 0; // losing this.nbrruns, which is stashed in myNbrRuns.

    // could try using unsignedInterleavedBinarySearch(valueslength, 0, nbrruns, rangeStart) instead
    // of sequential scan
    // to find the starting location

    for (; (k < myNbrRuns) && ((this.getValue(k)) < rangeStart); ++k) {
      // since it is atop self, there is no copying needed
      // ans.valueslength[2 * k] = this.valueslength[2 * k];
      // ans.valueslength[2 * k + 1] = this.valueslength[2 * k + 1];
      ans.nbrruns++;
    }
    // We will work left to right, with a read pointer that always stays
    // left of the write pointer. However, we need to give the read pointer a head start.
    // use local variables so we are always reading 1 location ahead.

    char bufferedValue = 0, bufferedLength = 0; // 65535 start and 65535 length would be illegal,
                                                 // could use as sentinel
    char nextValue = 0, nextLength = 0;
    if (k < myNbrRuns) { // prime the readahead variables
      bufferedValue = getValue(k);
      bufferedLength = getLength(k);
    }

    ans.smartAppendExclusive((char) rangeStart, (char) (rangeEnd - rangeStart - 1));

    for (; k < myNbrRuns; ++k) {
      if (ans.nbrruns > k + 1) {
        throw new RuntimeException(
            "internal error in inot, writer has overtaken reader!! " + k + " " + ans.nbrruns);
      }
      if (k + 1 < myNbrRuns) {
        nextValue = getValue(k + 1); // readahead for next iteration
        nextLength = getLength(k + 1);
      }
      ans.smartAppendExclusive(bufferedValue, bufferedLength);
      bufferedValue = nextValue;
      bufferedLength = nextLength;
    }
    // the number of runs can increase by one, meaning (rarely) a bitmap will become better
    // or the cardinality can decrease by a lot, making an array better
    return ans.toEfficientContainer();
  }

  @Override
  public boolean intersects(ArrayContainer x) {
    if (this.nbrruns == 0) {
      return false;
    }
    int rlepos = 0;
    int arraypos = 0;
    int rleval = this.getValue(rlepos);
    int rlelength = this.getLength(rlepos);
    while (arraypos < x.cardinality) {
      int arrayval = (x.content[arraypos]);
      while (rleval + rlelength < arrayval) {// this will frequently be false
        ++rlepos;
        if (rlepos == this.nbrruns) {
          return false;
        }
        rleval = this.getValue(rlepos);
        rlelength = this.getLength(rlepos);
      }
      if (rleval > arrayval) {
        arraypos = Util.advanceUntil(x.content, arraypos, x.cardinality, this.getValue(rlepos));
      } else {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean intersects(BitmapContainer x) {
    for (int run = 0; run < this.nbrruns; ++run) {
      int runStart = this.getValue(run);
      int runEnd = runStart + this.getLength(run);
      if (x.intersects(runStart, runEnd + 1)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean intersects(RunContainer x) {
    int rlepos = 0;
    int xrlepos = 0;
    int start = this.getValue(rlepos);
    int end = start + this.getLength(rlepos) + 1;
    int xstart = x.getValue(xrlepos);
    int xend = xstart + x.getLength(xrlepos) + 1;
    while (rlepos < this.nbrruns && xrlepos < x.nbrruns) {
      if (end <= xstart) {
        if (ENABLE_GALLOPING_AND) {
          rlepos = skipAhead(this, rlepos, xstart); // skip over runs until we have end > xstart (or
                                                    // rlepos is advanced beyond end)
        } else {
          ++rlepos;
        }

        if (rlepos < this.nbrruns) {
          start = (this.getValue(rlepos));
          end = start + (this.getLength(rlepos)) + 1;
        }
      } else if (xend <= start) {
        // exit the second run
        if (ENABLE_GALLOPING_AND) {
          xrlepos = skipAhead(x, xrlepos, start);
        } else {
          ++xrlepos;
        }

        if (xrlepos < x.nbrruns) {
          xstart = (x.getValue(xrlepos));
          xend = xstart + (x.getLength(xrlepos)) + 1;
        }
      } else {// they overlap
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean intersects(int minimum, int supremum) {
    if((minimum < 0) || (supremum < minimum) || (supremum > (1<<16))) {
      throw new RuntimeException("This should never happen (bug).");
    }
    for (int i = 0; i < numberOfRuns(); ++i) {
      int runFirstValue = getValue(i);
      int runLastValue = (char) (runFirstValue + getLength(i)) + 1;
      if (supremum > runFirstValue && minimum < runLastValue) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Container ior(ArrayContainer x) {
    if (isFull()) {
      return this;
    }
    final int nbrruns = this.nbrruns;
    final int offset = Math.max(nbrruns, x.getCardinality());
    copyToOffset(offset);
    int rlepos = 0;
    this.nbrruns = 0;
    PeekableCharIterator i = x.getCharIterator();
    while (i.hasNext() && (rlepos < nbrruns)) {
      if (getValue(rlepos + offset) - i.peekNext() <= 0) {
        smartAppend(getValue(rlepos + offset), getLength(rlepos + offset));
        rlepos++;
      } else {
        smartAppend(i.next());
      }
    }
    if (i.hasNext()) {
      /*
       * if(this.nbrruns>0) { // this might be useful if the run container has just one very large
       * run int lastval = (getValue(nbrruns + offset - 1)) +
       * (getLength(nbrruns + offset - 1)) + 1; i.advanceIfNeeded((char)
       * lastval); }
       */
      while (i.hasNext()) {
        smartAppend(i.next());
      }
    } else {
      while (rlepos < nbrruns) {
        smartAppend(getValue(rlepos + offset), getLength(rlepos + offset));
        rlepos++;
      }
    }
    return toEfficientContainer();
  }

  @Override
  public Container ior(BitmapContainer x) {
    if (isFull()) {
      return this;
    }
    return or(x);
  }

  @Override
  public Container ior(RunContainer x) {
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

    // Add values length (smaller first)
    while ((rlepos < nbrruns) && (xrlepos < xnbrruns)) {
      final char value = this.getValue(offset + rlepos);
      final char xvalue = x.getValue(xrlepos);
      final char length = this.getLength(offset + rlepos);
      final char xlength = x.getLength(xrlepos);

      if (value - xvalue <= 0) {
        this.smartAppend(value, length);
        ++rlepos;
      } else {
        this.smartAppend(xvalue, xlength);
        ++xrlepos;
      }
    }

    while (rlepos < nbrruns) {
      this.smartAppend(this.getValue(offset + rlepos), this.getLength(offset + rlepos));
      ++rlepos;
    }

    while (xrlepos < xnbrruns) {
      this.smartAppend(x.getValue(xrlepos), x.getLength(xrlepos));
      ++xrlepos;
    }
    return this.toBitmapIfNeeded();
  }

  @Override
  public Container iremove(int begin, int end) {
    // TODO: it might be better and simpler to do return
    // toBitmapOrArrayContainer(getCardinality()).iremove(begin,end)
    if(end == begin) {
      return this;
    }
    if ((begin > end) || (end > (1 << 16))) {
      throw new IllegalArgumentException("Invalid range [" + begin + "," + end + ")");
    }
    if (begin == end - 1) {
      remove((char) begin);
      return this;
    }

    int bIndex = unsignedInterleavedBinarySearch(this.valueslength, 0, this.nbrruns, (char) begin);
    int eIndex =
        unsignedInterleavedBinarySearch(this.valueslength, 0, this.nbrruns, (char) (end - 1));

    // note, eIndex is looking for (end-1)

    if (bIndex >= 0) { // beginning marks beginning of a run
      if (eIndex < 0) {
        eIndex = -eIndex - 2;
      }
      // eIndex could be a run that begins exactly at "end"
      // or it might be an earlier run

      // if the end is before the first run, we'd have eIndex==-1. But bIndex makes this impossible.

      if (valueLengthContains(end, eIndex)) {
        initValueLength(end, eIndex); // there is something left in the run
        recoverRoomsInRange(bIndex - 1, eIndex - 1);
      } else {
        recoverRoomsInRange(bIndex - 1, eIndex); // nothing left in the run
      }

    } else if (eIndex >= 0) {
      // start does not coincide to a run start, but end does.
      bIndex = -bIndex - 2;

      if (bIndex >= 0) {
        if (valueLengthContains(begin, bIndex)) {
          closeValueLength(begin - 1, bIndex);
        }
      }

      // last run is one shorter
      if (getLength(eIndex) == 0) {// special case where we remove last run
        recoverRoomsInRange(eIndex - 1, eIndex);
      } else {
        incrementValue(eIndex);
        decrementLength(eIndex);
      }
      recoverRoomsInRange(bIndex, eIndex - 1);

    } else {
      bIndex = -bIndex - 2;
      eIndex = -eIndex - 2;

      if (eIndex >= 0) { // end-1 is not before first run.
        if (bIndex >= 0) { // nor is begin
          if (bIndex == eIndex) { // all removal nested properly between
                                  // one run start and the next
            if (valueLengthContains(begin, bIndex)) {
              if (valueLengthContains(end, eIndex)) {
                // proper nesting within a run, generates 2 sub-runs
                makeRoomAtIndex(bIndex);
                closeValueLength(begin - 1, bIndex);
                initValueLength(end, bIndex + 1);
                return this;
              }
              // removed area extends beyond run.
              closeValueLength(begin - 1, bIndex);
            }
          } else { // begin in one run area, end in a later one.
            if (valueLengthContains(begin, bIndex)) {
              closeValueLength(begin - 1, bIndex);
              // this cannot leave the bIndex run empty.
            }
            if (valueLengthContains(end, eIndex)) {
              // there is additional stuff in the eIndex run
              initValueLength(end, eIndex);
              eIndex--;
            }  // run ends at or before the range being removed, can delete it

            recoverRoomsInRange(bIndex, eIndex);
          }

        } else {
          // removed range begins before the first run
          if (valueLengthContains(end, eIndex)) { // had been end-1
            initValueLength(end, eIndex);
            recoverRoomsInRange(bIndex, eIndex - 1);
          } else { // removed range includes all the last run
            recoverRoomsInRange(bIndex, eIndex);
          }
        }

      } // eIndex == -1: whole range is before first run, nothing to delete...


    }
    return this;
  }

  @Override
  public boolean isFull() {
    return (this.nbrruns == 1) && (this.getValue(0) == 0) && (this.getLength(0) == 0xFFFF);
  }

  public static RunContainer full() {
    return new RunContainer(0, 1 << 16);
  }

  @Override
  public Iterator<Character> iterator() {
    final CharIterator i = getCharIterator();
    return new Iterator<Character>() {

      @Override
      public boolean hasNext() {
        return i.hasNext();
      }

      @Override
      public Character next() {
        return i.next();
      }

      @Override
      public void remove() {
        i.remove();
      }
    };

  }

  @Override
  public Container ixor(ArrayContainer x) {
    return xor(x);
  }

  @Override
  public Container ixor(BitmapContainer x) {
    return xor(x);
  }



  @Override
  public Container ixor(RunContainer x) {
    return xor(x);
  }

  private RunContainer lazyandNot(ArrayContainer x) {
    if (x.isEmpty()) {
      return this;
    }
    RunContainer answer = new RunContainer(new char[2 * (this.nbrruns + x.cardinality)], 0);
    int rlepos = 0;
    int xrlepos = 0;
    int start = (this.getValue(rlepos));
    int end = start + (this.getLength(rlepos)) + 1;
    int xstart = (x.content[xrlepos]);
    while ((rlepos < this.nbrruns) && (xrlepos < x.cardinality)) {
      if (end <= xstart) {
        // output the first run
        answer.valueslength[2 * answer.nbrruns] = (char) start;
        answer.valueslength[2 * answer.nbrruns + 1] = (char) (end - start - 1);
        answer.nbrruns++;
        rlepos++;
        if (rlepos < this.nbrruns) {
          start = (this.getValue(rlepos));
          end = start + (this.getLength(rlepos)) + 1;
        }
      } else if (xstart + 1 <= start) {
        // exit the second run
        xrlepos++;
        if (xrlepos < x.cardinality) {
          xstart = (x.content[xrlepos]);
        }
      } else {
        if (start < xstart) {
          answer.valueslength[2 * answer.nbrruns] = (char) start;
          answer.valueslength[2 * answer.nbrruns + 1] = (char) (xstart - start - 1);
          answer.nbrruns++;
        }
        if (xstart + 1 < end) {
          start = xstart + 1;
        } else {
          rlepos++;
          if (rlepos < this.nbrruns) {
            start = (this.getValue(rlepos));
            end = start + (this.getLength(rlepos)) + 1;
          }
        }
      }
    }
    if (rlepos < this.nbrruns) {
      answer.valueslength[2 * answer.nbrruns] = (char) start;
      answer.valueslength[2 * answer.nbrruns + 1] = (char) (end - start - 1);
      answer.nbrruns++;
      rlepos++;
      if (rlepos < this.nbrruns) {
        System.arraycopy(this.valueslength, 2 * rlepos, answer.valueslength, 2 * answer.nbrruns,
            2 * (this.nbrruns - rlepos));
        answer.nbrruns = answer.nbrruns + this.nbrruns - rlepos;
      }
    }
    return answer;
  }

  protected Container lazyor(ArrayContainer x) {
    return lazyorToRun(x);
  }

  private Container lazyorToRun(ArrayContainer x) {
    if (isFull()) {
      return full();
    }
    // TODO: should optimize for the frequent case where we have a single run
    RunContainer answer = new RunContainer(new char[2 * (this.nbrruns + x.getCardinality())], 0);
    int rlepos = 0;
    PeekableCharIterator i = x.getCharIterator();

    while (i.hasNext() && (rlepos < this.nbrruns)) {
      if (getValue(rlepos) - i.peekNext() <= 0) {
        answer.smartAppend(getValue(rlepos), getLength(rlepos));
        // in theory, this next code could help, in practice it doesn't.
        /*
         * int lastval = (answer.getValue(answer.nbrruns - 1)) +
         * (answer.getLength(answer.nbrruns - 1)) + 1; i.advanceIfNeeded((char)
         * lastval);
         */

        rlepos++;
      } else {
        answer.smartAppend(i.next());
      }
    }
    if (i.hasNext()) {
      /*
       * if(answer.nbrruns>0) { this might be useful if the run container has just one very large
       * run int lastval = (answer.getValue(answer.nbrruns - 1)) +
       * (answer.getLength(answer.nbrruns - 1)) + 1; i.advanceIfNeeded((char)
       * lastval); }
       */
      while (i.hasNext()) {
        answer.smartAppend(i.next());
      }
    } else {
      while (rlepos < this.nbrruns) {
        answer.smartAppend(getValue(rlepos), getLength(rlepos));
        rlepos++;
      }
    }
    if (answer.isFull()) {
      return full();
    }
    return answer.convertToLazyBitmapIfNeeded();
  }

  private Container lazyxor(ArrayContainer x) {
    if (x.isEmpty()) {
      return this;
    }
    if (this.nbrruns == 0) {
      return x;
    }
    RunContainer answer = new RunContainer(new char[2 * (this.nbrruns + x.getCardinality())], 0);
    int rlepos = 0;
    CharIterator i = x.getCharIterator();
    char cv = i.next();

    while (true) {
      if (getValue(rlepos) < cv) {
        answer.smartAppendExclusive(getValue(rlepos), getLength(rlepos));
        rlepos++;
        if (rlepos == this.nbrruns) {
          answer.smartAppendExclusive(cv);
          while (i.hasNext()) {
            answer.smartAppendExclusive(i.next());
          }
          break;
        }
      } else {
        answer.smartAppendExclusive(cv);
        if (!i.hasNext()) {
          while (rlepos < this.nbrruns) {
            answer.smartAppendExclusive(getValue(rlepos), getLength(rlepos));
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
  public Container limit(int maxcardinality) {
    if (maxcardinality >= getCardinality()) {
      return clone();
    }

    int r;
    int cardinality = 0;
    for (r = 0; r < this.nbrruns; ++r) {
      cardinality += (getLength(r)) + 1;
      if (maxcardinality <= cardinality) {
        break;
      }
    }

    RunContainer rc = new RunContainer(Arrays.copyOf(valueslength, 2 * (r+1)), r+1);
    rc.setLength(r ,
        (char) ((rc.getLength(r)) - cardinality + maxcardinality));
    return rc;
  }

  private void makeRoomAtIndex(int index) {
    if (2 * (nbrruns + 1) > valueslength.length) {
      increaseCapacity();
    }
    copyValuesLength(valueslength, index, valueslength, index + 1, nbrruns - index);
    nbrruns++;
  }

  // To merge values length from begin(inclusive) to end(inclusive)
  private void mergeValuesLength(int begin, int end) {
    if (begin < end) {
      int bValue = (getValue(begin));
      int eValue = (getValue(end));
      int eLength = (getLength(end));
      int newLength = eValue - bValue + eLength;
      setLength(begin, (char) newLength);
      recoverRoomsInRange(begin, end);
    }
  }

  @Override
  public Container not(int rangeStart, int rangeEnd) {
    if (rangeEnd <= rangeStart) {
      return this.clone();
    }
    RunContainer ans = new RunContainer(nbrruns + 1);
    int k = 0;
    for (; (k < this.nbrruns) && ((this.getValue(k)) < rangeStart); ++k) {
      ans.valueslength[2 * k] = this.valueslength[2 * k];
      ans.valueslength[2 * k + 1] = this.valueslength[2 * k + 1];
      ans.nbrruns++;
    }
    ans.smartAppendExclusive((char) rangeStart, (char) (rangeEnd - rangeStart - 1));
    for (; k < this.nbrruns; ++k) {
      ans.smartAppendExclusive(getValue(k), getLength(k));
    }
    // the number of runs can increase by one, meaning (rarely) a bitmap will become better
    // or the cardinality can decrease by a lot, making an array better
    return ans.toEfficientContainer();
  }

  @Override
  public int numberOfRuns() {
    return nbrruns;
  }

  @Override
  public Container or(ArrayContainer x) {
    // we guess that, often, the result will still be efficiently expressed as a run container
    return lazyor(x).repairAfterLazy();
  }

  @Override
  public Container or(BitmapContainer x) {
    if (isFull()) {
      return full();
    }
    // could be implemented as return toTemporaryBitmap().ior(x);
    BitmapContainer answer = x.clone();
    for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
      int start = (this.getValue(rlepos));
      int end = start + (this.getLength(rlepos)) + 1;
      int prevOnesInRange = answer.cardinalityInRange(start, end);
      Util.setBitmapRange(answer.bitmap, start, end);
      answer.updateCardinality(prevOnesInRange, end - start);
    }
    if (answer.isFull()) {
      return full();
    }
    return answer;
  }

  @Override
  public Container or(RunContainer x) {
    if (isFull()) {
      return full();
    }
    if (x.isFull()) {
      return full(); // cheap case that can save a lot of computation
    }
    // we really ought to optimize the rest of the code for the frequent case where there is a
    // single run
    RunContainer answer = new RunContainer(new char[2 * (this.nbrruns + x.nbrruns)], 0);
    int rlepos = 0;
    int xrlepos = 0;

    while ((xrlepos < x.nbrruns) && (rlepos < this.nbrruns)) {
      if (getValue(rlepos) - x.getValue(xrlepos) <= 0) {
        answer.smartAppend(getValue(rlepos), getLength(rlepos));
        rlepos++;
      } else {
        answer.smartAppend(x.getValue(xrlepos), x.getLength(xrlepos));
        xrlepos++;
      }
    }
    while (xrlepos < x.nbrruns) {
      answer.smartAppend(x.getValue(xrlepos), x.getLength(xrlepos));
      xrlepos++;
    }
    while (rlepos < this.nbrruns) {
      answer.smartAppend(getValue(rlepos), getLength(rlepos));
      rlepos++;
    }
    if (answer.isFull()) {
      return full();
    }
    return answer.toBitmapIfNeeded();
  }

  // Prepend a value length with all values starting from a given value
  private void prependValueLength(int value, int index) {
    int initialValue = (getValue(index));
    int length = (getLength(index));
    setValue(index, (char) value);
    setLength(index, (char) (initialValue - value + length));
  }

  @Override
  public int rank(char lowbits) {
    int answer = 0;
    for (int k = 0; k < this.nbrruns; ++k) {
      int value = (getValue(k));
      int length = (getLength(k));
      if ((int) (lowbits) < value) {
        return answer;
      } else if (value + length + 1 > (int) (lowbits)) {
        return answer + (int) (lowbits) - value + 1;
      }
      answer += length + 1;
    }
    return answer;
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException {
    deserialize(in);
  }

  private void recoverRoomAtIndex(int index) {
    copyValuesLength(valueslength, index + 1, valueslength, index, nbrruns - index - 1);
    nbrruns--;
  }

  // To recover rooms between begin(exclusive) and end(inclusive)
  private void recoverRoomsInRange(int begin, int end) {
    if (end + 1 < this.nbrruns) {
      copyValuesLength(this.valueslength, end + 1, this.valueslength, begin + 1,
          this.nbrruns - 1 - end);
    }
    this.nbrruns -= end - begin;
  }

  @Override
  public Container remove(int begin, int end) {
    RunContainer rc = (RunContainer) clone();
    return rc.iremove(begin, end);
  }

  @Override
  public Container remove(char x) {
    int index = unsignedInterleavedBinarySearch(valueslength, 0, nbrruns, x);
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
      int offset = (x) - (getValue(index));
      int le = (getLength(index));
      if (offset < le) {
        // need to break in two
        this.setLength(index, (char) (offset - 1));
        // need to insert
        int newvalue = (x) + 1;
        int newlength = le - offset - 1;
        makeRoomAtIndex(index + 1);
        this.setValue(index + 1, (char) newvalue);
        this.setLength(index + 1, (char) newlength);
        return this;

      } else if (offset == le) {
        decrementLength(index);
      }
    }
    // no match
    return this;
  }

  @Override
  public Container repairAfterLazy() {
    return toEfficientContainer();
  }

  /**
   * Convert to Array or Bitmap container if the serialized form would be shorter. Exactly the same
   * functionality as toEfficientContainer.
   */

  @Override
  public Container runOptimize() {
    return toEfficientContainer();
  }

  @Override
  public char select(int j) {
    int offset = 0;
    for (int k = 0; k < this.nbrruns; ++k) {
      int nextOffset = offset + (getLength(k)) + 1;
      if (nextOffset > j) {
        return (char) (getValue(k) + (j - offset));
      }
      offset = nextOffset;
    }
    throw new IllegalArgumentException(
        "Cannot select " + j + " since cardinality is " + getCardinality());
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    writeArray(out);
  }


  @Override
  public int serializedSizeInBytes() {
    return serializedSizeInBytes(nbrruns);
  }

  private void setLength(int index, char v) {
    setLength(valueslength, index, v);
  }



  private void setLength(char[] valueslength, int index, char v) {
    valueslength[2 * index + 1] = v;
  }

  private void setValue(int index, char v) {
    setValue(valueslength, index, v);
  }

  private void setValue(char[] valueslength, int index, char v) {
    valueslength[2 * index] = v;
  }



  // bootstrapping (aka "galloping") binary search. Always skips at least one.
  // On our "real data" benchmarks, enabling galloping is a minor loss
  // .."ifdef ENABLE_GALLOPING_AND" :)
  private int skipAhead(RunContainer skippingOn, int pos, int targetToExceed) {
    int left = pos;
    int span = 1;
    int probePos;
    int end;
    // jump ahead to find a spot where end > targetToExceed (if it exists)
    do {
      probePos = left + span;
      if (probePos >= skippingOn.nbrruns - 1) {
        // expect it might be quite common to find the container cannot be advanced as far as
        // requested. Optimize for it.
        probePos = skippingOn.nbrruns - 1;
        end = (skippingOn.getValue(probePos))
            + (skippingOn.getLength(probePos)) + 1;
        if (end <= targetToExceed) {
          return skippingOn.nbrruns;
        }
      }
      end = (skippingOn.getValue(probePos))
          + (skippingOn.getLength(probePos)) + 1;
      span *= 2;
    } while (end <= targetToExceed);
    int right = probePos;
    // left and right are both valid positions. Invariant: left <= targetToExceed && right >
    // targetToExceed
    // do a binary search to discover the spot where left and right are separated by 1, and
    // invariant is maintained.
    while (right - left > 1) {
      int mid = (right + left) / 2;
      int midVal = (skippingOn.getValue(mid))
          + (skippingOn.getLength(mid)) + 1;
      if (midVal > targetToExceed) {
        right = mid;
      } else {
        left = mid;
      }
    }
    return right;
  }

  private void smartAppend(char val) {
    int oldend;
    if ((nbrruns == 0)
        || (val > (oldend = (valueslength[2 * (nbrruns - 1)])
            + (valueslength[2 * (nbrruns - 1) + 1])) + 1)) { // we add a new one
      valueslength[2 * nbrruns] = val;
      valueslength[2 * nbrruns + 1] = 0;
      nbrruns++;
      return;
    }
    if (val == (char) (oldend + 1)) { // we merge
      valueslength[2 * (nbrruns - 1) + 1]++;
    }
  }

  void smartAppend(char start, char length) {
    int oldend;
    if ((nbrruns == 0) || ((start) > (oldend =
        (getValue(nbrruns - 1)) + (getLength(nbrruns - 1)))
        + 1)) { // we add a new one
      ensureCapacity(nbrruns + 1);
      valueslength[2 * nbrruns] = start;
      valueslength[2 * nbrruns + 1] = length;
      nbrruns++;
      return;
    }
    int newend = (start) + length + 1;
    if (newend > oldend) { // we merge
      setLength(nbrruns - 1, (char) (newend - 1 - (getValue(nbrruns - 1))));
    }
  }

  private void smartAppendExclusive(char val) {
    int oldend;
    if ((nbrruns == 0)
        || (val > (oldend = getValue(nbrruns - 1)
            + getLength(nbrruns - 1) + 1))) { // we add a new one
      valueslength[2 * nbrruns] = val;
      valueslength[2 * nbrruns + 1] = 0;
      nbrruns++;
      return;
    }
    // We have that val <= oldend.
    if (oldend == val) {
      // we merge
      valueslength[2 * (nbrruns - 1) + 1]++;
      return;
    }
    // We have that val < oldend.

    int newend = val + 1;
    // We have that newend = val + 1 and val < oldend.
    // so newend <= oldend.

    if (val == getValue(nbrruns - 1)) {
      // we wipe out previous
      if (newend != oldend) {
        setValue(nbrruns - 1, (char) newend);
        setLength(nbrruns - 1, (char) (oldend - newend - 1));
        return;
      } else { // they cancel out
        nbrruns--;
        return;
      }
    }
    setLength(nbrruns - 1, (char) (val - getValue(nbrruns - 1) - 1));
    if (newend < oldend) {
      setValue(nbrruns, (char) newend);
      setLength(nbrruns, (char) (oldend - newend - 1));
      nbrruns++;
    } // otherwise newend == oldend

  }

  private void smartAppendExclusive(char start, char length) {
    int oldend;
    if ((nbrruns == 0)
        || (start > (oldend = (getValue(nbrruns - 1))
            + (getLength(nbrruns - 1)) + 1))) { // we add a new one
      valueslength[2 * nbrruns] = start;
      valueslength[2 * nbrruns + 1] = length;
      nbrruns++;
      return;
    }
    if (oldend == start) {
      // we merge
      valueslength[2 * (nbrruns - 1) + 1] += length + 1;
      return;
    }

    int newend = start + length + 1;

    if (start == (getValue(nbrruns - 1))) {
      // we wipe out previous
      if (newend < oldend) {
        setValue(nbrruns - 1, (char) newend);
        setLength(nbrruns - 1, (char) (oldend - newend - 1));
        return;
      } else if (newend > oldend) {
        setValue(nbrruns - 1, (char) oldend);
        setLength(nbrruns - 1, (char) (newend - oldend - 1));
        return;
      } else { // they cancel out
        nbrruns--;
        return;
      }
    }
    setLength(nbrruns - 1, (char) (start - (getValue(nbrruns - 1)) - 1));
    if (newend < oldend) {
      setValue(nbrruns, (char) newend);
      setLength(nbrruns, (char) (oldend - newend - 1));
      nbrruns++;
    } else if (newend > oldend) {
      setValue(nbrruns, (char) oldend);
      setLength(nbrruns, (char) (newend - oldend - 1));
      nbrruns++;
    }
  }

  // convert to bitmap *if needed* (useful if you know it can't be an array)
  private Container toBitmapIfNeeded() {
    int sizeAsRunContainer = RunContainer.serializedSizeInBytes(this.nbrruns);
    int sizeAsBitmapContainer = BitmapContainer.serializedSizeInBytes(0);
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
  Container toBitmapOrArrayContainer(int card) {
    // int card = this.getCardinality();
    if (card <= ArrayContainer.DEFAULT_MAX_SIZE) {
      ArrayContainer answer = new ArrayContainer(card);
      answer.cardinality = 0;
      for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
        int runStart = (this.getValue(rlepos));
        int runEnd = runStart + (this.getLength(rlepos));

        for (int runValue = runStart; runValue <= runEnd; ++runValue) {
          answer.content[answer.cardinality++] = (char) runValue;
        }
      }
      return answer;
    }
    BitmapContainer answer = new BitmapContainer();
    for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
      int start = (this.getValue(rlepos));
      int end = start + (this.getLength(rlepos)) + 1;
      Util.setBitmapRange(answer.bitmap, start, end);
    }
    answer.cardinality = card;
    return answer;
  }

  // convert to bitmap or array *if needed*
  private Container toEfficientContainer() {
    int sizeAsRunContainer = RunContainer.serializedSizeInBytes(this.nbrruns);
    int sizeAsBitmapContainer = BitmapContainer.serializedSizeInBytes(0);
    int card = this.getCardinality();
    int sizeAsArrayContainer = ArrayContainer.serializedSizeInBytes(card);
    if (sizeAsRunContainer <= Math.min(sizeAsBitmapContainer, sizeAsArrayContainer)) {
      return this;
    }
    return toBitmapOrArrayContainer(card);
  }

  @Override
  public MappeableContainer toMappeableContainer() {
    return new MappeableRunContainer(this);
  }

  /**
   * Return the content of this container as a ShortBuffer. This creates a copy and might be
   * relatively slow.
   *
   * @return the ShortBuffer
   */
  public CharBuffer toCharBuffer() {
    CharBuffer sb = CharBuffer.allocate(this.nbrruns * 2);
    sb.put(this.valueslength, 0, this.nbrruns * 2);
    return sb;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (int k = 0; k < this.nbrruns; ++k) {
      sb.append("[");
      sb.append((int)(this.getValue(k)));
      sb.append(",");
      sb.append((this.getValue(k)) + (this.getLength(k)));
      sb.append("]");
    }
    return sb.toString();
  }


  @Override
  public void trim() {
    if (valueslength.length == 2 * nbrruns) {
      return;
    }
    valueslength = Arrays.copyOf(valueslength, 2 * nbrruns);
  }


  // To check if a value length contains a given value
  private boolean valueLengthContains(int value, int index) {
    int initialValue = (getValue(index));
    int length = (getLength(index));

    return value <= initialValue + length;
  }

  @Override
  public void writeArray(DataOutput out) throws IOException {
    out.writeShort(Character.reverseBytes((char) this.nbrruns));
    for (int k = 0; k < 2 * this.nbrruns; ++k) {
      out.writeShort(Character.reverseBytes(this.valueslength[k]));
    }
  }

  @Override
  public void writeArray(ByteBuffer buffer) {
    assert buffer.order() == ByteOrder.LITTLE_ENDIAN;
    CharBuffer buf = buffer.asCharBuffer();
    buf.put((char)nbrruns);
    buf.put(valueslength, 0, nbrruns * 2);
    int bytesWritten = (nbrruns * 2 + 1) * 2;
    buffer.position(buffer.position() + bytesWritten);
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    serialize(out);

  }

  @Override
  public Container xor(ArrayContainer x) {
    // if the cardinality of the array is small, guess that the output will still be a run container
    final int arbitrary_threshold = 32; // 32 is arbitrary here
    if (x.getCardinality() < arbitrary_threshold) {
      return lazyxor(x).repairAfterLazy();
    }
    // otherwise, we expect the output to be either an array or bitmap
    final int card = getCardinality();
    if (card <= ArrayContainer.DEFAULT_MAX_SIZE) {
      // if the cardinality is small, we construct the solution in place
      return x.xor(this.getCharIterator());
    }
    // otherwise, we generate a bitmap (even if runcontainer would be better)
    return toBitmapOrArrayContainer(card).ixor(x);
  }

  @Override
  public Container xor(BitmapContainer x) {
    // could be implemented as return toTemporaryBitmap().ixor(x);
    BitmapContainer answer = x.clone();
    for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
      int start = (this.getValue(rlepos));
      int end = start + (this.getLength(rlepos)) + 1;
      int prevOnes = answer.cardinalityInRange(start, end);
      Util.flipBitmapRange(answer.bitmap, start, end);
      answer.updateCardinality(prevOnes, end - start - prevOnes);
    }
    if (answer.getCardinality() > ArrayContainer.DEFAULT_MAX_SIZE) {
      return answer;
    } else {
      return answer.toArrayContainer();
    }
  }

  @Override
  public Container xor(RunContainer x) {
    if (x.nbrruns == 0) {
      return this.clone();
    }
    if (this.nbrruns == 0) {
      return x.clone();
    }
    RunContainer answer = new RunContainer(new char[2 * (this.nbrruns + x.nbrruns)], 0);
    int rlepos = 0;
    int xrlepos = 0;

    while (true) {
      if (getValue(rlepos) < x.getValue(xrlepos)) {
        answer.smartAppendExclusive(getValue(rlepos), getLength(rlepos));
        rlepos++;

        if (rlepos == this.nbrruns) {
          while (xrlepos < x.nbrruns) {
            answer.smartAppendExclusive(x.getValue(xrlepos), x.getLength(xrlepos));
            xrlepos++;
          }
          break;
        }
      } else {
        answer.smartAppendExclusive(x.getValue(xrlepos), x.getLength(xrlepos));

        xrlepos++;
        if (xrlepos == x.nbrruns) {
          while (rlepos < this.nbrruns) {
            answer.smartAppendExclusive(getValue(rlepos), getLength(rlepos));
            rlepos++;
          }
          break;
        }
      }
    }
    return answer.toEfficientContainer();
  }

  @Override
  public void forEach(char msb, IntConsumer ic) {
    int high = msb << 16;
    for(int k = 0; k < this.nbrruns; ++k) {
      int base = this.getValue(k) | high;
      int le = this.getLength(k);
      for(int l = base; l - le <= base; ++l) {
        ic.accept(l);
      }
    }
  }

  @Override
  public BitmapContainer toBitmapContainer() {
    int card = this.getCardinality();
    BitmapContainer answer = new BitmapContainer();
    for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
      int start = (this.getValue(rlepos));
      int end = start + (this.getLength(rlepos)) + 1;
      Util.setBitmapRange(answer.bitmap, start, end);
    }
    answer.cardinality = card;
    return answer;
  }

  @Override
  public int nextValue(char fromValue) {
    int index = unsignedInterleavedBinarySearch(valueslength, 0, nbrruns, fromValue);
    int effectiveIndex = index >= 0 ? index : -index - 2;
    if (effectiveIndex == -1) {
      return first();
    }
    int startValue = (getValue(effectiveIndex));
    int offset = (int) (fromValue) - startValue;
    int le = (getLength(effectiveIndex));
    if (offset <= le) {
      return fromValue;
    }
    if (effectiveIndex + 1 < numberOfRuns()) {
      return (getValue(effectiveIndex + 1));
    }
    return -1;
  }

  @Override
  public int previousValue(char fromValue) {
    int index = unsignedInterleavedBinarySearch(valueslength, 0, nbrruns, fromValue);
    int effectiveIndex = index >= 0 ? index : -index - 2;
    if (effectiveIndex == -1) {
      return -1;
    }
    int startValue = (getValue(effectiveIndex));
    int offset = (int) (fromValue) - startValue;
    int le = (getLength(effectiveIndex));
    if (offset >= 0 && offset <= le) {
      return fromValue;
    }
    return startValue + le;
  }

  @Override
  public int nextAbsentValue(char fromValue) {
    int index = unsignedInterleavedBinarySearch(valueslength, 0, nbrruns, fromValue);
    int effectiveIndex = index >= 0 ? index : -index - 2;
    if (effectiveIndex == -1) {
      return (fromValue);
    }
    int startValue = (getValue(effectiveIndex));
    int offset = (int) (fromValue) - startValue;
    int le = (getLength(effectiveIndex));
    return offset <= le ? startValue + le + 1 : (int) (fromValue);
  }

  @Override
  public int previousAbsentValue(char fromValue) {
    int index = unsignedInterleavedBinarySearch(valueslength, 0, nbrruns, fromValue);
    int effectiveIndex = index >= 0 ? index : -index - 2;
    if (effectiveIndex == -1) {
      return (fromValue);
    }
    int startValue = (getValue(effectiveIndex));
    int offset = (int) (fromValue) - startValue;
    int le = (getLength(effectiveIndex));
    return offset <= le ? startValue - 1 : (int) (fromValue);
  }

  @Override
  public int first() {
    assertNonEmpty(numberOfRuns() == 0);
    return (valueslength[0]);
  }

  @Override
  public int last() {
    assertNonEmpty(numberOfRuns() == 0);
    int index = numberOfRuns() - 1;
    int start = (getValue(index));
    int length = (getLength(index));
    return start + length;
  }

}


class RunContainerCharIterator implements PeekableCharIterator {
  int pos;
  int le = 0;
  int maxlength;
  int base;

  RunContainer parent;

  RunContainerCharIterator() {

  }

  RunContainerCharIterator(RunContainer p) {
    wrap(p);
  }

  @Override
  public PeekableCharIterator clone() {
    try {
      return (PeekableCharIterator) super.clone();
    } catch (CloneNotSupportedException e) {
      return null;// will not happen
    }
  }

  @Override
  public boolean hasNext() {
    return pos < parent.nbrruns;
  }

  @Override
  public char next() {
    char ans = (char) (base + le);
    le++;
    if (le > maxlength) {
      pos++;
      le = 0;
      if (pos < parent.nbrruns) {
        maxlength = (parent.getLength(pos));
        base = (parent.getValue(pos));
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
        maxlength = (parent.getLength(pos));
        base = (parent.getValue(pos));
      }
    }
    return ans;
  }

  @Override
  public void remove() {
    throw new RuntimeException("Not implemented");// TODO
  }

  void wrap(RunContainer p) {
    parent = p;
    pos = 0;
    le = 0;
    if (pos < parent.nbrruns) {
      maxlength = (parent.getLength(pos));
      base = (parent.getValue(pos));
    }
  }

  @Override
  public void advanceIfNeeded(char minval) {
    while (base + maxlength < (minval)) {
      pos++;
      le = 0;
      if (pos < parent.nbrruns) {
        maxlength = (parent.getLength(pos));
        base = (parent.getValue(pos));
      } else {
        return;
      }
    }
    if (base > (minval)) {
      return;
    }
    le = (minval) - base;
  }



  @Override
  public char peekNext() {
    return (char) (base + le);
  }

}

class RunContainerCharRankIterator extends RunContainerCharIterator
    implements PeekableCharRankIterator {

  private int nextRank = 1;

  RunContainerCharRankIterator(RunContainer p) {
    super(p);
  }

  @Override
  public char next() {
    ++nextRank;
    return super.next();
  }

  @Override
  public int nextAsInt() {
    ++nextRank;
    return super.nextAsInt();
  }

  @Override
  public void advanceIfNeeded(char minval) {
    while (base + maxlength < (minval)) {
      nextRank += maxlength - le + 1;

      pos++;
      le = 0;
      if (pos < parent.nbrruns) {
        maxlength = (parent.getLength(pos));
        base = (parent.getValue(pos));
      } else {
        return;
      }
    }

    if (base > (minval)) {
      return;
    }
    int nextLe = (minval) - base;

    nextRank += nextLe - le;
    le = nextLe;
  }

  @Override
  public int peekNextRank() {
    return nextRank;
  }

  @Override
  public RunContainerCharRankIterator clone() {
    return (RunContainerCharRankIterator) super.clone();
  }
}


final class ReverseRunContainerCharIterator implements PeekableCharIterator {
  int pos;
  private int le;
  private RunContainer parent;
  private int maxlength;
  private int base;


  ReverseRunContainerCharIterator() {

  }

  ReverseRunContainerCharIterator(RunContainer p) {
    wrap(p);
  }

  @Override
  public PeekableCharIterator clone() {
    try {
      return (PeekableCharIterator) super.clone();
    } catch (CloneNotSupportedException e) {
      return null;// will not happen
    }
  }

  @Override
  public boolean hasNext() {
    return pos >= 0;
  }

  @Override
  public char next() {
    char ans = (char) (base + maxlength - le);
    le++;
    if (le > maxlength) {
      pos--;
      le = 0;
      if (pos >= 0) {
        maxlength = (parent.getLength(pos));
        base = (parent.getValue(pos));
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
        maxlength = (parent.getLength(pos));
        base = (parent.getValue(pos));
      }
    }
    return ans;
  }

  @Override
  public void advanceIfNeeded(char maxval) {
    while (base > (maxval)) {
      pos--;
      le = 0;
      if (pos >= 0) {
        maxlength = (parent.getLength(pos));
        base = (parent.getValue(pos));
      } else {
        return;
      }
    }
    if (base + maxlength < (maxval)) {
      return;
    }
    le = maxlength + base - (maxval);
  }

  
  @Override
  public char peekNext() {
    return (char) (base + maxlength - le);
  }
  
  @Override
  public void remove() {
    throw new RuntimeException("Not implemented");// TODO
  }

  void wrap(RunContainer p) {
    parent = p;
    pos = parent.nbrruns - 1;
    le = 0;
    if (pos >= 0) {
      maxlength = (parent.getLength(pos));
      base = (parent.getValue(pos));
    }
  }

}
