/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */
package org.roaringbitmap.buffer;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.roaringbitmap.Util.resetBitmapRange;
import static org.roaringbitmap.Util.setBitmapRange;
import static org.roaringbitmap.buffer.MappeableBitmapContainer.MAX_CAPACITY;

import org.roaringbitmap.CharIterator;
import org.roaringbitmap.Container;
import org.roaringbitmap.ContainerBatchIterator;
import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.PeekableCharIterator;
import org.roaringbitmap.RunContainer;

import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Optional;

/**
 * This container takes the form of runs of consecutive values (effectively, run-length encoding).
 * Uses a CharBuffer to store data, unlike org.roaringbitmap.RunContainer. Otherwise similar.
 *
 *
 * Adding and removing content from this container might make it wasteful so regular calls to
 * "runOptimize" might be warranted.
 */
public final class MappeableRunContainer extends MappeableContainer implements Cloneable {
  private static final int DEFAULT_INIT_SIZE = 4;
  private static final long serialVersionUID = 1L;

  private static int branchyBufferedUnsignedInterleavedBinarySearch(
      final CharBuffer sb, final int begin, final int end, final char k) {
    int low = begin;
    int high = end - 1;
    while (low <= high) {
      final int middleIndex = (low + high) >>> 1;
      final int middleValue = (sb.get(2 * middleIndex));
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

  private static int branchyBufferedUnsignedInterleavedBinarySearch(
      final ByteBuffer sb, int position, final int begin, final int end, final char k) {
    int low = begin;
    int high = end - 1;
    while (low <= high) {
      final int middleIndex = (low + high) >>> 1;
      final int middleValue = (sb.getChar(position + 2 * middleIndex * 2));
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

  private static int bufferedUnsignedInterleavedBinarySearch(
      final CharBuffer sb, final int begin, final int end, final char k) {
    return branchyBufferedUnsignedInterleavedBinarySearch(sb, begin, end, k);
  }

  private static int bufferedUnsignedInterleavedBinarySearch(
      final ByteBuffer sb, int position, final int begin, final int end, final char k) {
    return branchyBufferedUnsignedInterleavedBinarySearch(sb, position, begin, end, k);
  }

  protected static int getArraySizeInBytes(int nbrruns) {
    return 2 + 4 * nbrruns;
  }

  private static char getLength(char[] vl, int index) {
    return vl[2 * index + 1];
  }

  private static char getValue(char[] vl, int index) {
    return vl[2 * index];
  }

  protected static int serializedSizeInBytes(int numberOfRuns) {
    return 2 + 2 * 2 * numberOfRuns; // each run requires 2 2-byte entries.
  }

  protected CharBuffer valueslength;

  protected int nbrruns = 0; // how many runs, this number should fit in 16 bits.

  /**
   * Create a container with default capacity
   */
  public MappeableRunContainer() {
    this(DEFAULT_INIT_SIZE);
  }

  @Override
  public Boolean validate() {
    if (nbrruns == 0) {
      return false;
    }
    int runEnd = -2;
    for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
      int runStart = (this.getValue(rlepos));
      if (runStart <= runEnd + 1) {
        return false;
      }
      runEnd = runStart + (this.getLength(rlepos));
      if (runStart > runEnd) {
        return false;
      }
    }

    int sizeAsRunContainer = MappeableArrayContainer.serializedSizeInBytes(this.nbrruns);
    int sizeAsBitmapContainer = MappeableBitmapContainer.serializedSizeInBytes(0);
    int card = this.getCardinality();
    int sizeAsArrayContainer = MappeableArrayContainer.serializedSizeInBytes(card);
    if (sizeAsRunContainer <= Math.min(sizeAsBitmapContainer, sizeAsArrayContainer)) {
      return true;
    }
    return false;
  }

  /**
   * Create an array container with specified capacity
   *
   * @param capacity The capacity of the container
   */
  public MappeableRunContainer(final int capacity) {
    valueslength = CharBuffer.allocate(2 * capacity);
  }

  private MappeableRunContainer(int nbrruns, final CharBuffer valueslength) {
    this.nbrruns = nbrruns;
    CharBuffer tmp = valueslength.duplicate(); // for thread safety
    this.valueslength = CharBuffer.allocate(Math.max(2 * nbrruns, tmp.limit()));
    tmp.rewind();
    this.valueslength.put(tmp); // may copy more than it needs to??
  }

  protected MappeableRunContainer(MappeableArrayContainer arr, int nbrRuns) {
    this.nbrruns = nbrRuns;
    valueslength = CharBuffer.allocate(2 * nbrRuns);
    char[] vl = valueslength.array();
    if (nbrRuns == 0) {
      return;
    }

    int prevVal = -2;
    int runLen = 0;
    int runCount = 0;
    if (BufferUtil.isBackedBySimpleArray(arr.content)) {
      char[] a = arr.content.array();
      for (int i = 0; i < arr.cardinality; i++) {
        int curVal = (a[i]);
        if (curVal == prevVal + 1) {
          ++runLen;
        } else {
          if (runCount > 0) {
            vl[2 * (runCount - 1) + 1] = (char) runLen;
          }
          // setLength(runCount - 1, (char) runLen);
          vl[2 * runCount] = (char) curVal;
          // setValue(runCount, (char) curVal);
          runLen = 0;
          ++runCount;
        }
        prevVal = curVal;
      }

    } else {
      for (int i = 0; i < arr.cardinality; i++) {
        int curVal = (arr.content.get(i));
        if (curVal == prevVal + 1) {
          ++runLen;
        } else {
          if (runCount > 0) {
            vl[2 * (runCount - 1) + 1] = (char) runLen;
          }
          // setLength(runCount - 1, (char) runLen);
          vl[2 * runCount] = (char) curVal;
          // setValue(runCount, (char) curVal);
          runLen = 0;
          ++runCount;
        }
        prevVal = curVal;
      }
    }
    // setLength(runCount-1, (char) runLen);
    vl[2 * (runCount - 1) + 1] = (char) runLen;
  }

  /**
   * Create an run container with a run of ones from firstOfRun to lastOfRun.
   *
   * @param firstOfRun first index
   * @param lastOfRun last index (range is exclusive)
   */
  public MappeableRunContainer(final int firstOfRun, final int lastOfRun) {
    this.nbrruns = 1;
    char[] vl = {(char) firstOfRun, (char) (lastOfRun - 1 - firstOfRun)};
    this.valueslength = CharBuffer.wrap(vl);
  }

  // convert a bitmap container to a run container somewhat efficiently.
  protected MappeableRunContainer(MappeableBitmapContainer bc, int nbrRuns) {
    this.nbrruns = nbrRuns;
    valueslength = CharBuffer.allocate(2 * nbrRuns);
    if (!BufferUtil.isBackedBySimpleArray(valueslength)) {
      throw new RuntimeException("Unexpected internal error.");
    }
    char[] vl = valueslength.array();
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
          // setValue(runCount, (char) runStart);
          vl[2 * runCount] = (char) runStart;
          // setLength(runCount, (char) (runEnd-runStart-1));
          vl[2 * runCount + 1] = (char) (runEnd - runStart - 1);
          return;
        }
        int localRunEnd = Long.numberOfTrailingZeros(~curWordWith1s);
        runEnd = localRunEnd + longCtr * 64;
        // setValue(runCount, (char) runStart);
        vl[2 * runCount] = (char) runStart;
        // setLength(runCount, (char) (runEnd-runStart-1));
        vl[2 * runCount + 1] = (char) (runEnd - runStart - 1);
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
          // setValue(runCount, (char) runStart);
          vl[2 * runCount] = (char) runStart;
          // setLength(runCount, (char) (runEnd-runStart-1));
          vl[2 * runCount + 1] = (char) (runEnd - runStart - 1);
          return;
        }
        int localRunEnd = Long.numberOfTrailingZeros(~curWordWith1s);
        runEnd = localRunEnd + longCtr * 64;
        // setValue(runCount, (char) runStart);
        vl[2 * runCount] = (char) runStart;
        // setLength(runCount, (char) (runEnd-runStart-1));
        vl[2 * runCount + 1] = (char) (runEnd - runStart - 1);
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
    this.valueslength = bc.toCharBuffer();
  }

  /**
   * Construct a new RunContainer backed by the provided CharBuffer. Note that if you modify the
   * RunContainer a new CharBuffer may be produced.
   *
   * @param array CharBuffer where the data is stored
   * @param numRuns number of runs (each using 2 chars in the buffer)
   *
   */
  public MappeableRunContainer(final CharBuffer array, final int numRuns) {
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
  public MappeableContainer add(char k) {
    // TODO: it might be better and simpler to do return
    // toBitmapOrArrayContainer(getCardinality()).add(k)
    int index = bufferedUnsignedInterleavedBinarySearch(valueslength, 0, nbrruns, k);
    if (index >= 0) {
      return this; // already there
    }
    index = -index - 2; // points to preceding value, possibly -1
    if (index >= 0) { // possible match
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
            setLength(index, (char) (getValue(index + 1) + getLength(index + 1) - getValue(index)));
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
          decrementValue();
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

    int rleval = (this.getValue(rlepos));
    int rlelength = (this.getLength(rlepos));
    while (arraypos < x.cardinality) {
      int arrayval = (x.content.get(arraypos));
      while (rleval + rlelength < arrayval) { // this will frequently be false
        ++rlepos;
        if (rlepos == this.nbrruns) {
          return ac; // we are done
        }
        rleval = (this.getValue(rlepos));
        rlelength = (this.getLength(rlepos));
      }
      if (rleval > arrayval) {
        arraypos = BufferUtil.advanceUntil(x.content, arraypos, x.cardinality, (char) rleval);
      } else {
        ac.content.put(ac.cardinality, (char) arrayval);
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
        int runStart = (this.getValue(rlepos));
        int runEnd = runStart + (this.getLength(rlepos));
        for (int runValue = runStart; runValue <= runEnd; ++runValue) {
          if (x.contains((char) runValue)) {
            answer.content.put(answer.cardinality++, (char) runValue);
          }
        }
      }
      return answer;
    }
    // we expect the answer to be a bitmap (if we are lucky)

    MappeableBitmapContainer answer = x.clone();
    int start = 0;
    for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
      int end = (this.getValue(rlepos));
      int prevOnes = answer.cardinalityInRange(start, end);
      BufferUtil.resetBitmapRange(answer.bitmap, start, end);
      answer.updateCardinality(prevOnes, 0);
      start = end + (this.getLength(rlepos)) + 1;
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
        new MappeableRunContainer(CharBuffer.allocate(2 * (this.nbrruns + x.nbrruns)), 0);
    char[] vl = answer.valueslength.array();
    int rlepos = 0;
    int xrlepos = 0;
    int start = (this.getValue(rlepos));
    int end = start + (this.getLength(rlepos)) + 1;
    int xstart = (x.getValue(xrlepos));
    int xend = xstart + (x.getLength(xrlepos)) + 1;
    while ((rlepos < this.nbrruns) && (xrlepos < x.nbrruns)) {
      if (end <= xstart) {
        // exit the first run
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
      } else { // they overlap
        final int lateststart = Math.max(start, xstart);
        int earliestend;
        if (end == xend) { // improbable
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

        } else { // end > xend
          earliestend = xend;
          xrlepos++;
          if (xrlepos < x.nbrruns) {
            xstart = (x.getValue(xrlepos));
            xend = xstart + (x.getLength(xrlepos)) + 1;
          }
        }
        vl[2 * answer.nbrruns] = (char) lateststart;
        vl[2 * answer.nbrruns + 1] = (char) (earliestend - lateststart - 1);
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
      ac.cardinality =
          org.roaringbitmap.Util.unsignedDifference(
              this.getCharIterator(), x.getCharIterator(), ac.content.array());
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
        int runStart = (this.getValue(rlepos));
        int runEnd = runStart + (this.getLength(rlepos));
        for (int runValue = runStart; runValue <= runEnd; ++runValue) {
          if (!x.contains((char) runValue)) {
            answer.content.put(answer.cardinality++, (char) runValue);
          }
        }
      }
      return answer;
    }
    // we expect the answer to be a bitmap (if we are lucky)
    MappeableBitmapContainer answer = x.clone();
    int lastPos = 0;
    for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
      int start = (this.getValue(rlepos));
      int end = start + (this.getLength(rlepos)) + 1;
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
        new MappeableRunContainer(CharBuffer.allocate(2 * (this.nbrruns + x.nbrruns)), 0);
    char[] vl = answer.valueslength.array();
    int rlepos = 0;
    int xrlepos = 0;
    int start = (this.getValue(rlepos));
    int end = start + (this.getLength(rlepos)) + 1;
    int xstart = (x.getValue(xrlepos));
    int xend = xstart + (x.getLength(xrlepos)) + 1;
    while ((rlepos < this.nbrruns) && (xrlepos < x.nbrruns)) {
      if (end <= xstart) {
        // output the first run
        vl[2 * answer.nbrruns] = (char) start;
        vl[2 * answer.nbrruns + 1] = (char) (end - start - 1);
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
          vl[2 * answer.nbrruns] = (char) start;
          vl[2 * answer.nbrruns + 1] = (char) (xstart - start - 1);
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
      vl[2 * answer.nbrruns] = (char) start;
      vl[2 * answer.nbrruns + 1] = (char) (end - start - 1);
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
  public MappeableContainer clone() {
    return new MappeableRunContainer(nbrruns, valueslength);
  }

  // To set the last value of a value length
  private void closeValueLength(int value, int index) {
    int initialValue = (getValue(index));
    setLength(index, (char) (value - initialValue));
  }

  @Override
  public boolean contains(char x) {
    int index = bufferedUnsignedInterleavedBinarySearch(valueslength, 0, nbrruns, x);
    if (index >= 0) {
      return true;
    }
    index = -index - 2; // points to preceding value, possibly -1
    if (index != -1) { // possible match
      int offset = (x) - (getValue(index));
      int le = (getLength(index));
      return offset <= le;
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
  public static boolean contains(ByteBuffer buf, int position, char x, final int numRuns) {
    int index = bufferedUnsignedInterleavedBinarySearch(buf, position, 0, numRuns, x);
    if (index >= 0) {
      return true;
    }
    index = -index - 2; // points to preceding value, possibly -1
    if (index != -1) { // possible match
      int offset = (x) - (buf.getChar(position + index * 2 * 2));
      int le = (buf.getChar(position + index * 2 * 2 + 2));
      return offset <= le;
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
        int start = (this.getValue(rlepos));
        int end = start + (this.getLength(rlepos)) + 1;
        BufferUtil.setBitmapRange(answer.bitmap, start, end);
      }
      answer.cardinality = -1;
      return answer;
    }
    return this;
  }

  // Push all values length to the end of the array (resize array if needed)
  private void copyToOffset(int offset) {
    int minCapacity = 2 * (offset + nbrruns);
    Optional<CharBuffer> newvalueslength = computeNewCapacity(valueslength.capacity(), minCapacity);
    if (newvalueslength.isPresent()) {
      // expensive case where we need to reallocate
      copyValuesLength(this.valueslength, 0, newvalueslength.get(), offset, nbrruns);
      this.valueslength = newvalueslength.get();
    } else {
      // efficient case where we just copy
      copyValuesLength(this.valueslength, 0, this.valueslength, offset, nbrruns);
    }
  }

  private static Optional<CharBuffer> computeNewCapacity(int oldCapacity, int minCapacity) {
    if (oldCapacity < minCapacity) {
      int newCapacity = oldCapacity;
      while ((newCapacity = computeNewCapacity(newCapacity)) < minCapacity) {}
      return Optional.of(CharBuffer.allocate(newCapacity));
    }
    return Optional.empty();
  }

  private static int computeNewCapacity(int oldCapacity) {
    return oldCapacity == 0
        ? DEFAULT_INIT_SIZE
        : oldCapacity < 64
            ? oldCapacity * 2
            : oldCapacity < 1024 ? oldCapacity * 3 / 2 : oldCapacity * 5 / 4;
  }

  private void copyValuesLength(
      CharBuffer src, int srcIndex, CharBuffer dst, int dstIndex, int length) {
    if (BufferUtil.isBackedBySimpleArray(src) && BufferUtil.isBackedBySimpleArray(dst)) {
      // common case.
      System.arraycopy(src.array(), 2 * srcIndex, dst.array(), 2 * dstIndex, 2 * length);
      return;
    }
    // source and destination may overlap
    // consider specialized code for various cases, rather than using a second buffer
    CharBuffer temp = CharBuffer.allocate(2 * length);
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
    valueslength.put(2 * index + 1, (char) (valueslength.get(2 * index + 1) - 1));
  }

  private void decrementValue() {
    valueslength.put(0, (char) (valueslength.get(0) - 1));
  }

  // not thread safe!
  // not actually used anywhere, but potentially useful
  private void ensureCapacity(int minNbRuns) {
    Optional<CharBuffer> nv = computeNewCapacity(valueslength.capacity(), 2 * minNbRuns);
    if (nv.isPresent()) {
      valueslength.rewind();
      nv.get().put(valueslength);
      valueslength = nv.get();
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
      CharIterator me = this.getCharIterator();
      CharIterator you = ((MappeableContainer) o).getCharIterator();
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
    for (char i = 0; i < nbrruns; ++i) {
      char runStart = getValue(i);
      int length = (getLength(i));
      if (pos + length >= arrayContainer.getCardinality()) {
        return false;
      }
      if (arrayContainer.select(pos) != runStart) {
        return false;
      }
      if (arrayContainer.select(pos + length) != (char) ((runStart) + length)) {
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
  public MappeableContainer flip(char x) {
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
    int limit = nbrruns * 2;
    if (isArrayBacked()) {
      char[] vl = valueslength.array();
      for (int k = 1; k < limit; k += 2) {
        sum += vl[k];
      }
    } else {
      for (int k = 1; k < limit; k += 2) {
        sum += valueslength.get(k);
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
  public char getLength(int index) {
    return valueslength.get(2 * index + 1);
  }

  @Override
  public CharIterator getReverseCharIterator() {
    if (isArrayBacked()) {
      return new RawReverseMappeableRunContainerCharIterator(this);
    }
    return new ReverseMappeableRunContainerCharIterator(this);
  }

  @Override
  public PeekableCharIterator getCharIterator() {
    if (isArrayBacked()) {
      return new RawMappeableRunContainerCharIterator(this);
    }
    return new MappeableRunContainerCharIterator(this);
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
  public char getValue(int index) {
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
    if (end == begin) {
      return this;
    }
    if ((begin > end) || (end > (1 << 16))) {
      throw new IllegalArgumentException("Invalid range [" + begin + "," + end + ")");
    }
    if (begin == end - 1) {
      add((char) begin);
      return this;
    }

    int bIndex =
        bufferedUnsignedInterleavedBinarySearch(this.valueslength, 0, this.nbrruns, (char) begin);
    int eIndex =
        bufferedUnsignedInterleavedBinarySearch(
            this.valueslength, bIndex >= 0 ? bIndex : -bIndex - 1, this.nbrruns, (char) (end - 1));

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

  MappeableContainer ilazyor(MappeableArrayContainer x) {
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
    char[] vl = valueslength.array();
    int rlepos = 0;
    this.nbrruns = 0;
    PeekableCharIterator i = x.getCharIterator();
    while (i.hasNext() && (rlepos < nbrruns)) {
      if ((getValue(vl, rlepos + offset)) - (i.peekNext()) <= 0) {
        smartAppend(vl, getValue(vl, rlepos + offset), getLength(vl, rlepos + offset));
        rlepos++;
      } else {
        smartAppend(vl, i.next());
      }
    }
    if (i.hasNext()) {
      /*
       * if(this.nbrruns>0) { // this might be useful if the run container has just one very large
       * run int lastval = (getValue(vl,nbrruns + offset - 1)) +
       * (getLength(vl,nbrruns + offset - 1)) + 1; i.advanceIfNeeded((char)
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
    int newCapacity = computeNewCapacity(valueslength.capacity());
    final CharBuffer nv = CharBuffer.allocate(newCapacity);
    valueslength.rewind();
    nv.put(valueslength);
    valueslength = nv;
  }

  private void incrementLength(int index) {
    valueslength.put(2 * index + 1, (char) (1 + valueslength.get(2 * index + 1)));
  }

  private void incrementValue(int index) {
    valueslength.put(2 * index, (char) (1 + valueslength.get(2 * index)));
  }

  // To set the first value of a value length
  private void initValueLength(int value, int index) {
    int initialValue = (getValue(index));
    int length = (getLength(index));
    setValue(index, (char) (value));
    setLength(index, (char) (length - (value - initialValue)));
  }

  @Override
  public MappeableContainer inot(int rangeStart, int rangeEnd) {
    if (rangeEnd <= rangeStart) {
      return this;
    }
    char[] vl = this.valueslength.array();

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

    MappeableRunContainer ans = this; // copy on top of self.
    int k = 0;
    ans.nbrruns = 0; // losing this.nbrruns, which is stashed in myNbrRuns.

    // could try using unsignedInterleavedBinarySearch(valueslength, 0, nbrruns, rangeStart) instead
    // of sequential scan
    // to find the starting location

    for (; k < myNbrRuns && (this.getValue(k)) < rangeStart; ++k) {
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
      bufferedValue = vl[2 * k]; // getValue(k);
      bufferedLength = vl[2 * k + 1]; // getLength(k);
    }

    ans.smartAppendExclusive(vl, (char) rangeStart, (char) (rangeEnd - rangeStart - 1));

    for (; k < myNbrRuns; ++k) {
      if (ans.nbrruns > k + 1) {
        throw new RuntimeException(
            "internal error in inot, writer has overtaken reader!! " + k + " " + ans.nbrruns);
      }
      if (k + 1 < myNbrRuns) {
        nextValue = vl[2 * (k + 1)]; // getValue(k+1); // readahead for next iteration
        nextLength = vl[2 * (k + 1) + 1]; // getLength(k+1);
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

    int rleval = (this.getValue(rlepos));
    int rlelength = (this.getLength(rlepos));
    while (arraypos < x.cardinality) {
      int arrayval = (x.content.get(arraypos));
      while (rleval + rlelength < arrayval) { // this will frequently be false
        ++rlepos;
        if (rlepos == this.nbrruns) {
          return false;
        }
        rleval = (this.getValue(rlepos));
        rlelength = (this.getLength(rlepos));
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
    for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
      int runStart = this.getValue(rlepos);
      int runEnd = runStart + this.getLength(rlepos);
      if (x.intersects(runStart, runEnd + 1)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean intersects(MappeableRunContainer x) {
    int rlepos = 0;
    int xrlepos = 0;
    int start = (this.getValue(rlepos));
    int end = start + (this.getLength(rlepos)) + 1;
    int xstart = (x.getValue(xrlepos));
    int xend = xstart + (x.getLength(xrlepos)) + 1;
    while ((rlepos < this.nbrruns) && (xrlepos < x.nbrruns)) {
      if (end <= xstart) {
        // exit the first run
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
      } else { // they overlap
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
    char[] vl = this.valueslength.array();
    int rlepos = 0;
    this.nbrruns = 0;
    PeekableCharIterator i = x.getCharIterator();
    while (i.hasNext() && (rlepos < nbrruns)) {
      if ((getValue(vl, rlepos + offset)) - (i.peekNext()) <= 0) {
        smartAppend(vl, getValue(vl, rlepos + offset), getLength(vl, rlepos + offset));
        rlepos++;
      } else {
        smartAppend(vl, i.next());
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
    char[] vl = this.valueslength.array();

    // Add values length (smaller first)
    while ((rlepos < nbrruns) && (xrlepos < xnbrruns)) {
      final char value = getValue(vl, offset + rlepos);
      final char xvalue = x.getValue(xrlepos);
      final char length = getLength(vl, offset + rlepos);
      final char xlength = x.getLength(xrlepos);

      if ((value) - (xvalue) <= 0) {
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
    return this.toEfficientContainer();
  }

  @Override
  // not thread-safe
  public MappeableContainer iremove(int begin, int end) {
    // TODO: it might be better and simpler to do return
    // toBitmapOrArrayContainer(getCardinality()).iremove(begin,end)
    if (end == begin) {
      return this;
    }
    if ((begin > end) || (end > (1 << 16))) {
      throw new IllegalArgumentException("Invalid range [" + begin + "," + end + ")");
    }
    if (begin == end - 1) {
      remove((char) begin);
      return this;
    }

    int bIndex =
        bufferedUnsignedInterleavedBinarySearch(this.valueslength, 0, this.nbrruns, (char) begin);
    int eIndex =
        bufferedUnsignedInterleavedBinarySearch(
            this.valueslength, bIndex >= 0 ? bIndex : -bIndex - 1, this.nbrruns, (char) (end - 1));

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

    } else if (eIndex >= 0) {
      bIndex = -bIndex - 2;

      if (bIndex >= 0) {
        if (valueLengthContains(begin, bIndex)) {
          closeValueLength(begin - 1, bIndex);
        }
      }
      // last run is one charer
      if (getLength(eIndex) == 0) { // special case where we remove last run
        recoverRoomsInRange(eIndex - 1, eIndex);
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

  @Override
  public boolean isFull() {
    return (this.nbrruns == 1) && (this.getValue(0) == 0) && (this.getLength(0) == 0xFFFF);
  }

  @Override
  public void orInto(long[] bits) {
    for (int r = 0; r < numberOfRuns(); ++r) {
      int start = this.valueslength.get(r << 1);
      int length = this.valueslength.get((r << 1) + 1);
      setBitmapRange(bits, start, start + length + 1);
    }
  }

  @Override
  public void andInto(long[] bits) {
    int prev = 0;
    for (int r = 0; r < numberOfRuns(); ++r) {
      int start = this.valueslength.get(r << 1);
      int length = this.valueslength.get((r << 1) + 1);
      resetBitmapRange(bits, prev, start);
      prev = start + length + 1;
    }
    resetBitmapRange(bits, prev, MAX_CAPACITY);
  }

  @Override
  public void removeFrom(long[] bits) {
    for (int r = 0; r < numberOfRuns(); ++r) {
      int start = this.valueslength.get(r << 1);
      int length = this.valueslength.get((r << 1) + 1);
      resetBitmapRange(bits, start, start + length + 1);
    }
  }

  public static MappeableRunContainer full() {
    return new MappeableRunContainer(0, 1 << 16);
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
        new MappeableRunContainer(CharBuffer.allocate(2 * (this.nbrruns + x.cardinality)), 0);
    char[] vl = answer.valueslength.array();
    int rlepos = 0;
    int xrlepos = 0;
    int start = (this.getValue(rlepos));
    int end = start + (this.getLength(rlepos)) + 1;
    int xstart = (x.content.get(xrlepos));
    while ((rlepos < this.nbrruns) && (xrlepos < x.cardinality)) {
      if (end <= xstart) {
        // output the first run
        vl[2 * answer.nbrruns] = (char) start;
        vl[2 * answer.nbrruns + 1] = (char) (end - start - 1);
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
          xstart = (x.content.get(xrlepos));
        }
      } else {
        if (start < xstart) {
          vl[2 * answer.nbrruns] = (char) start;
          vl[2 * answer.nbrruns + 1] = (char) (xstart - start - 1);
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
      vl[2 * answer.nbrruns] = (char) start;
      vl[2 * answer.nbrruns + 1] = (char) (end - start - 1);
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
        new MappeableRunContainer(CharBuffer.allocate(2 * (this.nbrruns + x.getCardinality())), 0);
    char[] vl = answer.valueslength.array();
    int rlepos = 0;
    PeekableCharIterator i = x.getCharIterator();

    while ((rlepos < this.nbrruns) && i.hasNext()) {
      if ((getValue(rlepos)) - (i.peekNext()) <= 0) {
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
       * run int lastval = (answer.getValue(answer.nbrruns - 1)) +
       * (answer.getLength(answer.nbrruns - 1)) + 1;
       * i.advanceIfNeeded((char) lastval); }
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
        new MappeableRunContainer(CharBuffer.allocate(2 * (this.nbrruns + x.getCardinality())), 0);
    char[] vl = answer.valueslength.array();
    int rlepos = 0;
    CharIterator i = x.getCharIterator();
    char cv = i.next();
    while (true) {
      if ((getValue(rlepos)) - (cv) < 0) {
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
      cardinality += (getLength(r)) + 1;
      if (maxcardinality <= cardinality) {
        break;
      }
    }
    CharBuffer newBuf;
    if (BufferUtil.isBackedBySimpleArray(valueslength)) {
      char[] newArray = Arrays.copyOf(valueslength.array(), 2 * (r + 1));
      newBuf = CharBuffer.wrap(newArray);
    } else {
      newBuf = CharBuffer.allocate(2 * (r + 1));
      for (int i = 0; i < 2 * (r + 1); i++) {
        newBuf.put(valueslength.get(i));
      }
    }
    MappeableRunContainer rc = new MappeableRunContainer(newBuf, r + 1);
    rc.setLength(r, (char) (rc.getLength(r) - cardinality + maxcardinality));
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
      int bValue = (getValue(begin));
      int eValue = (getValue(end));
      int eLength = (getLength(end));
      int newLength = eValue - bValue + eLength;
      setLength(begin, (char) newLength);
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
    char[] vl = ans.valueslength.array();
    int k = 0;

    if (isArrayBacked()) {
      char[] myVl = valueslength.array();
      for (; k < this.nbrruns && (getValue(myVl, k)) < rangeStart; ++k) {
        vl[2 * k] = myVl[2 * k];
        vl[2 * k + 1] = myVl[2 * k + 1];
        ans.nbrruns++;
      }
      ans.smartAppendExclusive(vl, (char) rangeStart, (char) (rangeEnd - rangeStart - 1));
      for (; k < this.nbrruns; ++k) {
        ans.smartAppendExclusive(vl, getValue(myVl, k), getLength(myVl, k));
      }
    } else { // not array backed

      for (; k < this.nbrruns && (this.getValue(k)) < rangeStart; ++k) {
        vl[2 * k] = getValue(k);
        vl[2 * k + 1] = getLength(k);
        ans.nbrruns++;
      }
      ans.smartAppendExclusive(vl, (char) rangeStart, (char) (rangeEnd - rangeStart - 1));
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
      int start = (this.getValue(rlepos));
      int end = start + (this.getLength(rlepos)) + 1;
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
        new MappeableRunContainer(CharBuffer.allocate(2 * (this.nbrruns + x.nbrruns)), 0);
    char[] vl = answer.valueslength.array();
    int rlepos = 0;
    int xrlepos = 0;

    while ((rlepos < this.nbrruns) && (xrlepos < x.nbrruns)) {
      if ((getValue(rlepos)) - (x.getValue(xrlepos)) <= 0) {
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
    return answer.toEfficientContainer();
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
    // little endian
    this.nbrruns = Character.reverseBytes(in.readChar());
    if (this.valueslength.capacity() < 2 * this.nbrruns) {
      this.valueslength = CharBuffer.allocate(2 * this.nbrruns);
    }
    for (int k = 0; k < 2 * this.nbrruns; ++k) {
      this.valueslength.put(k, Character.reverseBytes(in.readChar()));
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
  public MappeableContainer remove(char x) {
    int index = bufferedUnsignedInterleavedBinarySearch(valueslength, 0, nbrruns, x);
    if (index >= 0) {
      if (getLength(index) == 0) {
        recoverRoomAtIndex(index);
      } else {
        incrementValue(index);
        decrementLength(index);
      }
      return this; // already there
    }
    index = -index - 2; // points to preceding value, possibly -1
    if (index >= 0) { // possible match
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
  public int serializedSizeInBytes() {
    return serializedSizeInBytes(nbrruns);
  }

  private void setLength(int index, char v) {
    setLength(valueslength, index, v);
  }

  private void setLength(CharBuffer valueslength, int index, char v) {
    valueslength.put(2 * index + 1, v);
  }

  private void setValue(int index, char v) {
    setValue(valueslength, index, v);
  }

  private void setValue(CharBuffer valueslength, int index, char v) {
    valueslength.put(2 * index, v);
  }

  // assume that the (maybe) inplace operations
  // will never actually *be* in place if they are
  // to return ArrayContainer or BitmapContainer

  private void smartAppend(char[] vl, char val) {
    int oldend;
    if ((nbrruns == 0)
        || ((val)
            > (oldend = (vl[2 * (nbrruns - 1)]) + (vl[2 * (nbrruns - 1) + 1]))
                + 1)) { // we add a new one
      vl[2 * nbrruns] = val;
      vl[2 * nbrruns + 1] = 0;
      nbrruns++;
      return;
    }
    if (val == (char) (oldend + 1)) { // we merge
      vl[2 * (nbrruns - 1) + 1]++;
    }
  }

  void smartAppend(char start, char length) {
    int oldend;
    if ((nbrruns == 0)
        || ((start)
            > (oldend = (getValue(nbrruns - 1)) + (getLength(nbrruns - 1)))
                + 1)) { // we add a new one
      ensureCapacity(nbrruns + 1);
      valueslength.put(2 * nbrruns, start);
      valueslength.put(2 * nbrruns + 1, length);
      nbrruns++;
      return;
    }
    int newend = (start) + (length) + 1;
    if (newend > oldend) { // we merge
      setLength(nbrruns - 1, (char) (newend - 1 - (getValue(nbrruns - 1))));
    }
  }

  private void smartAppend(char[] vl, char start, char length) {
    int oldend;
    if ((nbrruns == 0)
        || ((start)
            > (oldend = (vl[2 * (nbrruns - 1)]) + (vl[2 * (nbrruns - 1) + 1]))
                + 1)) { // we add a new one
      vl[2 * nbrruns] = start;
      vl[2 * nbrruns + 1] = length;
      nbrruns++;
      return;
    }
    int newend = (start) + (length) + 1;
    if (newend > oldend) { // we merge
      vl[2 * (nbrruns - 1) + 1] = (char) (newend - 1 - (vl[2 * (nbrruns - 1)]));
    }
  }

  private void smartAppendExclusive(char[] vl, char val) {
    int oldend;
    if ((nbrruns == 0)
        || ((val)
            > (oldend =
                (getValue(nbrruns - 1)) + (getLength(nbrruns - 1)) + 1))) { // we add a new one
      vl[2 * nbrruns] = val;
      vl[2 * nbrruns + 1] = 0;
      nbrruns++;
      return;
    }
    // We have that val <= oldend.
    if (oldend == val) {
      // we merge
      vl[2 * (nbrruns - 1) + 1]++;
      return;
    }
    // We have that val < oldend.

    int newend = val + 1;
    // We have that newend = val + 1 and val < oldend.
    // so newend <= oldend.

    if ((val) == (getValue(nbrruns - 1))) {
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
    setLength(nbrruns - 1, (char) (val - (getValue(nbrruns - 1)) - 1));

    if (newend < oldend) {
      setValue(nbrruns, (char) newend);
      setLength(nbrruns, (char) (oldend - newend - 1));
      nbrruns++;
    } // otherwise newend == oldend
  }

  private void smartAppendExclusive(char[] vl, char start, char length) {
    int oldend;
    if ((nbrruns == 0)
        || (start
            > (oldend = getValue(nbrruns - 1) + getLength(nbrruns - 1) + 1))) { // we add a new one
      vl[2 * nbrruns] = start;
      vl[2 * nbrruns + 1] = length;
      nbrruns++;
      return;
    }
    if (oldend == start) {
      // we merge
      vl[2 * (nbrruns - 1) + 1] += length + 1;
      return;
    }

    int newend = start + (length) + 1;

    if (start == getValue(nbrruns - 1)) {
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
    setLength(nbrruns - 1, (char) (start - getValue(nbrruns - 1) - 1));

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
        int runStart = (this.getValue(rlepos));
        int runEnd = runStart + (this.getLength(rlepos));

        for (int runValue = runStart; runValue <= runEnd; ++runValue) {
          answer.content.put(answer.cardinality++, (char) runValue);
        }
      }
      return answer;
    }
    MappeableBitmapContainer answer = new MappeableBitmapContainer();
    for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
      int start = (this.getValue(rlepos));
      int end = start + (this.getLength(rlepos)) + 1;
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
        int runStart = (this.getValue(rlepos));
        int runEnd = runStart + (this.getLength(rlepos));
        // next bit could potentially be faster, test
        if (BufferUtil.isBackedBySimpleArray(answer.content)) {
          char[] ba = answer.content.array();
          for (int runValue = runStart; runValue <= runEnd; ++runValue) {
            ba[answer.cardinality++] = (char) runValue;
          }
        } else {
          for (int runValue = runStart; runValue <= runEnd; ++runValue) {
            answer.content.put(answer.cardinality++, (char) runValue);
          }
        }
      }
      return answer;
    }
    MappeableBitmapContainer answer = new MappeableBitmapContainer();
    for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
      int start = (this.getValue(rlepos));
      int end = start + (this.getLength(rlepos)) + 1;
      BufferUtil.setBitmapRange(answer.bitmap, start, end);
    }
    answer.cardinality = card;
    return answer;
  }

  /**
   * Create a copy of the content of this container as a char array. This creates a copy.
   *
   * @return copy of the content as a char array
   */
  public char[] toCharArray() {
    char[] answer = new char[2 * nbrruns];
    valueslength.rewind();
    valueslength.get(answer);
    return answer;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("[]".length() + "-123456789,".length() * nbrruns);
    for (int k = 0; k < this.nbrruns; ++k) {
      sb.append('[');
      sb.append((int) (this.getValue(k)));
      sb.append(',');
      sb.append((this.getValue(k)) + (this.getLength(k)));
      sb.append(']');
    }
    return sb.toString();
  }

  @Override
  public void trim() {
    if (valueslength.limit() == 2 * nbrruns) {
      return;
    }
    if (BufferUtil.isBackedBySimpleArray(valueslength)) {
      this.valueslength = CharBuffer.wrap(Arrays.copyOf(valueslength.array(), 2 * nbrruns));
    } else {

      final CharBuffer co = CharBuffer.allocate(2 * nbrruns);
      char[] a = co.array();
      for (int k = 0; k < 2 * nbrruns; ++k) {
        a[k] = this.valueslength.get(k);
      }
      this.valueslength = co;
    }
  }

  // To check if a value length contains a given value
  private boolean valueLengthContains(int value, int index) {
    int initialValue = (getValue(index));
    int length = (getLength(index));

    return value <= initialValue + length;
  }

  @Override
  protected void writeArray(DataOutput out) throws IOException {
    out.writeShort(Character.reverseBytes((char) this.nbrruns));
    for (int k = 0; k < 2 * this.nbrruns; ++k) {
      out.writeShort(Character.reverseBytes(this.valueslength.get(k)));
    }
  }

  @Override
  protected void writeArray(ByteBuffer buffer) {
    assert buffer.order() == LITTLE_ENDIAN;
    CharBuffer source = valueslength.duplicate();
    source.position(0);
    source.limit(nbrruns * 2);
    CharBuffer target = buffer.asCharBuffer();
    target.put((char) nbrruns);
    target.put(source);
    int bytesWritten = (nbrruns * 2 + 1) * 2;
    buffer.position(buffer.position() + bytesWritten);
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeShort(Character.reverseBytes((char) this.nbrruns));
    for (int k = 0; k < 2 * this.nbrruns; ++k) {
      out.writeShort(Character.reverseBytes(this.valueslength.get(k)));
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
      return x.xor(this.getCharIterator());
    }
    // otherwise, we generate a bitmap
    return toBitmapOrArrayContainer(card).ixor(x);
  }

  @Override
  public MappeableContainer xor(MappeableBitmapContainer x) {
    MappeableBitmapContainer answer = x.clone();
    for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
      int start = (this.getValue(rlepos));
      int end = start + (this.getLength(rlepos)) + 1;
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
        new MappeableRunContainer(CharBuffer.allocate(2 * (this.nbrruns + x.nbrruns)), 0);
    char[] vl = answer.valueslength.array();
    int rlepos = 0;
    int xrlepos = 0;

    while (true) {
      if ((getValue(rlepos)) - (x.getValue(xrlepos)) < 0) {
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
  public void forEach(char msb, IntConsumer ic) {
    int high = ((int) msb) << 16;
    for (int k = 0; k < this.nbrruns; ++k) {
      int base = (this.getValue(k) & 0xFFFF) | high;
      int le = this.getLength(k) & 0xFFFF;
      for (int l = base; l - le <= base; ++l) {
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
    int rleval = (this.getValue(rlepos));
    int rlelength = (this.getLength(rlepos));
    while (arraypos < x.cardinality) {
      int arrayval = (x.content.get(arraypos));
      while (rleval + rlelength < arrayval) { // this will frequently be false
        ++rlepos;
        if (rlepos == this.nbrruns) {
          return andCardinality; // we are done
        }
        rleval = (this.getValue(rlepos));
        rlelength = (this.getLength(rlepos));
      }
      if (rleval > arrayval) {
        arraypos =
            BufferUtil.advanceUntil(x.content, arraypos, x.cardinality, this.getValue(rlepos));
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
      int runStart = this.getValue(rlepos);
      int runEnd = runStart + this.getLength(rlepos);
      cardinality += x.cardinalityInRange(runStart, runEnd + 1);
    }
    return cardinality;
  }

  @Override
  public int andCardinality(MappeableRunContainer x) {
    int cardinality = 0;
    int rlepos = 0;
    int xrlepos = 0;
    int start = (this.getValue(rlepos));
    int end = start + (this.getLength(rlepos)) + 1;
    int xstart = (x.getValue(xrlepos));
    int xend = xstart + (x.getLength(xrlepos)) + 1;
    while ((rlepos < this.nbrruns) && (xrlepos < x.nbrruns)) {
      if (end <= xstart) {
        ++rlepos;
        if (rlepos < this.nbrruns) {
          start = (this.getValue(rlepos));
          end = start + (this.getLength(rlepos)) + 1;
        }
      } else if (xend <= start) {
        ++xrlepos;

        if (xrlepos < x.nbrruns) {
          xstart = (x.getValue(xrlepos));
          xend = xstart + (x.getLength(xrlepos)) + 1;
        }
      } else { // they overlap
        final int lateststart = Math.max(start, xstart);
        int earliestend;
        if (end == xend) { // improbable
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

        } else { // end > xend
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
  public MappeableBitmapContainer toBitmapContainer() {
    int card = this.getCardinality();
    MappeableBitmapContainer answer = new MappeableBitmapContainer();
    for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
      int start = (this.getValue(rlepos));
      int end = start + (this.getLength(rlepos)) + 1;
      BufferUtil.setBitmapRange(answer.bitmap, start, end);
    }
    answer.cardinality = card;
    return answer;
  }

  @Override
  public int first() {
    assertNonEmpty(numberOfRuns() == 0);
    return (getValue(0));
  }

  @Override
  public int last() {
    assertNonEmpty(numberOfRuns() == 0);
    int index = numberOfRuns() - 1;
    int start = (getValue(index));
    int length = (getLength(index));
    return start + length;
  }

  @Override
  public int nextValue(char fromValue) {
    int index = bufferedUnsignedInterleavedBinarySearch(valueslength, 0, nbrruns, fromValue);
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
    int index = bufferedUnsignedInterleavedBinarySearch(valueslength, 0, nbrruns, fromValue);
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
    int index = bufferedUnsignedInterleavedBinarySearch(valueslength, 0, nbrruns, fromValue);
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
    int index = bufferedUnsignedInterleavedBinarySearch(valueslength, 0, nbrruns, fromValue);
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
  protected boolean contains(MappeableRunContainer runContainer) {
    int i1 = 0, i2 = 0;
    while (i1 < numberOfRuns() && i2 < runContainer.numberOfRuns()) {
      int start1 = (getValue(i1));
      int stop1 = start1 + (getLength(i1));
      int start2 = (runContainer.getValue(i2));
      int stop2 = start2 + (runContainer.getLength(i2));
      if (start1 > start2) {
        return false;
      } else {
        if (stop1 > stop2) {
          i2++;
        } else if (stop1 == stop2) {
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
    while (ia < arrayContainer.getCardinality() && ir < runCount) {
      int start = (getValue(ir));
      int stop = start + (getLength(ir));
      int value = (arrayContainer.content.get(ia));
      if (value < start) {
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
    char ib = 0, ir = 0;
    int start = getValue(0);
    int stop = start + getLength(0);
    while (ib < MappeableBitmapContainer.MAX_CAPACITY / 64 && ir < runCount) {
      long w = bitmapContainer.bitmap.get(ib);
      while (w != 0) {
        long r = ib * 64 + Long.numberOfTrailingZeros(w);
        if (r < start) {
          return false;
        } else if (r > stop) {
          ++ir;
          if (ir == runCount) {
            break;
          }
          start = getValue(ir);
          stop = start + getLength(ir);

        } else if (ib * 64 + 64 < stop) {
          ib = (char) (stop / 64);
          w = bitmapContainer.bitmap.get(ib);
        } else {
          w &= w - 1;
        }
      }
      if (w == 0) {
        ++ib;
      } else {
        return false;
      }
    }
    if (ib < MappeableBitmapContainer.MAX_CAPACITY / 64) {
      for (; ib < MappeableBitmapContainer.MAX_CAPACITY / 64; ib++) {
        if (bitmapContainer.bitmap.get(ib) != 0) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public boolean intersects(int minimum, int supremum) {
    if ((minimum < 0) || (supremum < minimum) || (supremum > (1 << 16))) {
      throw new RuntimeException("This should never happen (bug).");
    }
    for (int i = 0; i < numberOfRuns(); ++i) {
      char runFirstValue = getValue(i);
      char runLastValue = (char) (runFirstValue + getLength(i));

      if ((runFirstValue) < supremum && (runLastValue) - ((char) minimum) >= 0) {
        return true;
      }
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
}

final class MappeableRunContainerCharIterator implements PeekableCharIterator {
  private int pos;
  private int le = 0;
  private int maxlength;
  private int base;

  private MappeableRunContainer parent;

  MappeableRunContainerCharIterator() {}

  MappeableRunContainerCharIterator(MappeableRunContainer p) {
    wrap(p);
  }

  @Override
  public PeekableCharIterator clone() {
    try {
      return (PeekableCharIterator) super.clone();
    } catch (CloneNotSupportedException e) {
      return null; // will not happen
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
        maxlength = parent.getLength(pos);
        base = parent.getValue(pos);
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
        maxlength = parent.getLength(pos);
        base = parent.getValue(pos);
      }
    }
    return ans;
  }

  @Override
  public void remove() {
    throw new RuntimeException("Not implemented"); // TODO
  }

  void wrap(MappeableRunContainer p) {
    parent = p;
    pos = 0;
    le = 0;
    if (pos < parent.nbrruns) {
      maxlength = parent.getLength(pos);
      base = parent.getValue(pos);
    }
  }

  @Override
  public void advanceIfNeeded(char minval) {
    while (base + maxlength < (minval)) {
      pos++;
      le = 0;
      if (pos < parent.nbrruns) {
        maxlength = parent.getLength(pos);
        base = parent.getValue(pos);
      } else {
        return;
      }
    }
    if (base > (minval)) {
      return;
    }
    le = minval - base;
  }

  @Override
  public char peekNext() {
    return (char) (base + le);
  }
}

final class RawMappeableRunContainerCharIterator implements PeekableCharIterator {
  private int pos;
  private int le = 0;
  private int maxlength;
  private int base;

  private MappeableRunContainer parent;
  private char[] vl;

  RawMappeableRunContainerCharIterator(MappeableRunContainer p) {
    wrap(p);
  }

  @Override
  public PeekableCharIterator clone() {
    try {
      return (PeekableCharIterator) super.clone();
    } catch (CloneNotSupportedException e) {
      return null; // will not happen
    }
  }

  private char getLength(int index) {
    return vl[2 * index + 1];
  }

  private char getValue(int index) {
    return vl[2 * index];
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
        maxlength = getLength(pos);
        base = getValue(pos);
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
        maxlength = getLength(pos);
        base = getValue(pos);
      }
    }
    return ans;
  }

  @Override
  public void remove() {
    throw new RuntimeException("Not implemented"); // TODO
  }

  private void wrap(MappeableRunContainer p) {
    parent = p;
    if (!parent.isArrayBacked()) {
      throw new RuntimeException("internal error");
    }
    vl = parent.valueslength.array();
    pos = 0;
    le = 0;
    if (pos < parent.nbrruns) {
      maxlength = getLength(pos);
      base = getValue(pos);
    }
  }

  @Override
  public void advanceIfNeeded(char minval) {
    while (base + maxlength < minval) {
      pos++;
      le = 0;
      if (pos < parent.nbrruns) {
        maxlength = parent.getLength(pos);
        base = parent.getValue(pos);
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

final class RawReverseMappeableRunContainerCharIterator implements CharIterator {
  private int pos;
  private int le;
  private int maxlength;
  private int base;
  private char[] vl;

  RawReverseMappeableRunContainerCharIterator(MappeableRunContainer p) {
    wrap(p);
  }

  @Override
  public CharIterator clone() {
    try {
      return (CharIterator) super.clone();
    } catch (CloneNotSupportedException e) {
      return null; // will not happen
    }
  }

  private char getLength(int index) {
    return vl[2 * index + 1];
  }

  private char getValue(int index) {
    return vl[2 * index];
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
        maxlength = getLength(pos);
        base = getValue(pos);
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
        maxlength = getLength(pos);
        base = getValue(pos);
      }
    }
    return ans;
  }

  @Override
  public void remove() {
    throw new RuntimeException("Not implemented"); // TODO
  }

  private void wrap(MappeableRunContainer p) {
    MappeableRunContainer parent = p;
    if (!parent.isArrayBacked()) {
      throw new RuntimeException("internal error");
    }
    vl = parent.valueslength.array();
    pos = parent.nbrruns - 1;
    le = 0;
    if (pos >= 0) {
      maxlength = getLength(pos);
      base = getValue(pos);
    }
  }
}

final class ReverseMappeableRunContainerCharIterator implements CharIterator {
  private int pos;
  private int le;
  private int maxlength;
  private int base;
  private MappeableRunContainer parent;

  ReverseMappeableRunContainerCharIterator() {}

  ReverseMappeableRunContainerCharIterator(MappeableRunContainer p) {
    wrap(p);
  }

  @Override
  public CharIterator clone() {
    try {
      return (CharIterator) super.clone();
    } catch (CloneNotSupportedException e) {
      return null; // will not happen
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
        maxlength = parent.getLength(pos);
        base = parent.getValue(pos);
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
        maxlength = parent.getLength(pos);
        base = parent.getValue(pos);
      }
    }
    return ans;
  }

  @Override
  public void remove() {
    throw new RuntimeException("Not implemented"); // TODO
  }

  void wrap(MappeableRunContainer p) {
    parent = p;
    pos = parent.nbrruns - 1;
    le = 0;
    if (pos >= 0) {
      maxlength = parent.getLength(pos);
      base = parent.getValue(pos);
    }
  }
}
