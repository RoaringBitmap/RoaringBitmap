/*
 * (c) the authors
 * Licensed under the Apache License, Version 2.0.
 */
package org.roaringbitmap.buffer;


import org.roaringbitmap.PeekableShortIterator;
import org.roaringbitmap.ShortIterator;

import java.io.*;
import java.nio.ShortBuffer;
import java.util.Arrays;
import java.util.Iterator;

/**
 * This container takes the form of runs of consecutive values (effectively,
 * run-length encoding).  Uses a ShortBuffer to store data, unlike
 * org.roaringbitmap.RunContainer.  Otherwise similar.
 * 
 * 
 * Adding and removing content from this container might make it wasteful
 * so regular calls to "runOptimize" might be warranted.
 */
public final class MappeableRunContainer extends MappeableContainer implements Cloneable {
    private static final int DEFAULT_INIT_SIZE = 4;
    protected ShortBuffer valueslength;

    protected int nbrruns = 0;// how many runs, this number should fit in 16 bits.


    private static final long serialVersionUID = 1L;
    
    private MappeableRunContainer(int nbrruns, final ShortBuffer valueslength) {
        this.nbrruns = nbrruns;
        this.valueslength = ShortBuffer.allocate(Math.max(2*nbrruns,valueslength.limit()));
        valueslength.rewind();
        this.valueslength.put(valueslength);  // may copy more than it needs to??
    }

    /**
     * Construct a new RunContainer backed by the provided ShortBuffer. Note
     * that if you modify the RunContainer a new ShortBuffer may be produced.
     * 
     * @param array
     *            ShortBuffer where the data is stored
     * @param numRuns
     *            number of runs (each using 2 shorts in the buffer)
     *            
     */
    public MappeableRunContainer(final ShortBuffer array,
            final int numRuns) {
        if (array.limit() < 2*numRuns)
            throw new RuntimeException(
                    "Mismatch between buffer and numRuns");
        this.nbrruns = numRuns;
        this.valueslength = array;
    }




    protected MappeableRunContainer( ShortIterator sIt, int nbrRuns) {
        this.nbrruns = nbrRuns;
        valueslength = ShortBuffer.allocate(2*nbrRuns);
        if (nbrRuns == 0) return;

        int prevVal = -2; 
        int runLen=0;
        int runCount=0;
        while (sIt.hasNext()) {
            int curVal = BufferUtil.toIntUnsigned(sIt.next());
            if (curVal == prevVal+1)
                ++runLen;
            else {
                if (runCount > 0)
                    setLength(runCount-1, (short) runLen); 
                setValue(runCount, (short) curVal);
                runLen=0;
                ++runCount;
            }
            prevVal = curVal;
        }
        setLength(runCount-1, (short) runLen);
    }

    // convert a bitmap container to a run container somewhat efficiently.
    protected MappeableRunContainer(MappeableBitmapContainer bc, int nbrRuns) {
        this.nbrruns = nbrRuns;
        valueslength = ShortBuffer.allocate(2 * nbrRuns);
        if(! BufferUtil.isBackedBySimpleArray(valueslength))
            throw new RuntimeException("Unexpected internal error.");
        short[] vl = valueslength.array();
        if (nbrRuns == 0) return;
        if(bc.isArrayBacked()) {
            long[] b = bc.bitmap.array();
            int longCtr = 0;  // index of current long in bitmap
            long curWord = b[0];  //its value
            int runCount=0;
            final int len = bc.bitmap.limit();
            while (true) {
                // potentially multiword advance to first 1 bit
                while (curWord == 0L && longCtr < len - 1)
                    curWord = b[ ++longCtr];

                if (curWord == 0L) {
                    // wrap up, no more runs
                    return;
                }
                int localRunStart = Long.numberOfTrailingZeros(curWord);
                int runStart = localRunStart   + 64*longCtr;
                // stuff 1s into number's LSBs
                long curWordWith1s = curWord | ((1L << runStart) - 1);

                // find the next 0, potentially in a later word
                int runEnd = 0;
                while (curWordWith1s == -1L && longCtr < len - 1)
                    curWordWith1s = b[++longCtr];

                if (curWordWith1s == -1L) {
                    // a final unterminated run of 1s (32 of them)
                    runEnd = Long.numberOfTrailingZeros(~curWordWith1s) + longCtr*64;
                    //setValue(runCount, (short) runStart);
                    vl[2 * runCount ] = (short) runStart;
                    //setLength(runCount, (short) (runEnd-runStart-1));
                    vl[2 * runCount + 1 ] = (short) (runEnd-runStart-1);
                    return;
                }
                int localRunEnd = Long.numberOfTrailingZeros(~curWordWith1s);
                runEnd = localRunEnd + longCtr*64;
                // setValue(runCount, (short) runStart);
                vl[2 * runCount ] = (short) runStart;
                // setLength(runCount, (short) (runEnd-runStart-1));
                vl[2 * runCount + 1 ] = (short) (runEnd-runStart-1);
                runCount++;
                // now, zero out everything right of runEnd.

                curWord = (curWordWith1s >>> localRunEnd) << localRunEnd;
                // We've lathered and rinsed, so repeat...
            }
        } else {
            int longCtr = 0;  // index of current long in bitmap
            long curWord = bc.bitmap.get(0);  //its value
            int runCount=0;
            final int len = bc.bitmap.limit();
            while (true) {
                // potentially multiword advance to first 1 bit
                while (curWord == 0L && longCtr < len - 1)
                    curWord = bc.bitmap.get( ++longCtr);

                if (curWord == 0L) {
                    // wrap up, no more runs
                    return;
                }
                int localRunStart = Long.numberOfTrailingZeros(curWord);
                int runStart = localRunStart   + 64*longCtr;
                // stuff 1s into number's LSBs
                long curWordWith1s = curWord | ((1L << runStart) - 1);

                // find the next 0, potentially in a later word
                int runEnd = 0;
                while (curWordWith1s == -1L && longCtr < len - 1)
                    curWordWith1s = bc.bitmap.get(++longCtr);

                if (curWordWith1s == -1L) {
                    // a final unterminated run of 1s (32 of them)
                    runEnd = Long.numberOfTrailingZeros(~curWordWith1s) + longCtr*64;
                    //setValue(runCount, (short) runStart);
                    vl[2 * runCount ] = (short) runStart;
                    //setLength(runCount, (short) (runEnd-runStart-1));
                    vl[2 * runCount + 1 ] = (short) (runEnd-runStart-1);
                    return;
                }
                int localRunEnd = Long.numberOfTrailingZeros(~curWordWith1s);
                runEnd = localRunEnd + longCtr*64;
                // setValue(runCount, (short) runStart);
                vl[2 * runCount ] = (short) runStart;
                // setLength(runCount, (short) (runEnd-runStart-1));
                vl[2 * runCount + 1 ] = (short) (runEnd-runStart-1);
                runCount++;
                // now, zero out everything right of runEnd.

                curWord = (curWordWith1s >>> localRunEnd) << localRunEnd;
                // We've lathered and rinsed, so repeat...
            }

        }            
    }


    @Override
    int numberOfRuns() {
        return this.nbrruns;
    }

    /**
     * Convert the container to either a Bitmap or an Array Container, depending
     * on the cardinality.
     * @param card the current cardinality
     * @return new container
     */
    MappeableContainer toBitmapOrArrayContainer(int card) {
    	//int card = this.getCardinality();
    	if(card <= MappeableArrayContainer.DEFAULT_MAX_SIZE) {
        	MappeableArrayContainer answer = new MappeableArrayContainer(card);
        	answer.cardinality=0;
            for (int rlepos=0; rlepos < this.nbrruns; ++rlepos) {
                int runStart = BufferUtil.toIntUnsigned(this.getValue(rlepos));
                int runEnd = runStart + BufferUtil.toIntUnsigned(this.getLength(rlepos));
        
                for (int runValue = runStart; runValue <= runEnd; ++runValue) {
                    answer.content.put(answer.cardinality++,(short) runValue);
                }
            }
            return answer;
    	}
    	MappeableBitmapContainer answer = new MappeableBitmapContainer();
        for (int rlepos=0; rlepos < this.nbrruns; ++rlepos) {
            int start = BufferUtil.toIntUnsigned(this.getValue(rlepos));
            int end = start + BufferUtil.toIntUnsigned(this.getLength(rlepos)) + 1;
            BufferUtil.setBitmapRange(answer.bitmap, start, end); 
        }
        answer.cardinality = card;
        return answer;
    }


    // convert to bitmap or array *if needed*
    private MappeableContainer toEfficientContainer() { 
        int sizeAsRunContainer = MappeableRunContainer.serializedSizeInBytes(this.nbrruns);
        int sizeAsBitmapContainer = MappeableBitmapContainer.serializedSizeInBytes(0);
        int card = this.getCardinality();
        int sizeAsArrayContainer = MappeableArrayContainer.serializedSizeInBytes(card);
        if(sizeAsRunContainer <= Math.min(sizeAsBitmapContainer, sizeAsArrayContainer))
            return this;
        if(card <= MappeableArrayContainer.DEFAULT_MAX_SIZE) {
            MappeableArrayContainer answer = new MappeableArrayContainer(card);
            answer.cardinality=0;
            for (int rlepos=0; rlepos < this.nbrruns; ++rlepos) {
                int runStart = BufferUtil.toIntUnsigned(this.getValue(rlepos));
                int runEnd = runStart + BufferUtil.toIntUnsigned(this.getLength(rlepos));
                // next bit could potentially be faster, test 
                if (BufferUtil.isBackedBySimpleArray(answer.content)) {
                    short[] ba = answer.content.array();
                    for (int runValue = runStart; runValue <= runEnd; ++runValue) {
                        ba[answer.cardinality++] = (short) runValue;
                    }
                } else {
                    for (int runValue = runStart; runValue <= runEnd; ++runValue) {
                        answer.content.put(answer.cardinality++,
                                (short) runValue);
                    }
                }
            }
            return answer;
        }
        MappeableBitmapContainer answer = new MappeableBitmapContainer();
        for (int rlepos=0; rlepos < this.nbrruns; ++rlepos) {
            int start = BufferUtil.toIntUnsigned(this.getValue(rlepos));
            int end = start + BufferUtil.toIntUnsigned(this.getLength(rlepos)) + 1;
            BufferUtil.setBitmapRange(answer.bitmap, start, end); 
        }
        answer.cardinality = card;
        return answer;
    }


    // convert to bitmap  *if needed* (useful if you know it can't be an array)
    private MappeableContainer toBitmapIfNeeded() {
        int sizeAsRunContainer = MappeableRunContainer.serializedSizeInBytes(this.nbrruns);
        int sizeAsBitmapContainer = MappeableBitmapContainer.serializedSizeInBytes(0);
        if(sizeAsBitmapContainer > sizeAsRunContainer)
            return this;
        int card = this.getCardinality();
        MappeableBitmapContainer answer = new MappeableBitmapContainer();
        for (int rlepos=0; rlepos < this.nbrruns; ++rlepos) {
            int start = BufferUtil.toIntUnsigned(this.getValue(rlepos));
            int end = start + BufferUtil.toIntUnsigned(this.getLength(rlepos)) + 1;
            BufferUtil.setBitmapRange(answer.bitmap, start, end); 
        }
        answer.cardinality = card;
        return answer;        
    }
    /** 
     *  Convert to Array or Bitmap container if the serialized form would be shorter
     */

     @Override
     public MappeableContainer runOptimize() {
         int currentSize = getArraySizeInBytes(); 
         int card = getCardinality(); 
         if (card <= MappeableArrayContainer.DEFAULT_MAX_SIZE) {
             if (currentSize > MappeableArrayContainer.getArraySizeInBytes(card))  
                 return toBitmapOrArrayContainer(card);
         }
         else if (currentSize > MappeableBitmapContainer.getArraySizeInBytes(card)) {  
             return toBitmapOrArrayContainer(card);
         }
         return this;
     }

    private void increaseCapacity() {
        int newCapacity = (valueslength.capacity() == 0) ? DEFAULT_INIT_SIZE : valueslength.capacity() < 64 ? valueslength.capacity() * 2
            : valueslength.capacity() < 1024 ? valueslength.capacity() * 3 / 2
            : valueslength.capacity() * 5 / 4;

        final ShortBuffer nv = ShortBuffer.allocate(newCapacity);
        valueslength.rewind();
        nv.put(valueslength);
        valueslength = nv;
    }
    
    // Push all values length to the end of the array (resize array if needed)
    private void copyToOffset(int offset) {
        final int minCapacity = 2 * (offset + nbrruns) ;
        if (valueslength.capacity() < minCapacity) {
            // expensive case where we need to reallocate
            int newCapacity = valueslength.capacity();
            while (newCapacity < minCapacity) {
                newCapacity = (newCapacity == 0) ? DEFAULT_INIT_SIZE
                        : newCapacity < 64 ? newCapacity * 2
                        : newCapacity < 1024 ? newCapacity * 3 / 2
                        : newCapacity * 5 / 4;
            }
            ShortBuffer newvalueslength = ShortBuffer.allocate(newCapacity);
            copyValuesLength(this.valueslength, 0, newvalueslength, offset, nbrruns);
            this.valueslength = newvalueslength;
        } else {
            // efficient case where we just copy 
            copyValuesLength(this.valueslength, 0, this.valueslength, offset, nbrruns);
        }
    }
    
    // not actually used anywhere, but potentially useful
    protected void   ensureCapacity(int minNbRuns) {
        final int minCapacity = 2 * minNbRuns;
        if (valueslength.capacity() < minCapacity) {            
            int newCapacity = valueslength.capacity();
            while (newCapacity < minCapacity) {
                newCapacity = (newCapacity == 0) ? DEFAULT_INIT_SIZE
                        : newCapacity < 64 ? newCapacity * 2
                        : newCapacity < 1024 ? newCapacity * 3 / 2
                        : newCapacity * 5 / 4;
            }
            final ShortBuffer nv = ShortBuffer.allocate(newCapacity);
            valueslength.rewind();
            nv.put(valueslength);
            valueslength = nv;
        }
    }

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


    @Override
    public Iterator<Short> iterator() {
        final ShortIterator i  = getShortIterator();
        return new Iterator<Short>() {

            @Override
            public boolean hasNext() {
               return  i.hasNext();
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
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeShort(Short.reverseBytes((short) this.nbrruns));
        for (int k = 0; k < 2 * this.nbrruns; ++k) {
            out.writeShort(Short.reverseBytes(this.valueslength.get(k)));
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        // little endian
        this.nbrruns = 0xFFFF & Short.reverseBytes(in.readShort());
        if (this.valueslength.capacity() < 2*this.nbrruns)
            this.valueslength = ShortBuffer.allocate(2*this.nbrruns);
        for (int k = 0; k < 2*this.nbrruns; ++k) {
            this.valueslength.put(k,Short.reverseBytes(in.readShort()));
        }
    }

    @Override
    public MappeableContainer flip(short x) {
        if(this.contains(x))
            return this.remove(x);
        else return this.add(x);
    }

    @Override
    public MappeableContainer add(short k) {
        // TODO: it might be better and simpler to do return toBitmapOrArrayContainer(getCardinality()).add(k) 
        int index = bufferedUnsignedInterleavedBinarySearch(valueslength, 0, nbrruns, k);
        if(index >= 0) return this;// already there
        index = - index - 2;// points to preceding value, possibly -1
        if(index >= 0) {// possible match
            int offset = BufferUtil.toIntUnsigned(k) - BufferUtil.toIntUnsigned(getValue(index));
            int le =     BufferUtil.toIntUnsigned(getLength(index)); 
            if(offset <= le) return this;
            if(offset == le + 1) {
                // we may need to fuse
                if(index + 1 < nbrruns) {
                    if(BufferUtil.toIntUnsigned(getValue(index + 1))  == BufferUtil.toIntUnsigned(k) + 1) {
                        // indeed fusion is needed
                        setLength(index, (short) (getValue(index + 1) + getLength(index + 1) - getValue(index)));
                        recoverRoomAtIndex(index + 1);
                        return this;
                    }
                }
                incrementLength(index);
                return this;
            }
            if(index + 1 < nbrruns) {
                // we may need to fuse
                if(BufferUtil.toIntUnsigned(getValue(index + 1))  == BufferUtil.toIntUnsigned(k) + 1) {
                    // indeed fusion is needed
                    setValue(index+1, k);
                    setLength(index+1, (short) (getLength(index + 1) + 1));
                    return this;
                }
            }
        }
        if(index == -1) {
            // we may need to extend the first run
            if(0 < nbrruns) {
                if(getValue(0)  == k + 1) {
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
    public MappeableContainer add(int begin, int end) {
        MappeableRunContainer rc = (MappeableRunContainer) clone();
        return rc.iadd(begin, end);
    }

    @Override
    public MappeableContainer and(MappeableArrayContainer x) {
        MappeableArrayContainer ac = new MappeableArrayContainer(x.cardinality);
        if(this.nbrruns == 0) return ac;
        int rlepos = 0;
        int arraypos = 0;

        int rleval = BufferUtil.toIntUnsigned(this.getValue(rlepos));
        int rlelength = BufferUtil.toIntUnsigned(this.getLength(rlepos));
        while(arraypos < x.cardinality)  {
            int arrayval = BufferUtil.toIntUnsigned(x.content.get(arraypos));
            while(rleval + rlelength < arrayval) {// this will frequently be false
                ++rlepos;
                if(rlepos == this.nbrruns) {
                    return ac;// we are done
                }
                rleval = BufferUtil.toIntUnsigned(this.getValue(rlepos));
                rlelength = BufferUtil.toIntUnsigned(this.getLength(rlepos));
            }
            if(rleval > arrayval)  {
                arraypos = BufferUtil.advanceUntil(x.content,arraypos,x.cardinality,this.getValue(rlepos));
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
        if (card <=  MappeableArrayContainer.DEFAULT_MAX_SIZE) {
            // result can only be an array (assuming that we never make a RunContainer)
        	MappeableArrayContainer answer = new MappeableArrayContainer(card);
        	answer.cardinality=0;
            for (int rlepos=0; rlepos < this.nbrruns; ++rlepos) {
                int runStart = BufferUtil.toIntUnsigned(this.getValue(rlepos));
                int runEnd = runStart + BufferUtil.toIntUnsigned(this.getLength(rlepos));
                for (int runValue = runStart; runValue <= runEnd; ++runValue) {
                    if ( x.contains((short) runValue)) {
                        answer.content.put(answer.cardinality++,(short) runValue);
                    }
                }
            }
            return answer;
        }
        // we expect the answer to be a bitmap (if we are lucky)

    	MappeableBitmapContainer answer = x.clone();
        int start = 0;
        for(int rlepos = 0; rlepos < this.nbrruns; ++rlepos ) {
            int end = BufferUtil.toIntUnsigned(this.getValue(rlepos));
            BufferUtil.resetBitmapRange(answer.bitmap, start, end);  
            start = end + BufferUtil.toIntUnsigned(this.getLength(rlepos)) + 1;
        }
        BufferUtil.resetBitmapRange(answer.bitmap, start, BufferUtil.maxLowBitAsInteger() + 1); 
        answer.computeCardinality();
        if(answer.getCardinality() > MappeableArrayContainer.DEFAULT_MAX_SIZE)
            return answer;
        else return answer.toArrayContainer();

    }


    
    @Override
    public MappeableContainer andNot(MappeableBitmapContainer x) {
    	int card = this.getCardinality();
        if (card <=  MappeableArrayContainer.DEFAULT_MAX_SIZE) {
            // result can only be an array (assuming that we never make a RunContainer)
        	MappeableArrayContainer answer = new MappeableArrayContainer(card);
        	answer.cardinality=0;
            for (int rlepos=0; rlepos < this.nbrruns; ++rlepos) {
                int runStart = BufferUtil.toIntUnsigned(this.getValue(rlepos));
                int runEnd = runStart + BufferUtil.toIntUnsigned(this.getLength(rlepos));
                for (int runValue = runStart; runValue <= runEnd; ++runValue) {
                    if ( ! x.contains((short) runValue)) {
                        answer.content.put(answer.cardinality++,(short) runValue);
                    }
                }
            }
            return answer;
        }
        // we expect the answer to be a bitmap (if we are lucky)
    	MappeableBitmapContainer answer = x.clone();
        int lastPos = 0;
        for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
            int start = BufferUtil.toIntUnsigned(this.getValue(rlepos));
            int end = start + BufferUtil.toIntUnsigned(this.getLength(rlepos)) + 1;
            BufferUtil.resetBitmapRange(answer.bitmap, lastPos, start); 
            BufferUtil.flipBitmapRange(answer.bitmap, start, end);
            lastPos = end;
        }
        BufferUtil.resetBitmapRange(answer.bitmap, lastPos, answer.bitmap.capacity()*64);
        answer.computeCardinality();
        if (answer.getCardinality() > MappeableArrayContainer.DEFAULT_MAX_SIZE)
            return answer;
        else
            return answer.toArrayContainer();
    }


    @Override
    public MappeableContainer andNot(MappeableArrayContainer x) {
        // when x is small, we guess that the result will still be a run container
        final int arbitrary_threshold = 32; // this is arbitrary
        if(x.getCardinality() < arbitrary_threshold)
            return lazyandNot(x).toEfficientContainer();
        // otherwise we generate either an array or bitmap container
        final int card = getCardinality();
        if(card <= MappeableArrayContainer.DEFAULT_MAX_SIZE) {
            // if the cardinality is small, we construct the solution in place
            MappeableArrayContainer ac = new MappeableArrayContainer(card);
            ac.cardinality = org.roaringbitmap.Util.unsignedDifference(this.getShortIterator(), x.getShortIterator(), ac.content.array());
            return ac;
        }
        // otherwise, we generate a bitmap
        return toBitmapOrArrayContainer(card).iandNot(x);
    }



    @Override
    public void clear() {
        nbrruns = 0;
    }

    @Override
    public MappeableContainer clone() {
        return new MappeableRunContainer(nbrruns, valueslength);
    }

    @Override
    public boolean contains(short x) {
        int index = bufferedUnsignedInterleavedBinarySearch(valueslength, 0, nbrruns, x);
        if(index >= 0) return true;
        index = - index - 2; // points to preceding value, possibly -1
        if (index != -1)  {// possible match
            int offset = BufferUtil.toIntUnsigned(x) - BufferUtil.toIntUnsigned(getValue(index));
            int le =     BufferUtil.toIntUnsigned(getLength(index)); 
            if(offset <= le) return true;
        }
        return false;
    }

    @Override
    public void fillLeastSignificant16bits(int[] x, int i, int mask) {
        int pos = i;
        for (int k = 0; k < this.nbrruns; ++k) {
            final int limit = BufferUtil.toIntUnsigned(this.getLength(k));
            final int base = BufferUtil.toIntUnsigned(this.getValue(k));
            for(int le = 0; le <= limit; ++le) {
                x[pos++] = (base + le) | mask;
            }
        }
    }

    @Override
    protected int getArraySizeInBytes() {
        return 2+4*this.nbrruns;  // "array" includes its size
    }

    protected static int getArraySizeInBytes(int nbrruns) {
        return 2+4*nbrruns;  
    }


    @Override
    public int getCardinality() {
        int sum = 0;
        for(int k = 0; k < nbrruns; ++k)
            sum = sum + BufferUtil.toIntUnsigned(getLength(k)) + 1;
        return sum;
    }

    @Override
    public ShortIterator getShortIterator() {
        if(isArrayBacked())
            return new RawMappeableRunContainerShortIterator(this);
        return new MappeableRunContainerShortIterator(this);
    }

    @Override
    public ShortIterator getReverseShortIterator() {
        if(isArrayBacked())
            return new RawReverseMappeableRunContainerShortIterator(this);
        return new ReverseMappeableRunContainerShortIterator(this);
    }

    @Override
    public int getSizeInBytes() {
        return this.nbrruns*4+4;  // not sure about how exact it will be
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
    public MappeableContainer iandNot(MappeableArrayContainer x) {
        return andNot(x);
    }

    @Override
    public MappeableContainer iandNot(MappeableBitmapContainer x) {
        return andNot(x);
    }

    @Override
    public MappeableContainer inot(int rangeStart, int rangeEnd) {
        if (rangeEnd <= rangeStart) return this;  
        else
          return not( rangeStart, rangeEnd);  // TODO: inplace option?
    }

    @Override
    public MappeableContainer ior(MappeableArrayContainer x) {
        if(isFull()) return this;
        final int nbrruns = this.nbrruns;
        final int offset = Math.max(nbrruns, x.getCardinality());
        copyToOffset(offset);
        short[] vl = this.valueslength.array();
        int rlepos = 0;
        this.nbrruns = 0;
        PeekableShortIterator i = (PeekableShortIterator) x.getShortIterator();
        while (i.hasNext() && (rlepos < nbrruns) ) {
            if(BufferUtil.compareUnsigned(getValue(vl,rlepos + offset), i.peekNext()) <= 0) {
                smartAppend(vl,getValue(vl,rlepos + offset), getLength(vl,rlepos + offset));
                rlepos++;
            } else {
                smartAppend(vl,i.next());
            }
        }        
        if (i.hasNext()) {
            /*if(this.nbrruns>0) {
                // this might be useful if the run container has just one very large run
                int lastval = BufferUtil.toIntUnsigned(getValue(nbrruns + offset - 1))
                        + BufferUtil.toIntUnsigned(getLength(nbrruns + offset - 1)) + 1;
                i.advanceIfNeeded((short) lastval);
            }*/
            while (i.hasNext()) {
                smartAppend(vl,i.next());
            }
        } else {
            while (rlepos < nbrruns) {
                smartAppend(vl,getValue(vl,rlepos + offset), getLength(vl,rlepos + offset));
                rlepos++;
            }
        }
        return toEfficientContainer();
    }

    @Override
    public MappeableContainer ior(MappeableBitmapContainer x) {
        if(isFull()) return this;
        return or(x);
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
    public MappeableContainer not(int rangeStart, int rangeEnd) {
        if (rangeEnd <= rangeStart) return this.clone();
        MappeableRunContainer ans = new MappeableRunContainer(nbrruns+1);
        if(!ans.isArrayBacked())
            throw new RuntimeException("internal bug");
        short[] vl = ans.valueslength.array();
        int k = 0;
        for(; (k < this.nbrruns) && ((BufferUtil.toIntUnsigned(this.getValue(k)) < rangeStart)) ; ++k) {
                vl[2 * k] = getValue(k);//TODO: optimize when this is an array
                vl[2 * k + 1] = getLength(k);
                ans.nbrruns++;
        }
        ans.smartAppendExclusive(vl,(short)rangeStart,(short)(rangeEnd-rangeStart-1));
        for(; k < this.nbrruns ; ++k) {
            ans.smartAppendExclusive(vl,getValue(k), getLength(k));
        }
        return ans;
    }

    @Override
    public MappeableContainer or(MappeableArrayContainer x) {
        // we guess that, often, the result will still be efficiently expressed as a run container
        return lazyorToRun(x).repairAfterLazy();
    }
    

    protected boolean isFull() {
        return (this.nbrruns == 1) && (this.getValue(0) == 0) && (this.getLength(0) == -1);
    }
    protected MappeableContainer ilazyor(MappeableArrayContainer x) {
        if(isFull()) return this;  // this can sometimes solve a lot of computation!
        return ilazyorToRun(x);
    }
   
    protected MappeableContainer lazyor(MappeableArrayContainer x) {
        return lazyorToRun(x);
    }

    private MappeableContainer lazyorToRun(MappeableArrayContainer x) {
        if(isFull()) return this.clone();
        // TODO: should optimize for the frequent case where we have a single run
        MappeableRunContainer answer = new MappeableRunContainer(ShortBuffer.allocate(2 * (this.nbrruns + x.getCardinality())),0);
        short[] vl = answer.valueslength.array();
        int rlepos = 0;
        PeekableShortIterator i = (PeekableShortIterator) x.getShortIterator();

        while ((rlepos < this.nbrruns ) && i.hasNext()) {
            if(BufferUtil.compareUnsigned(getValue(rlepos), i.peekNext()) <= 0) {
                answer.smartAppend(vl,getValue(rlepos), getLength(rlepos));
                // could call i.advanceIfNeeded(minval);
                rlepos++;
            } else {
                answer.smartAppend(vl,i.next());
            }
        }   
        if (i.hasNext()) {
            /*if(answer.nbrruns>0) {
                // this might be useful if the run container has just one very large run
                int lastval = BufferUtil.toIntUnsigned(answer.getValue(answer.nbrruns - 1))
                        + BufferUtil.toIntUnsigned(answer.getLength(answer.nbrruns - 1)) + 1;
                i.advanceIfNeeded((short) lastval);
            }*/
            while (i.hasNext()) {
                answer.smartAppend(vl,i.next());
            }
        } else {

            while (rlepos < this.nbrruns) {
                answer.smartAppend(vl,getValue(rlepos), getLength(rlepos));
                rlepos++;
            }
        }
        return answer.convertToLazyBitmapIfNeeded();
    }

    private MappeableContainer ilazyorToRun(MappeableArrayContainer x) {
        if(isFull()) return this.clone();
        final int nbrruns = this.nbrruns;
        final int offset = Math.max(nbrruns, x.getCardinality());
        copyToOffset(offset);
        short[] vl = valueslength.array();
        int rlepos = 0;
        this.nbrruns = 0;
        PeekableShortIterator i = (PeekableShortIterator) x.getShortIterator();
        while (i.hasNext() && (rlepos < nbrruns) ) {
            if(BufferUtil.compareUnsigned(getValue(vl,rlepos + offset), i.peekNext()) <= 0) {
                smartAppend(vl,getValue(vl,rlepos + offset), getLength(vl,rlepos + offset));
                rlepos++;
            } else {
                smartAppend(vl,i.next());
            }
        }        
        if (i.hasNext()) {
            /*if(this.nbrruns>0) {
                // this might be useful if the run container has just one very large run
                int lastval = BufferUtil.toIntUnsigned(getValue(vl,nbrruns + offset - 1))
                        + BufferUtil.toIntUnsigned(getLength(vl,nbrruns + offset - 1)) + 1;
                i.advanceIfNeeded((short) lastval);
            }*/
            while (i.hasNext()) {
                smartAppend(vl,i.next());
            }
        } else {
            while (rlepos < nbrruns) {
                smartAppend(vl,getValue(vl,rlepos + offset), getLength(vl,rlepos + offset));
                rlepos++;
            }
        }
        return convertToLazyBitmapIfNeeded();
    }

    
    private MappeableContainer lazyxor(MappeableArrayContainer x) {
        if(x.getCardinality() == 0) return this;
        if(this.nbrruns == 0) return x;
        MappeableRunContainer answer = new MappeableRunContainer(ShortBuffer.allocate(2 * (this.nbrruns + x.getCardinality())),0);
        short[] vl = answer.valueslength.array();
        int rlepos = 0;
        ShortIterator i = x.getShortIterator();
        short cv = i.next();
        while (true) {
            if(BufferUtil.compareUnsigned(getValue(rlepos), cv) < 0) {
                answer.smartAppendExclusive(vl,getValue(rlepos), getLength(rlepos));
                rlepos++;
                if(rlepos == this.nbrruns )  {
                    answer.smartAppendExclusive(vl,cv);
                    while (i.hasNext()) {
                        answer.smartAppendExclusive(vl,i.next());
                    }
                    break;
                }
            } else {
                answer.smartAppendExclusive(vl,cv);
                if(! i.hasNext() ) {
                    while (rlepos < this.nbrruns) {
                        answer.smartAppendExclusive(vl,getValue(rlepos), getLength(rlepos));
                        rlepos++;
                    }
                    break;
                } else cv = i.next();
            }
        }        
        return answer;
    }
    
    @Override
    public MappeableContainer or(MappeableBitmapContainer x) {
        if(isFull()) return clone();
        MappeableBitmapContainer answer = x.clone();
        for(int rlepos = 0; rlepos < this.nbrruns; ++rlepos ) {
            int start = BufferUtil.toIntUnsigned(this.getValue(rlepos));
            int end = start + BufferUtil.toIntUnsigned(this.getLength(rlepos)) + 1;
            BufferUtil.setBitmapRange(answer.bitmap, start, end);
        }
        answer.computeCardinality();
        return answer;
    }

    @Override
    public MappeableContainer remove(short x) {
        int index = bufferedUnsignedInterleavedBinarySearch(valueslength, 0, nbrruns, x);
        if(index >= 0) {
            int le =  BufferUtil.toIntUnsigned(getLength(index));
            if(le == 0) {
                recoverRoomAtIndex(index); 
            } else {
                incrementValue(index);
                decrementLength(index);
            }
            return this;// already there
        }
        index = - index - 2;// points to preceding value, possibly -1
        if(index >= 0) {// possible match
            int offset = BufferUtil.toIntUnsigned(x) - BufferUtil.toIntUnsigned(getValue(index));
            int le =     BufferUtil.toIntUnsigned(getLength(index)); 
            if(offset < le) {
                // need to break in two
                this.setLength(index, (short) (offset - 1));
                // need to insert
                int newvalue = BufferUtil.toIntUnsigned(x) + 1;
                int newlength = le - offset - 1;
                makeRoomAtIndex(index+1); 
                this.setValue(index+1, (short) newvalue);
                this.setLength(index+1, (short) newlength);
                return this;
            } else if(offset == le) {
                decrementLength(index);
            }
        }
        // no match
        return this;
    }

    @Override
    public int serializedSizeInBytes() {
        return serializedSizeInBytes(nbrruns);
    }

    protected static int serializedSizeInBytes( int numberOfRuns) {
        return 2 + 2 * 2 * numberOfRuns;  // each run requires 2 2-byte entries.
    }


    @Override
    public void trim() {
        if(valueslength.limit() == 2 * nbrruns) return;
        if (BufferUtil.isBackedBySimpleArray(valueslength)) {
            this.valueslength = ShortBuffer.wrap(Arrays.copyOf(
                    valueslength.array(), 2 * nbrruns));
        } else {

            final ShortBuffer co = ShortBuffer.allocate(2 * nbrruns);
            short[] a = co.array();
            for (int k = 0; k < 2 * nbrruns; ++k)
                a[k] = this.valueslength.get(k);
            this.valueslength = co;
        }
    }

    @Override
    protected boolean isArrayBacked() {
        return BufferUtil.isBackedBySimpleArray(this.valueslength);
    }

    @Override
    protected void writeArray(DataOutput out) throws IOException {
        out.writeShort(Short.reverseBytes((short) this.nbrruns));
        for (int k = 0; k < 2 * this.nbrruns; ++k) {
            out.writeShort(Short.reverseBytes(this.valueslength.get(k)));
        }
    }

    @Override
    public MappeableContainer xor(MappeableArrayContainer x) {
        // if the cardinality of the array is small, guess that the output will still be a run container
        final int arbitrary_threshold = 32; // 32 is arbitrary here
        if(x.getCardinality() < arbitrary_threshold)
          return lazyxor(x).repairAfterLazy();
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
            int start = BufferUtil.toIntUnsigned(this.getValue(rlepos));
            int end = start + BufferUtil.toIntUnsigned(this.getLength(rlepos)) + 1;
            BufferUtil.flipBitmapRange(answer.bitmap, start, end);
        }
        answer.computeCardinality();
        if (answer.getCardinality() > MappeableArrayContainer.DEFAULT_MAX_SIZE)
            return answer;
        else
            return answer.toArrayContainer();
    }

    @Override
    public int rank(short lowbits) {
        int x = BufferUtil.toIntUnsigned(lowbits);
        int answer = 0;
        for (int k = 0; k < this.nbrruns; ++k) {
            int value = BufferUtil.toIntUnsigned(getValue(k));
            int length = BufferUtil.toIntUnsigned(getLength(k));
            if (x < value) {
                return answer;
            } else if (value + length + 1 >= x) {
                return answer + x - value + 1;
            }
            answer += length + 1;
        }
        return answer;
    }

    @Override
    public short select(int j) {
        int offset = 0;
        for (int k = 0; k < this.nbrruns; ++k) {
            int nextOffset = offset + BufferUtil.toIntUnsigned(getLength(k)) + 1;
            if(nextOffset > j) {
                return (short)(getValue(k) + (j - offset));
            }
            offset = nextOffset;
        }
        throw new IllegalArgumentException("Cannot select "+j+" since cardinality is "+getCardinality());        
    }

    @Override
    public MappeableContainer limit(int maxcardinality) {
        if(maxcardinality >= getCardinality()) {
            return clone();
        }

        int r;
        int cardinality = 0;
        for (r = 1; r <= this.nbrruns; ++r) {
            cardinality += BufferUtil.toIntUnsigned(getLength(r)) + 1;
            if (maxcardinality <= cardinality) {
                break;
            }
        }
        
        ShortBuffer newBuf = ShortBuffer.allocate(2*maxcardinality);
        for (int i=0; i < 2*r; ++i)
            newBuf.put( valueslength.get(i)); // could be optimized
        MappeableRunContainer rc = new MappeableRunContainer(newBuf,r);

        rc.setLength(r - 1, (short) (BufferUtil.toIntUnsigned(rc.getLength(r - 1)) - cardinality + maxcardinality));
        return rc;
    }

    @Override
    public MappeableContainer iadd(int begin, int end) {
        // TODO: it might be better and simpler to do return toBitmapOrArrayContainer(getCardinality()).iadd(begin,end)
        if((begin >= end) || (end > (1<<16))) {
            throw new IllegalArgumentException("Invalid range [" + begin + "," + end + ")");
        }

        if(begin == end-1) {
            add((short) begin);
            return this;
        }

        int bIndex = bufferedUnsignedInterleavedBinarySearch(this.valueslength, 0, this.nbrruns, (short) begin);
        int eIndex = bufferedUnsignedInterleavedBinarySearch(this.valueslength, 0, this.nbrruns, (short) (end-1));

        if(bIndex>=0 && eIndex>=0) {
            mergeValuesLength(bIndex, eIndex);
            return this;

        } else if(bIndex>=0 && eIndex<0) {
            eIndex = -eIndex - 2;

            if(canPrependValueLength(end-1, eIndex+1)) {
                mergeValuesLength(bIndex, eIndex+1);
                return this;
            }

            appendValueLength(end-1, eIndex);
            mergeValuesLength(bIndex, eIndex);
            return this;

        } else if(bIndex<0 && eIndex>=0) {
            bIndex = -bIndex - 2;

            if(bIndex>=0) {
                if(valueLengthContains(begin-1, bIndex)) {
                    mergeValuesLength(bIndex, eIndex);
                    return this;
                }
            }
            prependValueLength(begin, bIndex+1);
            mergeValuesLength(bIndex+1, eIndex);
            return this;

        } else {
            bIndex = -bIndex - 2;
            eIndex = -eIndex - 2;

            if(eIndex>=0) {
                if(bIndex>=0) {
                    if(!valueLengthContains(begin-1, bIndex)) {
                        if(bIndex==eIndex) {
                            if(canPrependValueLength(end-1, eIndex+1)) {
                                prependValueLength(begin, eIndex+1);
                                return this;
                            }
                            makeRoomAtIndex(eIndex+1);
                            setValue(eIndex+1, (short) begin);
                            setLength(eIndex+1, (short) (end - 1 - begin));
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

                if(canPrependValueLength(end-1, eIndex+1)) {
                    mergeValuesLength(bIndex, eIndex + 1);
                    return this;
                }

                appendValueLength(end-1, eIndex);
                mergeValuesLength(bIndex, eIndex);
                return this;

            } else {
                if(canPrependValueLength(end-1, 0)) {
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
    public MappeableContainer iremove(int begin, int end) {
        // TODO: it might be better and simpler to do return toBitmapOrArrayContainer(getCardinality()).iremove(begin,end)
        if((begin >= end) || (end > (1<<16))) {
            throw new IllegalArgumentException("Invalid range [" + begin + "," + end + ")");
        }

        if(begin == end-1) {
            remove((short) begin);
            return this;
        }

        int bIndex = bufferedUnsignedInterleavedBinarySearch(this.valueslength, 0, this.nbrruns, (short) begin);
        int eIndex = bufferedUnsignedInterleavedBinarySearch(this.valueslength, 0, this.nbrruns, (short) (end-1));

        if(bIndex>=0) {
            if(eIndex<0) {
                eIndex = -eIndex - 2;
            }

            if(valueLengthContains(end, eIndex)) {
                initValueLength(end, eIndex);
                recoverRoomsInRange(bIndex-1, eIndex-1);
            } else {
                recoverRoomsInRange(bIndex-1, eIndex);
            }

        } else if(bIndex<0 && eIndex>=0) {
            bIndex = -bIndex - 2;

            if(bIndex >= 0) {
                if (valueLengthContains(begin, bIndex)) {
                    closeValueLength(begin - 1, bIndex);
                }
            }
            incrementValue(eIndex);
            decrementLength(eIndex);
            recoverRoomsInRange(bIndex, eIndex-1);

        } else {
            bIndex = -bIndex - 2;
            eIndex = -eIndex - 2;

            if(eIndex>=0) {
                if(bIndex>=0) {
                    if(bIndex==eIndex) {
                        if (valueLengthContains(begin, bIndex)) {
                            if (valueLengthContains(end, eIndex)) {
                                makeRoomAtIndex(bIndex);
                                closeValueLength(begin-1, bIndex);
                                initValueLength(end, bIndex+1);
                                return this;
                            }
                            closeValueLength(begin-1, bIndex);
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
                    if(valueLengthContains(end, eIndex)) { // was end-1
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
    public MappeableContainer remove(int begin, int end) {
        MappeableRunContainer rc = (MappeableRunContainer) clone();
        return rc.iremove(begin, end);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof MappeableRunContainer) {
            MappeableRunContainer srb = (MappeableRunContainer) o;
            if (srb.nbrruns != this.nbrruns)
                return false;
            for (int i = 0; i < nbrruns; ++i) {
                if (this.getValue(i) != srb.getValue(i))
                    return false;
                if (this.getLength(i) != srb.getLength(i))
                    return false;
            }
            return true;
        } else if(o instanceof MappeableContainer) {
            if(((MappeableContainer) o).getCardinality() != this.getCardinality())
                return false; // should be a frequent branch if they differ
            // next bit could be optimized if needed:
            ShortIterator me = this.getShortIterator();
            ShortIterator you = ((MappeableContainer) o).getShortIterator();
            while(me.hasNext()) {
                if(me.next() != you.next())
                    return false;
            }
            return true;
        }
        return false;
    }


    private static int bufferedUnsignedInterleavedBinarySearch(final ShortBuffer sb,
            final int begin, final int end, final short k) {
        int ikey = BufferUtil.toIntUnsigned(k);
        int low = begin;
        int high = end - 1;
        while (low <= high) {
            final int middleIndex = (low + high) >>> 1;
            final int middleValue = BufferUtil.toIntUnsigned(sb.get(2 * middleIndex));
            if (middleValue < ikey)
                low = middleIndex + 1;
            else if (middleValue > ikey)
                high = middleIndex - 1;
            else
                return middleIndex;
        }
        return -(low + 1);
    }

    short getValue(int index) {
        return valueslength.get(2*index);
    }

    short getLength(int index) {
        return valueslength.get(2*index + 1);
    }
    
    static short getValue(short[] vl, int index) {
        return vl[2*index];
    }

    static short getLength(short[] vl, int index) {
        return vl[2*index + 1];
    }

    private void incrementLength(int index) {
          valueslength.put(2*index + 1, (short) (1 + valueslength.get(2*index+1))); 
    }
    
    private void incrementValue(int index) {
        valueslength.put(2*index, (short) (1 + valueslength.get(2*index)));
    }

    private void decrementLength(int index) {
        valueslength.put(2*index + 1, (short) (valueslength.get(2*index+1)-1));
    }

    private void decrementValue(int index) {
        valueslength.put(2*index, (short) (valueslength.get(2*index)-1));
    }

    private void setLength(int index, short v) {
        setLength(valueslength, index, v);
    }

    private void setLength(ShortBuffer valueslength, int index, short v) {
        valueslength.put(2*index + 1,v);
    }

    private void setValue(int index, short v) {
        setValue(valueslength, index, v);
    }

    private void setValue(ShortBuffer valueslength, int index, short v) {
        valueslength.put(2*index, v);
    }


    private void makeRoomAtIndex(int index) {
        if (2*(nbrruns+1) > valueslength.capacity())
            increaseCapacity();
        copyValuesLength(valueslength, index, valueslength, index + 1, nbrruns - index);
        nbrruns++;
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

    // To merge values length from begin(inclusive) to end(inclusive)
    private void mergeValuesLength(int begin, int end) {
        if(begin < end) {
            int bValue = BufferUtil.toIntUnsigned(getValue(begin));
            int eValue = BufferUtil.toIntUnsigned(getValue(end));
            int eLength = BufferUtil.toIntUnsigned(getLength(end));
            int newLength = eValue - bValue + eLength;
            setLength(begin, (short) newLength);
            recoverRoomsInRange(begin, end);
        }
    }

    // To check if a value length can be prepended with a given value
    private boolean canPrependValueLength(int value, int index) {
        if(index < this.nbrruns) {
            int nextValue = BufferUtil.toIntUnsigned(getValue(index));
            if(nextValue == value+1) {
                return true;
            }
        }
        return false;
    }

    // Prepend a value length with all values starting from a given value
    private void prependValueLength(int value, int index) {
        int initialValue = BufferUtil.toIntUnsigned(getValue(index));
        int length = BufferUtil.toIntUnsigned(getLength(index));
        setValue(index, (short) value);
        setLength(index, (short) (initialValue - value + length));
    }

    // Append a value length with all values until a given value
    private void appendValueLength(int value, int index) {
        int previousValue = BufferUtil.toIntUnsigned(getValue(index));
        int length = BufferUtil.toIntUnsigned(getLength(index));
        int offset = value - previousValue;
        if(offset>length) {
            setLength(index, (short) offset);
        }
    }

    // To check if a value length contains a given value
    private boolean valueLengthContains(int value, int index) {
        int initialValue = BufferUtil.toIntUnsigned(getValue(index));
        int length = BufferUtil.toIntUnsigned(getLength(index));

        if(value <= initialValue + length) {
            return true;
        }
        return false;
    }

    // To set the first value of a value length
    private void initValueLength(int value, int index) {
        int initialValue = BufferUtil.toIntUnsigned(getValue(index));
        int length = BufferUtil.toIntUnsigned(getLength(index));
        setValue(index, (short) (value));
        setLength(index, (short) (length - (value - initialValue)));
    }

    // To set the last value of a value length
    private void closeValueLength(int value, int index) {
        int initialValue = BufferUtil.toIntUnsigned(getValue(index));
        setLength(index, (short) (value - initialValue));
    }



    private void copyValuesLength(ShortBuffer src, int srcIndex, ShortBuffer dst, int dstIndex, int length) {
        if(BufferUtil.isBackedBySimpleArray(src) && BufferUtil.isBackedBySimpleArray(dst)) {
            // common case.
            System.arraycopy(src.array(), 2*srcIndex, dst.array(), 2*dstIndex, 2*length);
            return;
        }
        // source and destination may overlap
        // consider specialized code for various cases, rather than using a second buffer
        ShortBuffer temp = ShortBuffer.allocate(2*length);
        for (int i=0; i < 2*length; ++i)
            temp.put( src.get(2*srcIndex + i));
        temp.flip();
        for (int i=0; i < 2*length; ++i)
            dst.put( 2*dstIndex+i, temp.get());
    }




    @Override
    public MappeableContainer and(MappeableRunContainer x) {
        MappeableRunContainer answer = new MappeableRunContainer(ShortBuffer.allocate(2 * (this.nbrruns + x.nbrruns)),0);
        short[] vl = answer.valueslength.array();
        int rlepos = 0;
        int xrlepos = 0;
        int start = BufferUtil.toIntUnsigned(this.getValue(rlepos));
        int end = start + BufferUtil.toIntUnsigned(this.getLength(rlepos)) + 1;
        int xstart = BufferUtil.toIntUnsigned(x.getValue(xrlepos));
        int xend = xstart + BufferUtil.toIntUnsigned(x.getLength(xrlepos)) + 1;
        while ((rlepos < this.nbrruns ) && (xrlepos < x.nbrruns )) {
            if (end  <= xstart) {
                // exit the first run
                rlepos++;
                if(rlepos < this.nbrruns ) {
                    start = BufferUtil.toIntUnsigned(this.getValue(rlepos));
                    end = start + BufferUtil.toIntUnsigned(this.getLength(rlepos)) + 1;
                }
            } else if (xend <= start) {
                // exit the second run
                xrlepos++;
                if(xrlepos < x.nbrruns ) {
                    xstart = BufferUtil.toIntUnsigned(x.getValue(xrlepos));
                    xend = xstart + BufferUtil.toIntUnsigned(x.getLength(xrlepos)) + 1;
                }
            } else {// they overlap
                final int lateststart = start > xstart ? start : xstart;
                int earliestend;
                if(end == xend) {// improbable
                    earliestend = end;
                    rlepos++;
                    xrlepos++;
                    if(rlepos < this.nbrruns ) {
                        start = BufferUtil.toIntUnsigned(this.getValue(rlepos));
                        end = start + BufferUtil.toIntUnsigned(this.getLength(rlepos)) + 1;
                    }
                    if(xrlepos < x.nbrruns) {
                        xstart = BufferUtil.toIntUnsigned(x.getValue(xrlepos));
                        xend = xstart + BufferUtil.toIntUnsigned(x.getLength(xrlepos)) + 1;
                    }
                } else if(end < xend) {
                    earliestend = end;
                    rlepos++;
                    if(rlepos < this.nbrruns ) {
                        start = BufferUtil.toIntUnsigned(this.getValue(rlepos));
                        end = start + BufferUtil.toIntUnsigned(this.getLength(rlepos)) + 1;
                    }

                } else {// end > xend
                    earliestend = xend;
                    xrlepos++;
                    if(xrlepos < x.nbrruns) {
                        xstart = BufferUtil.toIntUnsigned(x.getValue(xrlepos));
                        xend = xstart + BufferUtil.toIntUnsigned(x.getLength(xrlepos)) + 1;
                    }                
                }
                vl[2 * answer.nbrruns] = (short) lateststart;
                vl[2 * answer.nbrruns + 1] =  (short) (earliestend - lateststart - 1);
                answer.nbrruns++;
            }
        }
        return answer;
    }


    @Override
    public MappeableContainer andNot(MappeableRunContainer x) {
        MappeableRunContainer answer = new MappeableRunContainer(ShortBuffer.allocate(2 * (this.nbrruns + x.nbrruns)),0);
        short[] vl = answer.valueslength.array();
        int rlepos = 0;
        int xrlepos = 0;
        int start = BufferUtil.toIntUnsigned(this.getValue(rlepos));
        int end = start + BufferUtil.toIntUnsigned(this.getLength(rlepos)) + 1;
        int xstart = BufferUtil.toIntUnsigned(x.getValue(xrlepos));
        int xend = xstart + BufferUtil.toIntUnsigned(x.getLength(xrlepos)) + 1;
        while ((rlepos < this.nbrruns ) && (xrlepos < x.nbrruns )) {
            if (end  <= xstart) {
                // output the first run
                vl[2 * answer.nbrruns] =  (short) start;
                vl[2 * answer.nbrruns + 1] =   (short)(end - start - 1);
                answer.nbrruns++;
                rlepos++;
                if(rlepos < this.nbrruns ) {
                    start = BufferUtil.toIntUnsigned(this.getValue(rlepos));
                    end = start + BufferUtil.toIntUnsigned(this.getLength(rlepos)) + 1;
                }
            } else if (xend <= start) {
                // exit the second run
                xrlepos++;
                if(xrlepos < x.nbrruns ) {
                    xstart = BufferUtil.toIntUnsigned(x.getValue(xrlepos));
                    xend = xstart + BufferUtil.toIntUnsigned(x.getLength(xrlepos)) + 1;
                }
            } else {
                if ( start < xstart ) {
                    vl[2 * answer.nbrruns] =  (short) start;
                    vl[2 * answer.nbrruns + 1] =  (short) (xstart - start - 1);
                    answer.nbrruns++;
                }
                if(xend < end) {
                    start = xend;
                } else {
                    rlepos++;
                    if(rlepos < this.nbrruns ) {
                        start = BufferUtil.toIntUnsigned(this.getValue(rlepos));
                        end = start + BufferUtil.toIntUnsigned(this.getLength(rlepos)) + 1;
                    }
                }
            }
        }
        if(rlepos < this.nbrruns) {
            vl[2 * answer.nbrruns] =  (short) start;
            vl[2 * answer.nbrruns + 1] =  (short)(end - start - 1);
            answer.nbrruns++;
            rlepos++;
            if(rlepos < this.nbrruns) {                
              this.valueslength.position(2 * rlepos);
              this.valueslength.get(vl, 2 * answer.nbrruns, 2*(this.nbrruns-rlepos ));
              answer.nbrruns  = answer.nbrruns + this.nbrruns - rlepos;
            }
        } 
        return answer;
    }

    private MappeableRunContainer lazyandNot(MappeableArrayContainer x) {
        if(x.getCardinality() == 0) return this;
        MappeableRunContainer answer = new MappeableRunContainer(ShortBuffer.allocate(2 * (this.nbrruns + x.cardinality)),0);
        short[] vl = answer.valueslength.array();
        int rlepos = 0;
        int xrlepos = 0;
        int start = BufferUtil.toIntUnsigned(this.getValue(rlepos));
        int end = start + BufferUtil.toIntUnsigned(this.getLength(rlepos)) + 1;
        int xstart = BufferUtil.toIntUnsigned(x.content.get(xrlepos));
        while ((rlepos < this.nbrruns ) && (xrlepos < x.cardinality )) {
            if (end  <= xstart) {
                // output the first run
                vl[2 * answer.nbrruns] =  (short) start;
                vl[2 * answer.nbrruns + 1] =   (short)(end - start - 1);
                answer.nbrruns++;
                rlepos++;
                if(rlepos < this.nbrruns ) {
                    start = BufferUtil.toIntUnsigned(this.getValue(rlepos));
                    end = start + BufferUtil.toIntUnsigned(this.getLength(rlepos)) + 1;
                }
            } else if (xstart + 1 <= start) {
                // exit the second run
                xrlepos++;
                if(xrlepos < x.cardinality ) {
                    xstart = BufferUtil.toIntUnsigned(x.content.get(xrlepos));
                }
            } else {
                if ( start < xstart ) {
                    vl[2 * answer.nbrruns] =  (short) start;
                    vl[2 * answer.nbrruns + 1] =  (short) (xstart - start - 1);
                    answer.nbrruns++;
                }
                if(xstart + 1 < end) {
                    start = xstart + 1;
                } else {
                    rlepos++;
                    if(rlepos < this.nbrruns ) {
                        start = BufferUtil.toIntUnsigned(this.getValue(rlepos));
                        end = start + BufferUtil.toIntUnsigned(this.getLength(rlepos)) + 1;
                    }
                }
            }
        }
        if(rlepos < this.nbrruns) {
            vl[2 * answer.nbrruns] =  (short) start;
            vl[2 * answer.nbrruns + 1] =  (short)(end - start - 1);
            answer.nbrruns++;
            rlepos++;
            if(rlepos < this.nbrruns) {                
              this.valueslength.position(2 * rlepos);
              this.valueslength.get(vl, 2 * answer.nbrruns, 2*(this.nbrruns-rlepos ));
              answer.nbrruns  = answer.nbrruns + this.nbrruns - rlepos;
            }
        } 
        return answer;
    }

    
    
    // assume that the (maybe) inplace operations
    // will never actually *be* in place if they are 
    // to return ArrayContainer or BitmapContainer

    @Override
    public MappeableContainer iand(MappeableRunContainer x) {
        return and(x);
    }

    @Override
    public MappeableContainer iandNot(MappeableRunContainer x) {
        return andNot(x);
    }

    @Override
    public MappeableContainer ior(MappeableRunContainer x) {
        if(isFull()) return this;

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
            final short value = getValue(vl,offset + rlepos);
            final short xvalue = x.getValue(xrlepos);
            final short length = getLength(vl,offset + rlepos);
            final short xlength = x.getLength(xrlepos);

            if(BufferUtil.compareUnsigned(value, xvalue) <= 0) {
                this.smartAppend(vl, value, length);
                ++rlepos;
            } else {
                this.smartAppend(vl, xvalue, xlength);
                ++xrlepos;
            }
        }
        while (rlepos < nbrruns) {
            this.smartAppend(vl, getValue(vl,offset + rlepos), getLength(vl,offset + rlepos));
            ++rlepos;
        }
        while (xrlepos < xnbrruns) {
            this.smartAppend(vl, x.getValue(xrlepos), x.getLength(xrlepos));
            ++xrlepos;
        }
        return this.toBitmapIfNeeded();
    }

    @Override
    public MappeableContainer ixor(MappeableRunContainer x) {
        return xor(x);
    }
    
    
    private void smartAppend(short[] vl, short start, short length) {
        int oldend;
        if((nbrruns==0) ||
                (BufferUtil.toIntUnsigned(start) > 
                (oldend= BufferUtil.toIntUnsigned(vl[2 * (nbrruns - 1)]) + BufferUtil.toIntUnsigned(vl[2 * (nbrruns - 1) + 1])) + 1)) { // we add a new one
            vl[2 * nbrruns] =  start;
            vl[2 * nbrruns + 1] = length;
            nbrruns++;
            return;
        } 
        int newend = BufferUtil.toIntUnsigned(start) + BufferUtil.toIntUnsigned(length) + 1;
        if(newend > oldend)  { // we merge
            vl[2 * (nbrruns - 1) + 1] = (short) (newend - 1 - BufferUtil.toIntUnsigned(vl[2 * (nbrruns - 1)]));
        }
    }
    
    private void smartAppendExclusive(short[] vl, short start, short length) {
        int oldend;
        if((nbrruns==0) ||
                (BufferUtil.toIntUnsigned(start) > 
                (oldend = BufferUtil.toIntUnsigned(getValue(nbrruns - 1)) + BufferUtil.toIntUnsigned(getLength(nbrruns - 1)) + 1))) { // we add a new one
            vl[2 * nbrruns] =  start;
            vl[2 * nbrruns + 1] = length;
            nbrruns++;
            return;
        } 
        if(oldend == BufferUtil.toIntUnsigned(start)) {
            // we merge
            vl[2 * ( nbrruns - 1) + 1] += length + 1;
            return;
        }
        
        
        int newend = BufferUtil.toIntUnsigned(start) + BufferUtil.toIntUnsigned(length) + 1;

        if(BufferUtil.toIntUnsigned(start) == BufferUtil.toIntUnsigned(getValue(nbrruns - 1))) {
            // we wipe out previous
            if( newend < oldend ) {           
                setValue(nbrruns - 1, (short) newend);
                setLength(nbrruns - 1, (short) (oldend - newend - 1));
                return;
            } else if ( newend > oldend ) {  
                setValue(nbrruns - 1, (short) oldend);
                setLength(nbrruns - 1, (short) (newend - oldend - 1));
                return;
            } else { // they cancel out
                nbrruns--;
                return;
            }
        }
        setLength(nbrruns - 1, (short) (start - BufferUtil.toIntUnsigned(getValue(nbrruns - 1)) -1));

        if( newend < oldend ) {           
            setValue(nbrruns, (short) newend);
            setLength(nbrruns, (short) (oldend - newend - 1));
            nbrruns ++;
        } else if ( newend > oldend ) {  
            setValue(nbrruns, (short) oldend);
            setLength(nbrruns, (short) (newend - oldend - 1));
            nbrruns ++;
        }
    }

    private void smartAppendExclusive(short[] vl, short val) {
        int oldend;
        if((nbrruns==0) ||
                (BufferUtil.toIntUnsigned(val) > 
                (oldend = BufferUtil.toIntUnsigned(getValue(nbrruns - 1)) + BufferUtil.toIntUnsigned(getLength(nbrruns - 1)) + 1))) { // we add a new one
            vl[2 * nbrruns] =  val;
            vl[2 * nbrruns + 1] = 0;
            nbrruns++;
            return;
        } 
        if(oldend == BufferUtil.toIntUnsigned(val)) {
            // we merge
            vl[2 * ( nbrruns - 1) + 1] ++;
            return;
        }
        
        
        int newend = BufferUtil.toIntUnsigned(val) + 1;
        
        if(BufferUtil.toIntUnsigned(val) == BufferUtil.toIntUnsigned(getValue(nbrruns - 1))) {
            // we wipe out previous
            if( newend != oldend ) {
                setValue(nbrruns - 1, (short) newend);
                setLength(nbrruns - 1, (short) (oldend - newend - 1));
                return;
            } else { // they cancel out
                nbrruns--;
                return;
            }
        }
        setLength(nbrruns - 1, (short) (val - BufferUtil.toIntUnsigned(getValue(nbrruns - 1)) -1));

        if(newend < oldend) {
            setValue(nbrruns, (short) newend);
            setLength(nbrruns , (short) (oldend - newend - 1));
            nbrruns ++;
        } else if (oldend < newend) {
            setValue(nbrruns, (short) oldend);
            setLength(nbrruns , (short) (newend - oldend - 1));
            nbrruns ++;            
        }
    }
    

    public String toString() {
        StringBuffer sb = new StringBuffer();
        for(int k = 0; k < this.nbrruns; ++k) {
            sb.append("[");
            sb.append(BufferUtil.toIntUnsigned(this.getValue(k)));
            sb.append(",");
            sb.append(BufferUtil.toIntUnsigned(this.getValue(k)) +
                    BufferUtil.toIntUnsigned(this.getLength(k)) + 1);
            sb.append("]");
        }
        return sb.toString();
    }
    
    private void smartAppend(short[] vl, short val) {
        int oldend;
        if((nbrruns==0) ||
                (BufferUtil.toIntUnsigned(val) > 
                (oldend= BufferUtil.toIntUnsigned(vl[2 * (nbrruns - 1)]) + BufferUtil.toIntUnsigned(vl[2 * (nbrruns - 1) + 1])) + 1)) { // we add a new one
            vl[2 * nbrruns] =  val;
            vl[2 * nbrruns + 1] = 0;
            nbrruns++;
            return;
        } 
        if(val == (short)(oldend + 1))  { // we merge
            vl[2 * (nbrruns - 1) + 1]++;
        }
    }
    
    @Override
    public MappeableContainer or(MappeableRunContainer x) {
        if(isFull()) return clone();
        if(x.isFull()) return x.clone(); // cheap case that can save a lot of computation
        // we really ought to optimize the rest of the code for the frequent case where there is a single run
        MappeableRunContainer answer = new MappeableRunContainer(ShortBuffer.allocate(2 * (this.nbrruns + x.nbrruns)),0);
        short[] vl = answer.valueslength.array();
        int rlepos = 0;
        int xrlepos = 0;

        while ((rlepos < this.nbrruns) && (xrlepos < x.nbrruns)) {
            if(BufferUtil.compareUnsigned(getValue(rlepos), x.getValue(xrlepos)) <= 0) {
                answer.smartAppend(vl,getValue(rlepos), getLength(rlepos));
                rlepos++;
            } else {
                answer.smartAppend(vl,x.getValue(xrlepos), x.getLength(xrlepos));
                xrlepos++;
            }
        }
        while (xrlepos < x.nbrruns) {
            answer.smartAppend(vl,x.getValue(xrlepos), x.getLength(xrlepos));
            xrlepos++;
        }
        while (rlepos < this.nbrruns) {
            answer.smartAppend(vl,getValue(rlepos), getLength(rlepos));
            rlepos++;
        }
        return answer.toBitmapIfNeeded();
    }

    @Override
    public MappeableContainer xor(MappeableRunContainer x) {
        if(x.nbrruns == 0) return this.clone();
        if(this.nbrruns == 0) return x.clone();
        MappeableRunContainer answer = new MappeableRunContainer(ShortBuffer.allocate(2 * (this.nbrruns + x.nbrruns)),0);
        short[] vl = answer.valueslength.array();
        int rlepos = 0;
        int xrlepos = 0;

        while (true) {
            if(BufferUtil.compareUnsigned(getValue(rlepos), x.getValue(xrlepos)) < 0) {
                answer.smartAppendExclusive(vl,getValue(rlepos), getLength(rlepos));
                rlepos++;
                if(rlepos == this.nbrruns )  {
                    while (xrlepos < x.nbrruns) {
                        answer.smartAppendExclusive(vl,x.getValue(xrlepos), x.getLength(xrlepos));
                        xrlepos++;
                    }
                    break;
                }
            } else {
                answer.smartAppendExclusive(vl,x.getValue(xrlepos), x.getLength(xrlepos));
                xrlepos++;
                if(xrlepos == x.nbrruns ) {
                    while (rlepos < this.nbrruns) {
                        answer.smartAppendExclusive(vl,getValue(rlepos), getLength(rlepos));
                        rlepos++;
                    }
                    break;
                }
            }
        }        
        return answer.toEfficientContainer();
    }

    @Override
    public MappeableContainer repairAfterLazy() {
        return toEfficientContainer();
    }
    
    // a very cheap check... if you have more than 4096, then you should use a bitmap container.
    // this function avoids computing the cardinality
    private MappeableContainer convertToLazyBitmapIfNeeded() {
        // when nbrruns exceed MappeableArrayContainer.DEFAULT_MAX_SIZE, then we know it should be stored as a bitmap, always 
        if(this.nbrruns > MappeableArrayContainer.DEFAULT_MAX_SIZE) {
            MappeableBitmapContainer answer = new MappeableBitmapContainer();
            for (int rlepos=0; rlepos < this.nbrruns; ++rlepos) {
                int start = BufferUtil.toIntUnsigned(this.getValue(rlepos));
                int end = start + BufferUtil.toIntUnsigned(this.getLength(rlepos)) + 1;
                BufferUtil.setBitmapRange(answer.bitmap, start, end); 
            }
            answer.cardinality = -1;
            return answer;
        }
        return this;
    }

}


final class MappeableRunContainerShortIterator implements ShortIterator {
    int pos;
    int le = 0;
    int maxlength;
    int base;

    MappeableRunContainer parent;

    MappeableRunContainerShortIterator() {}

    MappeableRunContainerShortIterator(MappeableRunContainer p) {
        wrap(p);
    }
    
    void wrap(MappeableRunContainer p) {
        parent = p;
        pos = 0;
        le = 0;
        if(pos < parent.nbrruns) {
            maxlength = BufferUtil.toIntUnsigned(parent.getLength(pos));
            base = BufferUtil.toIntUnsigned(parent.getValue(pos));
        }
    }

    @Override
    public boolean hasNext() {
        return pos < parent.nbrruns;
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
    public short next() {
        short ans = (short) (base + le);
        le++;
        if(le > maxlength) {
            pos++;
            le = 0;
            if(pos < parent.nbrruns) {
                maxlength = BufferUtil.toIntUnsigned(parent.getLength(pos));
                base = BufferUtil.toIntUnsigned(parent.getValue(pos));
            }
        }
        return ans;
    }

    @Override
    public int nextAsInt() {
        int ans = base + le;
        le++;
        if(le > maxlength) {
            pos++;
            le = 0;
            if(pos < parent.nbrruns) {
                maxlength = BufferUtil.toIntUnsigned(parent.getLength(pos));
                base = BufferUtil.toIntUnsigned(parent.getValue(pos));
            }
        }
        return ans;
    }
    @Override
    public void remove() {
        throw new RuntimeException("Not implemented");// TODO
    }

};

final class ReverseMappeableRunContainerShortIterator implements ShortIterator {
    int pos;
    int le;
    int maxlength;
    int base;
    MappeableRunContainer parent;


    ReverseMappeableRunContainerShortIterator(){}

    ReverseMappeableRunContainerShortIterator(MappeableRunContainer p) {
        wrap(p);
    }
    
    void wrap(MappeableRunContainer p) {
        parent = p;
        pos = parent.nbrruns - 1;
        le = 0;
        if(pos >= 0) {
            maxlength = BufferUtil.toIntUnsigned(parent.getLength(pos));
            base = BufferUtil.toIntUnsigned(parent.getValue(pos));
        }
    }

    @Override
    public boolean hasNext() {
        return pos >= 0;
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
    public short next() {
        short ans = (short) (base + maxlength - le);
        le++;
        if(le > maxlength) {
            pos--;
            le = 0;
            if(pos >= 0) {
                maxlength = BufferUtil.toIntUnsigned(parent.getLength(pos));
                base = BufferUtil.toIntUnsigned(parent.getValue(pos));
            }
        }
        return ans;
    }

    @Override
    public int nextAsInt() {
        int ans = base + maxlength - le;
        le++;
        if(le > maxlength) {
            pos--;
            le = 0;
            if(pos >= 0) {
                maxlength = BufferUtil.toIntUnsigned(parent.getLength(pos));
                base = BufferUtil.toIntUnsigned(parent.getValue(pos));
            }
        }
        return ans;
    }
    @Override
    public void remove() {
        throw new RuntimeException("Not implemented");// TODO
    }

}


final class RawMappeableRunContainerShortIterator implements ShortIterator {
    int pos;
    int le = 0;
    int maxlength;
    int base;

    MappeableRunContainer parent;
    short[] vl;


    RawMappeableRunContainerShortIterator(MappeableRunContainer p) {
        wrap(p);
    }
    
    void wrap(MappeableRunContainer p) {
        parent = p;
        if(!parent.isArrayBacked())
            throw new RuntimeException("internal error");
        vl = parent.valueslength.array();        
        pos = 0;
        le = 0;
        if(pos < parent.nbrruns) {
            maxlength = BufferUtil.toIntUnsigned(getLength(pos));
            base = BufferUtil.toIntUnsigned(getValue(pos));
        }
    }

    @Override
    public boolean hasNext() {
        return pos < parent.nbrruns;
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
    public short next() {
        short ans = (short) (base + le);
        le++;
        if(le > maxlength) {
            pos++;
            le = 0;
            if(pos < parent.nbrruns) {
                maxlength = BufferUtil.toIntUnsigned(getLength(pos));
                base = BufferUtil.toIntUnsigned(getValue(pos));
            }
        }
        return ans;
    }

    @Override
    public int nextAsInt() {
        int ans = base + le;
        le++;
        if(le > maxlength) {
            pos++;
            le = 0;
            if(pos < parent.nbrruns) {
                maxlength = BufferUtil.toIntUnsigned(getLength(pos));
                base = BufferUtil.toIntUnsigned(getValue(pos));
            }
        }
        return ans;
    }
    
    
    short getValue(int index) {
        return vl[2*index];
    }

    short getLength(int index) {
        return vl[2*index + 1];
    }
    
    @Override
    public void remove() {
        throw new RuntimeException("Not implemented");// TODO
    }

};

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
    
    void wrap(MappeableRunContainer p) {
        parent = p;
        if(!parent.isArrayBacked())
            throw new RuntimeException("internal error");
        vl = parent.valueslength.array();
        pos = parent.nbrruns - 1;
        le = 0;
        if(pos >= 0) {
            maxlength = BufferUtil.toIntUnsigned(getLength(pos));
            base = BufferUtil.toIntUnsigned(getValue(pos));
        }
    }

    @Override
    public boolean hasNext() {
        return pos >= 0;
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
    public short next() {
        short ans = (short) (base + maxlength - le);
        le++;
        if(le > maxlength) {
            pos--;
            le = 0;
            if(pos >= 0) {
                maxlength = BufferUtil.toIntUnsigned(getLength(pos));
                base = BufferUtil.toIntUnsigned(getValue(pos));
            }
        }
        return ans;
    }

    @Override
    public int nextAsInt() {
        int ans = base + maxlength - le;
        le++;
        if(le > maxlength) {
            pos--;
            le = 0;
            if(pos >= 0) {
                maxlength = BufferUtil.toIntUnsigned(getLength(pos));
                base = BufferUtil.toIntUnsigned(getValue(pos));
            }
        }
        return ans;
    }
    
    short getValue(int index) {
        return vl[2*index];
    }

    short getLength(int index) {
        return vl[2*index + 1];
    }

    @Override
    public void remove() {
        throw new RuntimeException("Not implemented");// TODO
    }

}
