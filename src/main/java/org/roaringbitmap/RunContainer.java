/*
 * (c) the authors
 * Licensed under the Apache License, Version 2.0.
 */
package org.roaringbitmap;

import java.io.*;
import java.util.Arrays;
import java.util.Iterator;



/**
 * This container takes the form of runs of consecutive values (effectively,
 * run-length encoding).
 * 
 * Adding and removing content from this container might make it wasteful
 * so regular calls to "runOptimize" might be warranted.
 */
public final class RunContainer extends Container implements Cloneable {
    private static final int DEFAULT_INIT_SIZE = 4;
    private static final boolean ENABLE_GALLOPING_AND = false;

    private short[] valueslength;// we interleave values and lengths, so 
    // that if you have the values 11,12,13,14,15, you store that as 11,4 where 4 means that beyond 11 itself, there are
    // 4 contiguous values that follows.
    // Other example: e.g., 1, 10, 20,0, 31,2 would be a concise representation of  1, 2, ..., 11, 20, 31, 32, 33

    int nbrruns = 0;// how many runs, this number should fit in 16 bits.

    private static final long serialVersionUID = 1L;

    private RunContainer(int nbrruns, short[] valueslength) {
        this.nbrruns = nbrruns;
        this.valueslength = Arrays.copyOf(valueslength, valueslength.length);
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        for(int k = 0; k < this.nbrruns; ++k) {
            sb.append("[");
            sb.append(Util.toIntUnsigned(this.getValue(k)));
            sb.append(",");
            sb.append(Util.toIntUnsigned(this.getValue(k)) +
                    Util.toIntUnsigned(this.getLength(k)));
            sb.append("]");
        }
        return sb.toString();
    }

    /**
     * Construct a new RunContainer backed by the provided array. Note
     * that if you modify the RunContainer a new array may be produced.
     * 
     * @param array
     *            array where the data is stored
     * @param numRuns
     *            number of runs (each using 2 shorts in the buffer)
     *            
     */
    public RunContainer(final short[] array,
            final int numRuns) {
        if (array.length < 2*numRuns)
            throw new RuntimeException(
                    "Mismatch between buffer and numRuns");
        this.nbrruns = numRuns;
        this.valueslength = array;
    }



    // lower-level specialized implementations might be faster
    // unused method, can be used as part of unit testing
    protected RunContainer( ShortIterator sIt, int nbrRuns) {
        this.nbrruns = nbrRuns;
        valueslength = new short[ 2*nbrRuns];
        if (nbrRuns == 0) return;

        int prevVal = -2; 
        int runLen=0;
        int runCount=0;
        while (sIt.hasNext()) {
            int curVal = Util.toIntUnsigned(sIt.next());
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

    protected RunContainer( ArrayContainer arr, int nbrRuns) {
        this.nbrruns = nbrRuns;
        valueslength = new short[ 2*nbrRuns];
        if (nbrRuns == 0) return;

        int prevVal = -2; 
        int runLen=0;
        int runCount=0;
        
        for (int i = 0; i < arr.cardinality; i++) {
            int curVal = Util.toIntUnsigned(arr.content[i]);
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
    protected RunContainer( BitmapContainer bc, int nbrRuns) {
        this.nbrruns = nbrRuns;
        valueslength = new short[ 2*nbrRuns];
        if (nbrRuns == 0) return;
        
        int longCtr = 0;  // index of current long in bitmap
        long curWord = bc.bitmap[0];  //its value
        int runCount=0;
        while (true) {
            // potentially multiword advance to first 1 bit
            while (curWord == 0L && longCtr < bc.bitmap.length-1)
                curWord = bc.bitmap[ ++longCtr];
            
            if (curWord == 0L) {
                // wrap up, no more runs
                return;
            }
            int localRunStart = Long.numberOfTrailingZeros(curWord);
            int runStart = localRunStart   + 64*longCtr;
            // stuff 1s into number's LSBs
            long curWordWith1s = curWord | (curWord - 1);
            
            // find the next 0, potentially in a later word
            int runEnd = 0;
            while (curWordWith1s == -1L && longCtr < bc.bitmap.length-1)
                curWordWith1s = bc.bitmap[++longCtr];
            
            if (curWordWith1s == -1L) {
                // a final unterminated run of 1s (32 of them)
                runEnd = 64 + longCtr*64;
                setValue(runCount, (short) runStart);
                setLength(runCount, (short) (runEnd-runStart-1));
                return;
            }
            int localRunEnd = Long.numberOfTrailingZeros(~curWordWith1s);
            runEnd = localRunEnd + longCtr*64;
            setValue(runCount, (short) runStart);
            setLength(runCount, (short) (runEnd-runStart-1));
            runCount++;
            // now, zero out everything right of runEnd.
            curWord = curWordWith1s & (curWordWith1s + 1);
            // We've lathered and rinsed, so repeat...
        }
    }


    @Override
    int numberOfRuns() {
        return nbrruns;
    }

    
    // convert to bitmap or array *if needed*
    private Container toEfficientContainer() {
        int sizeAsRunContainer = RunContainer.serializedSizeInBytes(this.nbrruns);
        int sizeAsBitmapContainer = BitmapContainer.serializedSizeInBytes(0);
        int card = this.getCardinality();
        int sizeAsArrayContainer = ArrayContainer.serializedSizeInBytes(card);
        if(sizeAsRunContainer <= Math.min(sizeAsBitmapContainer, sizeAsArrayContainer))
            return this;
        if(card <= ArrayContainer.DEFAULT_MAX_SIZE) {
            ArrayContainer answer = new ArrayContainer(card);
            answer.cardinality=0;
            for (int rlepos=0; rlepos < this.nbrruns; ++rlepos) {
                int runStart = Util.toIntUnsigned(this.getValue(rlepos));
                int runEnd = runStart + Util.toIntUnsigned(this.getLength(rlepos));

                for (int runValue = runStart; runValue <= runEnd; ++runValue) {
                    answer.content[answer.cardinality++] = (short) runValue;
                }
            }
            return answer;
        }
        BitmapContainer answer = new BitmapContainer();
        for (int rlepos=0; rlepos < this.nbrruns; ++rlepos) {
            int start = Util.toIntUnsigned(this.getValue(rlepos));
            int end = start + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
            Util.setBitmapRange(answer.bitmap, start, end); 
        }
        answer.cardinality = card;
        return answer;
    }

    // convert to bitmap  *if needed* (useful if you know it can't be an array)
    private Container toBitmapIfNeeded() {
        int sizeAsRunContainer = RunContainer.serializedSizeInBytes(this.nbrruns);
        int sizeAsBitmapContainer = BitmapContainer.serializedSizeInBytes(0);
        if(sizeAsBitmapContainer > sizeAsRunContainer) return this;
        int card = this.getCardinality();
        BitmapContainer answer = new BitmapContainer();
        for (int rlepos=0; rlepos < this.nbrruns; ++rlepos) {
            int start = Util.toIntUnsigned(this.getValue(rlepos));
            int end = start + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
            Util.setBitmapRange(answer.bitmap, start, end); 
        }
        answer.cardinality = card;
        return answer;
    }


    /**
     * Convert the container to either a Bitmap or an Array Container, depending
     * on the cardinality.
     * @param card the current cardinality
     * @return new container
     */
    Container toBitmapOrArrayContainer(int card) {
        //int card = this.getCardinality();
        if(card <= ArrayContainer.DEFAULT_MAX_SIZE) {
            ArrayContainer answer = new ArrayContainer(card);
            answer.cardinality=0;
            for (int rlepos=0; rlepos < this.nbrruns; ++rlepos) {
                int runStart = Util.toIntUnsigned(this.getValue(rlepos));
                int runEnd = runStart + Util.toIntUnsigned(this.getLength(rlepos));

                for (int runValue = runStart; runValue <= runEnd; ++runValue) {
                    answer.content[answer.cardinality++] = (short) runValue;
                }
            }
            return answer;
        }
        BitmapContainer answer = new BitmapContainer();
        for (int rlepos=0; rlepos < this.nbrruns; ++rlepos) {
            int start = Util.toIntUnsigned(this.getValue(rlepos));
            int end = start + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
            Util.setBitmapRange(answer.bitmap, start, end); 
        }
        answer.cardinality = card;
        return answer;
    }




    /** 
     *  Convert to Array or Bitmap container if the serialized form would be shorter.
     *  Exactly the same functionality as toEfficientContainer.
     */

    @Override
    public Container runOptimize() {
        return toEfficientContainer();
    }

    private void increaseCapacity() {
        int newCapacity = (valueslength.length == 0) ? DEFAULT_INIT_SIZE : valueslength.length < 64 ? valueslength.length * 2
                : valueslength.length < 1024 ? valueslength.length * 3 / 2
                        : valueslength.length * 5 / 4;
        short[] nv = new short[newCapacity];
        System.arraycopy(valueslength, 0, nv, 0, 2 * nbrruns);
        valueslength = nv;
    }
    
 // Push all values length to the end of the array (resize array if needed)
    private void copyToOffset(int offset) {
        final int minCapacity = 2 * (offset + nbrruns) ;
        if (valueslength.length < minCapacity) {
            // expensive case where we need to reallocate
            int newCapacity = valueslength.length;
            while (newCapacity < minCapacity) {
                newCapacity = (newCapacity == 0) ? DEFAULT_INIT_SIZE
                        : newCapacity < 64 ? newCapacity * 2
                        : newCapacity < 1024 ? newCapacity * 3 / 2
                        : newCapacity * 5 / 4;
            }
            short[] newvalueslength = new short[newCapacity];
            copyValuesLength(this.valueslength, 0, newvalueslength, offset, nbrruns);
            this.valueslength = newvalueslength;
        } else {
            // efficient case where we just copy 
            copyValuesLength(this.valueslength, 0, this.valueslength, offset, nbrruns);
        }
    }


    // not actually used anywhere, but potentially useful
    protected void ensureCapacity(int minNbRuns) {
        final int minCapacity = 2 * minNbRuns;
        if(valueslength.length < minCapacity) {
            int newCapacity = valueslength.length;
            while (newCapacity < minCapacity) {
                newCapacity = (newCapacity == 0) ? DEFAULT_INIT_SIZE
                        : newCapacity < 64 ? newCapacity * 2
                        : newCapacity < 1024 ? newCapacity * 3 / 2
                        : newCapacity * 5 / 4;
            }
            short[] nv = new short[newCapacity];
            copyValuesLength(valueslength, 0, nv, 0, nbrruns);
            valueslength = nv;
        }
    }

    /**
     * Create a container with default capacity
     */
    public RunContainer() {
        this(DEFAULT_INIT_SIZE);
    }

    /**
     * Create an array container with specified capacity
     *
     * @param capacity The capacity of the container
     */
    public RunContainer(final int capacity) {
        valueslength = new short[2 * capacity];
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
        serialize(out);

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
    ClassNotFoundException {
        deserialize(in);
    }

    @Override
    public Container flip(short x) {
        if(this.contains(x))
            return this.remove(x);
        else return this.add(x);
    }

    @Override
    public Container add(short k) {
        // TODO: it might be better and simpler to do return toBitmapOrArrayContainer(getCardinality()).add(k)
        // but note that some unit tests use this method to build up test runcontainers without calling runOptimize
        int index = unsignedInterleavedBinarySearch(valueslength, 0, nbrruns, k);
        if(index >= 0) return this;// already there
        index = - index - 2;// points to preceding value, possibly -1
        if(index >= 0) {// possible match
            int offset = Util.toIntUnsigned(k) - Util.toIntUnsigned(getValue(index));
            int le =     Util.toIntUnsigned(getLength(index)); 
            if(offset <= le) return this;
            if(offset == le + 1) {
                // we may need to fuse
                if(index + 1 < nbrruns) {
                    if(Util.toIntUnsigned(getValue(index + 1))  == Util.toIntUnsigned(k) + 1) {
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
                if(Util.toIntUnsigned(getValue(index + 1))  == Util.toIntUnsigned(k) + 1) {
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
    public Container add(int begin, int end) {
        RunContainer rc = (RunContainer) clone();
        return rc.iadd(begin, end);
    }

    @Override
    public Container and(ArrayContainer x) {
        ArrayContainer ac = new ArrayContainer(x.cardinality);
        if(this.nbrruns == 0) return ac;
        int rlepos = 0;
        int arraypos = 0;

        int rleval = Util.toIntUnsigned(this.getValue(rlepos));
        int rlelength = Util.toIntUnsigned(this.getLength(rlepos));        
        while(arraypos < x.cardinality)  {
            int arrayval = Util.toIntUnsigned(x.content[arraypos]);
            while(rleval + rlelength < arrayval) {// this will frequently be false
                ++rlepos;
                if(rlepos == this.nbrruns) {
                    return ac;// we are done
                }
                rleval = Util.toIntUnsigned(this.getValue(rlepos));
                rlelength = Util.toIntUnsigned(this.getLength(rlepos));
            }
            if(rleval > arrayval)  {
                arraypos = Util.advanceUntil(x.content,arraypos,x.cardinality,this.getValue(rlepos));
            } else {
                ac.content[ac.cardinality] = (short) arrayval;
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
        if (card <=  ArrayContainer.DEFAULT_MAX_SIZE) {
            // result can only be an array (assuming that we never make a RunContainer)
            if(card > x.cardinality) card = x.cardinality;
            ArrayContainer answer = new ArrayContainer(card);
            answer.cardinality=0;
            for (int rlepos=0; rlepos < this.nbrruns; ++rlepos) {
                int runStart = Util.toIntUnsigned(this.getValue(rlepos));
                int runEnd = runStart + Util.toIntUnsigned(this.getLength(rlepos));
                for (int runValue = runStart; runValue <= runEnd; ++runValue) {
                    if ( x.contains((short) runValue)) {// it looks like contains() should be cheap enough if accessed sequentially
                        answer.content[answer.cardinality++] = (short) runValue;
                    }
                }
            }
            return answer;
        }
        // we expect the answer to be a bitmap (if we are lucky)
        BitmapContainer answer = x.clone();
        int start = 0;
        for(int rlepos = 0; rlepos < this.nbrruns; ++rlepos ) {
            int end = Util.toIntUnsigned(this.getValue(rlepos));
            Util.resetBitmapRange(answer.bitmap, start, end);  // had been x.bitmap
            start = end + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
        }
        Util.resetBitmapRange(answer.bitmap, start, Util.maxLowBitAsInteger() + 1);   // had been x.bitmap
        answer.computeCardinality();
        if(answer.getCardinality() > ArrayContainer.DEFAULT_MAX_SIZE)
            return answer;
        else return answer.toArrayContainer();
    }


    @Override
    public Container andNot(BitmapContainer x) {
        //could be implemented as toTemporaryBitmap().iandNot(x);
        int card = this.getCardinality();
        if (card <=  ArrayContainer.DEFAULT_MAX_SIZE) {
            // result can only be an array (assuming that we never make a RunContainer)
            ArrayContainer answer = new ArrayContainer(card);
            answer.cardinality=0;
            for (int rlepos=0; rlepos < this.nbrruns; ++rlepos) {
                int runStart = Util.toIntUnsigned(this.getValue(rlepos));
                int runEnd = runStart + Util.toIntUnsigned(this.getLength(rlepos));
                for (int runValue = runStart; runValue <= runEnd; ++runValue) {
                    if ( ! x.contains((short) runValue)) {// it looks like contains() should be cheap enough if accessed sequentially
                        answer.content[answer.cardinality++] = (short) runValue;
                    }
                }
            }
            return answer;
        }
        // we expect the answer to be a bitmap (if we are lucky)
        BitmapContainer answer = x.clone();
        int lastPos = 0;
        for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
            int start = Util.toIntUnsigned(this.getValue(rlepos));
            int end = start + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
            Util.resetBitmapRange(answer.bitmap, lastPos, start); 
            Util.flipBitmapRange(answer.bitmap, start, end);
            lastPos = end;
        }
        Util.resetBitmapRange(answer.bitmap, lastPos, answer.bitmap.length*64);
        answer.computeCardinality();
        if (answer.getCardinality() > ArrayContainer.DEFAULT_MAX_SIZE)
            return answer;
        else
            return answer.toArrayContainer();
    }


    @Override
    public Container andNot(ArrayContainer x) {
        // when x is small, we guess that the result will still be a run container
        final int arbitrary_threshold = 32; // this is arbitrary
        if(x.getCardinality() < arbitrary_threshold)
            return lazyandNot(x).toEfficientContainer();
        // otherwise we generate either an array or bitmap container
        final int card = getCardinality();
        if(card <= ArrayContainer.DEFAULT_MAX_SIZE) {
            // if the cardinality is small, we construct the solution in place
            ArrayContainer ac = new ArrayContainer(card);
            ac.cardinality = Util.unsignedDifference(this.getShortIterator(), x.getShortIterator(), ac.content);
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
    public Container clone() {
        return new RunContainer(nbrruns, valueslength);
    }

    @Override
    public boolean contains(short x) {
        int index = unsignedInterleavedBinarySearch(valueslength, 0, nbrruns, x);
        if(index >= 0) return true;
        index = - index - 2; // points to preceding value, possibly -1
        if (index != -1)  {// possible match
            int offset = Util.toIntUnsigned(x) - Util.toIntUnsigned(getValue(index));
            int le =     Util.toIntUnsigned(getLength(index)); 
            if(offset <= le) return true;
        }
        return false;
    }





    @Override
    public void deserialize(DataInput in) throws IOException {
        nbrruns = Short.reverseBytes(in.readShort());
        if(valueslength.length < 2 * nbrruns)
            valueslength = new short[2 * nbrruns];
        for (int k = 0; k < 2 * nbrruns; ++k) {
            this.valueslength[k] = Short.reverseBytes(in.readShort());
        }
    }

    @Override
    public void fillLeastSignificant16bits(int[] x, int i, int mask) {
        int pos = i;
        for (int k = 0; k < this.nbrruns; ++k) {
            final int limit = Util.toIntUnsigned(this.getLength(k));
            final int base = Util.toIntUnsigned(this.getValue(k));
            for(int le = 0; le <= limit; ++le) {
                x[pos++] = (base + le) | mask;
            }
        }
    }

    @Override
    protected int getArraySizeInBytes() {
        return 2+4*this.nbrruns;  // "array" includes its size
    }

    @Override
    public int getCardinality() {
        int sum = 0;
        for(int k = 0; k < nbrruns; ++k)
            sum = sum + Util.toIntUnsigned(getLength(k)) + 1;
        return sum;
    }

    @Override
    public ShortIterator getShortIterator() {
        return new RunContainerShortIterator(this);
    }

    @Override
    public ShortIterator getReverseShortIterator() {
        return new ReverseRunContainerShortIterator(this);
    }

    @Override
    public int getSizeInBytes() {
        return this.nbrruns * 4 + 4;
    }


    @Override
    public int hashCode() {
        int hash = 0;
        for (int k = 0; k < nbrruns * 2; ++k)
            hash += 31 * hash + valueslength[k];
        return hash;
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
    public Container iandNot(ArrayContainer x) {
        return andNot(x);
    }

    @Override
    public Container iandNot(BitmapContainer x) {
        return andNot(x);
    }

    @Override
    public Container inot(int rangeStart, int rangeEnd) {
        if (rangeEnd <= rangeStart) return this; 

        // TODO: write special case code for rangeStart=0; rangeEnd=65535
        // a "sliding" effect where each range records the gap adjacent it
        // can probably be quite fast.  Probably have 2 cases: start with a
        // 0 run vs start with a 1 run.  If you both start and end with 0s,
        // you will require room for expansion.
        
        // the +1 below is needed in case the valueslength.length is odd
        if (valueslength.length <= 2*nbrruns+1)  {
            // no room for expansion
            // analyze whether this is a case that will require expansion (that we cannot do) 
            // this is a bit costly now (4 "contains" checks)
            
            boolean lastValueBeforeRange= false;
            boolean firstValueInRange = false;
            boolean lastValueInRange = false;
            boolean firstValuePastRange = false;

            // contains is based on a binary search and is hopefully fairly fast.
            // however, one binary search could *usually* suffice to find both
            // lastValueBeforeRange AND firstValueInRange.  ditto for
            // lastVaueInRange and firstValuePastRange

            // find the start of the range
            if (rangeStart > 0)
                lastValueBeforeRange = contains( (short) (rangeStart-1));
            firstValueInRange = contains( (short) rangeStart);
            
            if (lastValueBeforeRange == firstValueInRange) {
                // expansion is required if also lastValueInRange==firstValuePastRange
                
                // tougher to optimize out, but possible.
                lastValueInRange = contains( (short) (rangeEnd-1));
                if (rangeEnd != 65536)
                    firstValuePastRange = contains( (short) rangeEnd);
                
                // there is definitely one more run after the operation.
                if (lastValueInRange==firstValuePastRange)  {
                    return not( rangeStart, rangeEnd);  // can't do in-place: true space limit
                }
            }
        }
        // either no expansion required, or we have room to handle any required expansion for it.
        
        // remaining code is just a minor variation on not()
        int myNbrRuns = nbrruns;

        RunContainer ans = this;  // copy on top of self.  
        int k = 0;
        ans.nbrruns=0;  // losing this.nbrruns, which is stashed in myNbrRuns.

        //could try using unsignedInterleavedBinarySearch(valueslength, 0, nbrruns, rangeStart) instead of sequential scan
        //to find the starting location

        for(; (k < myNbrRuns) && ((Util.toIntUnsigned(this.getValue(k)) < rangeStart)) ; ++k) {
            // since it is atop self, there is no copying needed
            //ans.valueslength[2 * k] = this.valueslength[2 * k];
            //ans.valueslength[2 * k + 1] = this.valueslength[2 * k + 1];
                ans.nbrruns++;
        }
        // We will work left to right, with a read pointer that always stays
        // left of the write pointer.  However, we need to give the read pointer a head start.
        // use local variables so we are always reading 1 location ahead.

        short bufferedValue = 0, bufferedLength = 0;  // 65535 start and 65535 length would be illegal, could use as sentinel
        short nextValue=0, nextLength=0;
        if (k < myNbrRuns) {  // prime the readahead variables
            bufferedValue = getValue(k);
            bufferedLength = getLength(k);
        }

        ans.smartAppendExclusive((short)rangeStart,(short)(rangeEnd-rangeStart-1));
        
        for (; k < myNbrRuns; ++k) {
            if (ans.nbrruns > k+1)
                throw new RuntimeException("internal error in inot, writer has overtaken reader!! "+ k + " " + ans.nbrruns);
            if (k+1 < myNbrRuns) {
                nextValue = getValue(k+1);  // readahead for next iteration
                nextLength= getLength(k+1);
            }
            ans.smartAppendExclusive(bufferedValue, bufferedLength);
            bufferedValue = nextValue; bufferedLength = nextLength;
        }
        // the number of runs can increase by one, meaning (rarely) a bitmap will become better
        // or the cardinality can decrease by a lot, making an array better
        return ans.toEfficientContainer();
    }

    @Override
    public Container ior(ArrayContainer x) {
        if(isFull()) return this;
        final int nbrruns = this.nbrruns;
        final int offset = Math.max(nbrruns, x.getCardinality());
        copyToOffset(offset);
        int rlepos = 0;
        this.nbrruns = 0;
        PeekableShortIterator i = (PeekableShortIterator) x.getShortIterator();
        while (i.hasNext() && (rlepos < nbrruns) ) {
            if(Util.compareUnsigned(getValue(rlepos + offset), i.peekNext()) <= 0) {
                smartAppend(getValue(rlepos + offset), getLength(rlepos + offset));
                rlepos++;
            } else {
                smartAppend(i.next());
            }
        }        
        if (i.hasNext()) {
            /*if(this.nbrruns>0) {
                // this might be useful if the run container has just one very large run
                int lastval = Util.toIntUnsigned(getValue(nbrruns + offset - 1))
                        + Util.toIntUnsigned(getLength(nbrruns + offset - 1)) + 1;
                i.advanceIfNeeded((short) lastval);
            }*/
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
        if(isFull()) return this;
        return or(x);
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
    public Container not(int rangeStart, int rangeEnd) {
        if (rangeEnd <= rangeStart) return this.clone();
        RunContainer ans = new RunContainer(nbrruns+1);
        int k = 0;
        for(; (k < this.nbrruns) && ((Util.toIntUnsigned(this.getValue(k)) < rangeStart)) ; ++k) {
                ans.valueslength[2 * k] = this.valueslength[2 * k];
                ans.valueslength[2 * k + 1] = this.valueslength[2 * k + 1];
                ans.nbrruns++;
        }
        ans.smartAppendExclusive((short)rangeStart,(short)(rangeEnd-rangeStart-1));
        for(; k < this.nbrruns ; ++k) {
            ans.smartAppendExclusive(getValue(k), getLength(k));
        }
        // the number of runs can increase by one, meaning (rarely) a bitmap will become better
        // or the cardinality can decrease by a lot, making an array better
        return ans.toEfficientContainer();
    }
        

    @Override
    public Container or(ArrayContainer x) {
        // we guess that, often, the result will still be efficiently expressed as a run container
        return lazyor(x).repairAfterLazy();
    }
    
    protected Container ilazyor(ArrayContainer x) {
        if(isFull()) return this; // this can sometimes solve a lot of computation!
        return ilazyorToRun(x);
     } 

    protected Container lazyor(ArrayContainer x) {
       return lazyorToRun(x);//lazyorToRun(x); 
    } 
    
    protected boolean isFull() {
        return (this.nbrruns == 1) && (this.getValue(0) == 0) && (this.getLength(0) == -1);
    }

    private Container lazyorToRun(ArrayContainer x) {
        if(isFull()) return this.clone();
        // TODO: should optimize for the frequent case where we have a single run
        RunContainer answer = new RunContainer(new short[2 * (this.nbrruns + x.getCardinality())],0);
        int rlepos = 0;
        PeekableShortIterator i = (PeekableShortIterator) x.getShortIterator();

        while (i.hasNext() && (rlepos < this.nbrruns) ) {
            if(Util.compareUnsigned(getValue(rlepos), i.peekNext()) <= 0) {
                answer.smartAppend(getValue(rlepos), getLength(rlepos));
                // in theory, this next code could help, in practice it doesn't.
                /*int lastval = Util.toIntUnsigned(answer.getValue(answer.nbrruns - 1))
                        + Util.toIntUnsigned(answer.getLength(answer.nbrruns - 1)) + 1;
                i.advanceIfNeeded((short) lastval);*/

                rlepos++;
            } else {
                answer.smartAppend(i.next());
            }
        }        
        if (i.hasNext()) {
            /*if(answer.nbrruns>0) {
                 this might be useful if the run container has just one very large run
                  int lastval = Util.toIntUnsigned(answer.getValue(answer.nbrruns - 1))
                          + Util.toIntUnsigned(answer.getLength(answer.nbrruns - 1)) + 1;
                i.advanceIfNeeded((short) lastval);
            }*/
            while (i.hasNext()) {
                answer.smartAppend(i.next());
            }
        } else {
            while (rlepos < this.nbrruns) {
                answer.smartAppend(getValue(rlepos), getLength(rlepos));
                rlepos++;
            }
        }
        return answer.convertToLazyBitmapIfNeeded();
    }

    private Container ilazyorToRun(ArrayContainer x) {
        if(isFull()) return this.clone();
        final int nbrruns = this.nbrruns;
        final int offset = Math.max(nbrruns, x.getCardinality());
        copyToOffset(offset);
        int rlepos = 0;
        this.nbrruns = 0;
        PeekableShortIterator i = (PeekableShortIterator) x.getShortIterator();
        while (i.hasNext() && (rlepos < nbrruns) ) {
            if(Util.compareUnsigned(getValue(rlepos + offset), i.peekNext()) <= 0) {
                smartAppend(getValue(rlepos + offset), getLength(rlepos + offset));
                rlepos++;
            } else {
                smartAppend(i.next());
            }
        }        
        if (i.hasNext()) {
            /*if(this.nbrruns>0) {
                // this might be useful if the run container has just one very large run
                int lastval = Util.toIntUnsigned(getValue(nbrruns + offset - 1))
                        + Util.toIntUnsigned(getLength(nbrruns + offset - 1)) + 1;
                i.advanceIfNeeded((short) lastval);
            }*/
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

    
    private Container lazyxor(ArrayContainer x) {
        if(x.getCardinality() == 0) return this;
        if(this.nbrruns == 0) return x;
        RunContainer answer = new RunContainer(new short[2 * (this.nbrruns + x.getCardinality())],0);
        int rlepos = 0;
        ShortIterator i = x.getShortIterator();
        short cv = i.next();

        while (true) {
            if(Util.compareUnsigned(getValue(rlepos), cv) < 0) {
                answer.smartAppendExclusive(getValue(rlepos), getLength(rlepos));
                rlepos++;
                if(rlepos == this.nbrruns )  {
                    answer.smartAppendExclusive(cv);
                    while (i.hasNext()) {
                        answer.smartAppendExclusive(i.next());
                    }
                    break;
                }
            } else {
                answer.smartAppendExclusive(cv);
                if(! i.hasNext() ) {
                    while (rlepos < this.nbrruns) {
                        answer.smartAppendExclusive(getValue(rlepos), getLength(rlepos));
                        rlepos++;
                    }
                    break;
                } else cv = i.next();
            }
        }        
        return answer;
    }

    
    @Override
    public Container or(BitmapContainer x) {
        if(isFull()) return clone();
        // could be implemented as  return toTemporaryBitmap().ior(x);
        BitmapContainer answer = x.clone();
        for(int rlepos = 0; rlepos < this.nbrruns; ++rlepos ) {
            int start = Util.toIntUnsigned(this.getValue(rlepos));
            int end = start + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
            Util.setBitmapRange(answer.bitmap, start, end);
        }
        answer.computeCardinality();
        return answer;
    }
    
    @Override
    public Container remove(short x) {
        int index = unsignedInterleavedBinarySearch(valueslength, 0, nbrruns, x);
        if(index >= 0) {
            int le =  Util.toIntUnsigned(getLength(index));
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
            int offset = Util.toIntUnsigned(x) - Util.toIntUnsigned(getValue(index));
            int le =     Util.toIntUnsigned(getLength(index)); 
            if(offset < le) {
                // need to break in two
                this.setLength(index, (short) (offset - 1));
                // need to insert
                int newvalue = Util.toIntUnsigned(x) + 1;
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
    public void serialize(DataOutput out) throws IOException {
        writeArray(out);
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
        if(valueslength.length == 2 * nbrruns) return;
        valueslength = Arrays.copyOf(valueslength, 2 * nbrruns);
    }

    @Override
    protected void writeArray(DataOutput out) throws IOException {
        out.writeShort(Short.reverseBytes((short) this.nbrruns));
        for (int k = 0; k < 2 * this.nbrruns; ++k) {
            out.writeShort(Short.reverseBytes(this.valueslength[k]));
        }
    }

    @Override
    public Container xor(ArrayContainer x) {
        // if the cardinality of the array is small, guess that the output will still be a run container
        final int arbitrary_threshold = 32; // 32 is arbitrary here
        if(x.getCardinality() < arbitrary_threshold) {
            return lazyxor(x).repairAfterLazy();
        }
        // otherwise, we expect the output to be either an array or bitmap
        final int card = getCardinality();
        if(card <= ArrayContainer.DEFAULT_MAX_SIZE) {
            // if the cardinality is small, we construct the solution in place
            return x.xor(this.getShortIterator());
        }
        // otherwise, we generate a bitmap (even if runcontainer would be better)
        return toBitmapOrArrayContainer(card).ixor(x);
    }

    @Override
    public Container xor(BitmapContainer x) {
        // could be implemented as return toTemporaryBitmap().ixor(x);
        BitmapContainer answer = x.clone();
        for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
            int start = Util.toIntUnsigned(this.getValue(rlepos));
            int end = start + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
            Util.flipBitmapRange(answer.bitmap, start, end);
        }
        answer.computeCardinality();
        if (answer.getCardinality() > ArrayContainer.DEFAULT_MAX_SIZE)
            return answer;
        else
            return answer.toArrayContainer();
    }

    @Override
    public int rank(short lowbits) {
        int x = Util.toIntUnsigned(lowbits);
        int answer = 0;
        for (int k = 0; k < this.nbrruns; ++k) {
            int value = Util.toIntUnsigned(getValue(k));
            int length = Util.toIntUnsigned(getLength(k));
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
            int nextOffset = offset + Util.toIntUnsigned(getLength(k)) + 1;
            if(nextOffset > j) {
                return (short)(getValue(k) + (j - offset));
            }
            offset = nextOffset;
        }
        throw new IllegalArgumentException("Cannot select "+j+" since cardinality is "+getCardinality());        
    }

    @Override
    public Container limit(int maxcardinality) {
        if(maxcardinality >= getCardinality()) {
            return clone();
        }

        int r;
        int cardinality = 0;
        for (r = 1; r <= this.nbrruns; ++r) {
            cardinality += Util.toIntUnsigned(getLength(r)) + 1;
            if (maxcardinality <= cardinality) {
                break;
            }
        }
        RunContainer rc = new RunContainer( Arrays.copyOf(valueslength, 2*r),r);
        rc.setLength(r - 1, (short) (Util.toIntUnsigned(rc.getLength(r - 1)) - cardinality + maxcardinality));
        return rc;
    }

    @Override
    public Container iadd(int begin, int end) {
        // TODO: it might be better and simpler to do return toBitmapOrArrayContainer(getCardinality()).iadd(begin,end)
        if((begin >= end) || (end > (1<<16))) {
            throw new IllegalArgumentException("Invalid range [" + begin + "," + end + ")");
        }

        if(begin == end-1) {
            add((short) begin);
            return this;
        }

        int bIndex = unsignedInterleavedBinarySearch(this.valueslength, 0, this.nbrruns, (short) begin);
        int eIndex = unsignedInterleavedBinarySearch(this.valueslength, 0, this.nbrruns, (short) (end-1));

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
    public Container iremove(int begin, int end) {
        // TODO: it might be better and simpler to do return toBitmapOrArrayContainer(getCardinality()).iremove(begin,end)
        if((begin >= end) || (end > (1<<16))) {
            throw new IllegalArgumentException("Invalid range [" + begin + "," + end + ")");
        }
        if(begin == end-1) {
            remove((short) begin);
            return this;
        }

        int bIndex = unsignedInterleavedBinarySearch(this.valueslength, 0, this.nbrruns, (short) begin);
        int eIndex = unsignedInterleavedBinarySearch(this.valueslength, 0, this.nbrruns, (short) (end-1));

        // note, eIndex is looking for (end-1)

        if(bIndex>=0) {  // beginning marks beginning of a run
            if(eIndex<0) {
                eIndex = -eIndex - 2;
            }
            // eIndex could be a run that begins exactly at "end"
            // or it might be an earlier run
            
            // if the end is before the first run, we'd have eIndex==-1. But bIndex makes this impossible.

            if(valueLengthContains(end, eIndex)) {
                initValueLength(end, eIndex); // there is something left in the run
                recoverRoomsInRange(bIndex-1, eIndex-1);
            } else {
                recoverRoomsInRange(bIndex-1, eIndex); // nothing left in the run
            }

        } else if(bIndex<0 && eIndex>=0) {
            // start does not coincide to a run start, but end does.
            bIndex = -bIndex - 2;
            
            if(bIndex >= 0) {
                if (valueLengthContains(begin, bIndex)) {
                    closeValueLength(begin - 1, bIndex);
                }
            }
            // last run is one shorter
            incrementValue(eIndex);
            decrementLength(eIndex);
            recoverRoomsInRange(bIndex, eIndex-1);

        } else {
            bIndex = -bIndex - 2;
            eIndex = -eIndex - 2;

            if(eIndex>=0) { // end-1 is not before first run.
                if(bIndex>=0) { // nor is begin
                    if(bIndex==eIndex) { // all removal nested properly between
                                         // one run start and the next
                        if (valueLengthContains(begin, bIndex)) {
                            if (valueLengthContains(end, eIndex)) {
                                // proper nesting within a run, generates 2 sub-runs
                                makeRoomAtIndex(bIndex);
                                closeValueLength(begin-1, bIndex);
                                initValueLength(end, bIndex+1);
                                return this;
                            }
                            // removed area extends beyond run.
                            closeValueLength(begin-1, bIndex);
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
                        }
                        else {
                            // run ends at or before the range being removed, can delete it
                        }
                        recoverRoomsInRange(bIndex, eIndex);
                    }

                } else { 
                    // removed range begins before the first run
                    if(valueLengthContains(end, eIndex)) { // had been end-1
                        initValueLength(end, eIndex);
                        recoverRoomsInRange(bIndex, eIndex - 1);
                    } else {  // removed range includes all the last run
                        recoverRoomsInRange(bIndex, eIndex);
                    }
                }

            }
            else {
                // eIndex == -1: whole range is before first run, nothing to delete...
            }

        }
        return this;
    }

    
    
    
    @Override
    public Container remove(int begin, int end) {
        RunContainer rc = (RunContainer) clone();
        return rc.iremove(begin, end);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof RunContainer) {
            RunContainer srb = (RunContainer) o;
            if (srb.nbrruns != this.nbrruns)
                return false;
            for (int i = 0; i < nbrruns; ++i) {
                if (this.getValue(i) != srb.getValue(i))
                    return false;
                if (this.getLength(i) != srb.getLength(i))
                    return false;
            }
            return true;
        } else if(o instanceof Container) {
            if(((Container) o).getCardinality() != this.getCardinality())
                return false; // should be a frequent branch if they differ
            // next bit could be optimized if needed:
            ShortIterator me = this.getShortIterator();
            ShortIterator you = ((Container) o).getShortIterator();
            while(me.hasNext()) {
                if(me.next() != you.next())
                    return false;
            }
            return true;
        }
        return false;
    }

    private static int unsignedInterleavedBinarySearch(final short[] array,
            final int begin, final int end, final short k) {
        if(Util.USE_HYBRID_BINSEARCH)
            return hybridUnsignedInterleavedBinarySearch(array,begin,end,k);
        else 
            return branchyUnsignedInterleavedBinarySearch(array,begin,end,k);
        
    }

    private static int branchyUnsignedInterleavedBinarySearch(final short[] array,
            final int begin, final int end, final short k) {
        int ikey = Util.toIntUnsigned(k);
        int low = begin;
        int high = end - 1;
        while (low <= high) {
            final int middleIndex = (low + high) >>> 1;
            final int middleValue = Util.toIntUnsigned(array[2 * middleIndex]);
            if (middleValue < ikey)
                low = middleIndex + 1;
            else if (middleValue > ikey)
                high = middleIndex - 1;
            else
                return middleIndex;
        }
        return -(low + 1);
    }
    
    // starts with binary search and finishes with a sequential search
    private static int hybridUnsignedInterleavedBinarySearch(final short[] array,
            final int begin, final int end, final short k) {
        int ikey = Util.toIntUnsigned(k);
        int low = begin;
        int high = end - 1;
        // 16 in the next line matches the size of a cache line
        while (low + 16 <= high) {
            final int middleIndex = (low + high) >>> 1;
            final int middleValue = Util.toIntUnsigned(array[2 * middleIndex]);
            if (middleValue < ikey)
                low = middleIndex + 1;
            else if (middleValue > ikey)
                high = middleIndex - 1;
            else
                return middleIndex;
        }
        // we finish the job with a sequential search 
        int x = low;
        for(; x <= high; ++x) {
            final int val = Util.toIntUnsigned(array[2*x]);
            if(val >= ikey) {
                if(val == ikey) return x;
                break;
            }
        }
        return -(x + 1);
    }    


    short getValue(int index) {
        return valueslength[2*index];
    }

    short getLength(int index) {
        return valueslength[2*index + 1];
    }

    private void incrementLength(int index) {
        valueslength[2*index + 1]++;
    }

    private void incrementValue(int index) {
        valueslength[2*index]++;
    }

    private void decrementLength(int index) {
        valueslength[2*index + 1]--;
    }

    private void decrementValue(int index) {
        valueslength[2*index]--;
    }

    private void setLength(int index, short v) {
        setLength(valueslength, index, v);
    }

    private void setLength(short[] valueslength, int index, short v) {
        valueslength[2*index + 1] = v;
    }

    private void setValue(int index, short v) {
        setValue(valueslength, index, v);
    }

    private void setValue(short[] valueslength, int index, short v) {
        valueslength[2*index] = v;
    }

    private void makeRoomAtIndex(int index) {
        if (2 * (nbrruns+1) > valueslength.length) increaseCapacity();
        copyValuesLength(valueslength, index, valueslength, index + 1, nbrruns - index);
        nbrruns++;
    }

    private void recoverRoomAtIndex(int index) {
        copyValuesLength(valueslength, index + 1, valueslength, index, nbrruns - index - 1);
        nbrruns--;
    }

    // To recover rooms between begin(exclusive) and end(inclusive)
    private void recoverRoomsInRange(int begin, int end) {
        if (end + 1 < this.nbrruns) {
            copyValuesLength(this.valueslength, end + 1, this.valueslength, begin + 1, this.nbrruns - 1 - end);
        }
        this.nbrruns -= end - begin;
    }

    // To merge values length from begin(inclusive) to end(inclusive)
    private void mergeValuesLength(int begin, int end) {
        if(begin < end) {
            int bValue = Util.toIntUnsigned(getValue(begin));
            int eValue = Util.toIntUnsigned(getValue(end));
            int eLength = Util.toIntUnsigned(getLength(end));
            int newLength = eValue - bValue + eLength;
            setLength(begin, (short) newLength);
            recoverRoomsInRange(begin, end);
        }
    }

    // To check if a value length can be prepended with a given value
    private boolean canPrependValueLength(int value, int index) {
        if(index < this.nbrruns) {
            int nextValue = Util.toIntUnsigned(getValue(index));
            if(nextValue == value+1) {
                return true;
            }
        }
        return false;
    }

    // Prepend a value length with all values starting from a given value
    private void prependValueLength(int value, int index) {
        int initialValue = Util.toIntUnsigned(getValue(index));
        int length = Util.toIntUnsigned(getLength(index));
        setValue(index, (short) value);
        setLength(index, (short) (initialValue - value + length));
    }

    // Append a value length with all values until a given value
    private void appendValueLength(int value, int index) {
        int previousValue = Util.toIntUnsigned(getValue(index));
        int length = Util.toIntUnsigned(getLength(index));
        int offset = value - previousValue;
        if(offset>length) {
            setLength(index, (short) offset);
        }
    }

    // To check if a value length contains a given value
    private boolean valueLengthContains(int value, int index) {
        int initialValue = Util.toIntUnsigned(getValue(index));
        int length = Util.toIntUnsigned(getLength(index));

        if(value <= initialValue + length) {
            return true;
        }
        return false;
    }

    // To set the first value of a value length
    private void initValueLength(int value, int index) {
        int initialValue = Util.toIntUnsigned(getValue(index));
        int length = Util.toIntUnsigned(getLength(index));
        setValue(index, (short) (value));
        setLength(index, (short) (length - (value - initialValue)));
    }

    // To set the last value of a value length
    private void closeValueLength(int value, int index) {
        int initialValue = Util.toIntUnsigned(getValue(index));
        setLength(index, (short) (value - initialValue));
    }

    private void copyValuesLength(short[] src, int srcIndex, short[] dst, int dstIndex, int length) {
        System.arraycopy(src, 2*srcIndex, dst, 2*dstIndex, 2*length);
    }
  


    // bootstrapping (aka "galloping")  binary search.  Always skips at least one.
    // On our "real data" benchmarks, enabling galloping is a minor loss
    //.."ifdef ENABLE_GALLOPING_AND"   :)
    private int skipAhead(RunContainer skippingOn, int pos, int targetToExceed) {
        int left=pos;
        int span=1;
        int probePos=0;
        int end;
        // jump ahead to find a spot where end > targetToExceed (if it exists)
        do {
            probePos = left + span;
            if (probePos >= skippingOn.nbrruns - 1)  {
                // expect it might be quite common to find the container cannot be advanced as far as requested.  Optimize for it.
                probePos = skippingOn.nbrruns - 1;
                end = Util.toIntUnsigned(skippingOn.getValue(probePos)) + Util.toIntUnsigned(skippingOn.getLength(probePos)) + 1; 
                if (end <= targetToExceed) 
                    return skippingOn.nbrruns;
            }
            end = Util.toIntUnsigned(skippingOn.getValue(probePos)) + Util.toIntUnsigned(skippingOn.getLength(probePos)) + 1;
            span *= 2;
        }  while (end <= targetToExceed);
        int right = probePos;
        // left and right are both valid positions.  Invariant: left <= targetToExceed && right > targetToExceed
        // do a binary search to discover the spot where left and right are separated by 1, and invariant is maintained.
        while (right - left > 1) {
            int mid =  (right + left)/2;
            int midVal =  Util.toIntUnsigned(skippingOn.getValue(mid)) + Util.toIntUnsigned(skippingOn.getLength(mid)) + 1; 
            if (midVal > targetToExceed) 
                right = mid;
            else
                left = mid;
        }
        return right;
    }

    @Override
      public Container and(RunContainer x) {
        RunContainer answer = new RunContainer(new short[2 * (this.nbrruns + x.nbrruns)],0);
        int rlepos = 0;
        int xrlepos = 0;
        int start = Util.toIntUnsigned(this.getValue(rlepos));
        int end = start + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
        int xstart = Util.toIntUnsigned(x.getValue(xrlepos));
        int xend = xstart + Util.toIntUnsigned(x.getLength(xrlepos)) + 1;
        while ((rlepos < this.nbrruns ) && (xrlepos < x.nbrruns )) {
            if (end  <= xstart) {
                if (ENABLE_GALLOPING_AND) {
                    rlepos = skipAhead(this, rlepos, xstart); // skip over runs until we have end > xstart  (or rlepos is advanced beyond end)
                }
                else
                    ++rlepos;

                if(rlepos < this.nbrruns ) {
                    start = Util.toIntUnsigned(this.getValue(rlepos));
                    end = start + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
                }
            } else if (xend <= start) {
                // exit the second run
                if (ENABLE_GALLOPING_AND) {
                    xrlepos = skipAhead(x, xrlepos, start);
                }
                else
                    ++xrlepos;

                if(xrlepos < x.nbrruns ) {
                    xstart = Util.toIntUnsigned(x.getValue(xrlepos));
                    xend = xstart + Util.toIntUnsigned(x.getLength(xrlepos)) + 1;
                }
            } else {// they overlap
                final int lateststart = start > xstart ? start : xstart;
                int earliestend;
                if(end == xend) {// improbable
                    earliestend = end;
                    rlepos++;
                    xrlepos++;
                    if(rlepos < this.nbrruns ) {
                        start = Util.toIntUnsigned(this.getValue(rlepos));
                        end = start + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
                    }
                    if(xrlepos < x.nbrruns) {
                        xstart = Util.toIntUnsigned(x.getValue(xrlepos));
                        xend = xstart + Util.toIntUnsigned(x.getLength(xrlepos)) + 1;
                    }
                } else if(end < xend) {
                    earliestend = end;
                    rlepos++;
                    if(rlepos < this.nbrruns ) {
                        start = Util.toIntUnsigned(this.getValue(rlepos));
                        end = start + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
                    }

                } else {// end > xend
                    earliestend = xend;
                    xrlepos++;
                    if(xrlepos < x.nbrruns) {
                        xstart = Util.toIntUnsigned(x.getValue(xrlepos));
                        xend = xstart + Util.toIntUnsigned(x.getLength(xrlepos)) + 1;
                    }                
                }
                answer.valueslength[2 * answer.nbrruns] = (short) lateststart;
                answer.valueslength[2 * answer.nbrruns + 1] = (short) (earliestend - lateststart - 1);
                answer.nbrruns++;
            }
        }
        return answer.toEfficientContainer();  // subsequent trim() may be required to avoid wasted space.
    }







    @Override
    public Container andNot(RunContainer x) {
        RunContainer answer = new RunContainer(new short[2 * (this.nbrruns + x.nbrruns)],0);
        int rlepos = 0;
        int xrlepos = 0;
        int start = Util.toIntUnsigned(this.getValue(rlepos));
        int end = start + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
        int xstart = Util.toIntUnsigned(x.getValue(xrlepos));
        int xend = xstart + Util.toIntUnsigned(x.getLength(xrlepos)) + 1;
        while ((rlepos < this.nbrruns ) && (xrlepos < x.nbrruns )) {
            if (end  <= xstart) {
                // output the first run
                answer.valueslength[2 * answer.nbrruns] = (short) start;
                answer.valueslength[2 * answer.nbrruns + 1] = (short)(end - start - 1);
                answer.nbrruns++;
                rlepos++;
                if(rlepos < this.nbrruns ) {
                    start = Util.toIntUnsigned(this.getValue(rlepos));
                    end = start + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
                }
            } else if (xend <= start) {
                // exit the second run
                xrlepos++;
                if(xrlepos < x.nbrruns ) {
                    xstart = Util.toIntUnsigned(x.getValue(xrlepos));
                    xend = xstart + Util.toIntUnsigned(x.getLength(xrlepos)) + 1;
                }
            } else {
                if ( start < xstart ) {
                    answer.valueslength[2 * answer.nbrruns] = (short) start;
                    answer.valueslength[2 * answer.nbrruns + 1] = (short) (xstart - start - 1);
                    answer.nbrruns++;
                }
                if(xend < end) {
                    start = xend;
                } else {
                    rlepos++;
                    if(rlepos < this.nbrruns ) {
                        start = Util.toIntUnsigned(this.getValue(rlepos));
                        end = start + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
                    }
                }
            }
        }
        if(rlepos < this.nbrruns) {
            answer.valueslength[2 * answer.nbrruns] = (short) start;
            answer.valueslength[2 * answer.nbrruns + 1] = (short)(end - start - 1);
            answer.nbrruns++;
            rlepos++;
            if(rlepos < this.nbrruns ) {
                System.arraycopy(this.valueslength, 2 * rlepos, answer.valueslength, 2 * answer.nbrruns, 2*(this.nbrruns-rlepos ));
                answer.nbrruns  = answer.nbrruns + this.nbrruns - rlepos;
            } 
        }
        return answer.toEfficientContainer();
    }

    private RunContainer lazyandNot(ArrayContainer x) {
        if(x.getCardinality() == 0) return this;
        RunContainer answer = new RunContainer(new short[2 * (this.nbrruns + x.cardinality)],0);
        int rlepos = 0;
        int xrlepos = 0;
        int start = Util.toIntUnsigned(this.getValue(rlepos));
        int end = start + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
        int xstart = Util.toIntUnsigned(x.content[xrlepos]);
        while ((rlepos < this.nbrruns ) && (xrlepos < x.cardinality )) {
            if (end  <= xstart) {
                // output the first run
                answer.valueslength[2 * answer.nbrruns] = (short) start;
                answer.valueslength[2 * answer.nbrruns + 1] = (short)(end - start - 1);
                answer.nbrruns++;
                rlepos++;
                if(rlepos < this.nbrruns ) {
                    start = Util.toIntUnsigned(this.getValue(rlepos));
                    end = start + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
                }
            } else if (xstart + 1 <= start) {
                // exit the second run
                xrlepos++;
                if(xrlepos < x.cardinality ) {
                    xstart = Util.toIntUnsigned(x.content[xrlepos]);
                }
            } else {
                if ( start < xstart ) {
                    answer.valueslength[2 * answer.nbrruns] = (short) start;
                    answer.valueslength[2 * answer.nbrruns + 1] = (short) (xstart - start - 1);
                    answer.nbrruns++;
                }
                if(xstart + 1 < end) {
                    start = xstart + 1;
                } else {
                    rlepos++;
                    if(rlepos < this.nbrruns ) {
                        start = Util.toIntUnsigned(this.getValue(rlepos));
                        end = start + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
                    }
                }
            }
        }
        if(rlepos < this.nbrruns) {
            answer.valueslength[2 * answer.nbrruns] = (short) start;
            answer.valueslength[2 * answer.nbrruns + 1] = (short)(end - start - 1);
            answer.nbrruns++;
            rlepos++;
            if(rlepos < this.nbrruns ) {
                System.arraycopy(this.valueslength, 2 * rlepos, answer.valueslength, 2 * answer.nbrruns, 2*(this.nbrruns-rlepos ));
                answer.nbrruns  = answer.nbrruns + this.nbrruns - rlepos;
            } 
        }
        return answer;
    }
    
    @Override
    public Container iand(RunContainer x) {
        return and(x);
    }

    @Override
    public Container iandNot(RunContainer x) {
        return andNot(x);
    }

    @Override
    public Container ior(RunContainer x) {
        if (isFull()) return this;

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
            final short value = this.getValue(offset + rlepos);
            final short xvalue = x.getValue(xrlepos);
            final short length = this.getLength(offset + rlepos);
            final short xlength = x.getLength(xrlepos);

            if (Util.compareUnsigned(value, xvalue) <= 0) {
                this.smartAppend(value, length);
                ++rlepos;
            } else {
                this.smartAppend(xvalue, xlength);
                ++xrlepos;
            }
        }

        while (rlepos < nbrruns) {
            this.smartAppend(this.getValue(offset + rlepos), this.getLength((offset + rlepos)));
            ++rlepos;
        }

        while (xrlepos < xnbrruns) {
            this.smartAppend(x.getValue(xrlepos), x.getLength(xrlepos));
            ++xrlepos;
        }
        return this.toBitmapIfNeeded();
    }

    @Override
    public Container ixor(RunContainer x) {
        return xor(x);
    }

    private void smartAppend(short val) {
        int oldend;
        if((nbrruns==0) ||
                (Util.toIntUnsigned(val) > 
                (oldend= Util.toIntUnsigned(valueslength[2 * (nbrruns - 1)]) + Util.toIntUnsigned(valueslength[2 * (nbrruns - 1) + 1])) + 1)) { // we add a new one
            valueslength[2 * nbrruns] =  val;
            valueslength[2 * nbrruns + 1] = 0;
            nbrruns++;
            return;
        } 
        if(val == (short)(oldend + 1))  { // we merge
            valueslength[2 * (nbrruns - 1) + 1]++;
        }
    }
    private void smartAppendExclusive(short val) {
        int oldend;
        if((nbrruns==0) ||
                (Util.toIntUnsigned(val) > 
                (oldend = Util.toIntUnsigned(getValue(nbrruns - 1)) + Util.toIntUnsigned(getLength(nbrruns - 1)) + 1))) { // we add a new one
            valueslength[2 * nbrruns] =  val;
            valueslength[2 * nbrruns + 1] = 0;
            nbrruns++;
            return;
        } 
        if(oldend == Util.toIntUnsigned(val)) {
            // we merge
            valueslength[2 * ( nbrruns - 1) + 1]++;
            return;
        }
        int newend = Util.toIntUnsigned(val)  + 1;
        
        if(Util.toIntUnsigned(val) == Util.toIntUnsigned(getValue(nbrruns - 1))) {
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
        setLength(nbrruns - 1, (short) (val - Util.toIntUnsigned(getValue(nbrruns - 1)) -1));
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
    
    private void smartAppend(short start, short length) {
        int oldend;
        if((nbrruns==0) ||
                (Util.toIntUnsigned(start) > 
                (oldend = Util.toIntUnsigned(getValue(nbrruns - 1)) + Util.toIntUnsigned(getLength(nbrruns - 1))) + 1)) { // we add a new one
            valueslength[2 * nbrruns] =  start;
            valueslength[2 * nbrruns + 1] = length;
            nbrruns++;
            return;
        } 
        int newend = Util.toIntUnsigned(start) + Util.toIntUnsigned(length) + 1;
        if(newend > oldend)  { // we merge
            setLength(nbrruns - 1, (short) (newend - 1 - Util.toIntUnsigned(getValue(nbrruns - 1))));
        }
    }

    private void smartAppendExclusive(short start, short length) {
        int oldend;
        if((nbrruns==0) ||
                (Util.toIntUnsigned(start) > 
                (oldend = Util.toIntUnsigned(getValue(nbrruns - 1)) + Util.toIntUnsigned(getLength(nbrruns - 1)) + 1))) { // we add a new one
            valueslength[2 * nbrruns] =  start;
            valueslength[2 * nbrruns + 1] = length;
            nbrruns++;
            return;
        } 
        if(oldend == Util.toIntUnsigned(start)) {
            // we merge
            valueslength[2 * ( nbrruns - 1) + 1] += length + 1;
            return;
        }
        
        int newend = Util.toIntUnsigned(start) + Util.toIntUnsigned(length) + 1;
        
        if(Util.toIntUnsigned(start) == Util.toIntUnsigned(getValue(nbrruns - 1))) {
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
        setLength(nbrruns - 1, (short) (start - Util.toIntUnsigned(getValue(nbrruns - 1)) -1));
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

    @Override
    public Container or(RunContainer x) {
        if(isFull()) return clone();
        if(x.isFull()) return x.clone(); // cheap case that can save a lot of computation
        // we really ought to optimize the rest of the code for the frequent case where there is a single run
        RunContainer answer = new RunContainer(new short[2 * (this.nbrruns + x.nbrruns)],0);
        int rlepos = 0;
        int xrlepos = 0;

        while ((xrlepos < x.nbrruns) && (rlepos < this.nbrruns)) {
            if(Util.compareUnsigned(getValue(rlepos), x.getValue(xrlepos)) <= 0) {
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

        return answer.toBitmapIfNeeded();
    }


    @Override
    public Container xor(RunContainer x) {
        if(x.nbrruns == 0) return this.clone();
        if(this.nbrruns == 0) return x.clone();
        RunContainer answer = new RunContainer(new short[2 * (this.nbrruns + x.nbrruns)],0);
        int rlepos = 0;
        int xrlepos = 0;

        while (true) {
            if(Util.compareUnsigned(getValue(rlepos), x.getValue(xrlepos)) < 0) {
                answer.smartAppendExclusive(getValue(rlepos), getLength(rlepos));
                rlepos++;

                if(rlepos == this.nbrruns )  {
                    while (xrlepos < x.nbrruns) {
                        answer.smartAppendExclusive(x.getValue(xrlepos), x.getLength(xrlepos));
                        xrlepos++;
                    }
                    break;
                }
            } else {
                answer.smartAppendExclusive(x.getValue(xrlepos), x.getLength(xrlepos));

                xrlepos++;
                if(xrlepos == x.nbrruns ) {
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
    public Container repairAfterLazy() {
        return toEfficientContainer();
    }
    
    // a very cheap check... if you have more than 4096, then you should use a bitmap container.
    // this function avoids computing the cardinality
    private Container convertToLazyBitmapIfNeeded() {
        // when nbrruns exceed ArrayContainer.DEFAULT_MAX_SIZE, then we know it should be stored as a bitmap, always 
        if(this.nbrruns > ArrayContainer.DEFAULT_MAX_SIZE) {
            BitmapContainer answer = new BitmapContainer();
            for (int rlepos=0; rlepos < this.nbrruns; ++rlepos) {
                int start = Util.toIntUnsigned(this.getValue(rlepos));
                int end = start + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
                Util.setBitmapRange(answer.bitmap, start, end); 
            }
            answer.cardinality = -1;
            return answer;
        }
        return this;
    }

    @Override
    public boolean intersects(ArrayContainer x) {
        if(this.nbrruns == 0) return false;
        int rlepos = 0;
        int arraypos = 0;
        int rleval = Util.toIntUnsigned(this.getValue(rlepos));
        int rlelength = Util.toIntUnsigned(this.getLength(rlepos));        
        while(arraypos < x.cardinality)  {
            int arrayval = Util.toIntUnsigned(x.content[arraypos]);
            while(rleval + rlelength < arrayval) {// this will frequently be false
                ++rlepos;
                if(rlepos == this.nbrruns) {
                    return false;
                }
                rleval = Util.toIntUnsigned(this.getValue(rlepos));
                rlelength = Util.toIntUnsigned(this.getLength(rlepos));
            }
            if(rleval > arrayval)  {
                arraypos = Util.advanceUntil(x.content,arraypos,x.cardinality,this.getValue(rlepos));
            } else {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean intersects(BitmapContainer x) {
        // TODO: this is probably not optimally fast
        for (int rlepos=0; rlepos < this.nbrruns; ++rlepos) {
            int runStart = Util.toIntUnsigned(this.getValue(rlepos));
            int runEnd = runStart + Util.toIntUnsigned(this.getLength(rlepos));
            for (int runValue = runStart; runValue <= runEnd; ++runValue) {
                if ( x.contains((short) runValue)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public boolean intersects(RunContainer x) {
        int rlepos = 0;
        int xrlepos = 0;
        int start = Util.toIntUnsigned(this.getValue(rlepos));
        int end = start + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
        int xstart = Util.toIntUnsigned(x.getValue(xrlepos));
        int xend = xstart + Util.toIntUnsigned(x.getLength(xrlepos)) + 1;
        while ((rlepos < this.nbrruns ) && (xrlepos < x.nbrruns )) {
            if (end  <= xstart) {
                if (ENABLE_GALLOPING_AND) {
                    rlepos = skipAhead(this, rlepos, xstart); // skip over runs until we have end > xstart  (or rlepos is advanced beyond end)
                }
                else
                    ++rlepos;

                if(rlepos < this.nbrruns ) {
                    start = Util.toIntUnsigned(this.getValue(rlepos));
                    end = start + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
                }
            } else if (xend <= start) {
                // exit the second run
                if (ENABLE_GALLOPING_AND) {
                    xrlepos = skipAhead(x, xrlepos, start);
                }
                else
                    ++xrlepos;

                if(xrlepos < x.nbrruns ) {
                    xstart = Util.toIntUnsigned(x.getValue(xrlepos));
                    xend = xstart + Util.toIntUnsigned(x.getLength(xrlepos)) + 1;
                }
            } else {// they overlap
                return true;
            }
        }
        return false;
    }

}


final class RunContainerShortIterator implements ShortIterator {
    int pos;
    int le = 0;
    int maxlength;
    int base;

    RunContainer parent;

    RunContainerShortIterator() {}

    RunContainerShortIterator(RunContainer p) {
        wrap(p);
    }

    void wrap(RunContainer p) {
        parent = p;
        pos = 0;
        le = 0;
        if(pos < parent.nbrruns) {
            maxlength = Util.toIntUnsigned(parent.getLength(pos));
            base = Util.toIntUnsigned(parent.getValue(pos));
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
                maxlength = Util.toIntUnsigned(parent.getLength(pos));
                base = Util.toIntUnsigned(parent.getValue(pos));
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
                maxlength = Util.toIntUnsigned(parent.getLength(pos));
                base = Util.toIntUnsigned(parent.getValue(pos));
            }
        }
        return ans;
    }

    @Override
    public void remove() {
        throw new RuntimeException("Not implemented");// TODO
    }

};

final class ReverseRunContainerShortIterator implements ShortIterator {
    int pos;
    int le;
    RunContainer parent;
    int maxlength;
    int base;


    ReverseRunContainerShortIterator(){}

    ReverseRunContainerShortIterator(RunContainer p) {
        wrap(p);
    }

    void wrap(RunContainer p) {
        parent = p;
        pos = parent.nbrruns - 1;
        le = 0;
        if(pos >= 0) {
            maxlength = Util.toIntUnsigned(parent.getLength(pos));
            base = Util.toIntUnsigned(parent.getValue(pos));
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
                maxlength = Util.toIntUnsigned(parent.getLength(pos));
                base = Util.toIntUnsigned(parent.getValue(pos));
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
                maxlength = Util.toIntUnsigned(parent.getLength(pos));
                base = Util.toIntUnsigned(parent.getValue(pos));
            }
        }
        return ans;
    }

    @Override
    public void remove() {
        throw new RuntimeException("Not implemented");// TODO
    }

}

