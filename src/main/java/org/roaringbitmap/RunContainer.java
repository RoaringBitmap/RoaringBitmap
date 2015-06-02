/*
 * (c) Daniel Lemire, Owen Kaser, Samy Chambi, Jon Alvarado, Rory Graves, Björn Sperber
 * Licensed under the Apache License, Version 2.0.
 */
package org.roaringbitmap;

import java.io.*;
import java.util.Arrays;
import java.util.Iterator;

/**
 * This container takes the form of runs of consecutive values (effectively,
 * run-length encoding).
 */
public class RunContainer extends Container implements Cloneable, Serializable {
    private static final int DEFAULT_INIT_SIZE = 4;
    private short[] valueslength;// we interleave values and lengths, so 
    // that if you have the values 11,12,13,14,15, you store that as 11,4 where 4 means that beyond 11 itself, there are
    // 4 contiguous values that follows.
    // Other example: e.g., 1, 10, 20,0, 31,2 would be a concise representation of  1, 2, ..., 11, 20, 31, 32, 33

    int nbrruns = 0;// how many runs, this number should fit in 16 bits.

    
    private RunContainer(int nbrruns, short[] valueslength) {
        this.nbrruns = nbrruns;
        this.valueslength = Arrays.copyOf(valueslength, valueslength.length);
    }
    
    /**
     * Convert the container to either a Bitmap or an Array Container, depending
     * on the cardinality.
     * @return new container
     */
    protected Container toBitmapOrArrayContainer() {
    	int card = this.getCardinality();
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
            int end = Util.toIntUnsigned(this.getValue(rlepos)) + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
            Util.setBitmapRange(answer.bitmap, start, end); 
        }
        answer.cardinality = card;
        return answer;
    }


    private void increaseCapacity() {
        int newCapacity = (valueslength.length == 0) ? DEFAULT_INIT_SIZE : valueslength.length < 64 ? valueslength.length * 2
                : valueslength.length < 1024 ? valueslength.length * 3 / 2
                : valueslength.length * 5 / 4;
        short[] nv = new short[newCapacity];
        System.arraycopy(valueslength, 0, nv, 0, 2 * nbrruns);
        valueslength = nv;
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
        int rlepos = 0;
        int arraypos = 0;
        while((arraypos < x.cardinality) && (rlepos < this.nbrruns)) {
            if(Util.toIntUnsigned(this.getValue(rlepos)) + Util.toIntUnsigned(this.getLength(rlepos)) < Util.toIntUnsigned(x.content[arraypos])) {
                ++rlepos;
            } else if(Util.toIntUnsigned(this.getValue(rlepos)) > Util.toIntUnsigned(x.content[arraypos]))  {
                arraypos = Util.advanceUntil(x.content,arraypos,x.cardinality,this.getValue(rlepos));
            } else {
                ac.content[ac.cardinality ++ ] = x.content[arraypos++];
            }
        }
        return ac;
    }
    

    // TODO: kept temporarily for perf. testing
    public Container andold(BitmapContainer x) {
        BitmapContainer answer = x.clone();
        int start = 0;
        for(int rlepos = 0; rlepos < this.nbrruns; ++rlepos ) {
            int end = Util.toIntUnsigned(this.getValue(rlepos));
            Util.resetBitmapRange(answer.bitmap, start, end);  // had been x.bitmap
            start = Util.toIntUnsigned(this.getValue(rlepos)) + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
        }
        Util.resetBitmapRange(answer.bitmap, start, Util.maxLowBitAsInteger() + 1);   // had been x.bitmap
        answer.computeCardinality();
        if(answer.getCardinality() > ArrayContainer.DEFAULT_MAX_SIZE)
            return answer;
        else return answer.toArrayContainer();
    }

    @Override
    public Container and(BitmapContainer x) {
    	int card = this.getCardinality();
        if (card <=  ArrayContainer.DEFAULT_MAX_SIZE) {
            // result can only be an array (assuming that we never make a RunContainer)
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
            start = Util.toIntUnsigned(this.getValue(rlepos)) + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
        }
        Util.resetBitmapRange(answer.bitmap, start, Util.maxLowBitAsInteger() + 1);   // had been x.bitmap
        answer.computeCardinality();
        if(answer.getCardinality() > ArrayContainer.DEFAULT_MAX_SIZE)
            return answer;
        else return answer.toArrayContainer();
    }


    // TODO: kept temporarily for perf. testing
    public Container andNotold(BitmapContainer x) {
        BitmapContainer answer = x.clone();
        int lastPos = 0;
        for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
            int start = Util.toIntUnsigned(this.getValue(rlepos));
            int end = Util.toIntUnsigned(this.getValue(rlepos)) + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
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
    public Container andNot(BitmapContainer x) {
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
            int end = Util.toIntUnsigned(this.getValue(rlepos)) + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
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
    	// TODO: this is lazy, but is this wise?
    	return toBitmapOrArrayContainer().iandNot(x);
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
            for(int le = 0; le <= Util.toIntUnsigned(this.getLength(k)); ++le) {
              x[pos++] = (Util.toIntUnsigned(this.getValue(k)) + le) | mask;
            }
        }
    }

    @Override
    protected int getArraySizeInBytes() {
        return 2 + 4 * this.nbrruns;
    }

    @Override
    public int getCardinality() {
        /**
         * TODO: Daniel has a concern with this part of the
         * code. Lots of code may assume that we can query
         * the cardinality in constant-time. That is the case
         * with other containers. So it might be worth
         * the effort to have a pre-computed cardinality somewhere.
         * The only downsides are: (1) slight increase in memory
         * usage (probably negligible) (2) slower updates
         * (this container type is probably not the subject of
         * frequent updates).
         * 
         * On the other hand, storing a precomputed cardinality
         * separately is maybe wasteful and introduces extra
         * code. 
         * 
         * Current verdict: keep things as they are, but be
         * aware that  getCardinality might become a bottleneck.
         * 
         * 
         */
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
        else
          return not( rangeStart, rangeEnd);  // TODO: inplace option?
    }

    @Override
    public Container ior(ArrayContainer x) {
        return or(x);
    }

    @Override
    public Container ior(BitmapContainer x) {
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


    // handles any required fusion, assumes space available
    private int addRun(int outputRlePos, int runStart, int runLength) {
        // check whether fusion is required
        if (outputRlePos > 0) { // there is a previous run
            int prevRunStart = Util.toIntUnsigned(this.getValue(outputRlePos-1));
            int prevRunEnd = prevRunStart + Util.toIntUnsigned(this.getLength(outputRlePos-1));
            if (prevRunEnd+1 == runStart) { // we must fuse
                int newRunEnd = prevRunEnd+(1+runLength);
                int newRunLen = newRunEnd-prevRunStart;
                setLength(outputRlePos-1, (short) newRunLen);
                return outputRlePos; // do not advance, nbrruns unchanged
            }
        }
        // cases without fusion
        setValue(outputRlePos, (short) runStart);
        setLength(outputRlePos, (short) runLength);
        nbrruns=outputRlePos;

        return  ++outputRlePos;
    }


    @Override
    public Container not(int rangeStart, int rangeEnd) {

        // This code will be a pain to test...

        if (rangeEnd <= rangeStart) return this.clone();
 
        // A container that is best stored as a run container
        // is frequently going to have its "inot" also best stored
        // as a run container. This would violate an implicit
        // "results are array or bitmaps only" rule, if we had one.
        
        // The number of runs in the result can be bounded
        // not clear, but guessing the bound is a max increase of 1
        RunContainer ans = new RunContainer(nbrruns+1);

        // annoying special case: there is no run.  Then the range becomes the run
        if (nbrruns==0) {
            ans.addRun(0, rangeStart, rangeEnd-1);
            return ans;
        }
        
        int outputRlepos = 0;
        int rlepos;
        // copy all runs before the range.
        for (rlepos=0; rlepos < nbrruns; ++rlepos) {
            int runStart = Util.toIntUnsigned(this.getValue(rlepos));
            int runEnd = runStart + Util.toIntUnsigned(this.getLength(rlepos));

            if (runEnd >=  rangeStart) break;
            outputRlepos = ans.addRun(outputRlepos, runStart, runEnd);
        }

        if (rlepos < nbrruns) {
            // there may be a run that starts before the range but
            //  intersects with the range; copy the part before the intersection.
            
            int runStart = Util.toIntUnsigned(this.getValue(rlepos));
            if (runStart < rangeStart) {
                outputRlepos = ans.addRun(outputRlepos, runStart, rangeStart-1);
                ++rlepos; 
            }
        }
        

        // any further runs start after the range has begun
        for (; rlepos < nbrruns; ++rlepos) {
            int runStart = Util.toIntUnsigned(this.getValue(rlepos));
            int runEnd = runStart + Util.toIntUnsigned(this.getLength(rlepos));

            if (runStart >= rangeEnd) break; // handle these next
            
            int endOfPriorRun;
            if (rlepos == 0)
                endOfPriorRun=-1;
            else
                endOfPriorRun = Util.toIntUnsigned(this.getValue(rlepos)) + Util.toIntUnsigned(this.getLength(rlepos));

            // but only gap locations after the start of the range count.
            int startOfInterRunGap = Math.max(endOfPriorRun+1, rangeStart);

            int lastOfInterRunGap = Math.min(runStart-1, rangeEnd-1);            
            // and only gap locations before (strictly) the rangeEnd count

            outputRlepos = ans.addRun(outputRlepos, startOfInterRunGap, lastOfInterRunGap);
        }

        // any more runs are totally after the range, copy them
        for (; rlepos < nbrruns; ++rlepos) {
            int runStart = Util.toIntUnsigned(this.getValue(rlepos));
            int runEnd = runStart + Util.toIntUnsigned(this.getLength(rlepos));

            outputRlepos = ans.addRun(outputRlepos, runStart, runEnd);
        }

        // if the last run ends before the range, special processing needed.
        int lastRunEnd =   Util.toIntUnsigned(this.getValue(nbrruns-1)) + 
            Util.toIntUnsigned(this.getLength(nbrruns-1));

        if (lastRunEnd < rangeEnd-1) {
            int startOfFlippedRun = Math.max(rangeStart, lastRunEnd+1);
            outputRlepos = ans.addRun(outputRlepos, startOfFlippedRun, rangeEnd-1);
        }
        return ans;
        // could do a size check here and convert to
        // array or bitmap (implying it was probably silly
        // for the original container to be a Runcontainer..)
    }

    @Override
    public Container or(ArrayContainer x) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Container or(BitmapContainer x) {
        BitmapContainer answer = x.clone();
        for(int rlepos = 0; rlepos < this.nbrruns; ++rlepos ) {
            int start = Util.toIntUnsigned(this.getValue(rlepos));
            int end = Util.toIntUnsigned(this.getValue(rlepos)) + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
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
        return 2 + 2 * nbrruns;
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
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Container xor(BitmapContainer x) {
        BitmapContainer answer = x.clone();
        for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
            int start = Util.toIntUnsigned(this.getValue(rlepos));
            int end = Util.toIntUnsigned(this.getValue(rlepos)) + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
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
        RunContainer rc = new RunContainer(r, Arrays.copyOf(valueslength, 2*r));
        rc.setLength(r - 1, (short) (Util.toIntUnsigned(rc.getLength(r - 1)) - cardinality + maxcardinality));
        return rc;
    }

    @Override
    public Container iadd(int begin, int end) {
        if(begin >= end) {
            throw new IllegalArgumentException("Invalid range [" + begin + "," + end + "]");
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
        if(begin >= end) {
            throw new IllegalArgumentException("Invalid range [" + begin + "," + end + "]");
        }

        if(begin == end-1) {
            remove((short) begin);
            return this;
        }

        int bIndex = unsignedInterleavedBinarySearch(this.valueslength, 0, this.nbrruns, (short) begin);
        int eIndex = unsignedInterleavedBinarySearch(this.valueslength, 0, this.nbrruns, (short) (end-1));

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
                    if(valueLengthContains(end-1, eIndex)) {
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


    protected static int unsignedInterleavedBinarySearch(final short[] array,
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

    @Override
    public Container and(RunContainer x) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Container andNot(RunContainer x) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Container iand(RunContainer x) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Container iandNot(RunContainer x) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Container ior(RunContainer x) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Container ixor(RunContainer x) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Container or(RunContainer x) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Container xor(RunContainer x) {
        // TODO Auto-generated method stub
        return null;
    }

}


final class RunContainerShortIterator implements ShortIterator {
    int pos;
    int le = 0;

    RunContainer parent;

    RunContainerShortIterator(RunContainer p) {
        wrap(p);
    }
    
    void wrap(RunContainer p) {
        parent = p;
        pos = 0;
        le = 0;
    }

    @Override
    public boolean hasNext() {
        return (pos < parent.nbrruns) && (le <= Util.toIntUnsigned(parent.getLength(pos)));
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
        short ans = (short) (parent.getValue(pos) + le);
        le++;
        if(le > Util.toIntUnsigned(parent.getLength(pos))) {
            pos++;
            le = 0;
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

    ReverseRunContainerShortIterator(RunContainer p) {
        wrap(p);
    }
    
    void wrap(RunContainer p) {
        parent = p;
        pos = parent.nbrruns - 1;
        le = 0;
    }

    @Override
    public boolean hasNext() {
        return (pos >= 0) && (le <= Util.toIntUnsigned(parent.getLength(pos)));
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
        short ans = (short) (parent.getValue(pos) + Util.toIntUnsigned(parent.getLength(pos)) - le);
        le++;
        if(le > Util.toIntUnsigned(parent.getLength(pos))) {
            pos--;
            le = 0;
        }
        return ans;
    }

    @Override
    public void remove() {
        throw new RuntimeException("Not implemented");// TODO
    }

}

