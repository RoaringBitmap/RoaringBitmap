/*
 * (c) the authors
 * Licensed under the Apache License, Version 2.0.
 */
package org.roaringbitmap.buffer;


import org.roaringbitmap.ShortIterator;

import java.io.*;
import java.nio.ShortBuffer;
import java.util.Iterator;

/**
 * This container takes the form of runs of consecutive values (effectively,
 * run-length encoding).  Uses a ShortBuffer to store data, unlike
 * org.roaringbitmap.RunContainer.  Otherwise similar.
 */

//OFK: not sure why we need serializable
public class MappeableRunContainer extends MappeableContainer implements Cloneable, Serializable {
    private static final int DEFAULT_INIT_SIZE = 4;
    private ShortBuffer valueslength;

    protected int nbrruns = 0;// how many runs, this number should fit in 16 bits.

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
        if (array.limit() != 2*numRuns)
            throw new RuntimeException(
                    "Mismatch between buffer and numRuns");
        this.nbrruns = numRuns;
        this.valueslength = array;
    }



    
    // needed for deserialization
    public MappeableRunContainer(ShortBuffer valueslength) {
        this(valueslength.limit()/2, valueslength);
    }

    public MappeableRunContainer( ShortIterator sIt, int nbrRuns) {
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
    
    /**
     * Convert the container to either a Bitmap or an Array Container, depending
     * on the cardinality.
     * @return new container
     */
    protected MappeableContainer toBitmapOrArrayContainer() {
    	int card = this.getCardinality();
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
            int end = BufferUtil.toIntUnsigned(this.getValue(rlepos)) + BufferUtil.toIntUnsigned(this.getLength(rlepos)) + 1;
            BufferUtil.setBitmapRange(answer.bitmap, start, end); 
        }
        answer.cardinality = card;
        return answer;
    }
    
    // force conversion to bitmap irrespective of cardinality, result is not a valid container
    // this is potentially unsafe, use at your own risks
    protected MappeableBitmapContainer toTemporaryBitmap() {
    	MappeableBitmapContainer answer = new MappeableBitmapContainer();
        for (int rlepos=0; rlepos < this.nbrruns; ++rlepos) {
            int start = BufferUtil.toIntUnsigned(this.getValue(rlepos));
            int end = BufferUtil.toIntUnsigned(this.getValue(rlepos)) + BufferUtil.toIntUnsigned(this.getLength(rlepos)) + 1;
            BufferUtil.setBitmapRange(answer.bitmap, start, end); 
        }
        return answer;
    }


    /** 
     *  Convert to Array or Bitmap container if the serialized form would be shorter
     */

     @Override
     public MappeableContainer runOptimize() {
         int currentSize = getArraySizeInBytes(); //serializedSizeInBytes();
         int card = getCardinality(); 
         if (card <= MappeableArrayContainer.DEFAULT_MAX_SIZE) {
             if (currentSize > MappeableArrayContainer.getArraySizeInBytes(card))  
                 return toBitmapOrArrayContainer();
         }
         else if (currentSize > MappeableBitmapContainer.getArraySizeInBytes(card)) {  
             return toBitmapOrArrayContainer();
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
        out.write(this.nbrruns & 0xFF);
        out.write((this.nbrruns >>> 8) & 0xFF);
        // little endian
        for (int k = 0; k < 2*this.nbrruns; ++k) {
            out.write(this.valueslength.get(k) & 0xFF);
            out.write((this.valueslength.get(k) >>> 8) & 0xFF);
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
        /*  could be code for the simple array 
        int index = unsignedInterleavedBinarySearch(valueslength, 0, nbrruns, k);
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
       */
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
    

    // TODO: kept temporarily for perf. testing
    public MappeableContainer andold(MappeableBitmapContainer x) {
        MappeableBitmapContainer answer = x.clone();
        /*        
        int start = 0;
        for(int rlepos = 0; rlepos < this.nbrruns; ++rlepos ) {
            int end = BufferUtil.toIntUnsigned(this.getValue(rlepos));
            Util.resetBitmapRange(answer.bitmap, start, end);  // had been x.bitmap
            start = BufferUtil.toIntUnsigned(this.getValue(rlepos)) + BufferUtil.toIntUnsigned(this.getLength(rlepos)) + 1;
        }
        Util.resetBitmapRange(answer.bitmap, start, Util.maxLowBitAsInteger() + 1);   // had been x.bitmap
        answer.computeCardinality();
        if(answer.getCardinality() > ArrayContainer.DEFAULT_MAX_SIZE)
            return answer;
        else return answer.toArrayContainer();
        */
        int start = 0;
        for(int rlepos = 0; rlepos < this.nbrruns; ++rlepos ) {
            int end = BufferUtil.toIntUnsigned(this.getValue(rlepos));
            BufferUtil.resetBitmapRange(answer.bitmap, start, end); 
            start = BufferUtil.toIntUnsigned(this.getValue(rlepos)) + BufferUtil.toIntUnsigned(this.getLength(rlepos)) + 1;
        }
        BufferUtil.resetBitmapRange(answer.bitmap, start, BufferUtil.maxLowBitAsInteger() + 1); 
        answer.computeCardinality();
        if(answer.getCardinality() > MappeableArrayContainer.DEFAULT_MAX_SIZE)
            return answer;
        else return answer.toArrayContainer();

    }

    @Override
    public MappeableContainer and(MappeableBitmapContainer x) {
        /*
    	int card = this.getCardinality();
        if (card <=  ArrayContainer.DEFAULT_MAX_SIZE) {
            // result can only be an array (assuming that we never make a RunContainer)
        	ArrayContainer answer = new ArrayContainer(card);
        	answer.cardinality=0;
            for (int rlepos=0; rlepos < this.nbrruns; ++rlepos) {
                int runStart = BufferUtil.toIntUnsigned(this.getValue(rlepos));
                int runEnd = runStart + BufferUtil.toIntUnsigned(this.getLength(rlepos));
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
            int end = BufferUtil.toIntUnsigned(this.getValue(rlepos));
            Util.resetBitmapRange(answer.bitmap, start, end);  // had been x.bitmap
            start = BufferUtil.toIntUnsigned(this.getValue(rlepos)) + BufferUtil.toIntUnsigned(this.getLength(rlepos)) + 1;
        }
        Util.resetBitmapRange(answer.bitmap, start, Util.maxLowBitAsInteger() + 1);   // had been x.bitmap
        answer.computeCardinality();
        if(answer.getCardinality() > ArrayContainer.DEFAULT_MAX_SIZE)
            return answer;
        else return answer.toArrayContainer();
        */

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
            start = BufferUtil.toIntUnsigned(this.getValue(rlepos)) + BufferUtil.toIntUnsigned(this.getLength(rlepos)) + 1;
        }
        BufferUtil.resetBitmapRange(answer.bitmap, start, BufferUtil.maxLowBitAsInteger() + 1); 
        answer.computeCardinality();
        if(answer.getCardinality() > MappeableArrayContainer.DEFAULT_MAX_SIZE)
            return answer;
        else return answer.toArrayContainer();

    }


    // TODO: kept temporarily for perf. testing
    public MappeableContainer andNotold(MappeableBitmapContainer x) {
        /*
        BitmapContainer answer = x.clone();
        int lastPos = 0;
        for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
            int start = BufferUtil.toIntUnsigned(this.getValue(rlepos));
            int end = BufferUtil.toIntUnsigned(this.getValue(rlepos)) + BufferUtil.toIntUnsigned(this.getLength(rlepos)) + 1;
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
        */

        MappeableBitmapContainer answer = x.clone();
        int lastPos = 0;
        for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
            int start = BufferUtil.toIntUnsigned(this.getValue(rlepos));
            int end = BufferUtil.toIntUnsigned(this.getValue(rlepos)) + BufferUtil.toIntUnsigned(this.getLength(rlepos)) + 1;
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
    public MappeableContainer andNot(MappeableBitmapContainer x) {
        /*
    	int card = this.getCardinality();
        if (card <=  ArrayContainer.DEFAULT_MAX_SIZE) {
            // result can only be an array (assuming that we never make a RunContainer)
        	ArrayContainer answer = new ArrayContainer(card);
        	answer.cardinality=0;
            for (int rlepos=0; rlepos < this.nbrruns; ++rlepos) {
                int runStart = BufferUtil.toIntUnsigned(this.getValue(rlepos));
                int runEnd = runStart + BufferUtil.toIntUnsigned(this.getLength(rlepos));
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
            int start = BufferUtil.toIntUnsigned(this.getValue(rlepos));
            int end = BufferUtil.toIntUnsigned(this.getValue(rlepos)) + BufferUtil.toIntUnsigned(this.getLength(rlepos)) + 1;
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
        */
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
            int end = BufferUtil.toIntUnsigned(this.getValue(rlepos)) + BufferUtil.toIntUnsigned(this.getLength(rlepos)) + 1;
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
    	return toBitmapOrArrayContainer().iandNot(x);
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

    /*
    @Override
    public void deserialize(DataInput in) throws IOException {
        nbrruns = Short.reverseBytes(in.readShort());
        if(valueslength.length < 2 * nbrruns)
            valueslength = new short[2 * nbrruns];
        for (int k = 0; k < 2 * nbrruns; ++k) {
            this.valueslength[k] = Short.reverseBytes(in.readShort());
        }
    }
    */

    @Override
    public void fillLeastSignificant16bits(int[] x, int i, int mask) {
        int pos = i;
        for (int k = 0; k < this.nbrruns; ++k) {
            for(int le = 0; le <= BufferUtil.toIntUnsigned(this.getLength(k)); ++le) {
              x[pos++] = (BufferUtil.toIntUnsigned(this.getValue(k)) + le) | mask;
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
            sum = sum + BufferUtil.toIntUnsigned(getLength(k)) + 1;
        return sum;
    }

    @Override
    public ShortIterator getShortIterator() {
        return new MappeableRunContainerShortIterator(this);
    }

    @Override
    public ShortIterator getReverseShortIterator() {
        return new ReverseMappeableRunContainerShortIterator(this);
    }

    @Override
    public int getSizeInBytes() {
        return this.nbrruns*4+4;  // not sure about how exact it will be
     // valueslength.limit()*4 + 4;  //this.nbrruns * 4 + 4;
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
        return or(x);
    }

    @Override
    public MappeableContainer ior(MappeableBitmapContainer x) {
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


    // handles any required fusion, assumes space available
    private int addRun(int outputRlePos, int runStart, int lastRunElement) {
        int runLength = lastRunElement - runStart;
        // check whether fusion is required
        if (outputRlePos > 0) { // there is a previous run
            int prevRunStart = BufferUtil.toIntUnsigned(this.getValue(outputRlePos-1));
            int prevRunEnd = prevRunStart + BufferUtil.toIntUnsigned(this.getLength(outputRlePos-1));
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
        nbrruns=outputRlePos+1;

        return  ++outputRlePos;
    }


    @Override
    public MappeableContainer not(int rangeStart, int rangeEnd) {

        if (rangeEnd <= rangeStart) return this.clone();
 
        // A container that is best stored as a run container
        // is frequently going to have its "inot" also best stored
        // as a run container. This would violate an implicit
        // "results are array or bitmaps only" rule, if we had one.
        
        MappeableRunContainer ans = new MappeableRunContainer(nbrruns+1);

        // annoying special case: there is no run.  Then the range becomes the run.
        if (nbrruns==0) {
            ans.addRun(0, rangeStart, rangeEnd-1);
            return ans;
        }
        
        int outputRlepos = 0;
        int rlepos;
        // copy all runs before the range.
        for (rlepos=0; rlepos < nbrruns; ++rlepos) {
            int runStart = BufferUtil.toIntUnsigned(this.getValue(rlepos));
            int runEnd = runStart + BufferUtil.toIntUnsigned(this.getLength(rlepos));

            if (runEnd >=  rangeStart) break;
            outputRlepos = ans.addRun(outputRlepos, runStart, runEnd);
        }

        if (rlepos < nbrruns) {
            // there may be a run that starts before the range but
            //  intersects with the range; copy the part before the intersection.
            
            int runStart = BufferUtil.toIntUnsigned(this.getValue(rlepos));
            if (runStart < rangeStart) {
                outputRlepos = ans.addRun(outputRlepos, runStart, rangeStart-1);
                // do not increase rlepos, as the rest of this run needs to be handled
            }
        }
        
        
        for (; rlepos < nbrruns; ++rlepos) {
            int runStart = BufferUtil.toIntUnsigned(this.getValue(rlepos));
            int runEnd = runStart + BufferUtil.toIntUnsigned(this.getLength(rlepos));

            if (runStart >= rangeEnd) break; // handle these next
            
            int endOfPriorRun;
            if (rlepos == 0)
                endOfPriorRun=-1;
            else
                endOfPriorRun = BufferUtil.toIntUnsigned(this.getValue(rlepos-1)) + BufferUtil.toIntUnsigned(this.getLength(rlepos-1));

            // but only gap locations after the start of the range count.
            int startOfInterRunGap = Math.max(endOfPriorRun+1, rangeStart);

            int lastOfInterRunGap = Math.min(runStart-1, rangeEnd-1);            
            // and only gap locations before (strictly) the rangeEnd count

            if (lastOfInterRunGap >= startOfInterRunGap)
                outputRlepos = ans.addRun(outputRlepos, startOfInterRunGap, lastOfInterRunGap);
            // else we had a run that started before the range, and thus no gap
      

            // there can be a run that begins before the end of the range but ends afterward.
            // the portion that extends beyond the range needs to be copied.
            if (runEnd >= rangeEnd) // recall: runEnd is inclusive, rangeEnd is exclusive
                outputRlepos = ans.addRun(outputRlepos, rangeEnd, runEnd);
        }

        // if the kth run is entirely within the range and the k+1st entirely outside,
        // then we need to pick up the gap between the end of the kth run and the range's end
        if (rlepos > 0) {
            int endOfPriorRun = BufferUtil.toIntUnsigned(this.getValue(rlepos-1)) + BufferUtil.toIntUnsigned(this.getLength(rlepos-1));
            if (rlepos < nbrruns) {
               int  runStart= BufferUtil.toIntUnsigned(this.getValue(rlepos));
               if (endOfPriorRun >= rangeStart &&
                   endOfPriorRun < rangeEnd-1 && // there is a nonempty gap
                   runStart >= rangeEnd)
                   outputRlepos = ans.addRun(outputRlepos, endOfPriorRun+1, rangeEnd-1);
            }
            // else is handled by special processing for "last run ends before the range"
        }


        // handle case where range occurs before first run
        if (rlepos == 0)  {
            outputRlepos = ans.addRun(outputRlepos, rangeStart, rangeEnd-1);
        }


        // any more runs are totally after the range, copy them
        for (; rlepos < nbrruns; ++rlepos) {
            int runStart = BufferUtil.toIntUnsigned(this.getValue(rlepos));
            int runEnd = runStart + BufferUtil.toIntUnsigned(this.getLength(rlepos));

            outputRlepos = ans.addRun(outputRlepos, runStart, runEnd);
        }

        // if the last run ends before the range, special processing needed.
        int lastRunEnd =   BufferUtil.toIntUnsigned(this.getValue(nbrruns-1)) + 
            BufferUtil.toIntUnsigned(this.getLength(nbrruns-1));

        if (lastRunEnd < rangeEnd-1) {
            int startOfFlippedRun = Math.max(rangeStart, lastRunEnd+1);
            outputRlepos = ans.addRun(outputRlepos, startOfFlippedRun, rangeEnd-1);
        }
        return ans;
    }

    @Override
    public MappeableContainer or(MappeableArrayContainer x) {
        return x.or(getShortIterator());   // performance may not be great, depending on iterator overheads...
    }

    @Override
    public MappeableContainer or(MappeableBitmapContainer x) {
        MappeableBitmapContainer answer = x.clone();
        for(int rlepos = 0; rlepos < this.nbrruns; ++rlepos ) {
            int start = BufferUtil.toIntUnsigned(this.getValue(rlepos));
            int end = BufferUtil.toIntUnsigned(this.getValue(rlepos)) + BufferUtil.toIntUnsigned(this.getLength(rlepos)) + 1;
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
            } else if(offset == le) {
                decrementLength(index);
            }
        }
        // no match
        return this;
    }

    /*
    @Override
    public void serialize(DataOutput out) throws IOException {
        writeArray(out);
    }
    */

    /*
    @Override
    public int serializedSizeInBytes() {
        return serializedSizeInBytes(nbrruns);
    }

    public static int serializedSizeInBytes( int numberOfRuns) {
        return 2 + 2 * 2 * numberOfRuns;  // each run requires 2 2-byte entries.
    }
    */

    /*
    @Override
    public void trim() {
        if(valueslength.length == 2 * nbrruns) return;
        valueslength = Arrays.copyOf(valueslength, 2 * nbrruns);
    }
    */

 @Override
    public void trim() {
        // could we do nothing if size is already ok?
        final ShortBuffer co = ShortBuffer.allocate(2*nbrruns);
        for (int k = 0; k < 2*nbrruns; ++k)
            co.put(this.valueslength.get(k));
        this.valueslength = co;
    }


    @Override
    protected void writeArray(DataOutput out) throws IOException {
        out.writeShort(Short.reverseBytes((short) this.nbrruns));
        //System.out.println("MRC: I wrote a short and will now write "+(2*nbrruns)+" more");
        for (int k = 0; k < 2 * this.nbrruns; ++k) {
            out.writeShort(Short.reverseBytes(this.valueslength.get(k)));
        }
    }

    @Override
    public MappeableContainer xor(MappeableArrayContainer x) {
        return x.xor(getShortIterator());   
    }

    @Override
    public MappeableContainer xor(MappeableBitmapContainer x) {
        MappeableBitmapContainer answer = x.clone();
        for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
            int start = BufferUtil.toIntUnsigned(this.getValue(rlepos));
            int end = BufferUtil.toIntUnsigned(this.getValue(rlepos)) + BufferUtil.toIntUnsigned(this.getLength(rlepos)) + 1;
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
        
        //MappeableRunContainer rc = new MappeableRunContainer(r, Arrays.copyOf(valueslength, 2*r));
        ShortBuffer newBuf = ShortBuffer.allocate(2*maxcardinality);
        valueslength.rewind();
        for (int i=0; i < 2*r; ++i)
            newBuf.put( valueslength.get());
        MappeableRunContainer rc = new MappeableRunContainer(r, newBuf);

        rc.setLength(r - 1, (short) (BufferUtil.toIntUnsigned(rc.getLength(r - 1)) - cardinality + maxcardinality));
        return rc;
    }

    @Override
    public MappeableContainer iadd(int begin, int end) {
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

/*
    protected static int unsignedInterleavedBinarySearch(final short[] array,
            final int begin, final int end, final short k) {
        int ikey = BufferUtil.toIntUnsigned(k);
        int low = begin;
        int high = end - 1;
        while (low <= high) {
            final int middleIndex = (low + high) >>> 1;
            final int middleValue = BufferUtil.toIntUnsigned(array[2 * middleIndex]);
            if (middleValue < ikey)
                low = middleIndex + 1;
            else if (middleValue > ikey)
                high = middleIndex - 1;
            else
                return middleIndex;
        }
        return -(low + 1);
    }
*/

    protected static int bufferedUnsignedInterleavedBinarySearch(final ShortBuffer sb,
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
        // if (BufferUtil.isBackedBySimpleArray(valueslength)) {
        //     short [] sarray = valueslength.array();
        //     if (2 * (nbrruns+1) > sarray.length) {
        //         increaseCapacity();
        //         sarray = valueslength.array();
        //     }
        //     copyValuesLength(sarray, index, sarray, index + 1, nbrruns - index);
        // }
        // else {
            if (2*(nbrruns+1) > valueslength.capacity())
                increaseCapacity();
            copyValuesLength(valueslength, index, valueslength, index + 1, nbrruns - index);
        // }
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

    /*
    private void copyValuesLength(short[] src, int srcIndex, short[] dst, int dstIndex, int length) {
        System.arraycopy(src, 2*srcIndex, dst, 2*dstIndex, 2*length);
    }
    */


    private void copyValuesLength(ShortBuffer src, int srcIndex, ShortBuffer dst, int dstIndex, int length) {
        // source and destination may overlap
        //System.arraycopy(src, 2*srcIndex, dst, 2*dstIndex, 2*length);
        // consider specialized code for various cases, rather than using a second buffer

        ShortBuffer temp = ShortBuffer.allocate(2*length);
        for (int i=0; i < 2*length; ++i)
            temp.put( src.get(2*srcIndex + i));
        temp.flip();
        for (int i=0; i < 2*length; ++i)
            dst.put( 2*dstIndex+i, temp.get());
    }



    // used for estimates of whether to prefer Array or Bitmap container results
    // when combining two RunContainers

    private double getDensity() {
        int myCard = getCardinality();
        return ((double) myCard) / (1 << 16);
    }


    private final double ARRAY_MAX_DENSITY  = ( (double) MappeableArrayContainer.DEFAULT_MAX_SIZE)  / (1<<16);
    
    private static final int OP_AND=0, OP_ANDNOT=1, OP_OR=2, OP_XOR=3;

    // borrowed this tuned-looking code from ArrayContainer.
    // except: DEFAULT_INIT_SIZE is private...borrowed current setting
    // OFK: compare to similar increaseCapacity method elsewhere...
    private ShortBuffer increaseCapacity(ShortBuffer content) {
        int newCapacity = (content.capacity() == 0) ? 4 : content.capacity() < 64 ? content.capacity() * 2
            : content.capacity() < 1024 ? content.capacity() * 3 / 2
            : content.capacity() * 5 / 4;
        // allow it to exceed DEFAULT_MAX_SIZE
        ShortBuffer ans = ShortBuffer.allocate(newCapacity);
        content.rewind();
        ans.put(content);
        return ans;
        //return Arrays.copyOf(content, newCapacity);
    }

    // generic merge algorithm, Array output.  Should be possible to
    // improve on it for AND and ANDNOT, at least.

    private MappeableContainer operationArrayGuess(MappeableRunContainer x, int opcode) {
        /*
        short [] ansArray = new short[10]; 
        int card = 0;
        int thisHead, xHead; // -1 means end of run
        
        // hoping that iterator overhead can be largely optimized away, dunno...

        ShortIterator it = getShortIterator();  // rely on unsigned ordering
        ShortIterator xIt = x.getShortIterator();
        
        thisHead = (it.hasNext() ?  BufferUtil.toIntUnsigned(it.next()) : -1);
        xHead =  (xIt.hasNext() ?  BufferUtil.toIntUnsigned(xIt.next()) : -1);

        while (thisHead != -1 && xHead != -1) {

            if (thisHead > xHead) {
                // item present in x only: we want for OR and XOR only
                if (opcode == OP_OR|| opcode == OP_XOR) {
                    // emit item to array
                    if (card == ansArray.length) ansArray = increaseCapacity(ansArray);
                    ansArray[card++] = (short) xHead;
                }
                xHead =  (xIt.hasNext() ?  BufferUtil.toIntUnsigned(xIt.next()) : -1);
            }
            else if (thisHead < xHead) {
                // item present in this only.  We want for OR, XOR plus ANDNOT  (all except AND)
                if (opcode != OP_AND) {
                    // emit to array
                    if (card == ansArray.length) ansArray = increaseCapacity(ansArray);
                    ansArray[card++] = (short) thisHead;
                }
                thisHead = (it.hasNext() ?  BufferUtil.toIntUnsigned(it.next()) : -1);
            }
            else { // item is present in both x and this;   AND and OR should get it, but not XOR or ANDNOT
                if (opcode == OP_AND || opcode == OP_OR) {
                    // emit to array
                    if (card == ansArray.length) ansArray = increaseCapacity(ansArray);
                    ansArray[card++] = (short) thisHead;
                }
                thisHead = (it.hasNext() ?  BufferUtil.toIntUnsigned(it.next()) : -1);
                xHead =  (xIt.hasNext() ?  BufferUtil.toIntUnsigned(xIt.next()) : -1);
            }
        }

        // AND does not care if there are extra items in either 
        if (opcode != OP_AND) {
            
            // OR, ANDNOT, XOR all want extra items in this sequence
            while (thisHead != -1) {
                // emit to array
                if (card == ansArray.length) ansArray = increaseCapacity(ansArray);
                ansArray[card++] = (short) thisHead;
                thisHead = (it.hasNext() ?  BufferUtil.toIntUnsigned(it.next()) : -1);
            }

            // OR and XOR want extra items in x sequence
            if (opcode == OP_OR || opcode == OP_XOR)
                while (xHead != -1) {
                    // emit to array
                    if (card == ansArray.length) ansArray = increaseCapacity(ansArray);
                    ansArray[card++] = (short) xHead;
                    xHead =  (xIt.hasNext() ?  BufferUtil.toIntUnsigned(xIt.next()) : -1);
                } 
        }

        // double copy could be avoided if the private card-is-parameter constructor for ArrayContainer were protected rather than private.
        short [] content = Arrays.copyOf(ansArray, card);
        ArrayContainer ac = new ArrayContainer(content);
        if (card > ArrayContainer.DEFAULT_MAX_SIZE)
            return ac.toBitmapContainer();
        else
            return ac;
        */
        ShortBuffer ansArray = ShortBuffer.allocate(10); 
        int card = 0;
        int thisHead, xHead; // -1 means end of run
        
        ShortIterator it = getShortIterator();  // rely on unsigned ordering
        ShortIterator xIt = x.getShortIterator();
        
        thisHead = (it.hasNext() ?  BufferUtil.toIntUnsigned(it.next()) : -1);
        xHead =  (xIt.hasNext() ?  BufferUtil.toIntUnsigned(xIt.next()) : -1);

        while (thisHead != -1 && xHead != -1) {

            if (thisHead > xHead) {
                // item present in x only: we want for OR and XOR only
                if (opcode == OP_OR|| opcode == OP_XOR) {
                    // emit item to array
                    if (card == ansArray.capacity()) ansArray = increaseCapacity(ansArray);
                    ansArray.put((short) xHead);
                    card++;
                }
                xHead =  (xIt.hasNext() ?  BufferUtil.toIntUnsigned(xIt.next()) : -1);
            }
            else if (thisHead < xHead) {
                // item present in this only.  We want for OR, XOR plus ANDNOT  (all except AND)
                if (opcode != OP_AND) {
                    // emit to array
                    if (card == ansArray.capacity()) ansArray = increaseCapacity(ansArray);
                    ansArray.put((short) thisHead);
                    card++;
                }
                thisHead = (it.hasNext() ?  BufferUtil.toIntUnsigned(it.next()) : -1);
            }
            else { // item is present in both x and this;   AND and OR should get it, but not XOR or ANDNOT
                if (opcode == OP_AND || opcode == OP_OR) {
                    // emit to array
                    if (card == ansArray.capacity()) ansArray = increaseCapacity(ansArray);
                    ansArray.put((short) thisHead);
                    card++;
                }
                thisHead = (it.hasNext() ?  BufferUtil.toIntUnsigned(it.next()) : -1);
                xHead =  (xIt.hasNext() ?  BufferUtil.toIntUnsigned(xIt.next()) : -1);
            }
        }

        // AND does not care if there are extra items in either 
        if (opcode != OP_AND) {
            
            // OR, ANDNOT, XOR all want extra items in this sequence
            while (thisHead != -1) {
                // emit to array
                if (card == ansArray.capacity()) ansArray = increaseCapacity(ansArray);
                ansArray.put((short) thisHead);
                card++;
                thisHead = (it.hasNext() ?  BufferUtil.toIntUnsigned(it.next()) : -1);
            }

            // OR and XOR want extra items in x sequence
            if (opcode == OP_OR || opcode == OP_XOR)
                while (xHead != -1) {
                    // emit to array
                    if (card == ansArray.capacity()) ansArray = increaseCapacity(ansArray);
                    ansArray.put((short) xHead);
                    card++;
                    xHead =  (xIt.hasNext() ?  BufferUtil.toIntUnsigned(xIt.next()) : -1);
                } 
        }

        // we can end up with an empty result...allocate(0) may not be healthy??
        ShortBuffer content = ShortBuffer.allocate(card);
        ansArray.flip();
        
        content.put(ansArray);
        
        MappeableArrayContainer ac = new MappeableArrayContainer(content, card);
        if (card > MappeableArrayContainer.DEFAULT_MAX_SIZE)
            return ac.toBitmapContainer();
        else
            return ac;

    }


    // generic merge algorithm, copy-paste for bitmap output
    private MappeableContainer operationBitmapGuess(MappeableRunContainer x, int opcode) {
        /*
        BitmapContainer answer = new BitmapContainer();
        int thisHead, xHead; // -1 means end of run
        
        ShortIterator it = getShortIterator();  
        ShortIterator xIt = x.getShortIterator();
        
        thisHead = (it.hasNext() ?  BufferUtil.toIntUnsigned(it.next()) : -1);
        xHead =  (xIt.hasNext() ?  BufferUtil.toIntUnsigned(xIt.next()) : -1);

        while (thisHead != -1 && xHead != -1) {

            if (thisHead > xHead) {
                // item present in x only: we want for OR and XOR only
                if (opcode == OP_OR|| opcode == OP_XOR) 
                    answer.add((short) xHead);
                xHead =  (xIt.hasNext() ?  BufferUtil.toIntUnsigned(xIt.next()) : -1);
            }
            else if (thisHead < xHead) {
                // item present in this only.  We want for OR, XOR plus ANDNOT  (all except AND)
                if (opcode != OP_AND) 
                    answer.add((short) thisHead);
                thisHead = (it.hasNext() ?  BufferUtil.toIntUnsigned(it.next()) : -1);
            }
            else { // item is present in both x and this;   AND and OR should get it, but not XOR or ANDNOT
                if (opcode == OP_AND || opcode == OP_OR) 
                    answer.add((short) thisHead);
                thisHead = (it.hasNext() ?  BufferUtil.toIntUnsigned(it.next()) : -1);
                xHead =  (xIt.hasNext() ?  BufferUtil.toIntUnsigned(xIt.next()) : -1);
            }
        }

        // AND does not care if there are extra items in either 
        if (opcode != OP_AND) {
            
            // OR, ANDNOT, XOR all want extra items in this sequence
            while (thisHead != -1) {
                answer.add((short) thisHead);
                thisHead = (it.hasNext() ?  BufferUtil.toIntUnsigned(it.next()) : -1);
            }

            // OR and XOR want extra items in x sequence
            if (opcode == OP_OR || opcode == OP_XOR)
                while (xHead != -1) {
                    answer.add((short) xHead);
                    xHead =  (xIt.hasNext() ?  BufferUtil.toIntUnsigned(xIt.next()) : -1);
                } 
        }
        
        if (answer.getCardinality() <= ArrayContainer.DEFAULT_MAX_SIZE)
            return answer.toArrayContainer();
        else
            return answer;
        */

        MappeableBitmapContainer answer = new MappeableBitmapContainer();
        int thisHead, xHead; // -1 means end of run
        
        ShortIterator it = getShortIterator();  
        ShortIterator xIt = x.getShortIterator();
        
        thisHead = (it.hasNext() ?  BufferUtil.toIntUnsigned(it.next()) : -1);
        xHead =  (xIt.hasNext() ?  BufferUtil.toIntUnsigned(xIt.next()) : -1);

        while (thisHead != -1 && xHead != -1) {

            if (thisHead > xHead) {
                // item present in x only: we want for OR and XOR only
                if (opcode == OP_OR|| opcode == OP_XOR) 
                    answer.add((short) xHead);
                xHead =  (xIt.hasNext() ?  BufferUtil.toIntUnsigned(xIt.next()) : -1);
            }
            else if (thisHead < xHead) {
                // item present in this only.  We want for OR, XOR plus ANDNOT  (all except AND)
                if (opcode != OP_AND) 
                    answer.add((short) thisHead);
                thisHead = (it.hasNext() ?  BufferUtil.toIntUnsigned(it.next()) : -1);
            }
            else { // item is present in both x and this;   AND and OR should get it, but not XOR or ANDNOT
                if (opcode == OP_AND || opcode == OP_OR) 
                    answer.add((short) thisHead);
                thisHead = (it.hasNext() ?  BufferUtil.toIntUnsigned(it.next()) : -1);
                xHead =  (xIt.hasNext() ?  BufferUtil.toIntUnsigned(xIt.next()) : -1);
            }
        }

        // AND does not care if there are extra items in either 
        if (opcode != OP_AND) {
            
            // OR, ANDNOT, XOR all want extra items in this sequence
            while (thisHead != -1) {
                answer.add((short) thisHead);
                thisHead = (it.hasNext() ?  BufferUtil.toIntUnsigned(it.next()) : -1);
            }

            // OR and XOR want extra items in x sequence
            if (opcode == OP_OR || opcode == OP_XOR)
                while (xHead != -1) {
                    answer.add((short) xHead);
                    xHead =  (xIt.hasNext() ?  BufferUtil.toIntUnsigned(xIt.next()) : -1);
                } 
        }
        
        if (answer.getCardinality() <= MappeableArrayContainer.DEFAULT_MAX_SIZE)
            return answer.toArrayContainer();
        else
            return answer;
      
 }



    @Override
    public MappeableContainer and(MappeableRunContainer x) {
        double myDensity = getDensity();
        double xDensity = x.getDensity();
        double resultDensityEstimate = myDensity*xDensity;
        return (resultDensityEstimate < ARRAY_MAX_DENSITY ? operationArrayGuess(x, OP_AND) : operationBitmapGuess(x, OP_AND));
    }


    @Override
    public MappeableContainer andNot(MappeableRunContainer x) {
        double myDensity = getDensity();
        double xDensity = x.getDensity();
        double resultDensityEstimate = myDensity*(1-xDensity);
        return (resultDensityEstimate < ARRAY_MAX_DENSITY ? operationArrayGuess(x, OP_ANDNOT) : operationBitmapGuess(x, OP_ANDNOT));
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
        return or(x);
    }

    @Override
    public MappeableContainer ixor(MappeableRunContainer x) {
        return xor(x);
    }

    @Override
    public MappeableContainer or(MappeableRunContainer x) {
        double myDensity = getDensity();
        double xDensity = x.getDensity();
        double resultDensityEstimate = 1- (1-myDensity)*(1-xDensity);
        return (resultDensityEstimate < ARRAY_MAX_DENSITY ? operationArrayGuess(x, OP_OR) : operationBitmapGuess(x, OP_OR));
    }

    @Override
    public MappeableContainer xor(MappeableRunContainer x) {
        double myDensity = getDensity();
        double xDensity = x.getDensity();
        double resultDensityEstimate = 1- (1-myDensity)*(1-xDensity)  - myDensity*xDensity;  // I guess
        return (resultDensityEstimate < ARRAY_MAX_DENSITY ? operationArrayGuess(x, OP_XOR) : operationBitmapGuess(x, OP_XOR));
    }

}


final class MappeableRunContainerShortIterator implements ShortIterator {
    int pos;
    int le = 0;

    MappeableRunContainer parent;

    MappeableRunContainerShortIterator() {}

    MappeableRunContainerShortIterator(MappeableRunContainer p) {
        wrap(p);
    }
    
    void wrap(MappeableRunContainer p) {
        parent = p;
        pos = 0;
        le = 0;
    }

    @Override
    public boolean hasNext() {
        return (pos < parent.nbrruns) && (le <= BufferUtil.toIntUnsigned(parent.getLength(pos)));
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
        if(le > BufferUtil.toIntUnsigned(parent.getLength(pos))) {
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

final class ReverseMappeableRunContainerShortIterator implements ShortIterator {
    int pos;
    int le;
    MappeableRunContainer parent;


    ReverseMappeableRunContainerShortIterator(){}

    ReverseMappeableRunContainerShortIterator(MappeableRunContainer p) {
        wrap(p);
    }
    
    void wrap(MappeableRunContainer p) {
        parent = p;
        pos = parent.nbrruns - 1;
        le = 0;
    }

    @Override
    public boolean hasNext() {
        return (pos >= 0) && (le <= BufferUtil.toIntUnsigned(parent.getLength(pos)));
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
        short ans = (short) (parent.getValue(pos) + BufferUtil.toIntUnsigned(parent.getLength(pos)) - le);
        le++;
        if(le > BufferUtil.toIntUnsigned(parent.getLength(pos))) {
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

