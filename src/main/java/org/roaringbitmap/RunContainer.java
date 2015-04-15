/*
 * (c) Daniel Lemire, Owen Kaser, Samy Chambi, Jon Alvarado, Rory Graves, Björn Sperber
 * Licensed under the Apache License, Version 2.0.
 */
package org.roaringbitmap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * This container takes the form of runs of consecutive values (effectively,
 * run-length encoding).
 */
public class RunContainer extends Container implements Cloneable, Serializable {
    private static final int DEFAULT_INIT_SIZE = 4;
    private short[] valueslength;// we interleave values and lengths, e.g., 1, 10 indicates the run 1, 2, ..., 11
    // Lengths are expressed in number of extra repetitions, so 0 means 1 value in the sequence. 
    int nbrruns = 0;

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

    private void setLength(int index, short v) {
        valueslength[2*index + 1] = v;
    }
    
    private void setValue(int index, short v) {
        valueslength[2*index] = v;
    }
    private void decrementLength(int index) {
        valueslength[2*index + 1]--;
    }

    private void decrementValue(int index) {
        valueslength[2*index]--;
    }    
    
    private void makeRoomAtIndex(int index) {
        if(2 * nbrruns == valueslength.length) increaseCapacity();
        System.arraycopy(valueslength, 2 * index  , valueslength, 2 * index + 2 , 2 * nbrruns - 2 * index );
        nbrruns++;
    }

    private void recoverRoomAtIndex(int index) {
        System.arraycopy(valueslength, 2 * index + 2 , valueslength, 2* index  , 2 *  nbrruns - 2 * index - 2 );
        nbrruns--;
    }
    
    private RunContainer(int nbrruns, short[] valueslength) {
        this.nbrruns = nbrruns;
        this.valueslength = Arrays.copyOf(valueslength, valueslength.length);
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
    public Container add(short k) {
        int index = unsignedInterleavedBinarySearch(valueslength, 0, nbrruns, k);
        if(index >= 0) return this;// already there
        index = - index - 2;// points to preceding value, possibly -1
        if((index >= 0) && (index < nbrruns)) {// possible match
            int offset = Util.toIntUnsigned(k) - Util.toIntUnsigned(getValue(index));
            int le =     Util.toIntUnsigned(getLength(index)); 
            if(offset <= le) return this;
            if(offset == le + 1) {
                // we may need to fuse
                if(index + 1 < nbrruns ) {
                    if(Util.toIntUnsigned(getValue(index + 1))  == Util.toIntUnsigned(k) + 1) {
                        // indeed fusion is needed
                        recoverRoomAtIndex(index + 1);
                        setLength(index, (short) (getValue(index + 1) + getLength(index + 1) - getValue(index)));
                        return this;
                    }
                }
                incrementLength(index);
                return this;
            }
        }
        if( index == -1) {
            // we may need to extend the first run
            if(0 < nbrruns ) {
                if(getValue(0)  == k + 1) {
                    incrementLength(0);
                    decrementValue(0);
                    return this;
                }
            }
        }
        makeRoomAtIndex(index + 1);
        setValue(index+1,(short)k);
        setLength(index+1,(short)0);
        return this;
    }

    @Override
    public Container and(ArrayContainer x) {
        ArrayContainer answer = new ArrayContainer(x.getCardinality());
        // we use a very simple brute-force algorithm TODO: see if you can get cleverer
        if(x.getCardinality() == 0) return answer;
        ShortIterator si = x.getShortIterator();
        int n = Util.toIntUnsigned(si.next());
        for (int k = 0; (k < nbrruns) && si.hasNext(); ++k) {
            int begin = getValue(k);
            int length = (int) getLength(k);
            while(n < begin) {
                if(! si.hasNext()) return answer;
                n = si.next();
            }
            while(n <= begin + length) {
                answer.content[answer.cardinality++] = (short) n;
                if(! si.hasNext()) return answer;
                n = si.next();                
            }
        }
        return answer;
    }
    

    @Override
    public Container and(BitmapContainer x) {
            // TODO program
        return null;
    }

    @Override
    public Container andNot(ArrayContainer x) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Container andNot(BitmapContainer x) {
        // TODO Auto-generated method stub
        return null;
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
            byte[] buffer = new byte[2];
            // little endian
            in.readFully(buffer);
            nbrruns = (buffer[0] & 0xFF) | ((buffer[1] & 0xFF) << 8);
            if(valueslength.length < 2 * nbrruns)
                valueslength = new short[2 * nbrruns];
            for (int k = 0; k < nbrruns; ++k) {
                in.readFully(buffer);
                this.valueslength[k] = (short) (((buffer[1] & 0xFF) << 8) | (buffer[0] & 0xFF));
            }
    }

    @Override
    public void fillLeastSignificant16bits(int[] x, int i, int mask) {
        int pos = 0;
        for (int k = 0; k < this.nbrruns; ++k) {
            for(int le = 0; le <= this.getLength(k); ++le) {
              x[k + pos] = (Util.toIntUnsigned(this.getValue(k)) + le) | mask;
              pos++;
            }
        }
    }

    @Override
    protected int getArraySizeInBytes() {
        return 2 + 4 * this.nbrruns;
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
    public Container iand(ArrayContainer x) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Container iand(BitmapContainer x) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Container iandNot(ArrayContainer x) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Container iandNot(BitmapContainer x) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Container inot(int rangeStart, int rangeEnd) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Container ior(ArrayContainer x) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Container ior(BitmapContainer x) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Container ixor(ArrayContainer x) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Container ixor(BitmapContainer x) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Container not(int rangeStart, int rangeEnd) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Container or(ArrayContainer x) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Container or(BitmapContainer x) {
        // TODO Auto-generated method stub
        return null;
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
        if((index >= 0) && (index < nbrruns)) {// possible match
            int offset = Util.toIntUnsigned(x) - Util.toIntUnsigned(getValue(index));
            int le =     Util.toIntUnsigned(getLength(index)); 
            if(offset < le) {
                // need to break in two
                int currentlength  = Util.toIntUnsigned(getLength(index));
                this.setLength(index, (short) (offset - 1));
                // need to insert
                int newvalue = Util.toIntUnsigned(x) + 1;
                int newlength = currentlength  - offset - 1;
                makeRoomAtIndex(index+1);
                this.setValue(index+1, (short) newvalue);
                this.setLength(index+1, (short) newlength);
            } else if(offset == le ) {
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
        out.write((this.nbrruns) & 0xFF);
        out.write((this.nbrruns >>> 8) & 0xFF);
        for (int k = 0; k < 2 * this.nbrruns; ++k) {
            out.write((this.valueslength[k]) & 0xFF);
            out.write((this.valueslength[k] >>> 8) & 0xFF);
        }
    }

    @Override
    public Container xor(ArrayContainer x) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Container xor(BitmapContainer x) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int rank(short lowbits) {
        int answer = 0;
        for (int k = 0; k < this.nbrruns; ++k) {
            if(getValue(k) + getLength(k) + 1 < lowbits ) {
                answer += getLength(k) + 1;
            } else if (lowbits < getValue(k)) {
                return answer;
            } else if (getValue(k) + getLength(k) + 1 >= lowbits) {
                return answer +  lowbits  - getValue(k) + 1; 
            }
        }
        return answer;
    }

    @Override
    public short select(int j) {
        int card = 0;
        for (int k = 0; k < this.nbrruns; ++k) {
            if(card + getLength(k) > j ) {
                return (short)(getValue(j) + (j - card));
            }
            card += getLength(k) + 1;
        }
        throw new IllegalArgumentException("Cannot select "+j+" since cardinality is "+getCardinality());        
    }

    @Override
    public Container limit(int maxcardinality) {
        int card = 0;
        for (int k = 0; k < this.nbrruns; ++k) {
            if(card  >= maxcardinality) {
                // need to remove...
                this.nbrruns--;
                break;
            } else if(card + getLength(k) + 1 > maxcardinality) {
                setLength(k,(short) (maxcardinality - 1 - card));
                break;
            }
            card += getLength(k) + 1;
        }
        return this;
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


}


final class RunContainerShortIterator implements ShortIterator {
    int pos;
    int le = 0;

    RunContainer parent;
    
    RunContainerShortIterator() {
    }
    
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
    
    ReverseRunContainerShortIterator() {
    }
    
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

