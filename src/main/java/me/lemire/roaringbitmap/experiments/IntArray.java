package me.lemire.roaringbitmap.experiments;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class IntArray implements Cloneable {

        /**
         * Instantiates a new long array.
         */
        public IntArray() {
                this(8);
        }

        /**
         * Instantiates a new long array.
         * 
         * @param capacity
         *            the capacity
         */
        public IntArray(int capacity) {
                this.array = new int[capacity];
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#clone()
         */
        @Override
        public IntArray clone() throws java.lang.CloneNotSupportedException {
                IntArray clone;
                clone = (IntArray) super.clone();
                clone.size = this.size;
                clone.array = Arrays.copyOf(this.array, this.size);
                return clone;
        }

        /**
         * Clear.
         */
        public void clear() {
                this.size = 0;
        }

        /**
         * Gets the value at index
         * 
         * @param index
         *            the index
         * @return the value
         */
        public int get(int index) {
                return this.array[index];
        }

        /**
         * Size of the array (typically smaller than the capacity).
         * 
         * @return the int
         */
        public int size() {
                return this.size;
        }

        /**
         * Sets the value at index
         * 
         * @param index
         *            the index
         * @param value
         *            the value
         */
        public void set(int index, int value) {
                this.array[index] = value;
        }

        /**
         * Adds a value at the end of the array
         * 
         * @param value
         *            the value
         */
        public void add(int value) {
                // size + 1 could overflow
                if (this.size + 1 >= this.array.length) {
                        int newcapacity;
                        if (this.array.length < 4) {
                                newcapacity = 4;
                        } else if (this.array.length < 1024) {
                                newcapacity = 2 * this.array.length; // grow fast initially
                        } else {
                                newcapacity = 5 * this.array.length / 4; // inspired by Go, see
                                // http://golang.org/src/pkg/runtime/slice.c#L131
                        }
                        this.array = Arrays.copyOf(this.array, newcapacity);
                }
                this.array[this.size] = value;
                this.size++;
        }

        /**
         * Removes the last value (shrinks the array)
         */
        public void removeLast() {
                --this.size;
        }

        /**
         * Trim the array so that the capacity is equal to the size. This saves
         * memory.
         * 
         * @return the new array size (in 32-bit words)
         */
        public int trim() {
                this.array = Arrays.copyOf(this.array, this.size);
                return this.array.length;
        }

        /**
         * Serialize
         * 
         * @param out
         *            the stream where we write
         * @throws IOException
         *             Signals that an I/O exception has occurred.
         */
        public void serialize(DataOutput out) throws IOException {
                out.writeInt(this.size);
                for (int k = 0; k < this.size; ++k)
                        out.writeInt(this.array[k]);
        }

        /**
         * Deserialize
         * 
         * @param in
         *            the stream where we read
         * @throws IOException
         *             Signals that an I/O exception has occurred.
         */
        public void deserialize(DataInput in) throws IOException {
                this.size = in.readInt();
                if (this.array.length < this.size)
                        this.array = new int[this.size];
                for (int k = 0; k < this.size; ++k)
                        this.array[k] = in.readInt();
        }

        /**
         * Return the capacity of the array, typically larger than the size of the
         * array.
         * 
         * @return the capacity
         */
        public int capacity() {
                return this.array.length;
        }

        /**
         * Return a hash value for this object. Uses a Karp-Rabin hash function.
         * 
         * @return the hash value
         */
        @Override
        public int hashCode() {
                int buf = 0;
                for (int k = 0; k < this.size; ++k)
                        buf = 31 * buf + this.array[k];
                return buf;
        }

        /** The array. */
        private int[] array;

        /** The size. */
        private int size = 0;

}
