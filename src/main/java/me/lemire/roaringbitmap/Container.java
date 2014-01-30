package me.lemire.roaringbitmap;

public interface Container extends Iterable<Short>, Cloneable {

        public Container add(short x);

        public void clear();

        public Container clone();

        public boolean contains(short x);

        public int getCardinality();

        public ShortIterator getShortIterator();

        public int getSizeInBits();

        public int getSizeInBytes();

        public Container remove(short x);

        public void trim();

        public void validate();// TODO: should be pruned before release
}
