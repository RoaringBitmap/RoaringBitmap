package me.lemire.roaringbitmap;

public interface Container extends Iterable<Short>{
	
	public Container add(short x);
	public Container remove(short x);
	public boolean contains(short x);
	public int getCardinality();
    public int getSizeInBits();
}
