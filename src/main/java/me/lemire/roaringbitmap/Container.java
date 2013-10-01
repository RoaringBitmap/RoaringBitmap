package me.lemire.roaringbitmap;

public interface Container extends Iterable<Short>{
	
	public Container add(short x);
	public Container remove(short x);	
	public int getCardinality();
	public boolean contains(short x);
	public void clear();
    public int getSizeInBits();
    public int getSizeInBytes();
    public void validate();
    public ShortIterator getShortIterator();
    public void trim();
}
