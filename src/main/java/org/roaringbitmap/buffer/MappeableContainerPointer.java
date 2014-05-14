package org.roaringbitmap.buffer;

interface MappeableContainerPointer extends Comparable<MappeableContainerPointer>{
	MappeableContainer getContainer();
	void advance();
	short key();
}
