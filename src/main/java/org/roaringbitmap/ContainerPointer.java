package org.roaringbitmap;

interface ContainerPointer extends Comparable<ContainerPointer>{
	Container getContainer();
	void advance();
	short key();
}
