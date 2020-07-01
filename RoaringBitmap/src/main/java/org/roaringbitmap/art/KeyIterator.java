package org.roaringbitmap.art;

import java.util.Iterator;

public class KeyIterator implements Iterator<byte[]> {

  private LeafNode current;
  private LeafNodeIterator leafNodeIterator;

  public KeyIterator(Art art, Containers containers) {
    leafNodeIterator = new LeafNodeIterator(art, containers);
    current = null;
  }

  @Override
  public boolean hasNext() {
    boolean hasNext = leafNodeIterator.hasNext();
    if (hasNext) {
      current = leafNodeIterator.next();
    }
    return hasNext;
  }

  @Override
  public byte[] next() {
    return current.getKeyBytes();
  }

  public long currentContainerIdx() {
    return current.getContainerIdx();
  }

  public void remove() {
    leafNodeIterator.remove();
  }
}
