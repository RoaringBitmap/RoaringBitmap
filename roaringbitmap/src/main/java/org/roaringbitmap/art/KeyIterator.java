package org.roaringbitmap.art;

import org.roaringbitmap.Container;

import java.util.Iterator;

public class KeyIterator implements Iterator<byte[]> {

  private LeafNode current;
  private LeafNodeIterator leafNodeIterator;

  public KeyIterator(Art art) {
    leafNodeIterator = new LeafNodeIterator(art);
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

  public byte[] peekNext() {
    return leafNodeIterator.peekNext().getKeyBytes();
  }

  public long nextKey() {
    return current.getKey();
  }

  public Container currentContainer() {
    return current.getContainer();
  }
  public void replaceContainer(Container container) {
    current.setContainer( container);
  }

  @Override
  public void remove() {
    leafNodeIterator.remove();
  }
}
