package org.roaringbitmap.art;

import org.roaringbitmap.Container;

import java.util.Iterator;

public class KeyIterator implements Iterator<byte[]> {

  private LeafNode current;
  private LeafNodeIterator leafNodeIterator;
  private final Containers containers;

  public KeyIterator(Art art, Containers containers) {
    this.containers = containers;
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

  public byte[] peekNext() {
    return leafNodeIterator.peekNext().getKeyBytes();
  }

  public long nextKey() {
    return current.getKey();
  }

  public Container currentContainer() {
    return containers.getContainer(current.getContainerIdx());
  }
  public void replaceContainer(Container container) {
    containers.replace(current.getContainerIdx(), container);
  }

  @Override
  public void remove() {
    leafNodeIterator.remove();
  }
}
