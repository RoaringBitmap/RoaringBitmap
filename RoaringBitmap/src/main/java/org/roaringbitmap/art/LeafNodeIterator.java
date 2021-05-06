package org.roaringbitmap.art;

import java.util.NoSuchElementException;

import org.roaringbitmap.PeekableIterator;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class LeafNodeIterator implements PeekableIterator<LeafNode> {

  private Shuttle shuttle;
  private boolean hasCurrent;
  private LeafNode current;
  private boolean calledHasNext;
  private boolean isEmpty;

  /**
   * constructor
   * @param art the ART
   * @param containers the containers
   */
  public LeafNodeIterator(Art art, Containers containers) {
    this(art, false, containers);
  }

  /**
   * constructor
   * @param art the ART
   * @param reverse false: ascending order,true: the descending order
   * @param containers the containers
   */
  public LeafNodeIterator(Art art, boolean reverse, Containers containers) {
    isEmpty = art.isEmpty();
    if (isEmpty) {
      return;
    }
    if (!reverse) {
      shuttle = new ForwardShuttle(art, containers);
    } else {
      shuttle = new BackwardShuttle(art, containers);
    }
    shuttle.initShuttle();
    calledHasNext = false;
  }

  private boolean advance() {
    boolean hasLeafNode = shuttle.moveToNextLeaf();
    if (hasLeafNode) {
      hasCurrent = true;
      current = shuttle.getCurrentLeafNode();
    } else {
      hasCurrent = false;
      current = null;
    }
    return hasLeafNode;
  }

  @Override
  public boolean hasNext() {
    if (isEmpty) {
      return false;
    }
    if (!calledHasNext) {
      calledHasNext = true;
      return advance();
    } else {
      return hasCurrent;
    }
  }

  @Override
  public LeafNode next() {
    if (!calledHasNext) {
      hasNext();
    }
    if (!hasCurrent) {
      throw new NoSuchElementException();
    }
    calledHasNext = false;
    return current;
  }

  @Override
  public void remove() {
    shuttle.remove();
  }

  @Override
  public void advanceIfNeeded(LeafNode minval) {
    // TODO
    throw new NotImplementedException();
  }

  @Override
  public LeafNode peekNext() {
    if (!calledHasNext) {
      hasNext();
    }
    if (!hasCurrent) {
      throw new NoSuchElementException();
    }
    // don't set calledHasNext, so that multiple invocations don't advance
    return current;
  }
}
