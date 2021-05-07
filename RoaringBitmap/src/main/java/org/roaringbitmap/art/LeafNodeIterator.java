package org.roaringbitmap.art;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class LeafNodeIterator implements Iterator<LeafNode> {

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

  /**
   * constructor
   * @param art the ART
   * @param reverse false: ascending order,true: the descending order
   * @param containers the containers
   * @param from starting upper/lower bound
   */
  public LeafNodeIterator(Art art, boolean reverse, Containers containers, long from) {
    isEmpty = art.isEmpty();
    if (isEmpty) {
      return;
    }
    if (!reverse) {
      shuttle = new ForwardShuttle(art, containers);
    } else {
      shuttle = new BackwardShuttle(art, containers);
    }
    shuttle.initShuttleFrom(from);
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

  /**
   * Move this iterator to the leaf that contains `boundval`.
   *
   * If no leaf contains `boundval`, then move to the next largest (on forward iterators
   * or next smallest (on backwards iterators).
   */
  public void seek(long boundval) {
    shuttle.initShuttleFrom(boundval);
    calledHasNext = false;
  }

  /**
   * Return the next leaf without advancing the iterator.
   *
   * @return the next leaf
   */
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
