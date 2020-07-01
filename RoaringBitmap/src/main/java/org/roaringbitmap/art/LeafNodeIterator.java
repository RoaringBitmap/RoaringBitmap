package org.roaringbitmap.art;

import java.util.Iterator;

public class LeafNodeIterator implements Iterator<LeafNode> {

  private Shuttle shuttle;
  private LeafNode current;
  private boolean consumedCurrent;
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
    consumedCurrent = true;
  }

  @Override
  public boolean hasNext() {
    if (isEmpty) {
      return false;
    }
    if (!consumedCurrent) {
      return false;
    }
    boolean hasLeafNode = shuttle.moveToNextLeaf();
    if (hasLeafNode) {
      current = shuttle.getCurrentLeafNode();
      consumedCurrent = false;
    }
    return hasLeafNode;
  }

  @Override
  public LeafNode next() {
    consumedCurrent = true;
    return current;
  }

  @Override
  public void remove() {
    shuttle.remove();
  }
}
