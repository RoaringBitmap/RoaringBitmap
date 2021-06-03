package org.roaringbitmap.art;

import org.roaringbitmap.longlong.LongUtils;

/**
 * visit the leaf node space in ascending order.
 */
public class ForwardShuttle extends AbstractShuttle {

  ForwardShuttle(Art art, Containers containers) {
    super(art, containers);
  }

  @Override
  protected boolean currentBeforeHigh(byte[] current, byte[] high) {
    return LongUtils.compareHigh(current, high) < 0;
  }

  @Override
  protected int visitedNodeNextPosition(Node node, int pos) {
    return node.getNextLargerPos(pos);
  }

  @Override
  protected int boundaryNodePosition(Node node, boolean inRunDirection) {
    if (inRunDirection) {
      return node.getMaxPos();
    } else {
      return node.getMinPos();
    }
  }

  @Override
  protected boolean prefixMismatchIsInRunDirection(byte nodeValue, byte highValue) {
    return nodeValue < highValue;
  }

  @Override
  protected int searchMissNextPosition(SearchResult result) {
    return result.getNextLargerPos();
  }
}
