package org.roaringbitmap.art;

/**
 * visit the leaf node space in ascending order.
 */
public class ForwardShuttle extends AbstractShuttle {

  ForwardShuttle(Art art, Containers containers) {
    super(art, containers);
  }

  @Override
  protected boolean currentBeforeHigh(long current, long high) {
    return current < high;
  }

  @Override
  protected int visitedNodeNextPosition(BranchNode node, int pos) {
    return node.getNextLargerPos(pos);
  }

  @Override
  protected int boundaryNodePosition(BranchNode node, boolean inRunDirection) {
    if (inRunDirection) {
      return node.getMaxPos();
    } else {
      return node.getMinPos();
    }
  }

  @Override
  protected boolean prefixMismatchIsInRunDirection(byte nodeValue, byte highValue) {
    return Byte.toUnsignedInt(nodeValue) < Byte.toUnsignedInt(highValue);
  }

  @Override
  protected int searchMissNextPosition(SearchResult result) {
    return result.getNextLargerPos();
  }
}
