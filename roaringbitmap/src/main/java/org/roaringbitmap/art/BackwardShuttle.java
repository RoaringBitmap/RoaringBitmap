package org.roaringbitmap.art;

/**
 * visit the leaf node space in descending order
 */
public class BackwardShuttle extends AbstractShuttle {

  BackwardShuttle(Art art, Containers containers) {
    super(art, containers);
  }

  @Override
  protected boolean currentBeforeHigh(long current, long high) {
    return current > high;
  }

  @Override
  protected int visitedNodeNextPosition(BranchNode node, int pos) {
    return node.getNextSmallerPos(pos);
  }

  @Override
  protected int boundaryNodePosition(BranchNode node, boolean inRunDirection) {
    if (inRunDirection) {
      return node.getMinPos();
    } else {
      return node.getMaxPos();
    }
  }

  @Override
  protected boolean prefixMismatchIsInRunDirection(byte nodeValue, byte highValue) {
    return Byte.toUnsignedInt(nodeValue) > Byte.toUnsignedInt(highValue);
  }

  @Override
  protected int searchMissNextPosition(SearchResult result) {
    return result.getNextSmallerPos();
  }
}
