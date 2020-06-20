package org.roaringbitmap.art;

/**
 * visit the leaf node space in descending order
 */
public class BackwardShuttle extends AbstractShuttle {

  BackwardShuttle(Art art, Containers containers) {
    super(art, containers);
  }

  @Override
  protected int visitedNodeNextPosition(Node node, int pos) {
    return node.getNextSmallerPos(pos);
  }

  @Override
  protected int boundaryNodePosition(Node node) {
    return node.getMaxPos();
  }
}
