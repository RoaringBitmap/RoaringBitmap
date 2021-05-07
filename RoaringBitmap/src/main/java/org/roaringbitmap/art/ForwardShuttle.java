package org.roaringbitmap.art;

/**
 * visit the leaf node space in ascending order.
 */
public class ForwardShuttle extends AbstractShuttle {

  ForwardShuttle(Art art, Containers containers) {
    super(art, containers);
  }

  @Override
  protected int visitedNodeNextPosition(Node node, int pos) {
    return node.getNextLargerPos(pos);
  }

  @Override
  protected int boundaryNodePosition(Node node) {
    return node.getMinPos();
  }

  @Override
  protected int fromNodePosition(byte key, Node node) {
    int pos = node.getChildPos(key);
    if (pos == Node.ILLEGAL_IDX) {
      return node.getNextLargerPos(pos);
    } else {
      return pos;
    }
  }
}
