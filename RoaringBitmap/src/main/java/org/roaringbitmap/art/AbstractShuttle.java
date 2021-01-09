package org.roaringbitmap.art;

import org.roaringbitmap.art.Art.Toolkit;

/**
 * visit the art tree's space through a stack which records the deep first visiting paths.
 */
public abstract class AbstractShuttle implements Shuttle {

  protected static final int MAX_DEPTH = 6;
  protected NodeEntry[] stack = new NodeEntry[MAX_DEPTH];
  //started from 0
  protected int depth = -1;
  protected boolean hasRun = false;
  protected Art art;
  protected Containers containers;

  public AbstractShuttle(Art art, Containers containers) {
    this.art = art;
    this.containers = containers;
  }

  @Override
  public void initShuttle() {
    visitToLeaf(art.getRoot());
  }

  @Override
  public boolean moveToNextLeaf() {
    if (!hasRun) {
      hasRun = true;
      Node node = stack[depth].node;
      if (node.nodeType == NodeType.LEAF_NODE) {
        return true;
      } else {
        return false;
      }
    }
    if (depth < 0) {
      return false;
    }
    //skip the top leaf node
    Node node = stack[depth].node;
    if (node.nodeType == NodeType.LEAF_NODE) {
      depth--;
    }
    //visit parent node
    while (depth >= 0) {
      NodeEntry currentNodeEntry = stack[depth];
      if (currentNodeEntry.node.nodeType == NodeType.LEAF_NODE) {
        //found current leaf node's next sibling node to benefit the removing operation
        if (depth - 1 >= 0) {
          findNextSiblingKeyOfLeafNode();
        }
        return true;
      }
      //visit the next child node
      int pos;
      int nextPos;
      if (!currentNodeEntry.visited) {
        pos = boundaryNodePosition(currentNodeEntry.node);
        currentNodeEntry.position = pos;
        nextPos = pos;
        currentNodeEntry.visited = true;
      } else if (currentNodeEntry.startFromNextSiblingPosition) {
        nextPos = currentNodeEntry.position;
        currentNodeEntry.startFromNextSiblingPosition = false;
      } else  {
        pos = currentNodeEntry.position;
        nextPos = visitedNodeNextPosition(currentNodeEntry.node, pos);
      }
      if (nextPos != Node.ILLEGAL_IDX) {
        stack[depth].position = nextPos;
        depth++;
        //add a fresh entry on the top of the visiting stack
        NodeEntry freshEntry = new NodeEntry();
        freshEntry.node = currentNodeEntry.node.getChild(nextPos);
        stack[depth] = freshEntry;
      } else {
        //current internal node doesn't have anymore unvisited child,move to a top node
        depth--;
      }
    }
    return false;
  }

  protected abstract int visitedNodeNextPosition(Node node, int pos);

  @Override
  public LeafNode getCurrentLeafNode() {
    NodeEntry currentNode = stack[depth];
    return (LeafNode) currentNode.node;
  }

  @Override
  public void remove() {
    byte[] currentLeafKey = getCurrentLeafNode().getKeyBytes();
    Toolkit toolkit = art.removeSpecifyKey(art.getRoot(), currentLeafKey, 0);
    if (toolkit == null) {
      return;
    }
    if (containers != null) {
      containers.remove(toolkit.matchedContainerId);
    }
    Node node = toolkit.freshMatchedParentNode;
    if (depth - 1 >= 0) {
      //update the parent node to a fresh node as the parent node may changed by the
      //art adaptive removing logic
      NodeEntry oldEntry = stack[depth - 1];
      oldEntry.visited = oldEntry.node == node;
      oldEntry.node = node;
      oldEntry.startFromNextSiblingPosition = true;
      if (node.nodeType !=  NodeType.LEAF_NODE) {
        oldEntry.position = node.getChildPos(oldEntry.leafNodeNextSiblingKey);
      }
    }
  }

  private void visitToLeaf(Node node) {
    if (node == null) {
      return;
    }
    if (node == art.getRoot()) {
      NodeEntry nodeEntry = new NodeEntry();
      nodeEntry.node = node;
      this.depth = 0;
      stack[depth] = nodeEntry;
    }
    if (node.nodeType == NodeType.LEAF_NODE) {
      //leaf node's corresponding NodeEntry will not have the position member set.
      if (depth - 1 >= 0) {
        findNextSiblingKeyOfLeafNode();
      }
      return;
    }
    if (depth == MAX_DEPTH) {
      return;
    }
    //find next min child
    int pos = boundaryNodePosition(node);
    stack[depth].position = pos;
    stack[depth].visited = true;
    Node child = node.getChild(pos);
    NodeEntry childNodeEntry = new NodeEntry();
    childNodeEntry.node = child;
    this.depth++;
    stack[depth] = childNodeEntry;
    visitToLeaf(child);
  }

  protected abstract int boundaryNodePosition(Node node);

  private void findNextSiblingKeyOfLeafNode() {
    Node parentNode = stack[depth - 1].node;
    int nextSiblingPos = visitedNodeNextPosition(parentNode, stack[depth - 1].position);
    if (nextSiblingPos != Node.ILLEGAL_IDX) {
      byte nextSiblingKey = parentNode.getChildKey(nextSiblingPos);
      stack[depth - 1].leafNodeNextSiblingKey = nextSiblingKey;
    }
  }

  class NodeEntry {
    Node node = null;
    int position = Node.ILLEGAL_IDX;
    boolean visited = false;
    boolean startFromNextSiblingPosition = false;
    byte leafNodeNextSiblingKey;
  }
}
