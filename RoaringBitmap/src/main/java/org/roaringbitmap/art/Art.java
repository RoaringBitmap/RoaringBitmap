package org.roaringbitmap.art;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * See: https://db.in.tum.de/~leis/papers/ART.pdf a cpu cache friend main memory data structure. At
 * our case, the LeafNode's key is always 48 bit size. The high 48 bit keys here are compared using
 * the byte dictionary comparison.
 */
public class Art {

  private Node root;
  private long keySize = 0;

  public Art() {
    root = null;
  }

  public boolean isEmpty() {
    return root == null;
  }

  /**
   * insert the 48 bit key and the corresponding containerIdx
   * @param key the high 48 bit of the long data
   * @param containerIdx the container index
   */
  public void insert(byte[] key, long containerIdx) {
    Node freshRoot = insert(root, key, 0, containerIdx);
    if (freshRoot != root) {
      this.root = freshRoot;
    }
    keySize++;
  }

  /**
   *
   * @param key the high 48 bit of the long data
   * @return the key's corresponding containerIdx
   */
  public long findByKey(byte[] key) {
    Node node = findByKey(root, key, 0);
    if (node != null) {
      LeafNode leafNode = (LeafNode) node;
      return leafNode.containerIdx;
    }
    return Node.ILLEGAL_IDX;
  }

  private Node findByKey(Node node, byte[] key, int depth) {
    while (node != null) {
      if (node.nodeType == NodeType.LEAF_NODE) {
        LeafNode leafNode = (LeafNode) node;
        for (int i = depth; i < leafNode.key.length; i++) {
          if (leafNode.key[i] != key[i]) {
            return null;
          }
        }
        return leafNode;
      }
      if (node.prefixLength > 0) {
        for (int i = depth; i < node.prefixLength; i++) {
          if (key[i] != node.prefix[i]) {
            return null;
          }
        }
        //common prefix is the same ,then increase the depth
        depth += node.prefixLength;
      }
      int pos = node.getChildPos(key[depth]);
      if (pos == Node.ILLEGAL_IDX) {
        return null;
      }
      node = node.getChild(pos);
      depth++;
    }
    return null;
  }

  /**
   * a convenient method to traverse the key space in ascending order.
   */
  public KeyIterator iterator(Containers containers) {
    return new KeyIterator(this, containers);
  }

  /**
   * remove the key from the art if it's there.
   *
   * @return the corresponding containerIdx or -1 indicating not exist
   */
  public long remove(byte[] key) {
    Toolkit toolkit = removeSpecifyKey(root, key, 0);
    if (toolkit != null) {
      return toolkit.matchedContainerId;
    }
    return Node.ILLEGAL_IDX;
  }

  /**
   * @return if found matched key, return a meaning toolkit which has the found
   *containerIdx,otherwise, return a null object.
   */
  protected Toolkit removeSpecifyKey(Node node, byte[] key, int dep) {
    if (node == null) {
      return null;
    }
    if (node.nodeType == NodeType.LEAF_NODE) {
      //root is null
      LeafNode leafNode = (LeafNode) node;
      if (leafMatch(leafNode, key, dep)) {
        //remove this node
        if (node == this.root) {
          this.root = null;
        }
        return new Toolkit(null, leafNode.getContainerIdx(), null);
      } else {
        return null;
      }
    }
    if (node.prefixLength > 0) {
      if (Node.prefixMismatch(node, key, dep) != node.prefixLength) {
        //not found the matched key
        return null;
      }
      dep += node.prefixLength;
    }
    int pos = node.getChildPos(key[dep]);
    if (pos != Node.ILLEGAL_IDX) {
      Node child = node.getChild(pos);
      if (child.nodeType == NodeType.LEAF_NODE && leafMatch((LeafNode) child, key, dep)) {
        //found matched leaf node from the current node.
        Node freshNode = node.remove(pos);
        keySize--;
        if (node == this.root && freshNode != node) {
          this.root = freshNode;
        }
        long matchedContainerIdx = ((LeafNode) child).getContainerIdx();
        Toolkit toolkit = new Toolkit(freshNode, matchedContainerIdx, freshNode);
        return toolkit;
      } else {
        Toolkit toolkit = removeSpecifyKey(child, key, dep + 1);
        if (toolkit != null && toolkit.freshEntry != null && toolkit.freshEntry != child) {
          //meaning find the matched key and the shrinking happened
          node.replaceNode(pos, toolkit.freshEntry);
          return new Toolkit(child, toolkit.matchedContainerId, toolkit.freshEntry);
        }
        if (toolkit != null) {
          return toolkit;
        }
      }
    }
    return null;
  }

  class Toolkit {

    Node freshEntry;//indicating a fresh node while the original node shrunk and changed
    long matchedContainerId; //holding the matched key's corresponding container index id
    Node matchedParentNode; //holding the matched key's leaf node's fresh parent node

    Toolkit(Node freshEntry, long matchedContainerId, Node matchedParentNode) {
      this.freshEntry = freshEntry;
      this.matchedContainerId = matchedContainerId;
      this.matchedParentNode = matchedParentNode;
    }
  }

  private boolean leafMatch(LeafNode leafNode, byte[] key, int dep) {
    for (int i = dep; i < leafNode.key.length; i++) {
      if (leafNode.key[i] != key[i]) {
        return false;
      }
    }
    return true;
  }

  private Node insert(Node node, byte[] key, int depth, long containerIdx) {
    if (node == null) {
      LeafNode leafNode = new LeafNode(key, containerIdx);
      return leafNode;
    }
    if (node.nodeType == NodeType.LEAF_NODE) {
      LeafNode leafNode = (LeafNode) node;
      byte[] prefix = leafNode.key;
      int commonPrefix = commonPrefix(prefix, key, depth);
      Node4 node4 = new Node4(commonPrefix);
      //copy common prefix
      node4.prefixLength = commonPrefix;
      for (int i = 0; i < commonPrefix; i++) {
        node4.prefix[i] = key[depth + i];
      }
      //generate two leaf nodes as the children of the fresh node4
      Node4.insert(node4, leafNode, prefix[depth + commonPrefix]);
      LeafNode anotherLeaf = new LeafNode(key, containerIdx);
      Node4.insert(node4, anotherLeaf, key[depth + commonPrefix]);
      //replace the current node with this internal node4
      return node4;
    }
    //to a inner node case
    if (node.prefixLength > 0) {
      //find the common prefix
      int mismatchPos = findCommonPrefix(node, key, depth);
      if (mismatchPos != node.prefixLength) {
        Node4 node4 = new Node4(mismatchPos);
        //copy prefix
        node4.prefixLength = mismatchPos;
        for (int i = 0; i < mismatchPos; i++) {
          node4.prefix[i] = node.prefix[i];
        }
        //split the current internal node, spawn a fresh node4 and let the
        //current internal node as its children.
        Node4.insert(node4, node, node.prefix[mismatchPos]);
        node.prefixLength = node.prefixLength - (mismatchPos + 1);
        //move the remained common prefix of the initial internal node
        for (int i = 0; i < node.prefixLength; i++) {
          node.prefix[i] = node.prefix[mismatchPos + i + 1];
        }
        LeafNode leafNode = new LeafNode(key, containerIdx);
        Node4.insert(node4, leafNode, key[mismatchPos + depth]);
        return node4;
      }
      depth += node.prefixLength;
    }
    int pos = node.getChildPos(key[depth]);
    if (pos != Node.ILLEGAL_IDX) {
      //insert the key as current internal node's children's child node.
      Node child = node.getChild(pos);
      Node freshOne = insert(child, key, depth + 1, containerIdx);
      if (freshOne != child) {
        node.replaceNode(pos, freshOne);
      }
      return node;
    }
    //insert the key as a child leaf node of the current internal node
    LeafNode leafNode = new LeafNode(key, containerIdx);
    Node freshOne = Node.insertLeaf(node, leafNode, key[depth]);
    return freshOne;
  }

  /**
   * @return the position of the node's prefix that begin to differ from the key
   */
  private int findCommonPrefix(Node node, byte[] key, int depth) {
    int pos = 0;
    for (; pos < node.prefixLength; pos++) {
      if (node.prefix[pos] != key[depth + pos]) {
        break;
      }
    }
    return pos;
  }

  private int commonPrefix(byte[] key1, byte[] key2, int depth) {
    int commonPrefix = 0;
    for (; commonPrefix < key2.length; commonPrefix++) {
      if (key2[commonPrefix + depth] != key1[commonPrefix + depth]) {
        break;
      }
    }
    return commonPrefix;
  }

  public Node getRoot() {
    return root;
  }

  public void serializeArt(DataOutput dataOutput) throws IOException {
    serialize(root, dataOutput);
  }

  public void deserializeArt(DataInput dataInput) throws IOException {
    root = deserialize(dataInput);
  }

  public LeafNodeIterator leafNodeIterator(boolean reverse, Containers containers) {
    return new LeafNodeIterator(this, reverse, containers);
  }

  private void serialize(Node node, DataOutput dataOutput) throws IOException {
    if (node.nodeType != NodeType.LEAF_NODE) {
      //serialize the internal node itself first
      node.serialize(dataOutput);
      //then all the internal node's children
      int nexPos = node.getNextLargerPos(Node.ILLEGAL_IDX);
      while (nexPos != Node.ILLEGAL_IDX) {
        //serialize all the not null child node
        Node child = node.getChild(nexPos);
        serialize(child, dataOutput);
        nexPos = node.getNextLargerPos(nexPos);
      }
    } else {
      //serialize the leaf node
      node.serialize(dataOutput);
    }
  }

  private Node deserialize(DataInput dataInput) throws IOException {
    Node oneNode = Node.deserialize(dataInput);
    if (oneNode == null) {
      return null;
    }
    if (oneNode.nodeType == NodeType.LEAF_NODE) {
      return oneNode;
    } else {
      //internal node
      int count = oneNode.count;
      //all the not null child nodes
      Node[] children = new Node[count];
      for (int i = 0; i < count; i++) {
        Node child = deserialize(dataInput);
        children[i] = child;
      }
      oneNode.replaceChildren(children);
      return oneNode;
    }
  }

  public long serializeSizeInBytes() {
    return serializeSizeInBytes(this.root);
  }

  public long getKeySize() {
    return keySize;
  }

  private long serializeSizeInBytes(Node node) {
    if (node.nodeType != NodeType.LEAF_NODE) {
      //serialize the internal node itself first
      int currentNodeSize = node.serializeSizeInBytes();
      //then all the internal node's children
      long childrenTotalSize = 0L;
      int nexPos = node.getNextLargerPos(Node.ILLEGAL_IDX);
      while (nexPos != Node.ILLEGAL_IDX) {
        //serialize all the not null child node
        Node child = node.getChild(nexPos);
        long childSize = serializeSizeInBytes(child);
        nexPos = node.getNextLargerPos(nexPos);
        childrenTotalSize += childSize;
      }
      return currentNodeSize + childrenTotalSize;
    } else {
      //serialize the leaf node
      int nodeSize = node.serializeSizeInBytes();
      return nodeSize;
    }
  }
}
