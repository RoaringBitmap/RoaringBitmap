package org.roaringbitmap.art;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.roaringbitmap.ArraysShim;

/**
 * See: https://db.in.tum.de/~leis/papers/ART.pdf a cpu cache friendly main memory data structure.
 * At our case, the LeafNode's key is always 48 bit size. The high 48 bit keys here are compared
 * using the byte dictionary comparison.
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
   *
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
        byte[] leafNodeKeyBytes = leafNode.getKeyBytes();
        if (depth == LeafNode.LEAF_NODE_KEY_LENGTH_IN_BYTES) {
          return leafNode;
        }
        int mismatchIndex = ArraysShim
            .mismatch(leafNodeKeyBytes, depth, LeafNode.LEAF_NODE_KEY_LENGTH_IN_BYTES,
                key, depth, LeafNode.LEAF_NODE_KEY_LENGTH_IN_BYTES);
        if (mismatchIndex != -1) {
          return null;
        }
        return leafNode;
      }
      if (node.prefixLength > 0) {
        int commonLength = commonPrefixLength(key, depth, key.length, node.prefix, 0,
            node.prefixLength);
        if (commonLength != node.prefixLength) {
          return null;
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
   * @param containers input containers
   * @return the key iterator
   */
  public KeyIterator iterator(Containers containers) {
    return new KeyIterator(this, containers);
  }

  /**
   * remove the key from the art if it's there.
   * @param key the high 48 bit key
   * @return the corresponding containerIdx or -1 indicating not exist
   */
  public long remove(byte[] key) {
    Toolkit toolkit = removeSpecifyKey(root, key, 0);
    if (toolkit != null) {
      return toolkit.matchedContainerId;
    }
    return Node.ILLEGAL_IDX;
  }

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
        keySize--;
        return new Toolkit(null, leafNode.getContainerIdx(), null);
      } else {
        return null;
      }
    }
    if (node.prefixLength > 0) {
      int mismatchIndex = ArraysShim.mismatch(node.prefix, 0,
          node.prefixLength, key, dep, node.prefixLength);
      if (mismatchIndex != -1) {
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
        Toolkit toolkit = new Toolkit(freshNode, matchedContainerIdx, node);
        toolkit.needToVerifyReplacing = true;
        return toolkit;
      } else {
        Toolkit toolkit = removeSpecifyKey(child, key, dep + 1);
        if (toolkit != null && toolkit.needToVerifyReplacing
            && toolkit.freshMatchedParentNode != null && toolkit.freshMatchedParentNode
            != toolkit.originalMatchedParentNode) {
          //meaning find the matched key and the shrinking happened
          node.replaceNode(pos, toolkit.freshMatchedParentNode);
          toolkit.needToVerifyReplacing = false;
          return toolkit;
        }
        if (toolkit != null) {
          return toolkit;
        }
      }
    }
    return null;
  }

  class Toolkit {

    Node freshMatchedParentNode;//indicating a fresh parent node while the original
    // parent node shrunk and changed
    long matchedContainerId; //holding the matched key's corresponding container index id
    Node originalMatchedParentNode; //holding the matched key's leaf node's original old parent node
    boolean needToVerifyReplacing = false; //indicate whether the shrinking node's parent
    // node has replaced its corresponding child node

    Toolkit(Node freshMatchedParentNode, long matchedContainerId, Node originalMatchedParentNode) {
      this.freshMatchedParentNode = freshMatchedParentNode;
      this.matchedContainerId = matchedContainerId;
      this.originalMatchedParentNode = originalMatchedParentNode;
    }
  }

  private boolean leafMatch(LeafNode leafNode, byte[] key, int dep) {
    byte[] leafNodeKeyBytes = leafNode.getKeyBytes();
    int mismatchIndex = ArraysShim
        .mismatch(leafNodeKeyBytes, dep, LeafNode.LEAF_NODE_KEY_LENGTH_IN_BYTES,
            key, dep, LeafNode.LEAF_NODE_KEY_LENGTH_IN_BYTES);
    if (mismatchIndex == -1) {
      return true;
    } else {
      return false;
    }
  }

  private Node insert(Node node, byte[] key, int depth, long containerIdx) {
    if (node == null) {
      LeafNode leafNode = new LeafNode(key, containerIdx);
      return leafNode;
    }
    if (node.nodeType == NodeType.LEAF_NODE) {
      LeafNode leafNode = (LeafNode) node;
      byte[] prefix = leafNode.getKeyBytes();
      int commonPrefix = commonPrefixLength(prefix, depth, prefix.length, key, depth, key.length);
      Node4 node4 = new Node4(commonPrefix);
      //copy common prefix
      node4.prefixLength = (byte) commonPrefix;
      System.arraycopy(key, depth, node4.prefix, 0, commonPrefix);
      //generate two leaf nodes as the children of the fresh node4
      Node4.insert(node4, leafNode, prefix[depth + commonPrefix]);
      LeafNode anotherLeaf = new LeafNode(key, containerIdx);
      Node4.insert(node4, anotherLeaf, key[depth + commonPrefix]);
      //replace the current node with this internal node4
      return node4;
    }
    //to a inner node case
    if (node.prefixLength > 0) {
      //find the mismatch position
      int mismatchPos = ArraysShim.mismatch(node.prefix, 0, node.prefixLength,
          key, depth, key.length);
      if (mismatchPos != node.prefixLength) {
        Node4 node4 = new Node4(mismatchPos);
        //copy prefix
        node4.prefixLength = (byte) mismatchPos;
        System.arraycopy(node.prefix, 0, node4.prefix, 0, mismatchPos);
        //split the current internal node, spawn a fresh node4 and let the
        //current internal node as its children.
        Node4.insert(node4, node, node.prefix[mismatchPos]);
        int nodeOriginalPrefixLength = node.prefixLength;
        node.prefixLength = (byte) (nodeOriginalPrefixLength - (mismatchPos + (byte) 1));
        //move the remained common prefix of the initial internal node
        if (node.prefixLength > 0) {
          System.arraycopy(node.prefix, mismatchPos + 1, node.prefix, 0, node.prefixLength);
        } else {
          //TODO:to reduce the 0 prefix memory space,we could mark the prefix as null
          node.prefix = new byte[0];
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

  //find common prefix length
  private static int commonPrefixLength(byte[] key1, int aFromIndex, int aToIndex,
      byte[] key2, int bFromIndex, int bToIndex) {
    int aLength = aToIndex - aFromIndex;
    int bLength = bToIndex - bFromIndex;
    int minLength = Math.min(aLength, bLength);
    int mismatchIndex = ArraysShim.mismatch(key1, aFromIndex, aToIndex, key2, bFromIndex, bToIndex);

    if (aLength != bLength && mismatchIndex >= minLength) {
      return minLength;
    }
    return mismatchIndex;
  }

  public Node getRoot() {
    return root;
  }

  public void serializeArt(DataOutput dataOutput) throws IOException {
    dataOutput.writeLong(Long.reverseBytes(keySize));
    serialize(root, dataOutput);
  }

  public void deserializeArt(DataInput dataInput) throws IOException {
    keySize = Long.reverseBytes(dataInput.readLong());
    root = deserialize(dataInput);
  }

  public void serializeArt(ByteBuffer byteBuffer) throws IOException {
    byteBuffer.putLong(keySize);
    serialize(root, byteBuffer);
  }

  public void deserializeArt(ByteBuffer byteBuffer) throws IOException {
    keySize = byteBuffer.getLong();
    root = deserialize(byteBuffer);
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

  private void serialize(Node node, ByteBuffer byteBuffer) throws IOException {
    if (node.nodeType != NodeType.LEAF_NODE) {
      //serialize the internal node itself first
      node.serialize(byteBuffer);
      //then all the internal node's children
      int nexPos = node.getNextLargerPos(Node.ILLEGAL_IDX);
      while (nexPos != Node.ILLEGAL_IDX) {
        //serialize all the not null child node
        Node child = node.getChild(nexPos);
        serialize(child, byteBuffer);
        nexPos = node.getNextLargerPos(nexPos);
      }
    } else {
      //serialize the leaf node
      node.serialize(byteBuffer);
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

  private Node deserialize(ByteBuffer byteBuffer) throws IOException {
    Node oneNode = Node.deserialize(byteBuffer);
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
        Node child = deserialize(byteBuffer);
        children[i] = child;
      }
      oneNode.replaceChildren(children);
      return oneNode;
    }
  }

  public long serializeSizeInBytes() {
    return serializeSizeInBytes(this.root) + 8;
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
