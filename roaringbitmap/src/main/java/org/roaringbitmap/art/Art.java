package org.roaringbitmap.art;

import org.roaringbitmap.ArraysShim;
import org.roaringbitmap.longlong.LongUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * See: https://db.in.tum.de/~leis/papers/ART.pdf a cpu cache friendly main memory data structure.
 * At our case, the LeafNode's key is always 48 bit size. The high 48 bit keys here are compared
 * using the byte dictionary comparison.
 */
public class Art {

  private Node root;
  private long keySize = 0;

  final static byte[] EMPTY_BYTES = new byte[0];

  public Art() {
    root = null;
  }
  @Override
  public Art clone() {
    Art art = new Art();
    art.keySize = this.keySize;
    if (this.root != null) {
      art.root = this.root.clone();
    }
    return art;
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
    return BranchNode.ILLEGAL_IDX;
  }

  /**
   * @param key the high 48 bit of the long data
   * @return the key's corresponding containerIdx
   */
  public long findByKey(long key) {
    LeafNode node = findByKey(root, key);
    if (node != null) {
      return node.containerIdx;
    }
    return BranchNode.ILLEGAL_IDX;
  }

  private Node findByKey(Node node, byte[] key, int depth) {
    while (node != null) {
      if (node instanceof LeafNode) {
        LeafNode leafNode = (LeafNode) node;
        byte[] leafNodeKeyBytes = leafNode.getKeyBytes();
        if (depth == LeafNode.LEAF_NODE_KEY_LENGTH_IN_BYTES) {
          return leafNode;
        }
        int mismatchIndex =
            ArraysShim.mismatch(
                leafNodeKeyBytes,
                depth,
                LeafNode.LEAF_NODE_KEY_LENGTH_IN_BYTES,
                key,
                depth,
                LeafNode.LEAF_NODE_KEY_LENGTH_IN_BYTES);
        if (mismatchIndex != -1) {
          return null;
        }
        return leafNode;
      }
      BranchNode branchNode = (BranchNode) node;
      byte branchNodePrefixLength = branchNode.prefixLength();
      if (branchNodePrefixLength > 0) {
        int commonLength =
            commonPrefixLength(key, depth, key.length, branchNode.prefix, 0, branchNodePrefixLength);
        if (commonLength != branchNodePrefixLength) {
          return null;
        }
        // common prefix is the same ,then increase the depth
        depth += branchNodePrefixLength;
      }
      int pos = branchNode.getChildPos(key[depth]);
      if (pos == BranchNode.ILLEGAL_IDX) {
        return null;
      }
      node = branchNode.getChild(pos);
      depth++;
    }
    return null;
  }
  private LeafNode findByKey(Node node, long key) {
    int depth = 0;
    while (node != null) {
      //compare branch node first, its most common case
      if (node instanceof BranchNode) {
        BranchNode branchNode = (BranchNode) node;
        byte branchNodePrefixLength = branchNode.prefixLength();
        if (branchNodePrefixLength > 0) {
          //TODO - we should expose a prefix() that is a long. So much time spend looping here
          // when this could be a O(1) long mask & compare
          byte[] prefix = branchNode.prefix;
          for (int i = 0; i < branchNodePrefixLength; i++) {
            // compare the prefix byte with the key byte
            if (prefix[i] != LongUtils.getByte(key, depth + i)) {
              return null;
            }
          }
          // common prefix is the same ,then increase the depth
          depth += branchNodePrefixLength;
        }
        node = branchNode.getChildAtKey(LongUtils.getByte(key, depth));
        depth++;
      } else {
        LeafNode leafNode = (LeafNode) node;
        long leafNodeKey = leafNode.getKey();
        return leafNodeKey == LongUtils.rightShiftHighPart(key)? leafNode: null;
      }
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
    return BranchNode.ILLEGAL_IDX;
  }

  protected Toolkit removeSpecifyKey(Node node, byte[] key, int dep) {
    if (node == null) {
      return null;
    }
    if (node instanceof LeafNode) {
      // root is null
      LeafNode leafNode = (LeafNode) node;
      if (leafMatch(leafNode, key, dep)) {
        // remove this node
        if (leafNode == this.root) {
          this.root = null;
        }
        keySize--;
        return new Toolkit(null, leafNode.getContainerIdx(), null);
      } else {
        return null;
      }
    }
    BranchNode branchNode = (BranchNode) node;
    byte branchNodePrefixLength = branchNode.prefixLength();
    if (branchNodePrefixLength > 0) {
      int commonLength =
          commonPrefixLength(key, dep, key.length, branchNode.prefix, 0, branchNodePrefixLength);
      if (commonLength != branchNodePrefixLength) {
        return null;
      }
      dep += branchNodePrefixLength;
    }
    int pos = branchNode.getChildPos(key[dep]);
    if (pos != BranchNode.ILLEGAL_IDX) {
      Node child = branchNode.getChild(pos);
      if (child instanceof LeafNode && leafMatch((LeafNode) child, key, dep)) {
        // found matched leaf node from the current node.
        Node freshNode = branchNode.remove(pos);
        keySize--;
        if (branchNode == this.root && freshNode != branchNode) {
          this.root = freshNode;
        }
        long matchedContainerIdx = ((LeafNode) child).getContainerIdx();
        Toolkit toolkit = new Toolkit(freshNode, matchedContainerIdx, branchNode);
        toolkit.needToVerifyReplacing = true;
        return toolkit;
      } else {
        Toolkit toolkit = removeSpecifyKey(child, key, dep + 1);
        if (toolkit != null
            && toolkit.needToVerifyReplacing
            && toolkit.freshMatchedParentNode != null
            && toolkit.freshMatchedParentNode != toolkit.originalMatchedParentNode) {
          // meaning find the matched key and the shrinking happened
          branchNode.replaceNode(pos, toolkit.freshMatchedParentNode);
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

    Node freshMatchedParentNode; // indicating a fresh parent node while the original
    // parent node shrunk and changed
    long matchedContainerId; // holding the matched key's corresponding container index id
    Node
        originalMatchedParentNode; // holding the matched key's leaf node's original old parent node
    boolean needToVerifyReplacing = false; // indicate whether the shrinking node's parent

    // node has replaced its corresponding child node

    Toolkit(Node freshMatchedParentNode, long matchedContainerId, Node originalMatchedParentNode) {
      this.freshMatchedParentNode = freshMatchedParentNode;
      this.matchedContainerId = matchedContainerId;
      this.originalMatchedParentNode = originalMatchedParentNode;
    }
  }

  private boolean leafMatch(LeafNode leafNode, byte[] key, int dep) {
    byte[] leafNodeKeyBytes = leafNode.getKeyBytes();
    int mismatchIndex =
        ArraysShim.mismatch(
            leafNodeKeyBytes,
            dep,
            LeafNode.LEAF_NODE_KEY_LENGTH_IN_BYTES,
            key,
            dep,
            LeafNode.LEAF_NODE_KEY_LENGTH_IN_BYTES);
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
    if (node instanceof LeafNode) {
      LeafNode leafNode = (LeafNode) node;
      byte[] prefix = leafNode.getKeyBytes();
      int commonPrefix = commonPrefixLength(prefix, depth, prefix.length, key, depth, key.length);

      Node4 node4 = new Node4(commonPrefix);
      // copy common prefix
      System.arraycopy(key, depth, node4.prefix, 0, commonPrefix);
      // generate two leaf nodes as the children of the fresh node4
      node4.insert(leafNode, prefix[depth + commonPrefix]);
      LeafNode anotherLeaf = new LeafNode(key, containerIdx);
      node4.insert(anotherLeaf, key[depth + commonPrefix]);
      // replace the current node with this internal node4
      return node4;
    }
    BranchNode branchNode = (BranchNode) node;
    byte branchNodePrefixLength = branchNode.prefixLength();
    // to a inner node case
    if (branchNodePrefixLength > 0) {
      // find the mismatch position
      int mismatchPos =
          ArraysShim.mismatch(branchNode.prefix, 0, branchNodePrefixLength, key, depth, key.length);
      if (mismatchPos != branchNodePrefixLength) {
        Node4 node4 = new Node4(mismatchPos);
        // copy prefix
        System.arraycopy(branchNode.prefix, 0, node4.prefix, 0, mismatchPos);
        // split the current internal node, spawn a fresh node4 and let the
        // current internal node as its children.
        node4.insert(branchNode, branchNode.prefix[mismatchPos]);
        int newPrefixLength = (int) branchNodePrefixLength - (mismatchPos + 1);
        // move the remained common prefix of the initial internal node
        // as the new prefix is always > 0, we just allocate and fill the new prefix
        branchNode.prefix = Arrays.copyOfRange(branchNode.prefix,mismatchPos + 1, branchNodePrefixLength);

        LeafNode leafNode = new LeafNode(key, containerIdx);
        node4.insert(leafNode, key[mismatchPos + depth]);
        return node4;
      }
      depth += branchNodePrefixLength;
    }
    int pos = branchNode.getChildPos(key[depth]);
    if (pos != BranchNode.ILLEGAL_IDX) {
      // insert the key as current internal node's children's child node.
      Node child = branchNode.getChild(pos);
      Node freshOne = insert(child, key, depth + 1, containerIdx);
      if (freshOne != child) {
        branchNode.replaceNode(pos, freshOne);
      }
      return branchNode;
    }
    // insert the key as a child leaf node of the current internal node
    LeafNode leafNode = new LeafNode(key, containerIdx);
    return branchNode.insert(leafNode, key[depth]);
  }

  // find common prefix length
  static int commonPrefixLength(
      byte[] key1, int aFromIndex, int aToIndex, byte[] key2, int bFromIndex, int bToIndex) {
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

  private LeafNode getExtremeLeaf(boolean reverse) {
    Node parent = getRoot();
    for (int depth = 0; depth < AbstractShuttle.MAX_DEPTH; depth++) {
      if (parent instanceof BranchNode) {
        BranchNode branchNode = (BranchNode) parent;
        int childIndex = reverse ? branchNode.getMaxPos() : branchNode.getMinPos();
        parent = branchNode.getChild(childIndex);
      }
    }
    return (LeafNode) parent;
  }

  public LeafNode first() {
    return getExtremeLeaf(false);
  }

  public LeafNode last() {
    return getExtremeLeaf(true);
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

  public LeafNodeIterator leafNodeIteratorFrom(long bound, boolean reverse, Containers containers) {
    return new LeafNodeIterator(this, reverse, containers, bound);
  }

  private void serialize(Node node, DataOutput dataOutput) throws IOException {
    if (node instanceof BranchNode) {
      BranchNode branchNode = (BranchNode)node;
      // serialize the internal node itself first
      branchNode.serialize(dataOutput);
      // then all the internal node's children
      int nexPos = branchNode.getNextLargerPos(BranchNode.ILLEGAL_IDX);
      while (nexPos != BranchNode.ILLEGAL_IDX) {
        // serialize all the not null child node
        Node child = branchNode.getChild(nexPos);
        serialize(child, dataOutput);
        nexPos = branchNode.getNextLargerPos(nexPos);
      }
    } else {
      // serialize the leaf node
      node.serialize(dataOutput);
    }
  }

  private void serialize(Node node, ByteBuffer byteBuffer) throws IOException {
    if (node instanceof BranchNode) {
      BranchNode branchNode = (BranchNode)node;
      // serialize the internal node itself first
      branchNode.serialize(byteBuffer);
      // then all the internal node's children
      int nexPos = branchNode.getNextLargerPos(BranchNode.ILLEGAL_IDX);
      while (nexPos != BranchNode.ILLEGAL_IDX) {
        // serialize all the not null child node
        Node child = branchNode.getChild(nexPos);
        serialize(child, byteBuffer);
        nexPos = branchNode.getNextLargerPos(nexPos);
      }
    } else {
      // serialize the leaf node
      node.serialize(byteBuffer);
    }
  }

  private Node deserialize(DataInput dataInput) throws IOException {
    Node oneNode = Node.deserialize(dataInput);
    if (oneNode == null) {
      return null;
    }
    if (oneNode instanceof LeafNode) {
      return oneNode;
    } else {
      BranchNode branch = (BranchNode) oneNode;
      // internal node
      int count = branch.count;
      // all the not null child nodes
      Node[] children = new Node[count];
      for (int i = 0; i < count; i++) {
        Node child = deserialize(dataInput);
        children[i] = child;
      }
      branch.replaceChildren(children);
      return branch;
    }
  }

  private Node deserialize(ByteBuffer byteBuffer) throws IOException {
    Node oneNode = Node.deserialize(byteBuffer);
    if (oneNode == null) {
      return null;
    }
    if (oneNode instanceof LeafNode) {
      return oneNode;
    } else {
      BranchNode branchNode = (BranchNode) oneNode;
      // internal node
      int count = branchNode.count;
      // all the not null child nodes
      Node[] children = new Node[count];
      for (int i = 0; i < count; i++) {
        Node child = deserialize(byteBuffer);
        children[i] = child;
      }
      branchNode.replaceChildren(children);
      return branchNode;
    }
  }

  public long serializeSizeInBytes() {
    return serializeSizeInBytes(this.root) + 8;
  }

  public long getKeySize() {
    return keySize;
  }

  private long serializeSizeInBytes(Node node) {
    if (node instanceof BranchNode) {
      BranchNode branchNode = (BranchNode) node;
      // serialize the internal node itself first
      int currentNodeSize = branchNode.serializeSizeInBytes();
      // then all the internal node's children
      long childrenTotalSize = 0L;
      int nexPos = branchNode.getNextLargerPos(BranchNode.ILLEGAL_IDX);
      while (nexPos != BranchNode.ILLEGAL_IDX) {
        // serialize all the not null child node
        Node child = branchNode.getChild(nexPos);
        long childSize = serializeSizeInBytes(child);
        nexPos = branchNode.getNextLargerPos(nexPos);
        childrenTotalSize += childSize;
      }
      return currentNodeSize + childrenTotalSize;
    } else {
      // serialize the leaf node
      int nodeSize = node.serializeSizeInBytes();
      return nodeSize;
    }
  }
}
