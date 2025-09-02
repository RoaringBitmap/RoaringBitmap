package org.roaringbitmap.art;

import org.roaringbitmap.longlong.HighLowContainer;

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public abstract class BranchNode extends Node {

    // the compressed path path (prefix)
    protected byte[] prefix;
    // number of non-null children, the largest value will not beyond 255
    // to benefit calculation,we keep the value as a short type
    protected short count;
    public static final int ILLEGAL_IDX = -1;

    /**
     * constructor
     *
     * @param compressedPrefixSize the prefix byte array size,less than or equal to 6
     */
    public BranchNode(int compressedPrefixSize) {
        super();
        prefix = compressedPrefixSize == 0 ? Art.EMPTY_BYTES : new byte[compressedPrefixSize];
        count = 0;
    }
    public void postClone(BranchNode newlyCloned, Node[] oldChildren, Node[] newChildren) {
        newlyCloned.count = this.count;
        //we could potentially share the prefix, but it fragile and would safe so little
        if (this.prefix.length > 0) {
            newlyCloned.prefix = Arrays.copyOf(this.prefix, this.prefix.length);
        }
        for (int i = 0; i < oldChildren.length; i++) {
            if (oldChildren[i] != null) {
                newChildren[i] = oldChildren[i].clone();;
            }
        }
    }
    protected abstract NodeType nodeType();
    // length of compressed path(prefix)
    protected byte prefixLength() {
        return (byte) prefix.length;
    }

    /**
     * search the position of the input byte key in the node's key byte array part
     *
     * @param key the input key byte array
     * @param fromIndex inclusive
     * @param toIndex exclusive
     * @param k the target key byte value
     * @return the array offset of the target input key 'k' or -1 to not found
     */
    public static int binarySearch(byte[] key, int fromIndex, int toIndex, byte k) {
        int inputUnsignedByte = Byte.toUnsignedInt(k);
        int low = fromIndex;
        int high = toIndex - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            int midVal = Byte.toUnsignedInt(key[mid]);

            if (midVal < inputUnsignedByte) {
                low = mid + 1;
            } else if (midVal > inputUnsignedByte) {
                high = mid - 1;
            } else {
                return mid; // key found
            }
        }
        // key not found.
        return ILLEGAL_IDX;
    }

    static SearchResult binarySearchWithResult(byte[] key, int fromIndex, int toIndex, byte k) {
        int inputUnsignedByte = Byte.toUnsignedInt(k);
        int low = fromIndex;
        int high = toIndex - 1;

        while (low != high) {
            int mid = (low + high + 1) >>> 1; // ceil
            int midVal = Byte.toUnsignedInt(key[mid]);

            if (midVal > inputUnsignedByte) {
                high = mid - 1;
            } else {
                low = mid;
            }
        }
        int val = Byte.toUnsignedInt(key[low]);
        if (val == inputUnsignedByte) {
            return SearchResult.found(low);
        } else if (val < inputUnsignedByte) {
            int highIndex = low + 1;
            return SearchResult.notFound(low, highIndex < toIndex ? highIndex : BranchNode.ILLEGAL_IDX);
        } else {
            return SearchResult.notFound(low - 1, low); // low - 1 == ILLEGAL_IDX if low == 0
        }
    }

    /**
     * insert the LeafNode as a child of the current internal node
     *
     * @param childNode the leaf node
     * @param key       the key byte reference to the child leaf node
     * @return an adaptive changed node of the input 'current' node
     */
    protected abstract BranchNode insert(Node childNode, byte key);
    /**
     * copy the prefix between two nodes
     *
     * @param src the source node
     * @param dst the destination node
     */
    public static void copyPrefix(BranchNode src, BranchNode dst) {
        System.arraycopy(src.prefix, 0, dst.prefix, 0, src.prefixLength());
    }

    /**
     * get the position of a child corresponding to the input key 'k'
     *
     * @param k a key value of the byte range
     * @return the child position corresponding to the key 'k'
     */
    public abstract int getChildPos(byte k);

    /**
     * get the position of a child corresponding to the input key 'k' if present
     * <p>
     * if 'k' is not in the child, return the positions of the neighbouring nodes instead
     *
     * @param key a key value of the byte range
     * @return a result indicating whether or not the key was found and the positions of the
     * child corresponding to it or its neighbours
     */
    public abstract SearchResult getNearestChildPos(byte key);

    /**
     * get the corresponding key byte of the requested position
     *
     * @param pos the position
     * @return the corresponding key byte
     */
    public abstract byte getChildKey(int pos);

    /**
     * get the child at the specified position in the node, the 'pos' range from 0 to count
     *
     * @param pos the position
     * @return a Node corresponding to the input position
     */
    public abstract Node getChild(int pos);
    /**
     * get the child at the specified key in the node.
     * the behavior is equivalent to {@code
     *     int pos = getChildPos(key);
     *     return (pos != ILLEGAL_IDX) ? getChild(pos) : null;
     * }
     * but subclasses may be able to provide a more efficient implementation
     *
     * @param key the position
     * @return a Node corresponding to the input position, or null if not found
     */
    public abstract Node getChildAtKey(byte key);

    /**
     * replace the position child to the fresh one
     *
     * @param pos      the position
     * @param freshOne the fresh node to replace the old one
     */
    public abstract void replaceNode(int pos, Node freshOne);

    /**
     * get the position of the min element in current node.
     *
     * @return the minimum key's position
     */
    public abstract int getMinPos();

    /**
     * get the next position in the node
     *
     * @param pos current position,-1 to start from the min one
     * @return the next larger byte key's position which is close to 'pos' position,-1 for end
     */
    public abstract int getNextLargerPos(int pos);

    /**
     * get the max child's position
     *
     * @return the max byte key's position
     */
    public abstract int getMaxPos();

    /**
     * get the next smaller element's position
     *
     * @param pos the position,-1 to start from the largest one
     * @return the next smaller key's position which is close to input 'pos' position,-1 for end
     */
    public abstract int getNextSmallerPos(int pos);

    /**
     * remove the specified position child
     *
     * @param pos the position to remove
     * @return an adaptive changed fresh node of the current node
     */
    public abstract Node remove(int pos);

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof BranchNode)) {
            return false;
        }
        BranchNode other = (BranchNode) obj;
        if (this.count != other.count) {
            return false;
        }
        if (!(Arrays.equals(this.prefix, other.prefix))) {
            return false;
        }
        int pos = ILLEGAL_IDX;
        while ((pos = this.getNextLargerPos(pos)) != ILLEGAL_IDX) {
            byte key = this.getChildKey(pos);
            int otherPos = other.getChildPos(key);
            if (otherPos == ILLEGAL_IDX) {
                return false;
            }
            Node child = this.getChild(pos);
            Node otherChild = other.getChild(otherPos);
            if (!child.equals(otherChild)) {
                return false;
            }
        }
        return true;
    }

    @Override
    long serializeSizeInBytes(HighLowContainer highLow) {
      // count, prefix length + prefix
      long size = 1 + 1 +  prefixLength();
      for (int pos = getNextLargerPos(-1);
           pos != BranchNode.ILLEGAL_IDX;
           pos = getNextLargerPos(pos)) {
        //  key and node
        size += 1 // key
            + getChild(pos).serializeSizeInBytes(highLow); // node
      }
      return size;
    }

    @Override
    void serializeBody(DataOutput dataOutput, HighLowContainer highLow) throws IOException {
      // write the prefix length and prefix
      dataOutput.writeByte(prefixLength());
      dataOutput.write(prefix);
      // serialise each child node and index
      for (int pos =  getNextLargerPos(-1);
           pos != BranchNode.ILLEGAL_IDX;
           pos =  getNextLargerPos(pos)) {
        // write the key and node
        dataOutput.writeByte( getChildKey(pos));
        getChild(pos).serialize(dataOutput, highLow);
      }
    }
    @Override
    void serializeBody(ByteBuffer byteBuffer, HighLowContainer highLow) throws IOException {
      // write the prefix length and prefix
      byteBuffer.put(prefixLength());
      byteBuffer.put(prefix);
      // serialise each child node and index
      for (int pos =  getNextLargerPos(-1);
           pos != BranchNode.ILLEGAL_IDX;
           pos =  getNextLargerPos(pos)) {
        // write the key and node
        byteBuffer.put( getChildKey(pos));
        getChild(pos).serialize(byteBuffer, highLow);
      }
    }
    public static BranchNode deserializeBody(DataInput dataInput, int size, HighLowContainer highLow) throws IOException {
      int prefixLength = dataInput.readByte() & 0xFF ;
      BranchNode result;
      if (size <= 4) {
        result = new Node4(prefixLength);
      } else if (size <= 16) {
        result = new Node16(prefixLength);
      } else if (size <= 48) {
        result = new Node48(prefixLength);
      } else {
        result = new Node256(prefixLength);
      }
      dataInput.readFully(result.prefix);
      for (int i = 0; i < size; i++) {
        byte key = dataInput.readByte();
        Node child = Node.deserialize(dataInput, highLow);
        result.insert(child, key);
      }
      return result;
    }

    public static BranchNode deserializeBody(ByteBuffer byteBuffer, int size, HighLowContainer highLow) throws IOException {
      int prefixLength = byteBuffer.get() & 0xFF;
      BranchNode result;
      if (size <= 4) {
        result = new Node4(prefixLength);
      } else if (size <= 16) {
        result = new Node16(prefixLength);
      } else if (size <= 48) {
        result = new Node48(prefixLength);
      } else {
        result = new Node256(prefixLength);
      }
      byteBuffer.get(result.prefix);
      for (int i = 0; i < size; i++) {
        byte key = byteBuffer.get();
        Node child = Node.deserialize(byteBuffer, highLow);
        result.insert(child, key);
      }
      return result;
    }

}
