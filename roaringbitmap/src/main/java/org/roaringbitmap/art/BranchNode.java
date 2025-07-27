package org.roaringbitmap.art;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class BranchNode extends Node {

    // node type
    protected NodeType nodeType;
    // length of compressed path(prefix)
    protected byte prefixLength;
    // the compressed path path (prefix)
    protected byte[] prefix;
    // number of non-null children, the largest value will not beyond 255
    // to benefit calculation,we keep the value as a short type
    protected short count;
    public static final int ILLEGAL_IDX = -1;

    /**
     * constructor
     *
     * @param nodeType             the node type
     * @param compressedPrefixSize the prefix byte array size,less than or equal to 6
     */
    public BranchNode(NodeType nodeType, int compressedPrefixSize) {
        super();
        this.nodeType = nodeType;
        this.prefixLength = (byte) compressedPrefixSize;
        prefix = new byte[prefixLength];
        count = 0;
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
     * @param current   current internal node
     * @param childNode the leaf node
     * @param key       the key byte reference to the child leaf node
     * @return an adaptive changed node of the input 'current' node
     */
    public static BranchNode insertLeaf(BranchNode current, Node childNode, byte key) {
        switch (current.nodeType) {
            case NODE4:
                return Node4.insert(current, childNode, key);
            case NODE16:
                return Node16.insert(current, childNode, key);
            case NODE48:
                return Node48.insert(current, childNode, key);
            case NODE256:
                return Node256.insert(current, childNode, key);
            default:
                throw new IllegalArgumentException("Not supported node type!");
        }
    }

    /**
     * copy the prefix between two nodes
     *
     * @param src the source node
     * @param dst the destination node
     */
    public static void copyPrefix(BranchNode src, BranchNode dst) {
        dst.prefixLength = src.prefixLength;
        System.arraycopy(src.prefix, 0, dst.prefix, 0, src.prefixLength);
    }

    /**
     * replace the node's children according to the given children parameter while doing the
     * deserialization phase.
     *
     * @param children all the not null children nodes in key byte ascending order,no null element
     */
    abstract void replaceChildren(Node[] children);

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

    protected void serializeHeader(DataOutput dataOutput) throws IOException {
        // first byte: node type
        dataOutput.writeByte((byte) this.nodeType.ordinal());
        // non null object count
        dataOutput.writeShort(Short.reverseBytes(this.count));
        dataOutput.writeByte(this.prefixLength);
        if (prefixLength > 0) {
            dataOutput.write(this.prefix, 0, this.prefixLength);
        }
    }

    protected void serializeHeader(ByteBuffer byteBuffer) throws IOException {
        byteBuffer.put((byte) this.nodeType.ordinal());
        byteBuffer.putShort(this.count);
        byteBuffer.put(this.prefixLength);
        if (prefixLength > 0) {
            byteBuffer.put(this.prefix, 0, prefixLength);
        }
    }

    protected int serializeHeaderSizeInBytes() {
        return super.serializeHeaderSizeInBytes() + prefixLength;
    }


}
