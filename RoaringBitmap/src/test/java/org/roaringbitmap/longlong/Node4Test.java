package org.roaringbitmap.longlong;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.art.LeafNode;
import org.roaringbitmap.art.Node;
import org.roaringbitmap.art.Node4;

public class Node4Test {

  @Test
  public void test1() throws IOException {
    LeafNode leafNode1 = new LeafNode(1, 1);
    LeafNode leafNode2 = new LeafNode(2, 2);
    LeafNode leafNode3 = new LeafNode(3, 3);
    Node4 node4 = new Node4(0);
    byte key1 = 2;
    Node4.insert(node4, leafNode1, key1);
    Assertions.assertTrue(node4.getMaxPos() == 0);
    Assertions.assertTrue(node4.getMinPos() == 0);
    Assertions.assertTrue(node4.getChildPos(key1) == 0);
    byte key2 = 1;
    node4 = (Node4) Node4.insert(node4, leafNode2, key2);
    Assertions.assertTrue(node4.getChildPos(key2) == 0);
    Assertions.assertTrue(node4.getChildPos(key1) == 1);
    byte key3 = -1;
    node4 = (Node4) Node4.insert(node4, leafNode3, key3);
    Assertions.assertTrue(node4.getChildPos(key3) == 2);
    node4 = (Node4) node4.remove(1);
    Assertions.assertTrue(node4.getChildPos(key2) == 0);
    Assertions.assertTrue(node4.getChildPos(key3) == 1);
    Assertions.assertTrue(node4.getChildPos(key1) == Node.ILLEGAL_IDX);

    int bytesSize = node4.serializeSizeInBytes();
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    node4.serialize(dataOutputStream);
    Assertions.assertEquals(bytesSize, byteArrayOutputStream.toByteArray().length);
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(
        byteArrayOutputStream.toByteArray());
    DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);
    Node4 deserializedNode4 = (Node4) Node.deserialize(dataInputStream);
    Assertions.assertEquals(0, deserializedNode4.getChildPos(key2));
    Assertions.assertEquals(1, deserializedNode4.getChildPos(key3));
    Assertions.assertEquals(Node.ILLEGAL_IDX, deserializedNode4.getChildPos(key1));

    Node node = node4.remove(0);
    Assertions.assertTrue(node instanceof LeafNode);
  }
}
