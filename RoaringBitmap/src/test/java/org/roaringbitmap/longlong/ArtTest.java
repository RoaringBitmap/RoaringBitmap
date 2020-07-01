package org.roaringbitmap.longlong;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.art.Art;
import org.roaringbitmap.art.LeafNode;
import org.roaringbitmap.art.LeafNodeIterator;
import org.roaringbitmap.art.Node;

public class ArtTest {

  //one leaf node
  @Test
  public void testLeafNode() {
    byte[] key1 = new byte[]{1, 2, 3, 4, 5, 0};
    Art art = new Art();
    insert5PrefixCommonBytesIntoArt(art, 1);
    LeafNodeIterator leafNodeIterator = art.leafNodeIterator(false, null);
    boolean hasNext = leafNodeIterator.hasNext();
    Assertions.assertTrue(hasNext);
    LeafNode leafNode = leafNodeIterator.next();
    Assertions.assertTrue(BytesUtil.same(leafNode.getKeyBytes(), key1));
    Assertions.assertTrue(leafNode.getContainerIdx() == 0);
    hasNext = leafNodeIterator.hasNext();
    Assertions.assertTrue(!hasNext);
    art.remove(key1);
    Assertions.assertTrue(art.findByKey(key1) == Node.ILLEGAL_IDX);
  }

  //one node4 with two leaf nodes
  @Test
  public void testNode4() {
    byte[] key1 = new byte[]{1, 2, 3, 4, 5, 0};
    byte[] key2 = new byte[]{1, 2, 3, 4, 5, 1};
    Art art = new Art();
    insert5PrefixCommonBytesIntoArt(art, 2);
    LeafNodeIterator leafNodeIterator = art.leafNodeIterator(false, null);
    boolean hasNext = leafNodeIterator.hasNext();
    Assertions.assertTrue(hasNext);
    LeafNode leafNode = leafNodeIterator.next();
    Assertions.assertTrue(BytesUtil.same(leafNode.getKeyBytes(), key1));
    Assertions.assertEquals(0, leafNode.getContainerIdx());
    hasNext = leafNodeIterator.hasNext();
    Assertions.assertTrue(hasNext);
    leafNode = leafNodeIterator.next();
    Assertions.assertTrue(BytesUtil.same(leafNode.getKeyBytes(), key2));
    Assertions.assertEquals(1, leafNode.getContainerIdx());
    hasNext = leafNodeIterator.hasNext();
    Assertions.assertTrue(!hasNext);
    art.remove(key1);
    //shrink to leaf node
    long containerIdx2 = art.findByKey(key2);
    Assertions.assertEquals(1, containerIdx2);
  }

  //1 node16
  @Test
  public void testNode16() throws Exception {
    byte[] key1 = new byte[]{1, 2, 3, 4, 5, 0};
    byte[] key2 = new byte[]{1, 2, 3, 4, 5, 1};
    byte[] key3 = new byte[]{1, 2, 3, 4, 5, 2};
    byte[] key4 = new byte[]{1, 2, 3, 4, 5, 3};
    byte[] key5 = new byte[]{1, 2, 3, 4, 5, 4};
    Art art = new Art();
    insert5PrefixCommonBytesIntoArt(art, 5);
    LeafNodeIterator leafNodeIterator = art.leafNodeIterator(false, null);
    boolean hasNext = leafNodeIterator.hasNext();
    Assertions.assertTrue(hasNext);
    LeafNode leafNode = leafNodeIterator.next();
    Assertions.assertTrue(BytesUtil.same(leafNode.getKeyBytes(), key1));
    Assertions.assertEquals(0, leafNode.getContainerIdx());
    hasNext = leafNodeIterator.hasNext();
    Assertions.assertTrue(hasNext);
    leafNode = leafNodeIterator.next();
    Assertions.assertTrue(BytesUtil.same(leafNode.getKeyBytes(), key2));
    Assertions.assertEquals(1, leafNode.getContainerIdx());
    hasNext = leafNodeIterator.hasNext();
    Assertions.assertTrue(hasNext);
    long containerIdx = art.findByKey(key4);
    Assertions.assertEquals(3, containerIdx);
    containerIdx = art.findByKey(key5);
    Assertions.assertEquals(4, containerIdx);
    //ser/deser
    int sizeInBytes = (int) art.serializeSizeInBytes();
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    art.serializeArt(dataOutputStream);
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(
        byteArrayOutputStream.toByteArray());
    DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);
    Art deserArt = new Art();
    deserArt.deserializeArt(dataInputStream);
    Assertions.assertEquals(art.findByKey(key5), deserArt.findByKey(key5));

    ByteBuffer byteBuffer = ByteBuffer.allocate(sizeInBytes).order(ByteOrder.LITTLE_ENDIAN);
    art.serializeArt(byteBuffer);
    byteBuffer.flip();
    deserArt.deserializeArt(byteBuffer);
    Assertions.assertEquals(art.findByKey(key5), deserArt.findByKey(key5));

    art.remove(key5);
    art.remove(key4);
    //shrink to node4
    long containerIdx4 = art.findByKey(key3);
    Assertions.assertEquals(2, containerIdx4);
  }

  //node48
  @Test
  public void testNode48() throws Exception {
    Art art = new Art();
    insert5PrefixCommonBytesIntoArt(art, 17);
    byte[] key = new byte[]{1, 2, 3, 4, 5, 0};
    long containerIdx = art.findByKey(key);
    Assertions.assertTrue(containerIdx == 0);
    key = new byte[]{1, 2, 3, 4, 5, 10};
    containerIdx = art.findByKey(key);
    Assertions.assertTrue(containerIdx == 10);
    key = new byte[]{1, 2, 3, 4, 5, 12};
    containerIdx = art.findByKey(key);
    Assertions.assertTrue(containerIdx == 12);
    byte[] key13 = new byte[]{1, 2, 3, 4, 5, 12};
    //ser/deser
    int sizeInBytes = (int) art.serializeSizeInBytes();
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(sizeInBytes);
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    art.serializeArt(dataOutputStream);
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(
        byteArrayOutputStream.toByteArray());
    DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);
    Art deserArt = new Art();
    deserArt.deserializeArt(dataInputStream);
    Assertions.assertEquals(art.findByKey(key13), deserArt.findByKey(key13));

    ByteBuffer byteBuffer = ByteBuffer.allocate(sizeInBytes).order(ByteOrder.LITTLE_ENDIAN);
    art.serializeArt(byteBuffer);
    byteBuffer.flip();
    deserArt.deserializeArt(byteBuffer);
    Assertions.assertEquals(art.findByKey(key13), deserArt.findByKey(key13));
    Assertions.assertEquals(art.getKeySize(), deserArt.getKeySize());

    //shrink to node16
    for (int i = 0; i < 6; i++) {
      key13[5] = (byte) (12 + i);
      art.remove(key13);
    }
    byte[] key12 = new byte[]{1, 2, 3, 4, 5, 11};
    long containerIdx12 = art.findByKey(key12);
    Assertions.assertEquals(11, containerIdx12);
  }

  //node256
  @Test
  public void testNode256() throws IOException {
    Art art = new Art();
    insert5PrefixCommonBytesIntoArt(art, 50);
    byte[] key = new byte[]{1, 2, 3, 4, 5, 0};
    long containerIdx = art.findByKey(key);
    Assertions.assertTrue(containerIdx == 0);
    key = new byte[]{1, 2, 3, 4, 5, 10};
    containerIdx = art.findByKey(key);
    Assertions.assertEquals(10, containerIdx);
    key = new byte[]{1, 2, 3, 4, 5, 16};
    containerIdx = art.findByKey(key);
    Assertions.assertTrue(containerIdx == 16);
    key = new byte[]{1, 2, 3, 4, 5, 36};
    containerIdx = art.findByKey(key);
    Assertions.assertTrue(containerIdx == 36);
    key = new byte[]{1, 2, 3, 4, 5, 51};
    containerIdx = art.findByKey(key);
    Assertions.assertTrue(containerIdx == Node.ILLEGAL_IDX);
    long sizeInBytesL = art.serializeSizeInBytes();
    int sizeInBytesI = (int) sizeInBytesL;
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(sizeInBytesI);
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    art.serializeArt(dataOutputStream);
    Assertions.assertEquals(sizeInBytesI, byteArrayOutputStream.toByteArray().length);
    Art deserArt = new Art();
    DataInputStream dataInputStream = new DataInputStream(
        new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));
    deserArt.deserializeArt(dataInputStream);
    key = new byte[]{1, 2, 3, 4, 5, 36};
    containerIdx = deserArt.findByKey(key);
    Assertions.assertEquals(36, containerIdx);

    ByteBuffer byteBuffer = ByteBuffer.allocate(sizeInBytesI).order(ByteOrder.LITTLE_ENDIAN);
    art.serializeArt(byteBuffer);
    byteBuffer.flip();
    Art deserBBOne = new Art();
    deserBBOne.deserializeArt(byteBuffer);
    containerIdx = deserBBOne.findByKey(key);
    Assertions.assertEquals(36, containerIdx);

    //shrink to node48
    deserArt.remove(key);
    key = new byte[]{1, 2, 3, 4, 5, 10};
    containerIdx = deserArt.findByKey(key);
    Assertions.assertTrue(containerIdx == 10);
  }

  private void insert5PrefixCommonBytesIntoArt(Art art, int keyNum) {
    byte[] key = new byte[]{1, 2, 3, 4, 5, 0};
    byte b = 0;
    long containerIdx = 0;
    for (int i = 0; i < keyNum; i++) {
      key[5] = b;
      art.insert(key, containerIdx);
      key = new byte[]{1, 2, 3, 4, 5, 0};
      b++;
      containerIdx++;
    }
  }
}
