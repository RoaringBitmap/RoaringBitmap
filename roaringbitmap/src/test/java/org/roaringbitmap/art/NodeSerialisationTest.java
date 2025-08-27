package org.roaringbitmap.art;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.roaringbitmap.ArrayContainer;
import org.roaringbitmap.BitmapContainer;
import org.roaringbitmap.Container;
import org.roaringbitmap.RunContainer;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class NodeSerialisationTest {
    static Stream<Arguments> nodesData() {
        int leaf =  10 + smallContainer().serializedSizeInBytes(); // LeafNode with ArrayContainer with 1 element
        int leafWithPosition= leaf + 1; // LeafNode with ArrayContainer + 1 byte for position tracking
        return Stream.of(
                Arguments.of("leaf with empty array container", new LeafNode(1, new ArrayContainer(1)), leaf -2),
                Arguments.of("leaf with empty bitmap container", new LeafNode(1, new BitmapContainer()), 8204),
                Arguments.of("leaf with empty run container", new LeafNode(1, new RunContainer()), 10),

                Arguments.of("leaf with small array container", new LeafNode(1, new ArrayContainer(1,10)), leaf + 8 * 2),
                Arguments.of("leaf with small bitmap container", new LeafNode(1, new BitmapContainer(1,10)), 8204),
                Arguments.of("leaf with small run container", new LeafNode(1, new RunContainer(1,10)), 14),

                Arguments.of("leaf with full array container", new LeafNode(1, new ArrayContainer(0,65536)), leaf + 65535 * 2),
                Arguments.of("leaf with full bitmap container", new LeafNode(1, new BitmapContainer(0,65536)), 8204),
                Arguments.of("leaf with full run container", new LeafNode(1, new RunContainer(0,65536)), 14),

                Arguments.of("node4 prefix 0 with 2 leaves", populateBranchNode(new Node4(0), 2), 2 * leafWithPosition + 2),
                Arguments.of("node4 prefix 2 with 2 leaves", populateBranchNode(new Node4(2), 2), 2 * leafWithPosition + 4),
                Arguments.of("node4 prefix 0 with 4 leaves", populateBranchNode(new Node4(0), 4), 4 * leafWithPosition + 2),

                Arguments.of("node16 prefix 0 with 5 leaves", populateBranchNode(new Node16(0), 5), 5 * leafWithPosition + 2),
                Arguments.of("node16 prefix 2 with 5 leaves", populateBranchNode(new Node16(2), 5), 5 * leafWithPosition + 4),
                Arguments.of("node16 prefix 0 with 16 leaves", populateBranchNode(new Node16(0), 16), 16 * leafWithPosition + 2),

                Arguments.of("node48 prefix 0 with 17 leaves", populateBranchNode(new Node48(0), 17), 17 * leafWithPosition + 2),
                Arguments.of("node48 prefix 2 with 17 leaves", populateBranchNode(new Node48(2), 17), 17 * leafWithPosition + 4),
                Arguments.of("node48 prefix 0 with 48 leaves", populateBranchNode(new Node48(0), 48), 48 * leafWithPosition + 2),

                Arguments.of("node256 prefix 0 with 49 leaves", populateBranchNode(new Node256(0), 49), 49 * leafWithPosition + 2),
                Arguments.of("node256 prefix 2 with 49 leaves", populateBranchNode(new Node256(2), 49), 49 * leafWithPosition + 4),
                Arguments.of("node256 prefix 0 with 256 leaves", populateBranchNode(new Node256(0), 256), 256 * leafWithPosition + 2)
        );
    }

    private static Container smallContainer() {
        return (new ArrayContainer(1)).add((char) 0);
    }

    private static BranchNode populateBranchNode(BranchNode node, int childCount) {
        for (long i = 0; i < childCount; i++) {
            Node check = node.insert(new LeafNode(i << 16, new ArrayContainer(1).add((char)0)), (byte) i);
            assertSame(node, check);
        }
        return node;
    }

    @MethodSource("nodesData")
    @ParameterizedTest
    public void checkSizeVsExpected(String desc, Node node, int expectedSize) throws IOException {

        assertEquals(expectedSize, node.serializeSizeInBytes(), desc+" expected size mismatch");
    }
    @MethodSource("nodesData")
    @ParameterizedTest
    public void checkSizeVsByteBuffer(String desc, Node node, int expectedSize) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(expectedSize * 2);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        node.serialize(buffer);
        assertEquals(expectedSize, buffer.position(), desc+" byte buffer size mismatch");
    }
    @MethodSource("nodesData")
    @ParameterizedTest
    public void checkSizeVsDataOutput(String desc, Node node, int expectedSize) throws IOException {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput dos = new java.io.DataOutputStream(baos);
        node.serialize(dos);
        assertEquals(expectedSize, baos.size(), desc+" byte buffer size mismatch");
    }

    @MethodSource("nodesData")
    @ParameterizedTest
    public void checkFormatsAlign(String desc, Node node, int expectedSize) throws IOException {

        ByteBuffer buffer = ByteBuffer.allocate(expectedSize * 2);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        node.serialize(buffer);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput dos = new java.io.DataOutputStream(baos);
        node.serialize(dos);

        assertArrayEquals(Arrays.copyOf(buffer.array(),buffer.position()), baos.toByteArray(), desc+" serialized formats mismatch");
    }
    @MethodSource("nodesData")
    @ParameterizedTest
    public void roundTripBuffer(String desc, Node node, int expectedSize) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput dos = new java.io.DataOutputStream(baos);
        node.serialize(dos);

        ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        Node deserialized = Node.deserialize(buffer);
        assertEquals(node, deserialized, desc+" roundtrip mismatch");
    }
    @MethodSource("nodesData")
    @ParameterizedTest
    public void roundTripStream(String desc, Node node, int expectedSize) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput dos = new java.io.DataOutputStream(baos);
        node.serialize(dos);

        ByteArrayInputStream ins = new ByteArrayInputStream(baos.toByteArray());
        Node deserialized = Node.deserialize(new DataInputStream(ins));
        assertEquals(node, deserialized, desc+" roundtrip mismatch");
        assertEquals(node.getClass(), deserialized.getClass(), desc+" roundtrip mismatch");

        if (node instanceof LeafNode) {
            LeafNode l1 = (LeafNode) node;
            LeafNode l2 = (LeafNode) deserialized;
            assertEquals(l1.getContainer().getClass(), l2.getContainer().getClass(), desc+" roundtrip mismatch");
        }
    }
}
