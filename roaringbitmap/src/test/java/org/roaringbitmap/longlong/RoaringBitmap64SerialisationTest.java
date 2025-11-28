package org.roaringbitmap.longlong;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class RoaringBitmap64SerialisationTest {
    static Stream<Arguments> nodesData() {
        return Stream.of(
                Arguments.of("empty", sparseBits(0), 1),
                Arguments.of("sparse 1", sparseBits(1), 23),
                Arguments.of("sparse 10", sparseBits(10), 166),
                Arguments.of("sparse 100", sparseBits(100), 1533),
                Arguments.of("sparse 1000", sparseBits(1000), 15195),
                Arguments.of("sparse 10000", sparseBits(10000), 151811),
                Arguments.of("sparse 100000", sparseBits(100000), 1517969),
                Arguments.of("sparse 1000000", sparseBits(1000000), 15179528),

                Arguments.of("random 1", randomBits(1), 23),
                Arguments.of("random 10", randomBits(10), 161),
                Arguments.of("random 100", randomBits(100), 1604),
                Arguments.of("random 1000", randomBits(1000), 15287),
                Arguments.of("random 10000", randomBits(10000), 156444),
                Arguments.of("random 100000", randomBits(100000), 1551999),
                Arguments.of("random 1000000", randomBits(1000000), 15355276),

                Arguments.of("small packed", small(), 16426),
                Arguments.of("packed", packed(), 34161)
                );
    }

    private static Supplier<Roaring64Bitmap> small() {
        return () -> {
            Roaring64Bitmap rb1 = new Roaring64Bitmap();

            for (long x = 0; x < 100000; x++) {
                rb1.addLong(x);
            }
            return rb1;
        };
    }
    private static Supplier<Roaring64Bitmap> packed() {
        return () -> {
            Roaring64Bitmap rb1 = new Roaring64Bitmap();

            for (long x = 0; x < 100000; x++) {
                rb1.addLong(x);
            }

            for (long x = 100000; x < 200000; x += 100) {
                rb1.addLong(x);
            }

            for (long x = 200000; x < 300000; x += 2) {
                rb1.addLong(x);
            }
            return rb1;
        };
    }
    private static Supplier<Roaring64Bitmap> sparseBits(int size) {
        return () -> {
            Roaring64Bitmap rb = new Roaring64Bitmap();
            for (int i = 0; i < size; i++) {
                rb.add((long) i * 1_000_000);
            }
            return rb;
        };
    }

    private static Supplier<Roaring64Bitmap> randomBits(int size) {
        return () -> {
            Random r = new Random(0);
            Set<Long> dedup = new HashSet<>();
            while (dedup.size() < size) {
                dedup.add(r.nextLong() >>> 2);
            }
            Roaring64Bitmap rb = new Roaring64Bitmap();
            dedup.forEach(rb::add);
            return rb;
        };
    }

    @MethodSource("nodesData")
    @ParameterizedTest
    public void checkSizeVsExpected(String desc, Supplier<Roaring64Bitmap> bitmapSupplier, int expectedSize) throws IOException {
        Roaring64Bitmap bitmap = bitmapSupplier.get();

        assertEquals(expectedSize, bitmap.serializedSizeInBytes(), desc+" expected size mismatch");
    }
    @MethodSource("nodesData")
    @ParameterizedTest
    public void checkSizeVsByteBufferLE(String desc, Supplier<Roaring64Bitmap> bitmapSupplier, int expectedSize) throws IOException {
        Roaring64Bitmap bitmap = bitmapSupplier.get();

        ByteBuffer buffer = ByteBuffer.allocate(expectedSize * 2);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        bitmap.serialize(buffer);
        assertEquals(expectedSize, buffer.position(), desc+" byte buffer size mismatch");
   }
    @MethodSource("nodesData")
    @ParameterizedTest
    public void checkSizeVsDataOutput(String desc, Supplier<Roaring64Bitmap> bitmapSupplier, int expectedSize) throws IOException {
        Roaring64Bitmap bitmap = bitmapSupplier.get();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput dos = new java.io.DataOutputStream(baos);
        bitmap.serialize(dos);
        assertEquals(expectedSize, baos.size(), desc+" byte buffer size mismatch");
    }

    @MethodSource("nodesData")
    @ParameterizedTest
    public void checkFormatsAlign(String desc, Supplier<Roaring64Bitmap> bitmapSupplier, int expectedSize) throws IOException {
        Roaring64Bitmap bitmap = bitmapSupplier.get();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput dos = new java.io.DataOutputStream(baos);
        bitmap.serialize(dos);

        ByteBuffer buffer = ByteBuffer.allocate(expectedSize * 2);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        bitmap.serialize(buffer);

        assertArrayEquals(Arrays.copyOf(buffer.array(),expectedSize), baos.toByteArray(), desc+" serialized formats mismatch");

        buffer = ByteBuffer.allocate(expectedSize * 2);
        buffer.order(ByteOrder.BIG_ENDIAN);
        bitmap.serialize(buffer);

        assertArrayEquals(Arrays.copyOf(buffer.array(),expectedSize), baos.toByteArray(), desc+" serialized formats mismatch");
    }
    @MethodSource("nodesData")
    @ParameterizedTest
    public void roundTripBufferLE(String desc, Supplier<Roaring64Bitmap> bitmapSupplier, int expectedSize) throws IOException {
        Roaring64Bitmap bitmap = bitmapSupplier.get();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput dos = new java.io.DataOutputStream(baos);
        bitmap.serialize(dos);

        ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        Roaring64Bitmap deserialized = new Roaring64Bitmap();
        deserialized.deserialize(buffer);
        assertEquals(bitmap, deserialized, desc+" roundtrip mismatch");
    }
    @MethodSource("nodesData")
    @ParameterizedTest
    public void roundTripBufferBE(String desc, Supplier<Roaring64Bitmap> bitmapSupplier, int expectedSize) throws IOException {
        Roaring64Bitmap bitmap = bitmapSupplier.get();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput dos = new java.io.DataOutputStream(baos);
        bitmap.serialize(dos);

        ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());
        buffer.order(ByteOrder.BIG_ENDIAN);
        Roaring64Bitmap deserialized = new Roaring64Bitmap();
        deserialized.deserialize(buffer);
        assertEquals(bitmap, deserialized, desc+" roundtrip mismatch");
    }
    @MethodSource("nodesData")
    @ParameterizedTest
    public void roundTripStream(String desc, Supplier<Roaring64Bitmap> bitmapSupplier, int expectedSize) throws IOException {
        Roaring64Bitmap bitmap = bitmapSupplier.get();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput dos = new java.io.DataOutputStream(baos);
        bitmap.serialize(dos);

        ByteArrayInputStream ins = new ByteArrayInputStream(baos.toByteArray());
        Roaring64Bitmap deserialized = new Roaring64Bitmap();
        deserialized.deserialize(new DataInputStream(ins));

        assertEquals(bitmap, deserialized, desc+" roundtrip mismatch");
    }
}
