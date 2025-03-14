package org.roaringbitmap;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.Files.delete;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.stream.Stream;

@Execution(ExecutionMode.CONCURRENT)
public class TestSerializationViaByteBuffer {

  @AfterAll
  public static void cleanup() {
    System.gc();
  }

  public static Stream<Arguments> params() {
    return Stream.of(
        Arguments.of(2, ByteOrder.BIG_ENDIAN, true),
        Arguments.of(2, ByteOrder.LITTLE_ENDIAN, true),
        Arguments.of(3, ByteOrder.BIG_ENDIAN, true),
        Arguments.of(3, ByteOrder.LITTLE_ENDIAN, true),
        Arguments.of(4, ByteOrder.BIG_ENDIAN, true),
        Arguments.of(4, ByteOrder.LITTLE_ENDIAN, true),
        Arguments.of(5, ByteOrder.BIG_ENDIAN, true),
        Arguments.of(5, ByteOrder.LITTLE_ENDIAN, true),
        Arguments.of(6, ByteOrder.BIG_ENDIAN, true),
        Arguments.of(6, ByteOrder.LITTLE_ENDIAN, true),
        Arguments.of(7, ByteOrder.BIG_ENDIAN, true),
        Arguments.of(7, ByteOrder.LITTLE_ENDIAN, true),
        Arguments.of(8, ByteOrder.BIG_ENDIAN, true),
        Arguments.of(8, ByteOrder.LITTLE_ENDIAN, true),
        Arguments.of(9, ByteOrder.BIG_ENDIAN, true),
        Arguments.of(9, ByteOrder.LITTLE_ENDIAN, true),
        Arguments.of(10, ByteOrder.BIG_ENDIAN, true),
        Arguments.of(10, ByteOrder.LITTLE_ENDIAN, true),
        Arguments.of(11, ByteOrder.BIG_ENDIAN, true),
        Arguments.of(11, ByteOrder.LITTLE_ENDIAN, true),
        Arguments.of(12, ByteOrder.BIG_ENDIAN, true),
        Arguments.of(12, ByteOrder.LITTLE_ENDIAN, true),
        Arguments.of(13, ByteOrder.BIG_ENDIAN, true),
        Arguments.of(13, ByteOrder.LITTLE_ENDIAN, true),
        Arguments.of(14, ByteOrder.BIG_ENDIAN, true),
        Arguments.of(14, ByteOrder.LITTLE_ENDIAN, true),
        Arguments.of(15, ByteOrder.BIG_ENDIAN, true),
        Arguments.of(15, ByteOrder.LITTLE_ENDIAN, true),
        Arguments.of(2, ByteOrder.BIG_ENDIAN, false),
        Arguments.of(2, ByteOrder.LITTLE_ENDIAN, false),
        Arguments.of(3, ByteOrder.BIG_ENDIAN, false),
        Arguments.of(3, ByteOrder.LITTLE_ENDIAN, false),
        Arguments.of(4, ByteOrder.BIG_ENDIAN, false),
        Arguments.of(4, ByteOrder.LITTLE_ENDIAN, false),
        Arguments.of(5, ByteOrder.BIG_ENDIAN, false),
        Arguments.of(5, ByteOrder.LITTLE_ENDIAN, false),
        Arguments.of(6, ByteOrder.BIG_ENDIAN, false),
        Arguments.of(6, ByteOrder.LITTLE_ENDIAN, false),
        Arguments.of(7, ByteOrder.BIG_ENDIAN, false),
        Arguments.of(7, ByteOrder.LITTLE_ENDIAN, false),
        Arguments.of(8, ByteOrder.BIG_ENDIAN, false),
        Arguments.of(8, ByteOrder.LITTLE_ENDIAN, false),
        Arguments.of(9, ByteOrder.BIG_ENDIAN, false),
        Arguments.of(9, ByteOrder.LITTLE_ENDIAN, false),
        Arguments.of(10, ByteOrder.BIG_ENDIAN, false),
        Arguments.of(10, ByteOrder.LITTLE_ENDIAN, false),
        Arguments.of(11, ByteOrder.BIG_ENDIAN, false),
        Arguments.of(11, ByteOrder.LITTLE_ENDIAN, false),
        Arguments.of(12, ByteOrder.BIG_ENDIAN, false),
        Arguments.of(12, ByteOrder.LITTLE_ENDIAN, false),
        Arguments.of(13, ByteOrder.BIG_ENDIAN, false),
        Arguments.of(13, ByteOrder.LITTLE_ENDIAN, false),
        Arguments.of(14, ByteOrder.BIG_ENDIAN, false),
        Arguments.of(14, ByteOrder.LITTLE_ENDIAN, false),
        Arguments.of(15, ByteOrder.BIG_ENDIAN, false),
        Arguments.of(15, ByteOrder.LITTLE_ENDIAN, false));
  }

  private Path file;

  @BeforeEach
  public void before(@TempDir Path tempDir) throws IOException {
    this.file = tempDir;
    file = tempDir.resolve(UUID.randomUUID().toString());
    Files.createFile(file);
  }

  @AfterEach
  public void after() throws IOException {
    if (null != file) {
      if (!System.getProperty("os.name").toLowerCase().contains("windows")) {
        // nothing really works properly on Windows
        delete(file);
      }
    }
  }

  @ParameterizedTest(name = "{1}/{0} keys/runOptimise={2}")
  @MethodSource("params")
  public void testDeserializeFromMappedFile(int keys, ByteOrder order, boolean runOptimise)
      throws IOException {
    RoaringBitmap input = SeededTestData.randomBitmap(keys);
    byte[] serialised = serialise(input, runOptimise);
    try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
      ByteBuffer buffer = raf.getChannel().map(READ_WRITE, 0, serialised.length);
      buffer.put(serialised);
      buffer.flip();
      buffer.order(order);
      RoaringBitmap deserialised = new RoaringBitmap();
      deserialised.deserialize(buffer);
      assertTrue(deserialised.validate());
      assertEquals(input, deserialised);
    }
  }

  @ParameterizedTest(name = "{1}/{0} keys/runOptimise={2}")
  @MethodSource("params")
  public void testDeserializeFromHeap(int keys, ByteOrder order, boolean runOptimise)
      throws IOException {
    RoaringBitmap input = SeededTestData.randomBitmap(keys);
    byte[] serialised = serialise(input, runOptimise);
    ByteBuffer buffer = ByteBuffer.wrap(serialised).order(order);
    RoaringBitmap deserialised = new RoaringBitmap();
    deserialised.deserialize(buffer);
    assertTrue(deserialised.validate());
    assertEquals(input, deserialised);
  }

  @ParameterizedTest(name = "{1}/{0} keys/runOptimise={2}")
  @MethodSource("params")
  public void testDeserializeFromDirect(int keys, ByteOrder order, boolean runOptimise)
      throws IOException {
    RoaringBitmap input = SeededTestData.randomBitmap(keys);
    byte[] serialised = serialise(input, runOptimise);
    ByteBuffer buffer = ByteBuffer.allocateDirect(serialised.length).order(order);
    buffer.put(serialised);
    buffer.position(0);
    RoaringBitmap deserialised = new RoaringBitmap();
    deserialised.deserialize(buffer);
    assertTrue(deserialised.validate());
    assertEquals(input, deserialised);
  }

  @ParameterizedTest(name = "{1}/{0} keys/runOptimise={2}")
  @MethodSource("params")
  public void testDeserializeFromDirectWithOffset(int keys, ByteOrder order, boolean runOptimise)
      throws IOException {
    RoaringBitmap input = SeededTestData.randomBitmap(keys);
    byte[] serialised = serialise(input, runOptimise);
    ByteBuffer buffer = ByteBuffer.allocateDirect(10 + serialised.length).order(order);
    buffer.position(10);
    buffer.put(serialised);
    buffer.position(10);
    RoaringBitmap deserialised = new RoaringBitmap();
    deserialised.deserialize(buffer);
    assertTrue(deserialised.validate());
    assertEquals(input, deserialised);
  }

  @ParameterizedTest(name = "{1}/{0} keys/runOptimise={2}")
  @MethodSource("params")
  public void testSerializeCorrectOffset(int keys, ByteOrder order, boolean runOptimise)
      throws IOException {
    RoaringBitmap input = SeededTestData.randomBitmap(keys);
    byte[] serialised = serialise(input, runOptimise);
    ByteBuffer buffer = ByteBuffer.allocateDirect(10 + serialised.length).order(order);
    buffer.position(10);
    int serialisedSize = input.serializedSizeInBytes();
    input.serialize(buffer);
    assertEquals(10 + serialisedSize, buffer.position());
  }

  @ParameterizedTest(name = "{1}/{0} keys/runOptimise={2}")
  @MethodSource("params")
  public void testSerializeToByteBufferDeserializeViaStream(
      int keys, ByteOrder order, boolean runOptimise) throws IOException {
    RoaringBitmap input = SeededTestData.randomBitmap(keys);
    byte[] serialised = serialise(input, runOptimise);
    ByteBuffer buffer = ByteBuffer.allocate(serialised.length).order(order);
    input.serialize(buffer);
    assertEquals(0, buffer.remaining());
    RoaringBitmap roundtrip = new RoaringBitmap();
    try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(buffer.array()))) {
      roundtrip.deserialize(dis);
    }
    assertEquals(input, roundtrip);
  }

  @ParameterizedTest(name = "{1}/{0} keys/runOptimise={2}")
  @MethodSource("params")
  public void testSerializeToByteBufferDeserializeByteBuffer(
      int keys, ByteOrder order, boolean runOptimise) throws IOException {
    RoaringBitmap input = SeededTestData.randomBitmap(keys);
    byte[] serialised = serialise(input, runOptimise);
    ByteBuffer buffer = ByteBuffer.allocate(serialised.length).order(order);
    input.serialize(buffer);
    assertEquals(0, buffer.remaining());
    RoaringBitmap roundtrip = new RoaringBitmap();
    buffer.flip();
    roundtrip.deserialize(buffer);
    assertEquals(input, roundtrip);
  }

  private static byte[] serialise(RoaringBitmap input, boolean runOptimise) throws IOException {
    if (runOptimise) {
      input.runOptimize();
    }
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream(input.serializedSizeInBytes());
        DataOutputStream dos = new DataOutputStream(bos)) {
      input.serialize(dos);
      return bos.toByteArray();
    }
  }
}
