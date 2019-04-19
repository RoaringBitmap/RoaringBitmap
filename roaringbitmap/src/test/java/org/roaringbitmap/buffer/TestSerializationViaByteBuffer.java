package org.roaringbitmap.buffer;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.roaringbitmap.DirectoryCleanup;
import org.roaringbitmap.SeededTestData;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.Files.delete;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TestSerializationViaByteBuffer {

  private static Path dir;

  @BeforeClass
  public static void setup() throws IOException {
    dir = Paths.get(System.getProperty("user.dir")).resolve("target").resolve(UUID.randomUUID().toString());
    Files.createDirectory(dir);
  }

  @AfterClass
  public static void cleanup() throws IOException {
    System.gc();
    try {
      Files.walkFileTree(dir, new DirectoryCleanup());
    } catch (IOException e) { }
  }

  @Parameterized.Parameters(name = "{1}/{0} keys/runOptimise={2}")
  public static Object[][] params() {
    return new Object[][]{
            {2, ByteOrder.BIG_ENDIAN, true},
            {2, ByteOrder.LITTLE_ENDIAN, true},
            {3, ByteOrder.BIG_ENDIAN, true},
            {3, ByteOrder.LITTLE_ENDIAN, true},
            {4, ByteOrder.BIG_ENDIAN, true,},
            {4, ByteOrder.LITTLE_ENDIAN, true},
            {5, ByteOrder.BIG_ENDIAN, true},
            {5, ByteOrder.LITTLE_ENDIAN, true},
            {6, ByteOrder.BIG_ENDIAN, true},
            {6, ByteOrder.LITTLE_ENDIAN, true},
            {7, ByteOrder.BIG_ENDIAN, true},
            {7, ByteOrder.LITTLE_ENDIAN, true},
            {8, ByteOrder.BIG_ENDIAN, true},
            {8, ByteOrder.LITTLE_ENDIAN, true},
            {9, ByteOrder.BIG_ENDIAN, true},
            {9, ByteOrder.LITTLE_ENDIAN, true},
            {10, ByteOrder.BIG_ENDIAN, true},
            {10, ByteOrder.LITTLE_ENDIAN, true},
            {11, ByteOrder.BIG_ENDIAN, true},
            {11, ByteOrder.LITTLE_ENDIAN, true},
            {12, ByteOrder.BIG_ENDIAN, true},
            {12, ByteOrder.LITTLE_ENDIAN, true},
            {13, ByteOrder.BIG_ENDIAN, true},
            {13, ByteOrder.LITTLE_ENDIAN, true},
            {14, ByteOrder.BIG_ENDIAN, true},
            {14, ByteOrder.LITTLE_ENDIAN, true},
            {15, ByteOrder.BIG_ENDIAN, true},
            {15, ByteOrder.LITTLE_ENDIAN, true},
            {2, ByteOrder.BIG_ENDIAN, false},
            {2, ByteOrder.LITTLE_ENDIAN, false},
            {3, ByteOrder.BIG_ENDIAN, false},
            {3, ByteOrder.LITTLE_ENDIAN, false},
            {4, ByteOrder.BIG_ENDIAN, false},
            {4, ByteOrder.LITTLE_ENDIAN, false},
            {5, ByteOrder.BIG_ENDIAN, false},
            {5, ByteOrder.LITTLE_ENDIAN, false},
            {6, ByteOrder.BIG_ENDIAN, false},
            {6, ByteOrder.LITTLE_ENDIAN, false},
            {7, ByteOrder.BIG_ENDIAN, false},
            {7, ByteOrder.LITTLE_ENDIAN, false},
            {8, ByteOrder.BIG_ENDIAN, false},
            {8, ByteOrder.LITTLE_ENDIAN, false},
            {9, ByteOrder.BIG_ENDIAN, false},
            {9, ByteOrder.LITTLE_ENDIAN, false},
            {10, ByteOrder.BIG_ENDIAN, false},
            {10, ByteOrder.LITTLE_ENDIAN, false},
            {11, ByteOrder.BIG_ENDIAN, false},
            {11, ByteOrder.LITTLE_ENDIAN, false},
            {12, ByteOrder.BIG_ENDIAN, false},
            {12, ByteOrder.LITTLE_ENDIAN, false},
            {13, ByteOrder.BIG_ENDIAN, false},
            {13, ByteOrder.LITTLE_ENDIAN, false},
            {14, ByteOrder.BIG_ENDIAN, false},
            {14, ByteOrder.LITTLE_ENDIAN, false},
            {15, ByteOrder.BIG_ENDIAN, false},
            {15, ByteOrder.LITTLE_ENDIAN, false}
    };
  }

  private final MutableRoaringBitmap input;
  private final ByteOrder order;
  private final byte[] serialised;

  public TestSerializationViaByteBuffer(int keys, ByteOrder order, boolean runOptimise) throws IOException {
    MutableRoaringBitmap input = SeededTestData.randomBitmap(keys).toMutableRoaringBitmap();
    if (runOptimise) {
      input.runOptimize();
    }
    this.input = input;
    this.order = order;
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream(input.serializedSizeInBytes());
         DataOutputStream dos = new DataOutputStream(bos)) {
      input.serialize(dos);
      this.serialised = bos.toByteArray();
    }
  }

  @Test
  public void testDeserializeFromMappedFile() throws IOException {
    Path file = dir.resolve(UUID.randomUUID().toString());
    Files.createFile(file);
    try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw");
         FileChannel channel = raf.getChannel()) {
      ByteBuffer buffer = channel.map(READ_WRITE, 0, serialised.length);
      buffer.put(serialised);
      buffer.flip();
      buffer.order(order);
      MutableRoaringBitmap deserialised = new MutableRoaringBitmap();
      deserialised.deserialize(buffer);
      assertEquals(input, deserialised);
    } finally {
      if (!System.getProperty("os.name").toLowerCase().contains("windows")) {
        // nothing really works properly on Windows
        delete(file);
      }
    }
  }


  @Test
  public void testSerializeMappedBitmap() throws IOException {
    Path file = dir.resolve(UUID.randomUUID().toString());
    Files.createFile(file);
    try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw");
         FileChannel channel = raf.getChannel()) {
      ByteBuffer buffer = channel.map(READ_WRITE, 0, serialised.length);
      buffer.put(serialised);
      buffer.flip();
      ImmutableRoaringBitmap bitmap = new ImmutableRoaringBitmap(buffer);
      ByteBuffer buf = ByteBuffer.allocate(bitmap.serializedSizeInBytes());
      bitmap.serialize(buf);
      buf.flip();
      ImmutableRoaringBitmap deserialised = new ImmutableRoaringBitmap(buf);
      assertEquals(bitmap, deserialised);
    } finally {
      if (!System.getProperty("os.name").toLowerCase().contains("windows")) {
        // nothing really works properly on Windows
        delete(file);
      }
    }
  }

  @Test
  public void testDeserializeFromHeap() throws IOException {
    ByteBuffer buffer = ByteBuffer.wrap(serialised).order(order);
    MutableRoaringBitmap deserialised = new MutableRoaringBitmap();
    deserialised.deserialize(buffer);
    assertEquals(input, deserialised);
  }

  @Test
  public void testDeserializeFromDirect() throws IOException {
    ByteBuffer buffer = ByteBuffer.allocateDirect(serialised.length).order(order);
    buffer.put(serialised);
    buffer.position(0);
    MutableRoaringBitmap deserialised = new MutableRoaringBitmap();
    deserialised.deserialize(buffer);
    assertEquals(input, deserialised);
  }

  @Test
  public void testDeserializeFromDirectWithOffset() throws IOException {
    ByteBuffer buffer = ByteBuffer.allocateDirect(10 + serialised.length).order(order);
    buffer.position(10);
    buffer.put(serialised);
    buffer.position(10);
    MutableRoaringBitmap deserialised = new MutableRoaringBitmap();
    deserialised.deserialize(buffer);
    assertEquals(input, deserialised);
  }

  @Test
  public void testSerializeCorrectOffset() {
    ByteBuffer buffer = ByteBuffer.allocateDirect(10 + serialised.length).order(order);
    buffer.position(10);
    int serialisedSize = input.serializedSizeInBytes();
    input.serialize(buffer);
    assertEquals(10 + serialisedSize, buffer.position());
  }

  @Test
  public void testSerializeToByteBufferDeserializeViaStream() throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(serialised.length).order(order);
    input.serialize(buffer);
    assertEquals(0, buffer.remaining());
    MutableRoaringBitmap roundtrip = new MutableRoaringBitmap();
    try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(buffer.array()))) {
      roundtrip.deserialize(dis);
    }
    assertEquals(input, roundtrip);
  }

  @Test
  public void testSerializeToByteBufferDeserializeByteBuffer() throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(serialised.length).order(order);
    input.serialize(buffer);
    assertEquals(0, buffer.remaining());
    MutableRoaringBitmap roundtrip = new MutableRoaringBitmap();
    buffer.flip();
    roundtrip.deserialize(buffer);
    assertEquals(input, roundtrip);
  }
}
