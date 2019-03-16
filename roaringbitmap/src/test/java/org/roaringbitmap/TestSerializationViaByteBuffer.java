package org.roaringbitmap;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TestSerializationViaByteBuffer {

  @Parameterized.Parameters
  public static Object[][] params() {
    return new Object[][] {
            { SeededTestData.randomBitmap(2), ByteOrder.BIG_ENDIAN },
            { SeededTestData.randomBitmap(2), ByteOrder.LITTLE_ENDIAN },
            { SeededTestData.randomBitmap(3), ByteOrder.BIG_ENDIAN },
            { SeededTestData.randomBitmap(3), ByteOrder.LITTLE_ENDIAN },
            { SeededTestData.randomBitmap(4), ByteOrder.BIG_ENDIAN },
            { SeededTestData.randomBitmap(4), ByteOrder.LITTLE_ENDIAN },
            { SeededTestData.randomBitmap(5), ByteOrder.BIG_ENDIAN },
            { SeededTestData.randomBitmap(5), ByteOrder.LITTLE_ENDIAN },
            { SeededTestData.randomBitmap(6), ByteOrder.BIG_ENDIAN },
            { SeededTestData.randomBitmap(6), ByteOrder.LITTLE_ENDIAN },
            { SeededTestData.randomBitmap(7), ByteOrder.BIG_ENDIAN },
            { SeededTestData.randomBitmap(7), ByteOrder.LITTLE_ENDIAN },
            { SeededTestData.randomBitmap(8), ByteOrder.BIG_ENDIAN },
            { SeededTestData.randomBitmap(8), ByteOrder.LITTLE_ENDIAN },
            { SeededTestData.randomBitmap(9), ByteOrder.BIG_ENDIAN },
            { SeededTestData.randomBitmap(9), ByteOrder.LITTLE_ENDIAN },
            { SeededTestData.randomBitmap(10), ByteOrder.BIG_ENDIAN },
            { SeededTestData.randomBitmap(10), ByteOrder.LITTLE_ENDIAN },
            { SeededTestData.randomBitmap(11), ByteOrder.BIG_ENDIAN },
            { SeededTestData.randomBitmap(11), ByteOrder.LITTLE_ENDIAN },
            { SeededTestData.randomBitmap(12), ByteOrder.BIG_ENDIAN },
            { SeededTestData.randomBitmap(12), ByteOrder.LITTLE_ENDIAN },
            { SeededTestData.randomBitmap(13), ByteOrder.BIG_ENDIAN },
            { SeededTestData.randomBitmap(13), ByteOrder.LITTLE_ENDIAN },
            { SeededTestData.randomBitmap(14), ByteOrder.BIG_ENDIAN },
            { SeededTestData.randomBitmap(14), ByteOrder.LITTLE_ENDIAN },
            { SeededTestData.randomBitmap(15), ByteOrder.BIG_ENDIAN },
            { SeededTestData.randomBitmap(15), ByteOrder.LITTLE_ENDIAN },
    };
  }

  private final RoaringBitmap input;
  private final ByteOrder order;
  private final byte[] serialised;

  public TestSerializationViaByteBuffer(RoaringBitmap input, ByteOrder order) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (DataOutputStream dos = new DataOutputStream(bos)) {
      input.serialize(new DataOutputStream(bos));
    }
    this.serialised = bos.toByteArray();
    this.order = order;
    this.input = input;
  }

  @Test
  public void testDeserializeFromMappedFile() throws IOException {
    Path file = Paths.get(System.getProperty("user.dir")).resolve(UUID.randomUUID().toString());
    Files.createFile(file);
    try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
      ByteBuffer buffer = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, serialised.length);
      buffer.put(serialised);
      buffer.flip();
      RoaringBitmap deserialised = new RoaringBitmap();
      deserialised.deserialize(buffer);
      assertEquals(input, deserialised);
    } finally {
       Files.delete(file);
    }
  }

  @Test
  public void testDeserializeFromHeap() throws IOException {
    ByteBuffer buffer = ByteBuffer.wrap(serialised).order(order);
    RoaringBitmap deserialised = new RoaringBitmap();
    deserialised.deserialize(buffer);
    assertEquals(input, deserialised);
  }
}
