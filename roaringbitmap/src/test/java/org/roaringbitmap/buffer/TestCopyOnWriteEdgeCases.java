package org.roaringbitmap.buffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.ThrowingConsumer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Test edge cases and error conditions for copy-on-write functionality.
 */
public class TestCopyOnWriteEdgeCases {

  @Test
  public void testEmptyBitmap() throws IOException {
    // Test with empty bitmap
    MutableRoaringBitmap empty = new MutableRoaringBitmap();

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    empty.serialize(dos);
    dos.close();

    ByteBuffer bb = ByteBuffer.wrap(bos.toByteArray());
    ImmutableRoaringBitmap immutable = new ImmutableRoaringBitmap(bb);

    // Test copy-on-write with empty bitmap
    CopyOnWriteRoaringBitmap cow = immutable.toMutableRoaringBitmapCopyOnWrite();

    assertEquals(0, cow.getCardinality());
    assertTrue(cow.isEmpty());

    // Adding to empty bitmap should work
    cow.add(1);
    assertEquals(1, cow.getCardinality());
    assertTrue(cow.contains(1));

    // Original should still be empty
    assertEquals(0, immutable.getCardinality());
    assertTrue(immutable.isEmpty());
  }

  @Test
  public void testSingleElementBitmap() throws IOException {
    // Test with single element
    MutableRoaringBitmap single = new MutableRoaringBitmap();
    single.add(42);

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    single.serialize(dos);
    dos.close();

    ByteBuffer bb = ByteBuffer.wrap(bos.toByteArray());
    ImmutableRoaringBitmap immutable = new ImmutableRoaringBitmap(bb);

    CopyOnWriteRoaringBitmap cow = immutable.toMutableRoaringBitmapCopyOnWrite();

    assertEquals(1, cow.getCardinality());
    assertTrue(cow.contains(42));

    // Remove the only element
    cow.remove(42);
    assertEquals(0, cow.getCardinality());
    assertFalse(cow.contains(42));

    // Original should still have the element
    assertEquals(1, immutable.getCardinality());
    assertTrue(immutable.contains(42));
  }

  @Test
  public void testFullRangeContainer() throws IOException {
    // Test with a full range (bitmap container)
    MutableRoaringBitmap full = new MutableRoaringBitmap();
    for (int i = 0; i < 65536; i++) {
      full.add(i);
    }

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    full.serialize(dos);
    dos.close();

    ByteBuffer bb = ByteBuffer.wrap(bos.toByteArray());
    ImmutableRoaringBitmap immutable = new ImmutableRoaringBitmap(bb);

    CopyOnWriteRoaringBitmap cow = immutable.toMutableRoaringBitmapCopyOnWrite();

    assertEquals(65536, cow.getCardinality());
    assertTrue(cow.contains(0));
    assertTrue(cow.contains(32768));
    assertTrue(cow.contains(65535));

    // Modify the full container
    cow.remove(32768);
    assertEquals(65535, cow.getCardinality());
    assertFalse(cow.contains(32768));

    // Original should still be full
    assertEquals(65536, immutable.getCardinality());
    assertTrue(immutable.contains(32768));
  }

  @Test
  public void testSparseDistribution() throws IOException {
    // Test with sparse distribution across many containers
    MutableRoaringBitmap sparse = new MutableRoaringBitmap();
    for (int i = 0; i < 1000; i++) {
      sparse.add(i * 100000); // Spread across many containers
    }

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    sparse.serialize(dos);
    dos.close();

    ByteBuffer bb = ByteBuffer.wrap(bos.toByteArray());
    ImmutableRoaringBitmap immutable = new ImmutableRoaringBitmap(bb);

    CopyOnWriteRoaringBitmap cow = immutable.toMutableRoaringBitmapCopyOnWrite();

    assertEquals(1000, cow.getCardinality());

    // Modify elements in different containers
    cow.add(50000); // Container 0
    cow.add(150000); // Container 1
    cow.add(250000); // Container 2

    assertEquals(1003, cow.getCardinality());
    assertTrue(cow.contains(50000));
    assertTrue(cow.contains(150000));
    assertTrue(cow.contains(250000));

    // Original should be unchanged
    assertEquals(1000, immutable.getCardinality());
    assertFalse(immutable.contains(50000));
    assertFalse(immutable.contains(150000));
    assertFalse(immutable.contains(250000));
  }

  @Test
  public void testMultipleModificationsSameContainer() throws IOException {
    // Test multiple modifications to the same container
    MutableRoaringBitmap original = new MutableRoaringBitmap();
    original.add(1);
    original.add(2);
    original.add(3);

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    original.serialize(dos);
    dos.close();

    ByteBuffer bb = ByteBuffer.wrap(bos.toByteArray());
    ImmutableRoaringBitmap immutable = new ImmutableRoaringBitmap(bb);

    CopyOnWriteRoaringBitmap cow = immutable.toMutableRoaringBitmapCopyOnWrite();

    // Multiple modifications to the same container
    cow.add(4);
    cow.add(5);
    cow.remove(2);
    cow.flip(6);

    assertEquals(5, cow.getCardinality()); // 1,3,4,5,6
    assertTrue(cow.contains(1));
    assertFalse(cow.contains(2));
    assertTrue(cow.contains(3));
    assertTrue(cow.contains(4));
    assertTrue(cow.contains(5));
    assertTrue(cow.contains(6));

    // Original should be unchanged
    assertEquals(3, immutable.getCardinality());
    assertTrue(immutable.contains(1));
    assertTrue(immutable.contains(2));
    assertTrue(immutable.contains(3));
  }

  @Test
  public void testCloneOfCopyOnWrite() throws IOException {
    // Test cloning copy-on-write bitmaps
    MutableRoaringBitmap original = new MutableRoaringBitmap();
    original.add(1);
    original.add(2);
    original.add(3);

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    original.serialize(dos);
    dos.close();

    ByteBuffer bb = ByteBuffer.wrap(bos.toByteArray());
    ImmutableRoaringBitmap immutable = new ImmutableRoaringBitmap(bb);

    CopyOnWriteRoaringBitmap cow1 = immutable.toMutableRoaringBitmapCopyOnWrite();
    CopyOnWriteRoaringBitmap cow2 = cow1.clone();

    assertEquals(cow1.getCardinality(), cow2.getCardinality());
    assertEquals(cow1, cow2);

    // Modify one clone
    cow1.add(4);

    // Should not affect the other clone
    assertTrue(cow1.contains(4));
    assertFalse(cow2.contains(4));
    assertEquals(4, cow1.getCardinality());
    assertEquals(3, cow2.getCardinality());
  }

  @Test
  public void testOperationsOnCopyOnWrite() throws IOException {
    // Test set operations on copy-on-write bitmaps
    MutableRoaringBitmap original1 = new MutableRoaringBitmap();
    original1.add(1);
    original1.add(2);
    original1.add(3);

    MutableRoaringBitmap original2 = new MutableRoaringBitmap();
    original2.add(2);
    original2.add(3);
    original2.add(4);

    // Create copy-on-write bitmaps
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    original1.serialize(dos);
    dos.close();

    ByteBuffer bb = ByteBuffer.wrap(bos.toByteArray());
    ImmutableRoaringBitmap immutable = new ImmutableRoaringBitmap(bb);

    CopyOnWriteRoaringBitmap cow = immutable.toMutableRoaringBitmapCopyOnWrite();

    // Test operations
    cow.or(original2);
    assertEquals(4, cow.getCardinality());
    assertTrue(cow.contains(1));
    assertTrue(cow.contains(2));
    assertTrue(cow.contains(3));
    assertTrue(cow.contains(4));

    // Original should be unchanged
    assertEquals(3, immutable.getCardinality());
    assertTrue(immutable.contains(1));
    assertTrue(immutable.contains(2));
    assertTrue(immutable.contains(3));
    assertFalse(immutable.contains(4));
  }

  @Test
  public void testIterationAfterCopyOnWrite() throws IOException {
    // Test iteration after copy-on-write modifications
    MutableRoaringBitmap original = new MutableRoaringBitmap();
    for (int i = 0; i < 100; i++) {
      original.add(i);
    }

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    original.serialize(dos);
    dos.close();

    ByteBuffer bb = ByteBuffer.wrap(bos.toByteArray());
    ImmutableRoaringBitmap immutable = new ImmutableRoaringBitmap(bb);

    CopyOnWriteRoaringBitmap cow = immutable.toMutableRoaringBitmapCopyOnWrite();

    // Modify the bitmap
    cow.add(200);
    cow.remove(50);

    // Test iteration
    int count = 0;
    for (int value : cow) {
      count++;
    }

    assertEquals(100, count); // 100 original - 1 removed + 1 added
    assertTrue(cow.contains(200));
    assertFalse(cow.contains(50));

    // Original should still have 100 elements
    int originalCount = 0;
    for (int value : immutable) {
      originalCount++;
    }
    assertEquals(100, originalCount);
  }

  @Test
  public void testSerializationAfterCopyOnWrite() throws IOException {
    // Test serialization after copy-on-write modifications
    MutableRoaringBitmap original = new MutableRoaringBitmap();
    original.add(1);
    original.add(2);
    original.add(3);

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    original.serialize(dos);
    dos.close();

    ByteBuffer bb = ByteBuffer.wrap(bos.toByteArray());
    ImmutableRoaringBitmap immutable = new ImmutableRoaringBitmap(bb);

    CopyOnWriteRoaringBitmap cow = immutable.toMutableRoaringBitmapCopyOnWrite();

    // Modify the bitmap
    cow.add(4);
    cow.remove(2);

    // Serialize the modified bitmap
    ByteArrayOutputStream bos2 = new ByteArrayOutputStream();
    DataOutputStream dos2 = new DataOutputStream(bos2);
    cow.serialize(dos2);
    dos2.close();

    // Deserialize and check
    ByteBuffer bb2 = ByteBuffer.wrap(bos2.toByteArray());
    ImmutableRoaringBitmap deserialized = new ImmutableRoaringBitmap(bb2);

    assertEquals(3, deserialized.getCardinality());
    assertTrue(deserialized.contains(1));
    assertFalse(deserialized.contains(2));
    assertTrue(deserialized.contains(3));
    assertTrue(deserialized.contains(4));
  }

  @Test
  public void testCopyOnWriteWithRunOptimization() throws IOException {
    // Test copy-on-write with run optimization
    MutableRoaringBitmap original = new MutableRoaringBitmap();
    for (int i = 0; i < 10000; i++) {
      original.add(i);
    }
    original.runOptimize();

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    original.serialize(dos);
    dos.close();

    ByteBuffer bb = ByteBuffer.wrap(bos.toByteArray());
    ImmutableRoaringBitmap immutable = new ImmutableRoaringBitmap(bb);

    CopyOnWriteRoaringBitmap cow = immutable.toMutableRoaringBitmapCopyOnWrite();

    assertEquals(10000, cow.getCardinality());

    // Modify to break the run
    cow.remove(5000);

    assertEquals(9999, cow.getCardinality());
    assertFalse(cow.contains(5000));

    // Original should still have the run
    assertEquals(10000, immutable.getCardinality());
    assertTrue(immutable.contains(5000));
  }

  private static final int KEY2 = 2 * 65_536;
  private static final int KEY4 = 4 * 65_536;

  // key 0: an array container, key 2: a run container, key 4: a bitmap container
  private static MutableRoaringBitmap threeContainers() {
    MutableRoaringBitmap bitmap = MutableRoaringBitmap.bitmapOf(1, 2, 3);
    bitmap.add((long) KEY2, KEY2 + 1_000L);
    for (int i = 0; i < 10_000; i += 2) {
      bitmap.add(KEY4 + i);
    }
    bitmap.runOptimize();
    return bitmap;
  }

  private static ByteBuffer serialize(MutableRoaringBitmap bitmap) {
    ByteBuffer buffer = ByteBuffer.allocate(bitmap.serializedSizeInBytes());
    bitmap.serialize(buffer);
    buffer.flip();
    return buffer;
  }

  /**
   * Applies the write to a plain copy of the source and to copy-on-write bitmaps over three kinds of
   * source, which must all give the same result and leave the source unchanged.
   */
  private static void assertCopyOnWrite(
      String name, MutableRoaringBitmap source, Consumer<MutableRoaringBitmap> write) {
    MutableRoaringBitmap expected = source.clone();
    write.accept(expected);

    // a heap buffer: its bytes must not change
    ByteBuffer buffer = serialize(source);
    byte[] bytes = buffer.array().clone();
    CopyOnWriteRoaringBitmap fromBuffer =
        new ImmutableRoaringBitmap(buffer).toMutableRoaringBitmapCopyOnWrite();
    assertDoesNotThrow(() -> write.accept(fromBuffer), name);
    assertEquals(expected, fromBuffer, name);
    assertArrayEquals(bytes, buffer.array(), name);

    // a read-only buffer, as from a file mapped read-only: writing to it would throw
    CopyOnWriteRoaringBitmap fromReadOnly =
        new ImmutableRoaringBitmap(serialize(source).asReadOnlyBuffer())
            .toMutableRoaringBitmapCopyOnWrite();
    assertDoesNotThrow(() -> write.accept(fromReadOnly), name);
    assertEquals(expected, fromReadOnly, name);

    // a mutable bitmap, whose own containers are shared
    MutableRoaringBitmap mutable = source.clone();
    CopyOnWriteRoaringBitmap fromMutable = mutable.toMutableRoaringBitmapCopyOnWrite();
    assertDoesNotThrow(() -> write.accept(fromMutable), name);
    assertEquals(expected, fromMutable, name);
    assertEquals(source, mutable, name);
  }

  @Test
  public void testWritesDoNotChangeTheSource() {
    Map<String, Consumer<MutableRoaringBitmap>> writes = new LinkedHashMap<>();
    writes.put("add(int)", b -> b.add(KEY4 + 1));
    writes.put("remove(int)", b -> b.remove(KEY4 + 2));
    writes.put("flip(int)", b -> b.flip(KEY4 + 2));
    writes.put("add(long, long)", b -> b.add(KEY4 + 1L, KEY4 + 100L));
    writes.put("remove(long, long)", b -> b.remove(KEY4 + 200L, KEY4 + 400L));
    writes.put("remove(long, long) in a run container", b -> b.remove((long) KEY2, KEY2 + 10L));
    writes.put("flip(long, long)", b -> b.flip((long) KEY4, KEY4 + 100L));
    writes.put("add(int...)", b -> b.add(new int[] {KEY4 + 1, KEY4 + 3}));
    writes.put("addN", b -> b.addN(new int[] {KEY4 + 1, KEY4 + 3}, 0, 2));
    writes.put("checkedAdd", b -> b.checkedAdd(KEY4 + 1));
    writes.put("checkedRemove", b -> b.checkedRemove(KEY4 + 2));
    writes.put("orNot", b -> b.orNot(MutableRoaringBitmap.bitmapOf(KEY4 + 2), 300_000L));
    writes.put("or", b -> b.or(MutableRoaringBitmap.bitmapOf(KEY4 + 1)));
    writes.put(
        "naivelazyor",
        b -> {
          b.naivelazyor(MutableRoaringBitmap.bitmapOf(KEY4 + 1));
          b.repairAfterLazy();
        });
    writes.put(
        "lazyor",
        b -> {
          b.lazyor(MutableRoaringBitmap.bitmapOf(KEY4 + 1));
          b.repairAfterLazy();
        });
    for (Map.Entry<String, Consumer<MutableRoaringBitmap>> write : writes.entrySet()) {
      assertCopyOnWrite(write.getKey(), threeContainers(), write.getValue());
    }
  }

  @Test
  public void testWritesAfterContainersMove() {
    // a new container moves the following ones to a higher index
    assertCopyOnWrite(
        "add(int) of a new key, then remove(int)",
        threeContainers(),
        b -> {
          b.add(65_536);
          b.remove(KEY4 + 2);
        });
    assertCopyOnWrite(
        "add(int) of a new key, then or",
        threeContainers(),
        b -> {
          b.add(65_536);
          b.or(MutableRoaringBitmap.bitmapOf(KEY4 + 1));
        });
    // a removed container moves the following ones to a lower index
    assertCopyOnWrite(
        "remove(int) of a whole container, then remove(int)",
        threeContainers(),
        b -> {
          b.add(5);
          for (int value : new int[] {1, 2, 3, 5}) {
            b.remove(value);
          }
          b.remove(KEY2);
        });
  }

  @Test
  public void testWriteCopiesOnlyTheContainersItChanges() {
    CopyOnWriteRoaringBitmap cow =
        new ImmutableRoaringBitmap(serialize(threeContainers()))
            .toMutableRoaringBitmapCopyOnWrite();
    cow.add(KEY4 + 1L, KEY4 + 100L);
    cow.add(65_536);

    // keys 0 and 2 still read the source buffer, key 1 is new and key 4 was copied
    MutableRoaringArray containers = cow.getMappeableRoaringArray();
    assertFalse(containers.getContainerAtIndex(0).isArrayBacked());
    assertTrue(containers.getContainerAtIndex(1).isArrayBacked());
    assertFalse(containers.getContainerAtIndex(2).isArrayBacked());
    assertTrue(containers.getContainerAtIndex(3).isArrayBacked());
  }

  @Test
  public void testTrimDoesNotChangeTheSourceContainers() {
    // an array container with unused capacity, whose trim would replace its storage
    MutableRoaringBitmap source = MutableRoaringBitmap.bitmapOf(1, 2, 3, 65_536);
    MappeableArrayContainer sourceContainer =
        (MappeableArrayContainer) source.getMappeableRoaringArray().getContainerAtIndex(0);
    CharBuffer sourceContent = sourceContainer.content;
    assertTrue(sourceContent.limit() > sourceContainer.getCardinality());

    CopyOnWriteRoaringBitmap cow = source.toMutableRoaringBitmapCopyOnWrite();
    cow.add(65_537); // the container of key 1 is copied, the one of key 0 is still shared
    cow.trim();

    assertSame(sourceContent, sourceContainer.content);
    MappeableArrayContainer copied =
        (MappeableArrayContainer) cow.getMappeableRoaringArray().getContainerAtIndex(1);
    assertEquals(copied.getCardinality(), copied.content.limit());
    assertEquals(MutableRoaringBitmap.bitmapOf(1, 2, 3, 65_536, 65_537), cow);
    assertEquals(MutableRoaringBitmap.bitmapOf(1, 2, 3, 65_536), source);
  }

  @Test
  public void testReplacingTheContentForgetsTheSharedContainers() throws Throwable {
    MutableRoaringBitmap source = threeContainers();
    byte[] serialized = serialize(source).array();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
      source.writeExternal(oos);
    }
    byte[] external = bos.toByteArray();

    Map<String, ThrowingConsumer<CopyOnWriteRoaringBitmap>> replacements = new LinkedHashMap<>();
    replacements.put("clear", CopyOnWriteRoaringBitmap::clear);
    replacements.put(
        "deserialize(DataInput)",
        b -> b.deserialize(new DataInputStream(new ByteArrayInputStream(serialized))));
    replacements.put("deserialize(ByteBuffer)", b -> b.deserialize(ByteBuffer.wrap(serialized)));
    replacements.put(
        "readExternal",
        b -> {
          try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(external))) {
            b.readExternal(ois);
          }
        });
    for (Map.Entry<String, ThrowingConsumer<CopyOnWriteRoaringBitmap>> replacement :
        replacements.entrySet()) {
      CopyOnWriteRoaringBitmap cow =
          new ImmutableRoaringBitmap(serialize(source)).toMutableRoaringBitmapCopyOnWrite();
      assertFalse(cow.shared.isEmpty(), replacement.getKey());
      replacement.getValue().accept(cow);
      assertTrue(cow.shared.isEmpty(), replacement.getKey());
    }
  }
}
