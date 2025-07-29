package org.roaringbitmap.buffer;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Test the copy-on-write functionality for memory-mapped bitmaps.
 */
public class TestCopyOnWrite {

  @Test
  public void testCopyOnWriteBasic() throws IOException {
    // Create a memory-mapped bitmap
    MutableRoaringBitmap original = new MutableRoaringBitmap();
    for (int i = 1; i < 1000; i++) {
      original.add(i);
    }
    for (int i = 2000; i < 3000; i++) {
      original.add(i);
    }
    for (int i = 5000; i < 6000; i++) {
      original.add(i);
    }

    // Serialize to ByteBuffer
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    original.serialize(dos);
    dos.close();

    ByteBuffer bb = ByteBuffer.wrap(bos.toByteArray());
    ImmutableRoaringBitmap immutable = new ImmutableRoaringBitmap(bb);

    // Test that normal conversion still works
    MutableRoaringBitmap copy = immutable.toMutableRoaringBitmap();
    assertEquals(original.getCardinality(), copy.getCardinality());
    assertEquals(original, copy);

    // Test copy-on-write conversion
    CopyOnWriteRoaringBitmap cowCopy = immutable.toMutableRoaringBitmapCopyOnWrite();
    assertEquals(original.getCardinality(), cowCopy.getCardinality());
    assertEquals(original, cowCopy);

    // Verify both copies are functionally equivalent
    assertEquals(copy, cowCopy);

    // Test that modifications work correctly with copy-on-write
    cowCopy.add(10000);
    assertTrue(cowCopy.contains(10000));
    assertFalse(immutable.contains(10000));
    assertFalse(copy.contains(10000));

    // Test that operations on copy-on-write bitmap work
    cowCopy.remove(500);
    assertFalse(cowCopy.contains(500));
    assertTrue(immutable.contains(500));
    assertTrue(copy.contains(500));
  }

  @Test
  public void testCopyOnWritePerformance() throws IOException {
    // Create a large bitmap
    MutableRoaringBitmap large = new MutableRoaringBitmap();
    for (int i = 0; i < 100000; i += 10) {
      large.add(i);
    }

    // Serialize to ByteBuffer
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    large.serialize(dos);
    dos.close();

    ByteBuffer bb = ByteBuffer.wrap(bos.toByteArray());
    ImmutableRoaringBitmap immutable = new ImmutableRoaringBitmap(bb);

    // Time the copy-on-write conversion (should be fast)
    long startTime = System.nanoTime();
    CopyOnWriteRoaringBitmap cow = immutable.toMutableRoaringBitmapCopyOnWrite();
    long cowTime = System.nanoTime() - startTime;

    // Time the regular conversion (should be slower)
    startTime = System.nanoTime();
    MutableRoaringBitmap copy = immutable.toMutableRoaringBitmap();
    long copyTime = System.nanoTime() - startTime;

    // Copy-on-write should be significantly faster (though small bitmaps may have overhead)
    System.out.println("Copy-on-write time: " + cowTime + "ns, Full copy time: " + copyTime + "ns");

    // But both should be functionally equivalent
    assertEquals(copy, cow);
  }
}
