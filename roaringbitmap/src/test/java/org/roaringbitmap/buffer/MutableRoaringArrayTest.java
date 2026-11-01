package org.roaringbitmap.buffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.ByteBuffer;

public class MutableRoaringArrayTest {

  @Test
  public void resizeOnlyIfNecessary() {
    char[] keys = new char[1];
    int size = 0;
    MappeableContainer[] values = new MappeableContainer[1];
    MutableRoaringArray array = new MutableRoaringArray(keys, values, size);
    array.extendArray(1);
    assertSame(keys, array.keys, "Keys were not reallocated");
  }

  private static MutableRoaringBitmap sample() {
    MutableRoaringBitmap rb = MutableRoaringBitmap.bitmapOf(1, 2, 3, 70000, 140000);
    rb.add(200000L, 300000L);
    return rb;
  }

  private static ImmutableRoaringBitmap mapped(MutableRoaringBitmap rb) {
    ByteBuffer buffer = ByteBuffer.allocate(rb.serializedSizeInBytes());
    rb.serialize(buffer);
    buffer.flip();
    return new ImmutableRoaringBitmap(buffer);
  }

  // equals used to loop forever on two equal non-empty arrays, and would have
  // returned false anyway
  @Test
  @Timeout(10)
  public void equalsComparesContent() {
    MutableRoaringBitmap rb = sample();
    MutableRoaringBitmap other = sample();
    other.add(5);
    PointableRoaringArray mutable1 = rb.highLowContainer;
    PointableRoaringArray mutable2 = sample().highLowContainer;
    PointableRoaringArray immutable1 = mapped(rb).highLowContainer;
    PointableRoaringArray immutable2 = mapped(sample()).highLowContainer;
    PointableRoaringArray[] equal = {mutable1, mutable2, immutable1, immutable2};
    for (PointableRoaringArray a : equal) {
      for (PointableRoaringArray b : equal) {
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
      }
      assertNotEquals(a, other.highLowContainer);
      assertNotEquals(a, mapped(other).highLowContainer);
      assertNotEquals(a, new MutableRoaringBitmap().highLowContainer);
      assertNotEquals(a, mapped(new MutableRoaringBitmap()).highLowContainer);
    }
    assertEquals(
        new MutableRoaringBitmap().highLowContainer,
        mapped(new MutableRoaringBitmap()).highLowContainer);
  }
}
