/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */
package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.roaringbitmap.Util.toUnsignedLong;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.List;
import java.util.Map;

@Execution(ExecutionMode.CONCURRENT)
public class TestRoaringBitmapOrNot {

  @Test
  public void orNot1() {
    final RoaringBitmap rb = new RoaringBitmap();
    final RoaringBitmap rb2 = new RoaringBitmap();

    rb.add(2);
    rb.add(1);
    rb.add(1 << 16); // 65536
    rb.add(2 << 16); // 131072
    rb.add(3 << 16); // 196608

    rb2.add(1 << 16); // 65536
    rb2.add(3 << 16); // 196608

    rb.orNot(rb2, (4 << 16) - 1);

    assertEquals((4 << 16) - 1, rb.getCardinality());

    final IntIterator iterator = rb.getIntIterator();

    for (int i = 0; i < (4 << 16) - 1; ++i) {
      assertTrue(iterator.hasNext());
      assertEquals(i, iterator.next());
    }
    assertFalse(iterator.hasNext());
  }

  @Test
  public void orNot2() {
    final RoaringBitmap rb = new RoaringBitmap();
    final RoaringBitmap rb2 = new RoaringBitmap();

    rb.add(0);
    rb.add(1 << 16); // 65536
    rb.add(3 << 16); // 196608

    rb2.add((4 << 16) - 1); // 262143

    rb.orNot(rb2, 4 << 16);

    assertEquals((4 << 16) - 1, rb.getCardinality());

    final IntIterator iterator = rb.getIntIterator();

    for (int i = 0; i < (4 << 16) - 1; ++i) {
      assertTrue(iterator.hasNext(), "Error on iteration " + i);
      assertEquals(i, iterator.next());
    }
    assertFalse(iterator.hasNext());
  }

  @Test
  public void orNot3() {
    final RoaringBitmap rb = new RoaringBitmap();
    rb.add(2 << 16);

    final RoaringBitmap rb2 = new RoaringBitmap();
    rb2.add(1 << 14); // 16384
    rb2.add(3 << 16); // 196608

    rb.orNot(rb2, (5 << 16));
    assertEquals((5 << 16) - 2, rb.getCardinality());

    final IntIterator iterator = rb.getIntIterator();
    for (int i = 0; i < (5 << 16); ++i) {
      if ((i != (1 << 14)) && (i != (3 << 16))) {
        assertTrue(iterator.hasNext(), "Error on iteration " + i);
        assertEquals(i, iterator.next(), "Error on iteration " + i);
      }
    }
    assertFalse(iterator.hasNext());
  }

  @Test
  public void orNot4() {
    final RoaringBitmap rb = new RoaringBitmap();
    rb.add(1);

    final RoaringBitmap rb2 = new RoaringBitmap();
    rb2.add(3 << 16); // 196608

    rb.orNot(rb2, (2 << 16) + (2 << 14)); // 131072 + 32768 = 163840
    assertEquals((2 << 16) + (2 << 14), rb.getCardinality());

    final IntIterator iterator = rb.getIntIterator();
    for (int i = 0; i < (2 << 16) + (2 << 14); ++i) {
      assertTrue(iterator.hasNext(), "Error on iteration " + i);
      assertEquals(i, iterator.next(), "Error on iteration " + i);
    }
    assertFalse(iterator.hasNext());
  }

  @Test
  public void orNot5() {
    final RoaringBitmap rb = new RoaringBitmap();

    rb.add(1);
    rb.add(1 << 16); // 65536
    rb.add(2 << 16); // 131072
    rb.add(3 << 16); // 196608

    final RoaringBitmap rb2 = new RoaringBitmap();

    rb.orNot(rb2, (5 << 16));
    assertEquals((5 << 16), rb.getCardinality());

    final IntIterator iterator = rb.getIntIterator();
    for (int i = 0; i < (5 << 16); ++i) {
      assertTrue(iterator.hasNext(), "Error on iteration " + i);
      assertEquals(i, iterator.next(), "Error on iteration " + i);
    }
    assertFalse(iterator.hasNext());
  }

  @Test
  public void orNot6() {
    final RoaringBitmap rb = new RoaringBitmap();

    rb.add(1);
    rb.add((1 << 16) - 1); // 65535
    rb.add(1 << 16); // 65536
    rb.add(2 << 16); // 131072
    rb.add(3 << 16); // 196608

    final RoaringBitmap rb2 = new RoaringBitmap();

    rb.orNot(rb2, (1 << 14));

    // {[0, 2^14], 65535, 65536, 131072, 196608}
    assertEquals((1 << 14) + 4, rb.getCardinality());

    final IntIterator iterator = rb.getIntIterator();
    for (int i = 0; i < (1 << 14); ++i) {
      assertTrue(iterator.hasNext(), "Error on iteration " + i);
      assertEquals(i, iterator.next(), "Error on iteration " + i);
    }

    assertTrue(iterator.hasNext());
    assertEquals((1 << 16) - 1, iterator.next());

    assertTrue(iterator.hasNext());
    assertEquals(1 << 16, iterator.next());

    assertTrue(iterator.hasNext());
    assertEquals(2 << 16, iterator.next());

    assertTrue(iterator.hasNext());
    assertEquals(3 << 16, iterator.next());

    assertFalse(iterator.hasNext());
  }

  @Test
  public void orNot7() {
    final RoaringBitmap rb = new RoaringBitmap();

    rb.add(1 << 16); // 65536
    rb.add(2 << 16); // 131072
    rb.add(3 << 16); // 196608

    final RoaringBitmap rb2 = new RoaringBitmap();

    rb.orNot(rb2, (1 << 14));

    // {[0, 2^14], 65536, 131072, 196608}
    assertEquals((1 << 14) + 3, rb.getCardinality());

    final IntIterator iterator = rb.getIntIterator();
    for (int i = 0; i < (1 << 14); ++i) {
      assertTrue(iterator.hasNext(), "Error on iteration " + i);
      assertEquals(i, iterator.next(), "Error on iteration " + i);
    }

    assertTrue(iterator.hasNext());
    assertEquals(1 << 16, iterator.next());

    assertTrue(iterator.hasNext());
    assertEquals(2 << 16, iterator.next());

    assertTrue(iterator.hasNext());
    assertEquals(3 << 16, iterator.next());

    assertFalse(iterator.hasNext());
  }

  @Test
  public void orNot9() {
    final RoaringBitmap rb1 = new RoaringBitmap();

    rb1.add(1 << 16); // 65536
    rb1.add(2 << 16); // 131072
    rb1.add(3 << 16); // 196608

    {
      final RoaringBitmap rb2 = new RoaringBitmap();
      final RoaringBitmap answer1 = RoaringBitmap.orNot(rb1, rb2, (1 << 14));

      // {[0, 2^14] | {65536} {131072} {196608}}
      assertEquals((1 << 14) + 3, answer1.getCardinality());

      final IntIterator iterator1 = answer1.getIntIterator();
      for (int i = 0; i < (1 << 14); ++i) {
        assertTrue(iterator1.hasNext(), "Error on iteration " + i);
        assertEquals(i, iterator1.next(), "Error on iteration " + i);
      }
      assertEquals(1 << 16, iterator1.next());
      assertEquals(2 << 16, iterator1.next());
      assertEquals(3 << 16, iterator1.next());
    }

    {
      final RoaringBitmap rb2 = new RoaringBitmap();
      final RoaringBitmap answer = RoaringBitmap.orNot(rb1, rb2, (2 << 16));

      // {[0, 2^16] | 131072, 196608}
      assertEquals((2 << 16) + 2, answer.getCardinality());

      final IntIterator iterator = answer.getIntIterator();
      for (int i = 0; i < (2 << 16) + 1; ++i) {
        assertTrue(iterator.hasNext(), "Error on iteration " + i);
        assertEquals(i, iterator.next(), "Error on iteration " + i);
      }
      assertEquals(196608, iterator.next());
    }

    {
      final RoaringBitmap rb2 = new RoaringBitmap();
      rb2.add((1 << 16) + (1 << 13));
      rb2.add((1 << 16) + (1 << 14));
      rb2.add((1 << 16) + (1 << 15));
      final RoaringBitmap answer = RoaringBitmap.orNot(rb1, rb2, (2 << 16));

      // {[0, 2^16] | 196608}
      assertEquals((2 << 16) - 1, answer.getCardinality());

      final IntIterator iterator = answer.getIntIterator();
      for (int i = 0; i < (2 << 16) + 1; ++i) {
        if ((i != (1 << 16) + (1 << 13))
            && (i != (1 << 16) + (1 << 14))
            && (i != (1 << 16) + (1 << 15))) {
          assertTrue(iterator.hasNext(), "Error on iteration " + i);
          assertEquals(i, iterator.next(), "Error on iteration " + i);
        }
      }
      assertEquals(196608, iterator.next());
    }

    {
      final RoaringBitmap rb2 = new RoaringBitmap();
      rb2.add(1 << 16);
      rb2.add(3 << 16);
      rb2.add(4 << 16);

      final RoaringBitmap answer = RoaringBitmap.orNot(rb1, rb2, (5 << 16));

      // {[0, 2^16]}
      assertEquals((5 << 16) - 1, answer.getCardinality());

      final IntIterator iterator = answer.getIntIterator();
      for (int i = 0; i < (5 << 16); ++i) {
        if (i != (4 << 16)) {
          assertTrue(iterator.hasNext(), "Error on iteration " + i);
          assertEquals(i, iterator.next(), "Error on iteration " + i);
        }
      }
      assertFalse(iterator.hasNext(), "Number of elements " + (2 << 16));
    }

    {
      final RoaringBitmap rb2 = new RoaringBitmap();
      rb2.add(1 << 16);
      rb2.add(3 << 16);
      rb2.add(4 << 16);

      final RoaringBitmap answer = RoaringBitmap.orNot(rb2, rb1, (5 << 16));

      // {[0, 2^16]}
      assertEquals((5 << 16) - 1, answer.getCardinality());

      final IntIterator iterator = answer.getIntIterator();
      for (int i = 0; i < (5 << 16); ++i) {
        if (i != (2 << 16)) {
          assertTrue(iterator.hasNext(), "Error on iteration " + i);
          assertEquals(i, iterator.next(), "Error on iteration " + i);
        }
      }
      assertFalse(iterator.hasNext(), "Number of elements " + (2 << 16));
    }
  }

  @Test
  public void orNot10() {
    final RoaringBitmap rb = new RoaringBitmap();
    final RoaringBitmap rb2 = new RoaringBitmap();

    rb.add(5);
    rb2.add(10);

    rb.orNot(rb2, 6);

    assertEquals(5, rb.last());
  }

  @Test
  public void orNot11() {
    final RoaringBitmap rb = new RoaringBitmap();
    final RoaringBitmap rb2 = new RoaringBitmap();

    rb.add((int) (65535L * 65536L + 65523));
    rb2.add((int) (65493L * 65536L + 65520));

    RoaringBitmap rb3 = RoaringBitmap.orNot(rb, rb2, 65535L * 65536L + 65524);

    assertEquals((int) (65535L * 65536L + 65523), rb3.last());
  }

  @Test
  public void orNotAgainstFullBitmap() {
    RoaringBitmap rb = new RoaringBitmap();
    RoaringBitmap full = new RoaringBitmap();
    full.add(0, 0x40000L);
    rb.orNot(full, 0x30000L);
    assertTrue(rb.isEmpty());
  }

  @Test
  public void orNotNonEmptyAgainstFullBitmap() {
    RoaringBitmap rb = RoaringBitmap.bitmapOf(1, 0x10001, 0x20001);
    RoaringBitmap full = new RoaringBitmap();
    full.add((long) 0, (long) 0x40000);
    rb.orNot(full, 0x30000);
    assertEquals(RoaringBitmap.bitmapOf(1, 0x10001, 0x20001), rb);
  }

  @Test
  public void orNotAgainstFullBitmapStatic() {
    RoaringBitmap rb = new RoaringBitmap();
    RoaringBitmap full = new RoaringBitmap();
    full.add(0, 0x40000L);
    RoaringBitmap result = RoaringBitmap.orNot(rb, full, 0x30000L);
    assertTrue(result.isEmpty());
  }

  @Test
  public void orNotNonEmptyAgainstFullBitmapStatic() {
    RoaringBitmap rb = RoaringBitmap.bitmapOf(1, 0x10001, 0x20001);
    RoaringBitmap full = new RoaringBitmap();
    full.add(0, 0x40000L);
    assertEquals(
        RoaringBitmap.bitmapOf(1, 0x10001, 0x20001), RoaringBitmap.orNot(rb, full, 0x30000L));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBigOrNot() throws IOException {
    byte[] bytes =
        Files.readAllBytes(Paths.get("src/test/resources/testdata/ornot-fuzz-failure.json"));
    Map<String, Object> info = new ObjectMapper().readerFor(Map.class).readValue(bytes);
    List<String> base64Bitmaps = (List<String>) info.get("bitmaps");
    ByteBuffer lBuffer = ByteBuffer.wrap(Base64.getDecoder().decode(base64Bitmaps.get(0)));
    ByteBuffer rBuffer = ByteBuffer.wrap(Base64.getDecoder().decode(base64Bitmaps.get(1)));
    RoaringBitmap l = new RoaringBitmap();
    l.deserialize(lBuffer);
    assertTrue(l.validate());
    RoaringBitmap r = new RoaringBitmap();
    r.deserialize(rBuffer);
    assertTrue(r.validate());

    RoaringBitmap range = new RoaringBitmap();
    long limit = toUnsignedLong(l.last()) + 1;
    range.add(0, limit);
    range.andNot(r);
    RoaringBitmap expected = RoaringBitmap.or(l, range);

    RoaringBitmap actual = l.clone();
    actual.orNot(r, limit);
    assertEquals(expected, actual);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBigOrNotStatic() throws IOException {
    byte[] bytes =
        Files.readAllBytes(Paths.get("src/test/resources/testdata/ornot-fuzz-failure.json"));
    Map<String, Object> info = new ObjectMapper().readerFor(Map.class).readValue(bytes);
    List<String> base64Bitmaps = (List<String>) info.get("bitmaps");
    ByteBuffer lBuffer = ByteBuffer.wrap(Base64.getDecoder().decode(base64Bitmaps.get(0)));
    ByteBuffer rBuffer = ByteBuffer.wrap(Base64.getDecoder().decode(base64Bitmaps.get(1)));
    RoaringBitmap l = new RoaringBitmap();
    l.deserialize(lBuffer);
    RoaringBitmap r = new RoaringBitmap();
    r.deserialize(rBuffer);

    RoaringBitmap range = new RoaringBitmap();
    long limit = toUnsignedLong(l.last()) + 1;
    range.add(0, limit);
    range.andNot(r);
    RoaringBitmap expected = RoaringBitmap.or(l, range);

    RoaringBitmap actual = RoaringBitmap.orNot(l, r, limit);
    assertEquals(expected, actual);
  }
}
