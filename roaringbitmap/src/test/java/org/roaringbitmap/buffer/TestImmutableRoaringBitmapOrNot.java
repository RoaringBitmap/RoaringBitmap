package org.roaringbitmap.buffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.roaringbitmap.Util.toUnsignedLong;

import org.roaringbitmap.IntIterator;

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
public class TestImmutableRoaringBitmapOrNot {

  @Test
  public void orNot1() {
    MutableRoaringBitmap rb = new MutableRoaringBitmap();
    MutableRoaringBitmap rb2 = new MutableRoaringBitmap();

    rb.add(2);
    rb.add(1);
    rb.add(1 << 16); // 65536
    rb.add(2 << 16); // 131072
    rb.add(3 << 16); // 196608

    rb2.add(1 << 16); // 65536
    rb2.add(3 << 16); // 196608

    rb.orNot(rb2, (4 << 16) - 1);

    assertEquals((4 << 16) - 1, rb.getCardinality());

    IntIterator iterator = rb.getIntIterator();

    for (int i = 0; i < (4 << 16) - 1; ++i) {
      assertTrue(iterator.hasNext());
      assertEquals(i, iterator.next());
    }
    assertFalse(iterator.hasNext());
  }

  @Test
  public void orNot2() {
    MutableRoaringBitmap rb = new MutableRoaringBitmap();
    MutableRoaringBitmap rb2 = new MutableRoaringBitmap();

    rb.add(0);
    rb.add(1 << 16); // 65536
    rb.add(3 << 16); // 196608

    rb2.add((4 << 16) - 1); // 262143

    rb.orNot(rb2, 4 << 16);

    assertEquals((4 << 16) - 1, rb.getCardinality());

    IntIterator iterator = rb.getIntIterator();

    for (int i = 0; i < (4 << 16) - 1; ++i) {
      assertTrue(iterator.hasNext(), "Error on iteration " + i);
      assertEquals(i, iterator.next());
    }
    assertFalse(iterator.hasNext());
  }

  @Test
  public void orNot3() {
    MutableRoaringBitmap rb = new MutableRoaringBitmap();
    rb.add(2 << 16);

    MutableRoaringBitmap rb2 = new MutableRoaringBitmap();
    rb2.add(1 << 14); // 16384
    rb2.add(3 << 16); // 196608

    rb.orNot(rb2, (5 << 16));
    assertEquals((5 << 16) - 2, rb.getCardinality());

    IntIterator iterator = rb.getIntIterator();
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
    MutableRoaringBitmap rb = new MutableRoaringBitmap();
    rb.add(1);

    MutableRoaringBitmap rb2 = new MutableRoaringBitmap();
    rb2.add(3 << 16); // 196608

    rb.orNot(rb2, (2 << 16) + (2 << 14)); // 131072 + 32768 = 163840
    assertEquals((2 << 16) + (2 << 14), rb.getCardinality());

    IntIterator iterator = rb.getIntIterator();
    for (int i = 0; i < (2 << 16) + (2 << 14); ++i) {
      assertTrue(iterator.hasNext(), "Error on iteration " + i);
      assertEquals(i, iterator.next(), "Error on iteration " + i);
    }
    assertFalse(iterator.hasNext());
  }

  @Test
  public void orNot5() {
    MutableRoaringBitmap rb = new MutableRoaringBitmap();

    rb.add(1);
    rb.add(1 << 16); // 65536
    rb.add(2 << 16); // 131072
    rb.add(3 << 16); // 196608

    MutableRoaringBitmap rb2 = new MutableRoaringBitmap();

    rb.orNot(rb2, (5 << 16));
    assertEquals((5 << 16), rb.getCardinality());

    IntIterator iterator = rb.getIntIterator();
    for (int i = 0; i < (5 << 16); ++i) {
      assertTrue(iterator.hasNext(), "Error on iteration " + i);
      assertEquals(i, iterator.next(), "Error on iteration " + i);
    }
    assertFalse(iterator.hasNext());
  }

  @Test
  public void orNot6() {
    MutableRoaringBitmap rb = new MutableRoaringBitmap();

    rb.add(1);
    rb.add((1 << 16) - 1); // 65535
    rb.add(1 << 16); // 65536
    rb.add(2 << 16); // 131072
    rb.add(3 << 16); // 196608

    MutableRoaringBitmap rb2 = new MutableRoaringBitmap();

    rb.orNot(rb2, (1 << 14));

    // {[0, 2^14], 65535, 65536, 131072, 196608}
    assertEquals((1 << 14) + 4, rb.getCardinality());

    IntIterator iterator = rb.getIntIterator();
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
    MutableRoaringBitmap rb = new MutableRoaringBitmap();

    rb.add(1 << 16); // 65536
    rb.add(2 << 16); // 131072
    rb.add(3 << 16); // 196608

    MutableRoaringBitmap rb2 = new MutableRoaringBitmap();

    rb.orNot(rb2, (1 << 14));

    // {[0, 2^14], 65536, 131072, 196608}
    assertEquals((1 << 14) + 3, rb.getCardinality());

    IntIterator iterator = rb.getIntIterator();
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
    MutableRoaringBitmap rb1 = new MutableRoaringBitmap();

    rb1.add(1 << 16); // 65536
    rb1.add(2 << 16); // 131072
    rb1.add(3 << 16); // 196608

    {
      MutableRoaringBitmap rb2 = new MutableRoaringBitmap();
      MutableRoaringBitmap answer1 = ImmutableRoaringBitmap.orNot(rb1, rb2, (1 << 14));

      // {[0, 2^14] | {65536} {131072} {196608}}
      assertEquals((1 << 14) + 3, answer1.getCardinality());

      IntIterator iterator1 = answer1.getIntIterator();
      for (int i = 0; i < (1 << 14); ++i) {
        assertTrue(iterator1.hasNext(), "Error on iteration " + i);
        assertEquals(i, iterator1.next(), "Error on iteration " + i);
      }
      assertEquals(1 << 16, iterator1.next());
      assertEquals(2 << 16, iterator1.next());
      assertEquals(3 << 16, iterator1.next());
    }

    {
      MutableRoaringBitmap rb2 = new MutableRoaringBitmap();
      MutableRoaringBitmap answer = ImmutableRoaringBitmap.orNot(rb1, rb2, (2 << 16));

      // {[0, 2^16] | 131072, 196608}
      assertEquals((2 << 16) + 2, answer.getCardinality());

      IntIterator iterator = answer.getIntIterator();
      for (int i = 0; i < (2 << 16) + 1; ++i) {
        assertTrue(iterator.hasNext(), "Error on iteration " + i);
        assertEquals(i, iterator.next(), "Error on iteration " + i);
      }
      assertEquals(196608, iterator.next());
    }

    {
      MutableRoaringBitmap rb2 = new MutableRoaringBitmap();
      rb2.add((1 << 16) + (1 << 13));
      rb2.add((1 << 16) + (1 << 14));
      rb2.add((1 << 16) + (1 << 15));
      MutableRoaringBitmap answer = ImmutableRoaringBitmap.orNot(rb1, rb2, (2 << 16));

      // {[0, 2^16] | 196608}
      assertEquals((2 << 16) - 1, answer.getCardinality());

      IntIterator iterator = answer.getIntIterator();
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
      MutableRoaringBitmap rb2 = new MutableRoaringBitmap();
      rb2.add(1 << 16);
      rb2.add(3 << 16);
      rb2.add(4 << 16);

      MutableRoaringBitmap answer = ImmutableRoaringBitmap.orNot(rb1, rb2, (5 << 16));

      // {[0, 2^16]}
      assertEquals((5 << 16) - 1, answer.getCardinality());

      IntIterator iterator = answer.getIntIterator();
      for (int i = 0; i < (5 << 16); ++i) {
        if (i != (4 << 16)) {
          assertTrue(iterator.hasNext(), "Error on iteration " + i);
          assertEquals(i, iterator.next(), "Error on iteration " + i);
        }
      }
      assertFalse(iterator.hasNext(), "Number of elements " + (2 << 16));
    }

    {
      MutableRoaringBitmap rb2 = new MutableRoaringBitmap();
      rb2.add(1 << 16);
      rb2.add(3 << 16);
      rb2.add(4 << 16);

      MutableRoaringBitmap answer = ImmutableRoaringBitmap.orNot(rb2, rb1, (5 << 16));

      // {[0, 2^16]}
      assertEquals((5 << 16) - 1, answer.getCardinality());

      IntIterator iterator = answer.getIntIterator();
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
    MutableRoaringBitmap rb = new MutableRoaringBitmap();
    MutableRoaringBitmap rb2 = new MutableRoaringBitmap();

    rb.add(5);
    rb2.add(10);

    rb.orNot(rb2, 6);

    assertEquals(5, rb.last());
  }

  @Test
  public void orNot11() {
    MutableRoaringBitmap rb = new MutableRoaringBitmap();
    MutableRoaringBitmap rb2 = new MutableRoaringBitmap();

    rb.add((int) (65535L * 65536L + 65523));
    rb2.add((int) (65493L * 65536L + 65520));

    MutableRoaringBitmap rb3 = ImmutableRoaringBitmap.orNot(rb, rb2, 65535L * 65536L + 65524);

    assertEquals((int) (65535L * 65536L + 65523), rb3.last());
  }

  @Test
  public void orNotAgainstFullBitmap() {
    MutableRoaringBitmap rb = new MutableRoaringBitmap();
    MutableRoaringBitmap full = new MutableRoaringBitmap();
    full.add(0, 0x40000L);
    rb.orNot(full, 0x30000L);
    assertTrue(rb.isEmpty());
  }

  @Test
  public void orNotNonEmptyAgainstFullBitmap() {
    MutableRoaringBitmap rb = MutableRoaringBitmap.bitmapOf(1, 0x10001, 0x20001);
    MutableRoaringBitmap full = new MutableRoaringBitmap();
    full.add((long) 0, (long) 0x40000);
    rb.orNot(full, 0x30000);
    assertEquals(MutableRoaringBitmap.bitmapOf(1, 0x10001, 0x20001), rb);
  }

  @Test
  public void orNotAgainstFullBitmapStatic() {
    MutableRoaringBitmap rb = new MutableRoaringBitmap();
    MutableRoaringBitmap full = new MutableRoaringBitmap();
    full.add(0, 0x40000L);
    MutableRoaringBitmap result = ImmutableRoaringBitmap.orNot(rb, full, 0x30000L);
    assertTrue(result.isEmpty());
  }

  @Test
  public void orNotNonEmptyAgainstFullBitmapStatic() {
    MutableRoaringBitmap rb = MutableRoaringBitmap.bitmapOf(1, 0x10001, 0x20001);
    MutableRoaringBitmap full = new MutableRoaringBitmap();
    full.add(0, 0x40000L);
    assertEquals(
        MutableRoaringBitmap.bitmapOf(1, 0x10001, 0x20001),
        ImmutableRoaringBitmap.orNot(rb, full, 0x30000L));
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
    MutableRoaringBitmap l = new MutableRoaringBitmap();
    l.deserialize(lBuffer);
    assertTrue(l.validate());
    MutableRoaringBitmap r = new MutableRoaringBitmap();
    r.deserialize(rBuffer);
    assertTrue(r.validate());

    MutableRoaringBitmap range = new MutableRoaringBitmap();
    long limit = toUnsignedLong(l.last()) + 1;
    range.add(0, limit);
    range.andNot(r);
    MutableRoaringBitmap expected = MutableRoaringBitmap.or(l, range);

    MutableRoaringBitmap actual = l.clone();
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
    MutableRoaringBitmap l = new MutableRoaringBitmap();
    l.deserialize(lBuffer);
    assertTrue(l.validate());
    MutableRoaringBitmap r = new MutableRoaringBitmap();
    r.deserialize(rBuffer);
    assertTrue(r.validate());

    MutableRoaringBitmap range = new MutableRoaringBitmap();
    long limit = toUnsignedLong(l.last()) + 1;
    range.add(0, limit);
    range.andNot(r);
    MutableRoaringBitmap expected = MutableRoaringBitmap.or(l, range);

    MutableRoaringBitmap actual = ImmutableRoaringBitmap.orNot(l, r, limit);
    assertEquals(expected, actual);
  }
}
