package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Random;

class ByteBufferBackedInputStream extends InputStream {

  ByteBuffer buf;

  ByteBufferBackedInputStream(ByteBuffer buf) {
    this.buf = buf;
  }

  @Override
  public int available() throws IOException {
    return buf.remaining();
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  @Override
  public int read() throws IOException {
    if (!buf.hasRemaining()) {
      return -1;
    }
    return 0xFF & buf.get();
  }

  @Override
  public int read(byte[] bytes) throws IOException {
    int len = Math.min(bytes.length, buf.remaining());
    buf.get(bytes, 0, len);
    return len;
  }

  @Override
  public int read(byte[] bytes, int off, int len) throws IOException {
    len = Math.min(len, buf.remaining());
    buf.get(bytes, off, len);
    return len;
  }

  @Override
  public long skip(long n) {
    int len = Math.min((int) n, buf.remaining());
    buf.position(buf.position() + (int) n);
    return len;
  }
}

class ByteBufferBackedOutputStream extends OutputStream {
  ByteBuffer buf;

  ByteBufferBackedOutputStream(ByteBuffer buf) {
    this.buf = buf;
  }

  @Override
  public synchronized void write(byte[] bytes) throws IOException {
    buf.put(bytes);
  }

  @Override
  public synchronized void write(byte[] bytes, int off, int len) throws IOException {
    buf.put(bytes, off, len);
  }

  @Override
  public synchronized void write(int b) throws IOException {
    buf.put((byte) b);
  }
}

public class TestSerialization {
  static RoaringBitmap bitmap_a;

  static RoaringBitmap bitmap_a1;

  static RoaringBitmap bitmap_empty = new RoaringBitmap();

  static RoaringBitmap bitmap_b = new RoaringBitmap();

  static MutableRoaringBitmap bitmap_ar;

  static MutableRoaringBitmap bitmap_br = new MutableRoaringBitmap();

  static MutableRoaringBitmap bitmap_emptyr = new MutableRoaringBitmap();

  static ByteBuffer outbb;

  static ByteBuffer presoutbb;

  // Very small buffer to higher to chance to encounter edge-case
  byte[] buffer = new byte[16];

  @BeforeAll
  public static void init() throws IOException {
    final int[] data = takeSortedAndDistinct(new Random(0xcb000a2b9b5bdfb6L), 100000);
    bitmap_a = RoaringBitmap.bitmapOf(data);
    bitmap_ar = MutableRoaringBitmap.bitmapOf(data);
    bitmap_a1 = RoaringBitmap.bitmapOf(data);

    for (int k = 100000; k < 200000; ++k) {
      bitmap_a.add(3 * k); // bitmap density and too many little runs
      bitmap_ar.add(3 * k);
      bitmap_a1.add(3 * k);
    }

    for (int k = 700000; k < 800000; ++k) { // runcontainer would be best
      bitmap_a.add(k);
      bitmap_ar.add(k);
      bitmap_a1.add(k);
    }

    bitmap_a.runOptimize(); // mix of all 3 container kinds
    bitmap_ar.runOptimize(); // must stay in sync with bitmap_a
    /*
     * There is potentially some "slop" betweeen the size estimates used for RoaringBitmaps and
     * MutableRoaringBitmaps, so it is risky to assume that they will both *always* agree whether to
     * run encode a container. Nevertheless testMutableSerialize effectively does that, by using the
     * serialized size of one as the output buffer size for the other.
     */
    // do not runoptimize bitmap_a1

    outbb =
        ByteBuffer.allocate(
            bitmap_a.serializedSizeInBytes() + bitmap_empty.serializedSizeInBytes());
    presoutbb =
        ByteBuffer.allocate(
            bitmap_a.serializedSizeInBytes() + bitmap_empty.serializedSizeInBytes());
    ByteBufferBackedOutputStream out = new ByteBufferBackedOutputStream(presoutbb);

    DataOutputStream dos = new DataOutputStream(out);
    bitmap_empty.serialize(dos);
    bitmap_a.serialize(dos);
    presoutbb.flip();
  }

  private static int[] takeSortedAndDistinct(Random source, int count) {

    LinkedHashSet<Integer> ints = new LinkedHashSet<Integer>(count);

    for (int size = 0; size < count; size++) {
      int next;
      do {
        next = Math.abs(source.nextInt());
      } while (!ints.add(next));
    }

    int[] unboxed = toArray(ints);
    Arrays.sort(unboxed);
    return unboxed;
  }

  private static int[] toArray(LinkedHashSet<Integer> integers) {
    int[] ints = new int[integers.size()];
    int i = 0;
    for (Integer n : integers) {
      ints[i++] = n;
    }
    return ints;
  }

  @Test
  public void testDeserialize() throws IOException {
    presoutbb.rewind();
    ByteBufferBackedInputStream in = new ByteBufferBackedInputStream(presoutbb);
    DataInputStream dis = new DataInputStream(in);
    bitmap_empty.deserialize(dis);
    bitmap_b.deserialize(dis);
    assertTrue(bitmap_b.validate());
    assertTrue(bitmap_empty.validate());
  }

  @Test
  public void testDeserialize_buffer() throws IOException {
    presoutbb.rewind();
    ByteBufferBackedInputStream in = new ByteBufferBackedInputStream(presoutbb);
    DataInputStream dis = new DataInputStream(in);
    bitmap_empty.deserialize(dis, buffer);
    bitmap_b.deserialize(dis, buffer);
    assertTrue(bitmap_empty.validate());
    assertTrue(bitmap_b.validate());
    assertEquals(bitmap_a, bitmap_b);
  }

  @Test
  public void testImmutableBuildingBySerialization() {
    presoutbb.rewind();
    ImmutableRoaringBitmap imrempty = new ImmutableRoaringBitmap(presoutbb);
    presoutbb.position(presoutbb.position() + imrempty.serializedSizeInBytes());
    assertEquals(imrempty.isEmpty(), true);
    ImmutableRoaringBitmap imrb = new ImmutableRoaringBitmap(presoutbb);
    int cksum1 = 0, cksum2 = 0, count1 = 0, count2 = 0;
    for (int x : bitmap_a) { // or bitmap_a1 for a version without run
      cksum1 += x;
      ++count1;
    }
    for (int x : imrb) {
      cksum2 += x;
      ++count2;
    }

    Iterator<Integer> it1, it2;
    it1 = bitmap_a.iterator();
    // it1 = bitmap_a1.iterator();
    it2 = imrb.iterator();
    int blabcount = 0;
    int valcount = 0;
    while (it1.hasNext() && it2.hasNext()) {
      ++valcount;
      int val1 = it1.next(), val2 = it2.next();
      if (val1 != val2) {
        if (++blabcount < 10) {
          System.out.println(
              "disagree on " + valcount + " nonmatching values are " + val1 + " " + val2);
        }
      }
    }
    System.out.println("there were " + blabcount + " diffs");
    if (it1.hasNext() != it2.hasNext()) {
      System.out.println("one ran out earlier");
    }

    assertEquals(count1, count2);
    assertEquals(cksum1, cksum2);
  }

  @Test
  public void testImmutableBuildingBySerializationSimple() {
    System.out.println("testImmutableBuildingBySerializationSimple ");
    ByteBuffer bb1;
    MutableRoaringBitmap bm1 = new MutableRoaringBitmap();
    for (int k = 20; k < 30; ++k) { // runcontainer would be best
      bm1.add(k);
    }
    bm1.runOptimize();

    bb1 = ByteBuffer.allocate(bitmap_a.serializedSizeInBytes());
    ByteBufferBackedOutputStream out = new ByteBufferBackedOutputStream(bb1);
    try {
      bm1.serialize(new DataOutputStream(out));
    } catch (Exception e) {
      e.printStackTrace();
    }
    bb1.flip();
    ImmutableRoaringBitmap imrb = new ImmutableRoaringBitmap(bb1);
    int cksum1 = 0, cksum2 = 0, count1 = 0, count2 = 0;
    for (int x : bm1) {
      cksum1 += x;
      ++count1;
    }

    for (int x : imrb) {
      cksum2 += x;
      ++count2;
    }

    assertEquals(count1, count2);
    assertEquals(cksum1, cksum2);
  }

  @Test
  public void testMutableBuilding() {
    // did we build a mutable equal to the regular one?
    assertEquals(bitmap_emptyr.isEmpty(), true);
    assertEquals(bitmap_empty.isEmpty(), true);
    int cksum1 = 0, cksum2 = 0;
    for (int x : bitmap_a) {
      cksum1 += x;
    }
    for (int x : bitmap_ar) {
      cksum2 += x;
    }
    assertEquals(cksum1, cksum2);
  }

  @Test
  public void testMutableBuildingBySerialization() throws IOException {
    presoutbb.rewind();
    ByteBufferBackedInputStream in = new ByteBufferBackedInputStream(presoutbb);
    MutableRoaringBitmap emptyt = new MutableRoaringBitmap();
    MutableRoaringBitmap mrb = new MutableRoaringBitmap();
    DataInputStream dis = new DataInputStream(in);
    emptyt.deserialize(dis);
    assertEquals(emptyt.isEmpty(), true);
    mrb.deserialize(dis);
    int cksum1 = 0, cksum2 = 0;
    for (int x : bitmap_a) {
      cksum1 += x;
    }
    for (int x : mrb) {
      cksum2 += x;
    }
    assertEquals(cksum1, cksum2);
  }

  @Test
  public void testSerialization_bad() throws IOException {
    // This example binary comes from
    // https://github.com/RoaringBitmap/CRoaring/tree/master/tests/testdata
    String resourceName = "/testdata/bad-bitmap.bin";
    try (InputStream inputStream = TestAdversarialInputs.openInputstream(resourceName);
         DataInputStream dataInputStream = new DataInputStream(inputStream)) {
      RoaringBitmap bitmap = new RoaringBitmap();
      bitmap.deserialize(dataInputStream);
      assertFalse(bitmap.validate());
    }
  }

  @Test
  public void testMutableDeserializeMutable() throws IOException {
    presoutbb.rewind();
    ByteBufferBackedInputStream in = new ByteBufferBackedInputStream(presoutbb);
    DataInputStream dis = new DataInputStream(in);
    bitmap_emptyr.deserialize(dis);
    bitmap_br.deserialize(dis);
    assertTrue(bitmap_emptyr.validate());
    assertTrue(bitmap_br.validate());
  }

  @Test
  public void testMutableRunSerializationBasicDeserialization() throws java.io.IOException {
    final int[] data = takeSortedAndDistinct(new Random(07734), 100000);
    RoaringBitmap bitmap_a = RoaringBitmap.bitmapOf(data);
    RoaringBitmap bitmap_ar = RoaringBitmap.bitmapOf(data);

    MutableRoaringBitmap bitmap_am = MutableRoaringBitmap.bitmapOf(data);
    MutableRoaringBitmap bitmap_amr = MutableRoaringBitmap.bitmapOf(data);

    for (int k = 100000; k < 200000; ++k) {
      bitmap_a.add(3 * k); // bitmap density and too many little runs
      bitmap_ar.add(3 * k);
      bitmap_am.add(3 * k);
      bitmap_amr.add(3 * k);
    }

    for (int k = 700000; k < 800000; ++k) { // will choose a runcontainer on this
      bitmap_a.add(k); // bitmap density and too many little runs
      bitmap_ar.add(k);
      bitmap_am.add(k);
      bitmap_amr.add(k);
    }

    bitmap_ar.runOptimize();
    bitmap_amr.runOptimize();
    assertEquals(bitmap_a, bitmap_ar);
    assertEquals(bitmap_am, bitmap_amr);
    assertEquals(bitmap_am.serializedSizeInBytes(), bitmap_a.serializedSizeInBytes());
    assertEquals(bitmap_amr.serializedSizeInBytes(), bitmap_ar.serializedSizeInBytes());

    ByteBuffer outbuf =
        ByteBuffer.allocate(
            2 * (bitmap_a.serializedSizeInBytes() + bitmap_ar.serializedSizeInBytes()));
    DataOutputStream out = new DataOutputStream(new ByteBufferBackedOutputStream(outbuf));
    try {
      bitmap_a.serialize(out);
      bitmap_ar.serialize(out);
      bitmap_am.serialize(out);
      bitmap_amr.serialize(out);
    } catch (Exception e) {
      e.printStackTrace();
    }
    outbuf.flip();

    RoaringBitmap bitmap_c1 = new RoaringBitmap();
    RoaringBitmap bitmap_c2 = new RoaringBitmap();
    RoaringBitmap bitmap_c3 = new RoaringBitmap();
    RoaringBitmap bitmap_c4 = new RoaringBitmap();

    DataInputStream in = new DataInputStream(new ByteBufferBackedInputStream(outbuf));
    bitmap_c1.deserialize(in);
    bitmap_c2.deserialize(in);
    bitmap_c3.deserialize(in);
    bitmap_c4.deserialize(in);
    assertTrue(bitmap_c1.validate());
    assertTrue(bitmap_c2.validate());
    assertTrue(bitmap_c3.validate());
    assertTrue(bitmap_c4.validate());

    assertEquals(bitmap_a, bitmap_c1);
    assertEquals(bitmap_a, bitmap_c2);
    assertEquals(bitmap_a, bitmap_c3);
    assertEquals(bitmap_a, bitmap_c4);
    assertEquals(bitmap_ar, bitmap_c1);
    assertEquals(bitmap_ar, bitmap_c2);
    assertEquals(bitmap_ar, bitmap_c3);
    assertEquals(bitmap_ar, bitmap_c4);
  }

  @Test
  public void testMutableSerialize() throws IOException {
    System.out.println("testMutableSerialize");
    outbb.rewind();
    ByteBufferBackedOutputStream out = new ByteBufferBackedOutputStream(outbb);
    System.out.println("bitmap_ar is " + bitmap_ar.getClass().getName());
    DataOutputStream dos = new DataOutputStream(out);
    bitmap_emptyr.serialize(dos);
    bitmap_ar.serialize(dos);
  }

  @Test
  public void testRunSerializationDeserialization() throws java.io.IOException {
    final int[] data = takeSortedAndDistinct(new Random(07734), 100000);
    RoaringBitmap bitmap_a = RoaringBitmap.bitmapOf(data);
    RoaringBitmap bitmap_ar = RoaringBitmap.bitmapOf(data);

    for (int k = 100000; k < 200000; ++k) {
      bitmap_a.add(3 * k); // bitmap density and too many little runs
      bitmap_ar.add(3 * k);
    }

    for (int k = 700000; k < 800000; ++k) { // will choose a runcontainer on this
      bitmap_a.add(k);
      bitmap_ar.add(k);
    }

    bitmap_a.runOptimize(); // mix of all 3 container kinds

    ByteBuffer outbuf = ByteBuffer.allocate(bitmap_a.serializedSizeInBytes());
    ByteBufferBackedOutputStream out = new ByteBufferBackedOutputStream(outbuf);
    try {
      bitmap_a.serialize(new DataOutputStream(out));
    } catch (Exception e) {
      e.printStackTrace();
    }
    outbuf.flip();

    RoaringBitmap bitmap_c = new RoaringBitmap();

    ByteBufferBackedInputStream in = new ByteBufferBackedInputStream(outbuf);
    bitmap_c.deserialize(new DataInputStream(in));
    assertTrue(bitmap_c.validate());

    assertEquals(bitmap_a, bitmap_c);
  }

  @Test
  public void testSerialize() throws IOException {
    outbb.rewind();
    ByteBufferBackedOutputStream out = new ByteBufferBackedOutputStream(outbb);
    DataOutputStream dos = new DataOutputStream(out);
    bitmap_empty.serialize(dos);
    bitmap_a.serialize(dos);
  }

  // Encode the RoaringBitmap to a string representation
  public static String encodeToString(RoaringBitmap bitmap) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(bitmap.serializedSizeInBytes());
    bitmap.serialize(new DataOutputStream(baos));
    return Base64.getEncoder().encodeToString(baos.toByteArray());
  }

  // Decode the string representation and reconstruct the RoaringBitmap
  public static RoaringBitmap decodeFromString(String encodedString) throws IOException {
    byte[] decodedBytes = Base64.getDecoder().decode(encodedString);
    ByteArrayInputStream in = new ByteArrayInputStream(decodedBytes);
    DataInputStream dis = new DataInputStream(in);
    RoaringBitmap r = new RoaringBitmap();
    r.deserialize(dis);
    assertTrue(r.validate());
    return r;
  }

  @Test
  public void testStringification() throws IOException {
    RoaringBitmap bitmap = new RoaringBitmap();
    bitmap.add(1);
    bitmap.add(3);
    bitmap.add(5);
    String encodedString = encodeToString(bitmap);
    RoaringBitmap decoded = decodeFromString(encodedString);
    assertEquals(bitmap, decoded);
    outbb.rewind();
  }

  @Test
  public void testDeserializeSmallData() throws IOException {
    RoaringBitmap source = RoaringBitmap.bitmapOf(25286760);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    source.serialize(new DataOutputStream(outputStream));
    boolean expected = source.intersects(26244001, 27293761);

    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    RoaringBitmap target = new RoaringBitmap();
    target.deserialize(new DataInputStream(inputStream));
    assertTrue(target.validate());

    boolean actual = target.intersects(26244001, 27293761);
    assertEquals(actual, expected);
  }

  @Test
  public void testDeserializeSmallDataMutable() throws IOException {
    MutableRoaringBitmap source = MutableRoaringBitmap.bitmapOf(25286760);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    source.serialize(new DataOutputStream(outputStream));
    boolean expected = source.intersects(26244001, 27293761);

    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    MutableRoaringBitmap target = new MutableRoaringBitmap();
    target.deserialize(new DataInputStream(inputStream));

    boolean actual = target.intersects(26244001, 27293761);
    assertEquals(actual, expected);
  }
}
