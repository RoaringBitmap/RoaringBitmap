/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap.buffer;

import org.junit.Assert;
import org.junit.Test;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.OrderedWriter;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MappeableArrayContainer;
import org.roaringbitmap.buffer.MappeableBitmapContainer;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Generic testing of the roaring bitmaps
 */
@SuppressWarnings({"static-method"})
public class TestImmutableRoaringBitmap {
  @SuppressWarnings("resource")
  static ByteBuffer serializeRoaring(ImmutableRoaringBitmap mrb) throws IOException {
    byte[] backingArray = new byte[mrb.serializedSizeInBytes() + 1024];
    ByteBuffer outbb = ByteBuffer.wrap(backingArray, 1024, mrb.serializedSizeInBytes()).slice();
    DataOutputStream dos = new DataOutputStream(new OutputStream() {
      ByteBuffer mBB;

      OutputStream init(ByteBuffer mbb) {
        mBB = mbb;
        return this;
      }

      @Override
      public void write(byte[] b) {}

      @Override
      public void write(byte[] b, int off, int l) {
        mBB.put(b, off, l);
      }

      @Override
      public void write(int b) {
        mBB.put((byte) b);
      }
    }.init(outbb));
    mrb.serialize(dos);
    dos.close();


    return outbb;
  }

  @Test
  public void ANDNOTtest() {
    final MutableRoaringBitmap rr = new MutableRoaringBitmap();
    for (int k = 4000; k < 4256; ++k) {
      rr.add(k);
    }
    for (int k = 65536; k < 65536 + 4000; ++k) {
      rr.add(k);
    }
    for (int k = 3 * 65536; k < 3 * 65536 + 9000; ++k) {
      rr.add(k);
    }
    for (int k = 4 * 65535; k < 4 * 65535 + 7000; ++k) {
      rr.add(k);
    }
    for (int k = 6 * 65535; k < 6 * 65535 + 10000; ++k) {
      rr.add(k);
    }
    for (int k = 8 * 65535; k < 8 * 65535 + 1000; ++k) {
      rr.add(k);
    }
    for (int k = 9 * 65535; k < 9 * 65535 + 30000; ++k) {
      rr.add(k);
    }

    final MutableRoaringBitmap rr2 = new MutableRoaringBitmap();
    for (int k = 4000; k < 4256; ++k) {
      rr2.add(k);
    }
    for (int k = 65536; k < 65536 + 4000; ++k) {
      rr2.add(k);
    }
    for (int k = 3 * 65536 + 2000; k < 3 * 65536 + 6000; ++k) {
      rr2.add(k);
    }
    for (int k = 6 * 65535; k < 6 * 65535 + 1000; ++k) {
      rr2.add(k);
    }
    for (int k = 7 * 65535; k < 7 * 65535 + 1000; ++k) {
      rr2.add(k);
    }
    for (int k = 10 * 65535; k < 10 * 65535 + 5000; ++k) {
      rr2.add(k);
    }
    final MutableRoaringBitmap correct = MutableRoaringBitmap.andNot(rr, rr2);
    rr.andNot(rr2);
    Assert.assertTrue(correct.equals(rr));
    Assert.assertTrue(correct.hashCode() == rr.hashCode());

  }


  @Test
  public void andnottest4() {
    final MutableRoaringBitmap rb = new MutableRoaringBitmap();
    final MutableRoaringBitmap rb2 = new MutableRoaringBitmap();

    for (int i = 0; i < 200000; i += 4) {
      rb2.add(i);
    }
    for (int i = 200000; i < 400000; i += 14) {
      rb2.add(i);
    }
    rb2.getCardinality();

    // check op against an empty bitmap
    final MutableRoaringBitmap andNotresult = MutableRoaringBitmap.andNot(rb, rb2);
    final MutableRoaringBitmap off = MutableRoaringBitmap.andNot(rb2, rb);

    Assert.assertEquals(rb, andNotresult);
    Assert.assertEquals(rb2, off);
    rb2.andNot(rb);
    Assert.assertEquals(rb2, off);
  }



  @Test
  public void andnottest4A() {
    final MutableRoaringBitmap rb = new MutableRoaringBitmap();
    final MutableRoaringBitmap rb2 = new MutableRoaringBitmap();

    for (int i = 0; i < 200000; i++) {
      if (i % 9 < 6) {
        rb2.add(i);
      }
    }
    for (int i = 200000; i < 400000; i += 14) {
      rb2.add(i);
    }
    rb2.getCardinality();
    rb2.runOptimize();

    // check against an empty bitmap
    final MutableRoaringBitmap andNotresult = MutableRoaringBitmap.andNot(rb, rb2);
    final MutableRoaringBitmap off = MutableRoaringBitmap.andNot(rb2, rb);

    Assert.assertEquals(rb, andNotresult);
    Assert.assertEquals(rb2, off);
    rb2.andNot(rb);
    Assert.assertEquals(rb2, off);
  }



  @Test
  public void ANDNOTtestA() {
    // both have run containers
    final MutableRoaringBitmap rr = new MutableRoaringBitmap();
    for (int k = 4000; k < 4256; ++k) {
      rr.add(k);
    }
    for (int k = 65536; k < 65536 + 4000; ++k) {
      rr.add(k);
    }
    for (int k = 3 * 65536; k < 3 * 65536 + 9000; ++k) {
      rr.add(k);
    }
    for (int k = 4 * 65535; k < 4 * 65535 + 7000; ++k) {
      rr.add(k);
    }
    for (int k = 6 * 65535; k < 6 * 65535 + 10000; ++k) {
      rr.add(k);
    }
    for (int k = 8 * 65535; k < 8 * 65535 + 1000; ++k) {
      rr.add(k);
    }
    for (int k = 9 * 65535; k < 9 * 65535 + 30000; ++k) {
      rr.add(k);
    }

    rr.runOptimize();

    final MutableRoaringBitmap rr2 = new MutableRoaringBitmap();
    for (int k = 4000; k < 4256; ++k) {
      rr2.add(k);
    }
    for (int k = 65536; k < 65536 + 4000; ++k) {
      rr2.add(k);
    }
    for (int k = 3 * 65536 + 2000; k < 3 * 65536 + 6000; ++k) {
      rr2.add(k);
    }
    for (int k = 6 * 65535; k < 6 * 65535 + 1000; ++k) {
      rr2.add(k);
    }
    for (int k = 7 * 65535; k < 7 * 65535 + 1000; ++k) {
      rr2.add(k);
    }
    for (int k = 10 * 65535; k < 10 * 65535 + 5000; ++k) {
      rr2.add(k);
    }
    rr2.runOptimize();
    final MutableRoaringBitmap correct = MutableRoaringBitmap.andNot(rr, rr2);
    rr.andNot(rr2);
    Assert.assertTrue(correct.equals(rr));
    Assert.assertTrue(correct.hashCode() == rr.hashCode());

  }


  @Test
  public void ANDNOTtestB() {
    // first one has run containers but not second.
    final MutableRoaringBitmap rr = new MutableRoaringBitmap();
    for (int k = 4000; k < 4256; ++k) {
      rr.add(k);
    }
    for (int k = 65536; k < 65536 + 4000; ++k) {
      rr.add(k);
    }
    for (int k = 3 * 65536; k < 3 * 65536 + 9000; ++k) {
      rr.add(k);
    }
    for (int k = 4 * 65535; k < 4 * 65535 + 7000; ++k) {
      rr.add(k);
    }
    for (int k = 6 * 65535; k < 6 * 65535 + 10000; ++k) {
      rr.add(k);
    }
    for (int k = 8 * 65535; k < 8 * 65535 + 1000; ++k) {
      rr.add(k);
    }
    for (int k = 9 * 65535; k < 9 * 65535 + 30000; ++k) {
      rr.add(k);
    }

    rr.runOptimize();

    final MutableRoaringBitmap rr2 = new MutableRoaringBitmap();
    for (int k = 4000; k < 4256; ++k) {
      rr2.add(k);
    }
    for (int k = 65536; k < 65536 + 4000; ++k) {
      rr2.add(k);
    }
    for (int k = 3 * 65536 + 2000; k < 3 * 65536 + 6000; ++k) {
      rr2.add(k);
    }
    for (int k = 6 * 65535; k < 6 * 65535 + 1000; ++k) {
      rr2.add(k);
    }
    for (int k = 7 * 65535; k < 7 * 65535 + 1000; ++k) {
      rr2.add(k);
    }
    for (int k = 10 * 65535; k < 10 * 65535 + 5000; ++k) {
      rr2.add(k);
    }
    final MutableRoaringBitmap correct = MutableRoaringBitmap.andNot(rr, rr2);
    rr.andNot(rr2);
    Assert.assertTrue(correct.equals(rr));
    Assert.assertTrue(correct.hashCode() == rr.hashCode());

  }



  @Test
  public void ANDNOTtestC() {
    // second has run containers, but not first
    final MutableRoaringBitmap rr = new MutableRoaringBitmap();
    for (int k = 4000; k < 4256; ++k) {
      rr.add(k);
    }
    for (int k = 65536; k < 65536 + 4000; ++k) {
      rr.add(k);
    }
    for (int k = 3 * 65536; k < 3 * 65536 + 9000; ++k) {
      rr.add(k);
    }
    for (int k = 4 * 65535; k < 4 * 65535 + 7000; ++k) {
      rr.add(k);
    }
    for (int k = 6 * 65535; k < 6 * 65535 + 10000; ++k) {
      rr.add(k);
    }
    for (int k = 8 * 65535; k < 8 * 65535 + 1000; ++k) {
      rr.add(k);
    }
    for (int k = 9 * 65535; k < 9 * 65535 + 30000; ++k) {
      rr.add(k);
    }

    final MutableRoaringBitmap rr2 = new MutableRoaringBitmap();
    for (int k = 4000; k < 4256; ++k) {
      rr2.add(k);
    }
    for (int k = 65536; k < 65536 + 4000; ++k) {
      rr2.add(k);
    }
    for (int k = 3 * 65536 + 2000; k < 3 * 65536 + 6000; ++k) {
      rr2.add(k);
    }
    for (int k = 6 * 65535; k < 6 * 65535 + 1000; ++k) {
      rr2.add(k);
    }
    for (int k = 7 * 65535; k < 7 * 65535 + 1000; ++k) {
      rr2.add(k);
    }
    for (int k = 10 * 65535; k < 10 * 65535 + 5000; ++k) {
      rr2.add(k);
    }
    rr2.runOptimize();

    final MutableRoaringBitmap correct = MutableRoaringBitmap.andNot(rr, rr2);
    rr.andNot(rr2);
    Assert.assertTrue(correct.equals(rr));
    Assert.assertTrue(correct.hashCode() == rr.hashCode());
  }


  @Test
  public void fliptest1() {
    final MutableRoaringBitmap rb = new MutableRoaringBitmap();
    rb.add(0);
    rb.add(2);
    final MutableRoaringBitmap rb2 = MutableRoaringBitmap.flip(rb, 0, 3);
    final MutableRoaringBitmap result = new MutableRoaringBitmap();
    result.add(1);

    Assert.assertEquals(result, rb2);
  }



  @Test
  public void fliptest2() {
    final MutableRoaringBitmap rb = new MutableRoaringBitmap();
    rb.add(0);
    rb.add(2);
    final MutableRoaringBitmap rb2 = ImmutableRoaringBitmap.flip(rb, 0, 3);
    final MutableRoaringBitmap result = new MutableRoaringBitmap();
    result.add(1);

    Assert.assertEquals(result, rb2);
  }



  @Test
  public void MappeableContainersAccessTest() throws IOException {
    MutableRoaringBitmap mr = new MutableRoaringBitmap();
    for (int k = 4000; k < 4256; ++k) {
      mr.add(k);
    }
    for (int k = (1 << 16); k < (1 << 16) + 4000; ++k) {
      mr.add(k);
    }
    for (int k = (1 << 18); k < (1 << 18) + 9000; ++k) {
      mr.add(k);
    }
    for (int k = (1 << 19); k < (1 << 19) + 7000; ++k) {
      mr.add(k);
    }
    for (int k = (1 << 20); k < (1 << 20) + 10000; ++k) {
      mr.add(k);
    }
    for (int k = (1 << 23); k < (1 << 23) + 1000; ++k) {
      mr.add(k);
    }
    for (int k = (1 << 24); k < (1 << 24) + 30000; ++k) {
      mr.add(k);
    }
    ByteBuffer buffer = serializeRoaring(mr);
    buffer.rewind();
    ImmutableRoaringBitmap ir = new ImmutableRoaringBitmap(buffer);
    mr = ir.toMutableRoaringBitmap();
    Assert.assertTrue(
        (mr.getMappeableRoaringArray().getContainerAtIndex(0)) instanceof MappeableArrayContainer);
    Assert.assertTrue(
        (mr.getMappeableRoaringArray().getContainerAtIndex(1)) instanceof MappeableArrayContainer);
    Assert.assertTrue(
        (mr.getMappeableRoaringArray().getContainerAtIndex(2)) instanceof MappeableBitmapContainer);
    Assert.assertTrue(
        (mr.getMappeableRoaringArray().getContainerAtIndex(3)) instanceof MappeableBitmapContainer);
    Assert.assertTrue(
        (mr.getMappeableRoaringArray().getContainerAtIndex(4)) instanceof MappeableBitmapContainer);
    Assert.assertTrue(
        (mr.getMappeableRoaringArray().getContainerAtIndex(5)) instanceof MappeableArrayContainer);
    Assert.assertTrue(
        (mr.getMappeableRoaringArray().getContainerAtIndex(6)) instanceof MappeableBitmapContainer);

    Assert.assertTrue(mr.getMappeableRoaringArray().getContainerAtIndex(0).getCardinality() == 256);
    Assert
        .assertTrue(mr.getMappeableRoaringArray().getContainerAtIndex(1).getCardinality() == 4000);
    Assert
        .assertTrue(mr.getMappeableRoaringArray().getContainerAtIndex(2).getCardinality() == 9000);
    Assert
        .assertTrue(mr.getMappeableRoaringArray().getContainerAtIndex(3).getCardinality() == 7000);
    Assert
        .assertTrue(mr.getMappeableRoaringArray().getContainerAtIndex(4).getCardinality() == 10000);
    Assert
        .assertTrue(mr.getMappeableRoaringArray().getContainerAtIndex(5).getCardinality() == 1000);
    Assert
        .assertTrue(mr.getMappeableRoaringArray().getContainerAtIndex(6).getCardinality() == 30000);

    MutableRoaringBitmap mr2 = new MutableRoaringBitmap();
    for (int k = 4000; k < 4256; ++k) {
      mr2.add(k);
    }
    for (int k = (1 << 16); k < (1 << 16) + 4000; ++k) {
      mr2.add(k);
    }
    for (int k = (1 << 18) + 2000; k < (1 << 18) + 8000; ++k) {
      mr2.add(k);
    }
    for (int k = (1 << 21); k < (1 << 21) + 1000; ++k) {
      mr2.add(k);
    }
    for (int k = (1 << 22); k < (1 << 22) + 2000; ++k) {
      mr2.add(k);
    }
    for (int k = (1 << 25); k < (1 << 25) + 5000; ++k) {
      mr2.add(k);
    }
    buffer = serializeRoaring(mr2);
    buffer.rewind();
    ImmutableRoaringBitmap ir2 = new ImmutableRoaringBitmap(buffer);
    mr2 = ir2.toMutableRoaringBitmap();
    Assert.assertTrue(
        (mr2.getMappeableRoaringArray().getContainerAtIndex(0)) instanceof MappeableArrayContainer);
    Assert.assertTrue(
        (mr2.getMappeableRoaringArray().getContainerAtIndex(1)) instanceof MappeableArrayContainer);
    Assert.assertTrue((mr2.getMappeableRoaringArray()
        .getContainerAtIndex(2)) instanceof MappeableBitmapContainer);
    Assert.assertTrue(
        (mr2.getMappeableRoaringArray().getContainerAtIndex(3)) instanceof MappeableArrayContainer);
    Assert.assertTrue(
        (mr2.getMappeableRoaringArray().getContainerAtIndex(4)) instanceof MappeableArrayContainer);
    Assert.assertTrue((mr2.getMappeableRoaringArray()
        .getContainerAtIndex(5)) instanceof MappeableBitmapContainer);

    Assert
        .assertTrue(mr2.getMappeableRoaringArray().getContainerAtIndex(0).getCardinality() == 256);
    Assert
        .assertTrue(mr2.getMappeableRoaringArray().getContainerAtIndex(1).getCardinality() == 4000);
    Assert
        .assertTrue(mr2.getMappeableRoaringArray().getContainerAtIndex(2).getCardinality() == 6000);
    Assert
        .assertTrue(mr2.getMappeableRoaringArray().getContainerAtIndex(3).getCardinality() == 1000);
    Assert
        .assertTrue(mr2.getMappeableRoaringArray().getContainerAtIndex(4).getCardinality() == 2000);
    Assert
        .assertTrue(mr2.getMappeableRoaringArray().getContainerAtIndex(5).getCardinality() == 5000);

    MutableRoaringBitmap mr3 = new MutableRoaringBitmap();
    buffer = serializeRoaring(mr3);
    buffer.rewind();
    ImmutableRoaringBitmap ir3 = new ImmutableRoaringBitmap(buffer);
    mr3 = ir3.toMutableRoaringBitmap();

    Assert.assertTrue(mr3.getCardinality() == 0);
  }

  @Test
  public void MappeableContainersAccessTestA() throws IOException {
    // run containers too
    MutableRoaringBitmap mr = new MutableRoaringBitmap();
    for (int k = 4000; k < 4256; ++k) {
      mr.add(k);
    }
    for (int k = (1 << 16); k < (1 << 16) + 4000; ++k) {
      mr.add(k);
    }
    for (int k = (1 << 18); k < (1 << 18) + 9000; ++k) {
      mr.add(k);
    }
    for (int k = (1 << 19); k < (1 << 19) + 7000; ++k) {
      mr.add(k);
    }
    for (int k = (1 << 20); k < (1 << 20) + 10000; ++k) {
      mr.add(k);
    }
    for (int k = (1 << 23); k < (1 << 23) + 1000; ++k) {
      mr.add(k);
    }
    for (int k = (1 << 24); k < (1 << 24) + 30000; ++k) {
      mr.add(k);
    }
    mr.runOptimize();
    ByteBuffer buffer = serializeRoaring(mr);
    buffer.rewind();
    ImmutableRoaringBitmap ir = new ImmutableRoaringBitmap(buffer);
    mr = ir.toMutableRoaringBitmap();
    Assert.assertTrue(
        (mr.getMappeableRoaringArray().getContainerAtIndex(0)) instanceof MappeableRunContainer);
    Assert.assertTrue(
        (mr.getMappeableRoaringArray().getContainerAtIndex(1)) instanceof MappeableRunContainer);
    Assert.assertTrue(
        (mr.getMappeableRoaringArray().getContainerAtIndex(2)) instanceof MappeableRunContainer);
    Assert.assertTrue(
        (mr.getMappeableRoaringArray().getContainerAtIndex(3)) instanceof MappeableRunContainer);
    Assert.assertTrue(
        (mr.getMappeableRoaringArray().getContainerAtIndex(4)) instanceof MappeableRunContainer);
    Assert.assertTrue(
        (mr.getMappeableRoaringArray().getContainerAtIndex(5)) instanceof MappeableRunContainer);
    Assert.assertTrue(
        (mr.getMappeableRoaringArray().getContainerAtIndex(6)) instanceof MappeableRunContainer);

    Assert.assertTrue(mr.getMappeableRoaringArray().getContainerAtIndex(0).getCardinality() == 256);
    Assert
        .assertTrue(mr.getMappeableRoaringArray().getContainerAtIndex(1).getCardinality() == 4000);
    Assert
        .assertTrue(mr.getMappeableRoaringArray().getContainerAtIndex(2).getCardinality() == 9000);
    Assert
        .assertTrue(mr.getMappeableRoaringArray().getContainerAtIndex(3).getCardinality() == 7000);
    Assert
        .assertTrue(mr.getMappeableRoaringArray().getContainerAtIndex(4).getCardinality() == 10000);
    Assert
        .assertTrue(mr.getMappeableRoaringArray().getContainerAtIndex(5).getCardinality() == 1000);
    Assert
        .assertTrue(mr.getMappeableRoaringArray().getContainerAtIndex(6).getCardinality() == 30000);

    MutableRoaringBitmap mr2 = new MutableRoaringBitmap();
    for (int k = 4000; k < 4256; ++k) {
      mr2.add(k);
    }
    for (int k = (1 << 16); k < (1 << 16) + 4000; ++k) {
      mr2.add(k);
    }
    for (int k = (1 << 18) + 2000; k < (1 << 18) + 8000; ++k) {
      mr2.add(k);
    }
    for (int k = (1 << 21); k < (1 << 21) + 1000; ++k) {
      mr2.add(k);
    }
    for (int k = (1 << 22); k < (1 << 22) + 2000; ++k) {
      mr2.add(k);
    }
    for (int k = (1 << 25); k < (1 << 25) + 5000; ++k) {
      mr2.add(k);
    }
    mr2.runOptimize();
    buffer = serializeRoaring(mr2);
    buffer.rewind();
    ImmutableRoaringBitmap ir2 = new ImmutableRoaringBitmap(buffer);
    mr2 = ir2.toMutableRoaringBitmap();
    Assert.assertTrue(
        (mr2.getMappeableRoaringArray().getContainerAtIndex(0)) instanceof MappeableRunContainer);
    Assert.assertTrue(
        (mr2.getMappeableRoaringArray().getContainerAtIndex(1)) instanceof MappeableRunContainer);
    Assert.assertTrue(
        (mr2.getMappeableRoaringArray().getContainerAtIndex(2)) instanceof MappeableRunContainer);
    Assert.assertTrue(
        (mr2.getMappeableRoaringArray().getContainerAtIndex(3)) instanceof MappeableRunContainer);
    Assert.assertTrue(
        (mr2.getMappeableRoaringArray().getContainerAtIndex(4)) instanceof MappeableRunContainer);
    Assert.assertTrue(
        (mr2.getMappeableRoaringArray().getContainerAtIndex(5)) instanceof MappeableRunContainer);

    Assert
        .assertTrue(mr2.getMappeableRoaringArray().getContainerAtIndex(0).getCardinality() == 256);
    Assert
        .assertTrue(mr2.getMappeableRoaringArray().getContainerAtIndex(1).getCardinality() == 4000);
    Assert
        .assertTrue(mr2.getMappeableRoaringArray().getContainerAtIndex(2).getCardinality() == 6000);
    Assert
        .assertTrue(mr2.getMappeableRoaringArray().getContainerAtIndex(3).getCardinality() == 1000);
    Assert
        .assertTrue(mr2.getMappeableRoaringArray().getContainerAtIndex(4).getCardinality() == 2000);
    Assert
        .assertTrue(mr2.getMappeableRoaringArray().getContainerAtIndex(5).getCardinality() == 5000);

    MutableRoaringBitmap mr3 = new MutableRoaringBitmap();
    buffer = serializeRoaring(mr3);
    buffer.rewind();
    ImmutableRoaringBitmap ir3 = new ImmutableRoaringBitmap(buffer);
    mr3 = ir3.toMutableRoaringBitmap();

    Assert.assertTrue(mr3.getCardinality() == 0);
  }


  @Test
  public void testContains() throws IOException {
    System.out.println("test contains");
    MutableRoaringBitmap mr = new MutableRoaringBitmap();
    for (int k = 0; k < 1000; ++k) {
      mr.add(17 * k);
    }
    ByteBuffer buffer = serializeRoaring(mr);
    buffer.rewind();
    ImmutableRoaringBitmap ir = new ImmutableRoaringBitmap(buffer);
    for (int k = 0; k < 17 * 1000; ++k) {
      Assert.assertTrue(ir.contains(k) == (k / 17 * 17 == k));
    }
  }

  @Test
  public void testContainsA() throws IOException {
    System.out.println("test contains (runs too)");
    MutableRoaringBitmap mr = new MutableRoaringBitmap();
    for (int k = 0; k < 1000; ++k) {
      mr.add(17 * k);
    }
    for (int k = 200000; k < 210000; ++k) {
      if (k % 19 != 0) {
        mr.add(k);
      }
    }
    mr.runOptimize();

    ByteBuffer buffer = serializeRoaring(mr);
    buffer.rewind();
    ImmutableRoaringBitmap ir = new ImmutableRoaringBitmap(buffer);
    for (int k = 0; k < 17 * 1000; ++k) {
      Assert.assertTrue(ir.contains(k) == (k / 17 * 17 == k));
    }
  }


  @Test
  public void testHash() {
    MutableRoaringBitmap rbm1 = new MutableRoaringBitmap();
    rbm1.add(17);
    MutableRoaringBitmap rbm2 = new MutableRoaringBitmap();
    rbm2.add(17);
    Assert.assertTrue(rbm1.hashCode() == rbm2.hashCode());
    rbm2 = rbm1.clone();
    Assert.assertTrue(rbm1.hashCode() == rbm2.hashCode());
  }



  @Test
  public void testHighBits() throws IOException {
    // version without any runcontainers (earlier serialization version)
    for (int offset = 1 << 14; offset < 1 << 18; offset *= 2) {
      MutableRoaringBitmap rb = new MutableRoaringBitmap();
      for (long k = Integer.MIN_VALUE; k < Integer.MAX_VALUE; k += offset) {
        rb.add((int) k);
      }
      for (long k = Integer.MIN_VALUE; k < Integer.MAX_VALUE; k += offset) {
        Assert.assertTrue(rb.contains((int) k));
      }
      int[] array = rb.toArray();
      ByteBuffer b = ByteBuffer.allocate(rb.serializedSizeInBytes());
      rb.serialize(new DataOutputStream(new OutputStream() {
        ByteBuffer mBB;

        @Override
        public void close() {}

        @Override
        public void flush() {}

        OutputStream init(final ByteBuffer mbb) {
          mBB = mbb;
          return this;
        }

        @Override
        public void write(final byte[] b) {
          mBB.put(b);
        }

        @Override
        public void write(final byte[] b, final int off, final int l) {
          mBB.put(b, off, l);
        }

        @Override
        public void write(final int b) {
          mBB.put((byte) b);
        }
      }.init(b)));
      b.flip();
      ImmutableRoaringBitmap irb = new ImmutableRoaringBitmap(b);
      Assert.assertTrue(irb.equals(rb));
      for (long k = Integer.MIN_VALUE; k < Integer.MAX_VALUE; k += offset) {
        Assert.assertTrue(irb.contains((int) k));
      }
      Assert.assertTrue(Arrays.equals(array, irb.toArray()));
    }
  }

  @SuppressWarnings("resource")
  @Test
  public void testHighBitsA() throws IOException {
    // includes some run containers
    for (int offset = 1 << 14; offset < 1 << 18; offset *= 2) {
      MutableRoaringBitmap rb = new MutableRoaringBitmap();
      for (long k = Integer.MIN_VALUE; k < Integer.MAX_VALUE - 100000; k += offset) {
        rb.add((int) k);
      }
      for (long k = Integer.MIN_VALUE; k < Integer.MAX_VALUE - 100000; k += offset) {
        Assert.assertTrue(rb.contains((int) k));
      }

      int runlength = 99000;
      for (int k = Integer.MAX_VALUE - 100000; k < Integer.MAX_VALUE - 100000 + runlength; ++k) {
        rb.add(k);
      }

      rb.runOptimize();

      int[] array = rb.toArray();
      // int pos = 0;
      // check that it is in sorted order according to unsigned order
      for (int k = 0; k < array.length - 1; ++k) {
        Assert.assertTrue((0xFFFFFFFFL & array[k]) <= (0xFFFFFFFFL & array[k + 1]));
      }
      ///////////////////////
      // It was decided that Roaring would consider ints as unsigned
      ///////////////////////
      // for (long k = Integer.MIN_VALUE; k < Integer.MAX_VALUE-100000; k += offset) {
      // Assert.assertTrue(array[pos++] == (int)k);
      // }
      // Assert.assertTrue(pos+runlength == array.length);
      ByteBuffer b = ByteBuffer.allocate(rb.serializedSizeInBytes());
      rb.serialize(new DataOutputStream(new OutputStream() {
        ByteBuffer mBB;

        @Override
        public void close() {}

        @Override
        public void flush() {}

        OutputStream init(final ByteBuffer mbb) {
          mBB = mbb;
          return this;
        }

        @Override
        public void write(final byte[] b) {
          mBB.put(b);
        }

        @Override
        public void write(final byte[] b, final int off, final int l) {
          mBB.put(b, off, l);
        }

        @Override
        public void write(final int b) {
          mBB.put((byte) b);
        }
      }.init(b)));
      b.flip();
      ImmutableRoaringBitmap irb = new ImmutableRoaringBitmap(b);
      Assert.assertTrue(irb.equals(rb));
      for (long k = Integer.MIN_VALUE; k < Integer.MAX_VALUE - 100000; k += offset) {
        Assert.assertTrue(irb.contains((int) k));
      }

      for (int k = Integer.MAX_VALUE - 100000; k < Integer.MAX_VALUE - 100000 + runlength; ++k) {
        Assert.assertTrue(irb.contains(k));
      }

      array = irb.toArray();
      for (int k = 0; k < array.length - 1; ++k) {
        Assert.assertTrue((0xFFFFFFFFL & array[k]) <= (0xFFFFFFFFL & array[k + 1]));
      }
      // Assert.assertEquals(Integer.MAX_VALUE - 100000 +runlength-1, array[array.length-1]);
    }
  }



  @SuppressWarnings("resource")
  @Test
  public void testProperSerialization() throws IOException {
    final int SIZE = 500;
    final Random rand = new Random(0);
    for (int i = 0; i < SIZE; ++i) {
      MutableRoaringBitmap r = new MutableRoaringBitmap();
      for (int k = 0; k < 500000; ++k) {
        if (rand.nextDouble() < .5) {
          r.add(k);
        }
      }
      ByteBuffer b = ByteBuffer.allocate(r.serializedSizeInBytes());
      r.serialize(new DataOutputStream(new OutputStream() {
        ByteBuffer mBB;

        @Override
        public void close() {}

        @Override
        public void flush() {}

        OutputStream init(final ByteBuffer mbb) {
          mBB = mbb;
          return this;
        }

        @Override
        public void write(final byte[] b) {
          mBB.put(b);
        }

        @Override
        public void write(final byte[] b, final int off, final int l) {
          mBB.put(b, off, l);
        }

        @Override
        public void write(final int b) {
          mBB.put((byte) b);
        }
      }.init(b)));
      b.flip();
      ImmutableRoaringBitmap irb = new ImmutableRoaringBitmap(b);
      Assert.assertTrue(irb.equals(r));
      Assert.assertTrue(irb.hashCode() == r.hashCode());
      Assert.assertTrue(irb.getCardinality() == r.getCardinality());
    }
  }

  @SuppressWarnings("resource")
  @Test
  public void testProperSerializationA() throws IOException {
    // denser, so we should have run containers
    final int SIZE = 500;
    final Random rand = new Random(0);
    for (int i = 0; i < SIZE; ++i) {
      MutableRoaringBitmap r = new MutableRoaringBitmap();
      for (int k = 0; k < 500000; ++k) {
        if (rand.nextDouble() < .9) {
          r.add(k);
        }
      }
      r.runOptimize();
      ByteBuffer b = ByteBuffer.allocate(r.serializedSizeInBytes());
      r.serialize(new DataOutputStream(new OutputStream() {
        ByteBuffer mBB;

        @Override
        public void close() {}

        @Override
        public void flush() {}

        OutputStream init(final ByteBuffer mbb) {
          mBB = mbb;
          return this;
        }

        @Override
        public void write(final byte[] b) {
          mBB.put(b);
        }

        @Override
        public void write(final byte[] b, final int off, final int l) {
          mBB.put(b, off, l);
        }

        @Override
        public void write(final int b) {
          mBB.put((byte) b);
        }
      }.init(b)));
      b.flip();
      ImmutableRoaringBitmap irb = new ImmutableRoaringBitmap(b);
      Assert.assertTrue(irb.hashCode() == r.hashCode());
      Assert.assertTrue(irb.getCardinality() == r.getCardinality());
    }
  }

  @Test
  public void testSerializeImmutable() throws IOException {
    MutableRoaringBitmap mr = new MutableRoaringBitmap();
    mr.add(5);
    ByteBuffer buffer = serializeRoaring(mr);

    buffer.rewind();
    buffer = serializeRoaring(new ImmutableRoaringBitmap(buffer));

    buffer.rewind();
    ImmutableRoaringBitmap ir = new ImmutableRoaringBitmap(buffer);
    Assert.assertTrue(ir.contains(5));
  }

  @Test
  public void testSerializeImmutableA() throws IOException {
    // includes a runcontainer
    MutableRoaringBitmap mr = new MutableRoaringBitmap();
    for (int i = 1024 * 1024 - 20; i < 1024 * 1024 + 20; ++i) {
      mr.add(i);
    }
    mr.runOptimize();

    ByteBuffer buffer = serializeRoaring(mr);

    buffer.rewind();
    buffer = serializeRoaring(new ImmutableRoaringBitmap(buffer));

    buffer.rewind();
    ImmutableRoaringBitmap ir = new ImmutableRoaringBitmap(buffer);
    Assert.assertFalse(ir.contains(5));

    for (int i = 1024 * 1024 - 20; i < 1024 * 1024 + 20; ++i) {
      Assert.assertTrue(ir.contains(i));
    }
  }




  @Test
  public void testRangedOr() {
    int length = 1000;
    int NUM_ITER = 10;
    Random random = new Random(1234);// please use deterministic tests
    for (int test = 0; test < 50; ++test) {
      final MutableRoaringBitmap rb1 = new MutableRoaringBitmap();
      final MutableRoaringBitmap rb2 = new MutableRoaringBitmap();
      Set<Integer> set1 = new HashSet<>();
      Set<Integer> set2 = new HashSet<>();
      int numBitsToSet = length / 2;
      for (int i = 0; i < numBitsToSet; i++) {
        int val1 = random.nextInt(length);
        int val2 = random.nextInt(length);

        rb1.add(val1);
        set1.add(val1);

        rb2.add(val2);
        set2.add(val2);
      }
      Set<Integer> unionSet = new TreeSet<>();
      unionSet.addAll(set1);
      unionSet.addAll(set2);
      for (int iter = 0; iter < NUM_ITER; iter++) {
        int rangeStart = random.nextInt(length - 1);
        // +1 to ensure rangeEnd >rangeStart, may
        int rangeLength = random.nextInt(length - rangeStart) + 1;
        int rangeEnd = rangeStart + rangeLength;
        Set<Integer> expectedResultSet = new TreeSet<>();
        for (int i = rangeStart; i < rangeEnd; i++) {
          if (unionSet.contains(i)) {
            expectedResultSet.add(i);
          }
        }
        List<ImmutableRoaringBitmap> list = new ArrayList<>();
        list.add(rb1);
        list.add(rb2);
        MutableRoaringBitmap result =
            ImmutableRoaringBitmap.or(list.iterator(), rangeStart, rangeEnd);
        Set<Integer> actualResultSet = new TreeSet<>();
        IntIterator intIterator = result.getIntIterator();
        while (intIterator.hasNext()) {
          actualResultSet.add(intIterator.next());
        }
        Assert.assertEquals(expectedResultSet, actualResultSet);
      }
    }
  }

  @Test
  public void testRangedAnd() {
    int length = 1000;
    int NUM_ITER = 10;
    Random random = new Random(1234);// please use deterministic tests
    for (int test = 0; test < 50; ++test) {
      final MutableRoaringBitmap rb1 = new MutableRoaringBitmap();
      final MutableRoaringBitmap rb2 = new MutableRoaringBitmap();
      Set<Integer> set1 = new HashSet<>();
      Set<Integer> set2 = new HashSet<>();
      int numBitsToSet = length / 2;
      for (int i = 0; i < numBitsToSet; i++) {
        int val1 = random.nextInt(length);
        int val2 = random.nextInt(length);

        rb1.add(val1);
        set1.add(val1);

        rb2.add(val2);
        set2.add(val2);
      }
      Set<Integer> intersectionSet = new TreeSet<>(set1);
      intersectionSet.retainAll(set2);
      for (int iter = 0; iter < NUM_ITER; iter++) {
        int rangeStart = random.nextInt(length - 1);
        // +1 to ensure rangeEnd >rangeStart, may
        int rangeLength = random.nextInt(length - rangeStart) + 1;
        int rangeEnd = rangeStart + rangeLength;
        Set<Integer> expectedResultSet = new TreeSet<>();
        for (int i = rangeStart; i < rangeEnd; i++) {
          if (intersectionSet.contains(i)) {
            expectedResultSet.add(i);
          }
        }
        List<ImmutableRoaringBitmap> list = new ArrayList<>();
        list.add(rb1);
        list.add(rb2);
        MutableRoaringBitmap result =
            ImmutableRoaringBitmap.and(list.iterator(), rangeStart, rangeEnd);
        Set<Integer> actualResultSet = new TreeSet<>();
        IntIterator intIterator = result.getIntIterator();
        while (intIterator.hasNext()) {
          actualResultSet.add(intIterator.next());
        }
        Assert.assertEquals(expectedResultSet, actualResultSet);
      }
    }
  }

  @Test
  public void testRangedXor() {
    int length = 1000;
    int NUM_ITER = 10;
    Random random = new Random(1234);// please use deterministic tests
    for (int test = 0; test < 50; ++test) {
      final MutableRoaringBitmap rb1 = new MutableRoaringBitmap();
      final MutableRoaringBitmap rb2 = new MutableRoaringBitmap();
      Set<Integer> set1 = new HashSet<>();
      Set<Integer> set2 = new HashSet<>();
      int numBitsToSet = length / 2;
      for (int i = 0; i < numBitsToSet; i++) {
        int val1 = random.nextInt(length);
        int val2 = random.nextInt(length);

        rb1.add(val1);
        set1.add(val1);

        rb2.add(val2);
        set2.add(val2);
      }
      Set<Integer> xorSet = new TreeSet<>();
      xorSet.addAll(set1);
      xorSet.addAll(set2);
      Set<Integer> andSet = new TreeSet<>(set1);
      andSet.retainAll(set2);

      xorSet.removeAll(andSet);
      for (int iter = 0; iter < NUM_ITER; iter++) {
        int rangeStart = random.nextInt(length - 1);
        // +1 to ensure rangeEnd >rangeStart, may
        int rangeLength = random.nextInt(length - rangeStart) + 1;
        int rangeEnd = rangeStart + rangeLength;
        Set<Integer> expectedResultSet = new TreeSet<>();
        for (int i = rangeStart; i < rangeEnd; i++) {
          if (xorSet.contains(i)) {
            expectedResultSet.add(i);
          }
        }
        List<ImmutableRoaringBitmap> list = new ArrayList<>();
        list.add(rb1);
        list.add(rb2);
        MutableRoaringBitmap result =
            ImmutableRoaringBitmap.xor(list.iterator(), rangeStart, rangeEnd);
        Set<Integer> actualResultSet = new TreeSet<>();
        IntIterator intIterator = result.getIntIterator();
        while (intIterator.hasNext()) {
          actualResultSet.add(intIterator.next());
        }
        Assert.assertEquals(expectedResultSet, actualResultSet);
      }
    }
  }

  @Test
  public void testRangedAndNot() {
    int length = 1000;
    int NUM_ITER = 10;
    Random random = new Random(1234);// please use deterministic tests
    for (int test = 0; test < 50; ++test) {
      final MutableRoaringBitmap rb1 = new MutableRoaringBitmap();
      final MutableRoaringBitmap rb2 = new MutableRoaringBitmap();
      Set<Integer> set1 = new HashSet<>();
      Set<Integer> set2 = new HashSet<>();
      int numBitsToSet = length / 2;
      for (int i = 0; i < numBitsToSet; i++) {
        int val1 = random.nextInt(length);
        int val2 = random.nextInt(length);

        rb1.add(val1);
        set1.add(val1);

        rb2.add(val2);
        set2.add(val2);
      }
      Set<Integer> andNotSet = new TreeSet<>();
      for (int i : set1) {
        if (!set2.contains(i)) {
          andNotSet.add(i);
        }
      }
      for (int iter = 0; iter < NUM_ITER; iter++) {
        int rangeStart = random.nextInt(length - 1);
        // +1 to ensure rangeEnd >rangeStart, may
        int rangeLength = random.nextInt(length - rangeStart) + 1;
        int rangeEnd = rangeStart + rangeLength;
        Set<Integer> expectedResultSet = new TreeSet<>();
        for (int i = rangeStart; i < rangeEnd; i++) {
          if (andNotSet.contains(i)) {
            expectedResultSet.add(i);
          }
        }
        MutableRoaringBitmap result = ImmutableRoaringBitmap.andNot(rb1, rb2, rangeStart, rangeEnd);
        Set<Integer> actualResultSet = new TreeSet<>();
        IntIterator intIterator = result.getIntIterator();
        while (intIterator.hasNext()) {
          actualResultSet.add(intIterator.next());
        }
        Assert.assertEquals(expectedResultSet, actualResultSet);
      }
    }
  }

  @Test
  @SuppressWarnings( "deprecation" )
  public void testDeprecatedIteratorAnd() {

      MutableRoaringBitmap rb1 = new MutableRoaringBitmap();
      MutableRoaringBitmap rb2 = new MutableRoaringBitmap();

      List<MutableRoaringBitmap> list = new ArrayList<>();
      list.add(rb1);
      list.add(rb2);

      rb1.add(200000L, 400000L);  // two normal positive ranges
      rb2.add(300000L, 500000L);  // full overlap is on 300000 to 399999

      MutableRoaringBitmap result = ImmutableRoaringBitmap.and(list.iterator(), 350000L,  450000L); 
      MutableRoaringBitmap resultInt = ImmutableRoaringBitmap.and(list.iterator(), 350000,  450000);

      Assert.assertTrue(result.equals(resultInt));
      Assert.assertEquals(50000, result.getCardinality());

      
      // empty ranges get empty result
      resultInt = ImmutableRoaringBitmap.and(list.iterator(), 300000, 200000);
      result = ImmutableRoaringBitmap.and(list.iterator(), 300000L, 200000L);
      Assert.assertTrue(result.equals(resultInt));
      Assert.assertEquals(0, resultInt.getCardinality());
  }


  @Test
  @SuppressWarnings( "deprecation" )
  public void testDeprecatedIteratorOr() {

      MutableRoaringBitmap rb1 = new MutableRoaringBitmap();
      MutableRoaringBitmap rb2 = new MutableRoaringBitmap();

      List<MutableRoaringBitmap> list = new ArrayList<>();
      list.add(rb1);
      list.add(rb2);

      rb1.add(200000L, 400000L);  // two normal positive ranges
      rb2.add(300000L, 500000L);  // full union is 200000 to 499999

      MutableRoaringBitmap result = ImmutableRoaringBitmap.or(list.iterator(), 250000L,  550000L); 
      MutableRoaringBitmap resultInt = ImmutableRoaringBitmap.or(list.iterator(), 250000,  550000);

      
      Assert.assertTrue(result.equals(resultInt));
      Assert.assertEquals(250000, result.getCardinality());

      
      // empty ranges get empty result
      resultInt = ImmutableRoaringBitmap.or(list.iterator(), 300000, 200000);
      result = ImmutableRoaringBitmap.or(list.iterator(), 300000L, 200000L);
      Assert.assertTrue(result.equals(resultInt));
      Assert.assertEquals(0, resultInt.getCardinality());
  }


  @Test
  @SuppressWarnings( "deprecation" )
  public void testDeprecatedIteratorAndNot() {

      MutableRoaringBitmap rb1 = new MutableRoaringBitmap();
      MutableRoaringBitmap rb2 = new MutableRoaringBitmap();

      List<MutableRoaringBitmap> list = new ArrayList<>();
      list.add(rb1);
      list.add(rb2);

      rb1.add(200000L, 400000L);  // two normal positive ranges
      rb2.add(300000L, 500000L);  // full andNOToverlap is on 200000 to 299999

      MutableRoaringBitmap result = ImmutableRoaringBitmap.andNot(rb1, rb2, 250000L,  450000L); 
      MutableRoaringBitmap resultInt = ImmutableRoaringBitmap.andNot(rb1, rb2, 250000,  450000);

      Assert.assertTrue(result.equals(resultInt));
      Assert.assertEquals(50000, result.getCardinality());

      
      // empty ranges get empty result
      resultInt = ImmutableRoaringBitmap.andNot(rb1, rb2, 300000, 200000);
      result = ImmutableRoaringBitmap.andNot(rb1, rb2, 300000L, 200000L);
      Assert.assertTrue(result.equals(resultInt));
      Assert.assertEquals(0, resultInt.getCardinality());
  }


  @Test
  @SuppressWarnings( "deprecation" )
  public void testDeprecatedIteratorXor() {

      MutableRoaringBitmap rb1 = new MutableRoaringBitmap();
      MutableRoaringBitmap rb2 = new MutableRoaringBitmap();

      List<MutableRoaringBitmap> list = new ArrayList<>();
      list.add(rb1);
      list.add(rb2);

      rb1.add(200000L, 400000L);  // two normal positive ranges
      rb2.add(300000L, 500000L);  // full XOR is 200000 to 299999, 400000-4999999

      MutableRoaringBitmap result = ImmutableRoaringBitmap.xor(list.iterator(), 250000L,  450000L); 
      MutableRoaringBitmap resultInt = ImmutableRoaringBitmap.xor(list.iterator(), 250000,  450000);

      Assert.assertTrue(result.equals(resultInt));
      Assert.assertEquals(100000, result.getCardinality());

      
      // empty ranges get empty result
      resultInt = ImmutableRoaringBitmap.xor(list.iterator(), 300000, 200000);
      result = ImmutableRoaringBitmap.xor(list.iterator(), 300000L, 200000L);
      Assert.assertTrue(result.equals(resultInt));
      Assert.assertEquals(0, resultInt.getCardinality());
  }

  @Test
  public void testFirstLast_CreateSparseContainersAfterRun() {
    MutableRoaringBitmap rb = new MutableRoaringBitmap();
    rb.add(1L, 1 << 14);
    for(int i = 18; i < 31; ++i) {
      int x = 1 << i;
      rb.add(x);
      Assert.assertEquals(1, rb.toImmutableRoaringBitmap().first());
      rb.last();
      Assert.assertEquals(x, rb.toImmutableRoaringBitmap().last());
    }
  }

  @Test(expected = NoSuchElementException.class)
  public void testEmptyFirst() {
    new MutableRoaringBitmap().toImmutableRoaringBitmap().first();
  }

  @Test(expected = NoSuchElementException.class)
  public void testEmptyLast() {
    new MutableRoaringBitmap().toImmutableRoaringBitmap().last();
  }

  @Test
  public void testFirstLast() {
    MutableRoaringBitmap rb = new MutableRoaringBitmap();

    rb.add(2);
    rb.add(4);
    rb.add(8);
    Assert.assertEquals(2, rb.toImmutableRoaringBitmap().first());
    Assert.assertEquals(8, rb.toImmutableRoaringBitmap().last());

    rb.add(1L << 5, 1L << 14);
    Assert.assertEquals(2, rb.toImmutableRoaringBitmap().first());
    Assert.assertEquals((1 << 14) - 1, rb.toImmutableRoaringBitmap().last());

    rb.add(1L<< 15, 1L << 30);
    Assert.assertEquals(2, rb.toImmutableRoaringBitmap().first());
    Assert.assertEquals((1L << 30) - 1, rb.toImmutableRoaringBitmap().last());
  }

  @Test
  public void testIsHammingSimilar_AtStart() {
    // similar bitmaps in the first container
    ImmutableRoaringBitmap baseline = ImmutableRoaringBitmap.bitmapOf(2, 4, 8, 1 << 17, 1 << 22);
    assertTrue(baseline.isHammingSimilar(baseline, 0));
    MutableRoaringBitmap other = baseline.clone().toMutableRoaringBitmap();
    other.flip(0L, 9);
    for (int i = 0; i < 9; ++i) {
      assertFalse(baseline.isHammingSimilar(other, i));
    }
    assertTrue(baseline.isHammingSimilar(other, 9));
    other.add(0L, 9L);
    for (int i = 0; i < 6; ++i) {
      assertFalse(baseline.isHammingSimilar(other, i));
    }
    assertTrue(baseline.isHammingSimilar(other, 6));
  }

  @Test
  public void testHammingSimilarity_BigVsSmall() {
    MutableRoaringBitmap big = new MutableRoaringBitmap();
    big.add(1, 2, 3, 4);
    big.add(1L << 17, 1L << 30);
    big.flip((1 << 17) | (1 << 16));
    for(int i = 1 << 18; i < 1 << 19; ++i) {
      if(i % 3 == 0) {
        big.flip(i);
      }
    }
    MutableRoaringBitmap small = MutableRoaringBitmap.bitmapOf(1, 2, 3, 4);
    assertFalse(small.isHammingSimilar(big, 1));
    assertFalse(big.isHammingSimilar(small, 1));
  }

  @Test
  public void testHammingSimilarity_Shifted() {
    ImmutableRoaringBitmap baseline = ImmutableRoaringBitmap.bitmapOf(1, 2, 3, 4);
    ImmutableRoaringBitmap shifted = ImmutableRoaringBitmap.bitmapOf((1 << 17) + 1, (1 << 17) + 2,
            (1 << 17) + 3, (1 << 17) + 4);
    assertFalse(baseline.isHammingSimilar(shifted, 0));
  }

  @Test
  public void testIsHammingSimilar_AtEnd() {
    // reject bitmaps that are identical for many chunks but differ at the end
    MutableRoaringBitmap mutable = new MutableRoaringBitmap();
    for (int i = 0; i < 1 << 15; ++i) {
      if (i % 3 == 0) {
        mutable.add(i);
      }
    }
    mutable.add((1L << 16) + 1, 1L << 18);
    mutable.add((1L << 19) + 1, 1L << 20);
    mutable.add((1 << 21) + 1);
    mutable.add((1 << 21) + 3);
    mutable.add((1 << 21) + 5);
    ImmutableRoaringBitmap baseline = mutable.toImmutableRoaringBitmap();
    assertTrue(baseline.isHammingSimilar(baseline, 0));
    MutableRoaringBitmap other = baseline.clone().toMutableRoaringBitmap();
    other.flip((1 << 21) + 1);
    assertTrue(baseline.isHammingSimilar(other, 1));
    assertFalse(baseline.isHammingSimilar(other, 0));
    other.add((1 << 21) + 2);
    assertTrue(baseline.isHammingSimilar(other, 2));
    assertFalse(baseline.isHammingSimilar(other, 1));
    other.flip((1 << 21) + 3);
    assertTrue(baseline.isHammingSimilar(other, 3));
    assertFalse(baseline.isHammingSimilar(other, 2));
  }

  public void testXorCardinality() {
    final MutableRoaringBitmap rb = new MutableRoaringBitmap();
    for (int k = 0; k < 4000; ++k) {
      rb.add(k);
    }
    rb.add(100000);
    rb.add(110000);
    rb.add(1L << 20, 1L << 21);
    rb.flip((1 << 20) | (1 << 19));
    final MutableRoaringBitmap rb2 = new MutableRoaringBitmap();
    for (int k = 0; k < 4000; ++k) {
      rb2.add(k);
    }
    MutableRoaringBitmap xor = ImmutableRoaringBitmap.xor(rb, rb2);
    assertEquals(xor.getCardinality(), ImmutableRoaringBitmap.xorCardinality(rb, rb2));
  }

  @Test
  public void testAndNotCardinality() {
    final MutableRoaringBitmap rb = new MutableRoaringBitmap();
    for (int k = 0; k < 4000; ++k) {
      rb.add(k);
    }
    rb.add(100000);
    rb.add(110000);
    rb.add(1L << 20, 1L << 21);
    rb.flip((1 << 20) | (1 << 19));
    final MutableRoaringBitmap rb2 = new MutableRoaringBitmap();
    for (int k = 0; k < 4000; ++k) {
      rb2.add(k);
    }
    ImmutableRoaringBitmap andNot = ImmutableRoaringBitmap.andNot(rb, rb2);
    assertEquals(andNot.getCardinality(), ImmutableRoaringBitmap.andNotCardinality(rb, rb2));
  }

  @Test
  public void testRankOverflow() {
    Assert.assertEquals(0, ImmutableRoaringBitmap.bitmapOf(65537).rank(1));
    Assert.assertEquals(1, ImmutableRoaringBitmap.bitmapOf(65537).rank(65537));
    Assert.assertEquals(1, ImmutableRoaringBitmap.bitmapOf(65537).rank(65538));
  }


  @Test
  public void testNegativeAdd() {
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    bitmap.add(-7);

    Assert.assertEquals("{4294967289}", bitmap.toString());
  }

  @Test
  public void testNegative_last() {
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    bitmap.add(-7);
    bitmap.add(777);

    Assert.assertEquals(-7, bitmap.last());
  }

  @Test
  public void testContainsRange_ContiguousBitmap() {
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    bitmap.add(0L, 1_000_000L);
    assertTrue(bitmap.contains(1L, 999_999L));
    assertFalse(bitmap.contains(1L, 1_000_001L));
    bitmap.flip(500_000);
    assertFalse(bitmap.contains(1L, 999_999L));
    bitmap.flip(500_000);
    bitmap.flip(500_000L, 600_000L);
    assertFalse(bitmap.contains(1L, 999_999L));
    assertTrue(bitmap.contains(0L, 500_000L));
    assertFalse(bitmap.contains(2_000_001L, 10_000_000L));
  }

  @Test
  public void testContainsRange_SmallBitmap() {
    MutableRoaringBitmap bitmap = MutableRoaringBitmap.bitmapOf(1, 2, 3, 4, 5, 6);
    assertTrue(bitmap.contains(1, 6));
    assertTrue(bitmap.contains(1, 5));
    assertTrue(bitmap.contains(2, 6));
    assertTrue(bitmap.contains(2, 7));
    assertFalse(bitmap.contains(2, 8));
    assertFalse(bitmap.contains(0, 6));
    assertFalse(bitmap.contains(0, 1));
    assertFalse(bitmap.contains(6, 10));
    assertFalse(bitmap.contains(7, 1 << 16));
    assertFalse(bitmap.contains(1 << 17, 1 << 19));
  }

  @Test
  public void testContainsRange_DirtyBitmap() {
    OrderedWriter writer = new OrderedWriter();
    IntStream.range(0, 1_000_000)
            .map(i -> i * 2)
            .forEach(writer::add);
    writer.flush();
    MutableRoaringBitmap bitmap = writer.getUnderlying().toMutableRoaringBitmap();
    assertFalse(bitmap.contains(0L, 2_000_000L));
    assertFalse(bitmap.contains(0L, 2L));
    assertTrue(bitmap.contains(0L, 1L));
    assertTrue(bitmap.contains(1L << 10, 1| (1L << 10)));
    assertFalse(bitmap.contains(1L << 31, 1L << 32));
  }

}
