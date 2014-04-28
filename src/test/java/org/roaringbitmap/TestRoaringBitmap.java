/*
 * Copyright 2013-2014 by Daniel Lemire, Owen Kaser and Samy Chambi
 * Licensed under the Apache License, Version 2.0.
 */
package org.roaringbitmap;

import junit.framework.Assert;
import org.junit.Test;

import java.io.*;
import java.util.*;

/**
 * Generic testing of the roaring bitmaps
 */
@SuppressWarnings({"static-method", "deprecation", "javadoc"})
public class TestRoaringBitmap {

    @Test
    public void ANDNOTtest() {
        final RoaringBitmap rr = new RoaringBitmap();
        for (int k = 4000; k < 4256; ++k)
            rr.add(k);
        for (int k = 65536; k < 65536 + 4000; ++k)
            rr.add(k);
        for (int k = 3 * 65536; k < 3 * 65536 + 9000; ++k)
            rr.add(k);
        for (int k = 4 * 65535; k < 4 * 65535 + 7000; ++k)
            rr.add(k);
        for (int k = 6 * 65535; k < 6 * 65535 + 10000; ++k)
            rr.add(k);
        for (int k = 8 * 65535; k < 8 * 65535 + 1000; ++k)
            rr.add(k);
        for (int k = 9 * 65535; k < 9 * 65535 + 30000; ++k)
            rr.add(k);

        final RoaringBitmap rr2 = new RoaringBitmap();
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
        final RoaringBitmap correct = RoaringBitmap.andNot(rr, rr2);
        rr.andNot(rr2);
        Assert.assertTrue(correct.equals(rr));
    }

    @Test
    public void andnottest4() {
        final RoaringBitmap rb = new RoaringBitmap();
        final RoaringBitmap rb2 = new RoaringBitmap();

        for (int i = 0; i < 200000; i += 4)
            rb2.add(i);
        for (int i = 200000; i < 400000; i += 14)
            rb2.add(i);
        rb2.getCardinality();

        // check or against an empty bitmap
        final RoaringBitmap andNotresult = RoaringBitmap
                .andNot(rb, rb2);
        final RoaringBitmap off = RoaringBitmap.andNot(rb2, rb);

        Assert.assertEquals(rb, andNotresult);
        Assert.assertEquals(rb2, off);
        rb2.andNot(rb);
        Assert.assertEquals(rb2, off);

    }

    @Test
    public void andtest() {
        final RoaringBitmap rr = new RoaringBitmap();
        for (int k = 0; k < 4000; ++k) {
            rr.add(k);
        }
        rr.add(100000);
        rr.add(110000);
        final RoaringBitmap rr2 = new RoaringBitmap();
        rr2.add(13);
        final RoaringBitmap rrand = RoaringBitmap.and(rr, rr2);
        int[] array = rrand.toArray();

        Assert.assertEquals(array.length, 1);
        Assert.assertEquals(array[0], 13);
        rr.and(rr2);
        array = rr.toArray();
        Assert.assertEquals(array.length, 1);
        Assert.assertEquals(array[0], 13);

    }

    @Test
    public void ANDtest() {
        final RoaringBitmap rr = new RoaringBitmap();
        for (int k = 4000; k < 4256; ++k)
            rr.add(k);
        for (int k = 65536; k < 65536 + 4000; ++k)
            rr.add(k);
        for (int k = 3 * 65536; k < 3 * 65536 + 9000; ++k)
            rr.add(k);
        for (int k = 4 * 65535; k < 4 * 65535 + 7000; ++k)
            rr.add(k);
        for (int k = 6 * 65535; k < 6 * 65535 + 10000; ++k)
            rr.add(k);
        for (int k = 8 * 65535; k < 8 * 65535 + 1000; ++k)
            rr.add(k);
        for (int k = 9 * 65535; k < 9 * 65535 + 30000; ++k)
            rr.add(k);

        final RoaringBitmap rr2 = new RoaringBitmap();
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
        final RoaringBitmap correct = RoaringBitmap.and(rr, rr2);
        rr.and(rr2);
        Assert.assertTrue(correct.equals(rr));
    }

    @Test
    public void andtest2() {
        final RoaringBitmap rr = new RoaringBitmap();
        for (int k = 0; k < 4000; ++k) {
            rr.add(k);
        }
        rr.add(100000);
        rr.add(110000);
        final RoaringBitmap rr2 = new RoaringBitmap();
        rr2.add(13);
        final RoaringBitmap rrand = RoaringBitmap.and(rr, rr2);

        final int[] array = rrand.toArray();
        Assert.assertEquals(array.length, 1);
        Assert.assertEquals(array[0], 13);
    }

    @Test
    public void andtest3() {
        final int[] arrayand = new int[11256];
        int pos = 0;
        final RoaringBitmap rr = new RoaringBitmap();
        for (int k = 4000; k < 4256; ++k)
            rr.add(k);
        for (int k = 65536; k < 65536 + 4000; ++k)
            rr.add(k);
        for (int k = 3 * 65536; k < 3 * 65536 + 1000; ++k)
            rr.add(k);
        for (int k = 3 * 65536 + 1000; k < 3 * 65536 + 7000; ++k)
            rr.add(k);
        for (int k = 3 * 65536 + 7000; k < 3 * 65536 + 9000; ++k)
            rr.add(k);
        for (int k = 4 * 65536; k < 4 * 65536 + 7000; ++k)
            rr.add(k);
        for (int k = 6 * 65536; k < 6 * 65536 + 10000; ++k)
            rr.add(k);
        for (int k = 8 * 65536; k < 8 * 65536 + 1000; ++k)
            rr.add(k);
        for (int k = 9 * 65536; k < 9 * 65536 + 30000; ++k)
            rr.add(k);

        final RoaringBitmap rr2 = new RoaringBitmap();
        for (int k = 4000; k < 4256; ++k) {
            rr2.add(k);
            arrayand[pos++] = k;
        }
        for (int k = 65536; k < 65536 + 4000; ++k) {
            rr2.add(k);
            arrayand[pos++] = k;
        }
        for (int k = 3 * 65536 + 1000; k < 3 * 65536 + 7000; ++k) {
            rr2.add(k);
            arrayand[pos++] = k;
        }
        for (int k = 6 * 65536; k < 6 * 65536 + 1000; ++k) {
            rr2.add(k);
            arrayand[pos++] = k;
        }
        for (int k = 7 * 65536; k < 7 * 65536 + 1000; ++k) {
            rr2.add(k);
        }
        for (int k = 10 * 65536; k < 10 * 65536 + 5000; ++k) {
            rr2.add(k);
        }

        final RoaringBitmap rrand = RoaringBitmap.and(rr, rr2);

        final int[] arrayres = rrand.toArray();

        for (int i = 0; i < arrayres.length; i++)
            if (arrayres[i] != arrayand[i])
                System.out.println(arrayres[i]);

        Assert.assertTrue(Arrays.equals(arrayand, arrayres));

    }

    @Test
    public void andtest4() {
        final RoaringBitmap rb = new RoaringBitmap();
        final RoaringBitmap rb2 = new RoaringBitmap();

        for (int i = 0; i < 200000; i += 4)
            rb2.add(i);
        for (int i = 200000; i < 400000; i += 14)
            rb2.add(i);

        // check or against an empty bitmap
        final RoaringBitmap andresult = RoaringBitmap.and(rb, rb2);
        final RoaringBitmap off = RoaringBitmap.and(rb2, rb);
        Assert.assertTrue(andresult.equals(off));

        Assert.assertEquals(0, andresult.getCardinality());

        for (int i = 500000; i < 600000; i += 14)
            rb.add(i);
        for (int i = 200000; i < 400000; i += 3)
            rb2.add(i);
        // check or against an empty bitmap
        final RoaringBitmap andresult2 = RoaringBitmap.and(rb, rb2);
        Assert.assertEquals(0, andresult.getCardinality());

        Assert.assertEquals(0, andresult2.getCardinality());
        for (int i = 0; i < 200000; i += 4)
            rb.add(i);
        for (int i = 200000; i < 400000; i += 14)
            rb.add(i);
        Assert.assertEquals(0, andresult.getCardinality());
        final RoaringBitmap rc = RoaringBitmap.and(rb, rb2);
        rb.and(rb2);
        Assert.assertEquals(rc.getCardinality(), rb.getCardinality());

    }

    @Test
    public void ArrayContainerCardinalityTest() {
        final ArrayContainer ac = new ArrayContainer();
        for (short k = 0; k < 100; ++k) {
            ac.add(k);
            Assert.assertEquals(ac.getCardinality(), k + 1);
        }
        for (short k = 0; k < 100; ++k) {
            ac.add(k);
            Assert.assertEquals(ac.getCardinality(), 100);
        }
    }

    @Test
    public void arraytest() {
        final ArrayContainer rr = new ArrayContainer();
        rr.add((short) 110);
        rr.add((short) 114);
        rr.add((short) 115);
        final short[] array = new short[3];
        int pos = 0;
        for (final short i : rr)
            array[pos++] = i;
        Assert.assertEquals(array[0], (short) 110);
        Assert.assertEquals(array[1], (short) 114);
        Assert.assertEquals(array[2], (short) 115);
    }

    @Test
    public void basictest() {
        final RoaringBitmap rr = new RoaringBitmap();
        final int[] a = new int[4002];
        int pos = 0;
        for (int k = 0; k < 4000; ++k) {
            rr.add(k);
            a[pos++] = k;
        }
        rr.add(100000);
        a[pos++] = 100000;
        rr.add(110000);
        a[pos++] = 110000;
        final int[] array = rr.toArray();
        for (int i = 0; i < array.length; i++)
            if (array[i] != a[i])
                System.out.println("rr : " + array[i] + " a : "
                        + a[i]);

        Assert.assertTrue(Arrays.equals(array, a));
    }

    @Test
    public void BitmapContainerCardinalityTest() {
        final BitmapContainer ac = new BitmapContainer();
        for (short k = 0; k < 100; ++k) {
            ac.add(k);
            Assert.assertEquals(ac.getCardinality(), k + 1);
        }
        for (short k = 0; k < 100; ++k) {
            ac.add(k);
            Assert.assertEquals(ac.getCardinality(), 100);
        }
    }

    @Test
    public void bitmaptest() {
        final BitmapContainer rr = new BitmapContainer();
        rr.add((short) 110);
        rr.add((short) 114);
        rr.add((short) 115);
        final short[] array = new short[3];
        int pos = 0;
        for (final short i : rr)
            array[pos++] = i;
        Assert.assertEquals(array[0], (short) 110);
        Assert.assertEquals(array[1], (short) 114);
        Assert.assertEquals(array[2], (short) 115);
    }

    @Test
    public void cardinalityTest() {
        final int N = 1024;
        for (int gap = 7; gap < 100000; gap *= 10) {
            for (int offset = 2; offset <= 1024; offset *= 2) {
                final RoaringBitmap rb = new RoaringBitmap();
                // check the add of new values
                for (int k = 0; k < N; k++) {
                    rb.add(k * gap);
                    Assert.assertEquals(
                            rb.getCardinality(), k + 1);
                }
                Assert.assertEquals(rb.getCardinality(), N);
                // check the add of existing values
                for (int k = 0; k < N; k++) {
                    rb.add(k * gap);
                    Assert.assertEquals(
                            rb.getCardinality(), N);
                }

                final RoaringBitmap rb2 = new RoaringBitmap();

                for (int k = 0; k < N; k++) {
                    rb2.add(k * gap * offset);
                    Assert.assertEquals(
                            rb2.getCardinality(), k + 1);
                }

                Assert.assertEquals(rb2.getCardinality(), N);

                for (int k = 0; k < N; k++) {
                    rb2.add(k * gap * offset);
                    Assert.assertEquals(
                            rb2.getCardinality(), N);
                }
                Assert.assertEquals(RoaringBitmap.and(rb, rb2)
                        .getCardinality(), N / offset);
                Assert.assertEquals(RoaringBitmap.or(rb, rb2)
                        .getCardinality(), 2 * N - N / offset);
                Assert.assertEquals(RoaringBitmap.xor(rb, rb2)
                        .getCardinality(), 2 * N - 2 * N
                        / offset);
            }
        }
    }

    @Test
    public void clearTest() {
        final RoaringBitmap rb = new RoaringBitmap();
        for (int i = 0; i < 200000; i += 7)
            // dense
            rb.add(i);
        for (int i = 200000; i < 400000; i += 177)
            // sparse
            rb.add(i);

        final RoaringBitmap rb2 = new RoaringBitmap();
        final RoaringBitmap rb3 = new RoaringBitmap();
        for (int i = 0; i < 200000; i += 4)
            rb2.add(i);
        for (int i = 200000; i < 400000; i += 14)
            rb2.add(i);

        rb.clear();
        Assert.assertEquals(0, rb.getCardinality());
        Assert.assertTrue(0 != rb2.getCardinality());

        rb.add(4);
        rb3.add(4);
        final RoaringBitmap andresult = RoaringBitmap.and(rb, rb2);
        final RoaringBitmap orresult = RoaringBitmap.or(rb, rb2);

        Assert.assertEquals(1, andresult.getCardinality());
        Assert.assertEquals(rb2.getCardinality(),
                orresult.getCardinality());

        for (int i = 0; i < 200000; i += 4) {
            rb.add(i);
            rb3.add(i);
        }
        for (int i = 200000; i < 400000; i += 114) {
            rb.add(i);
            rb3.add(i);
        }

        final int[] arrayrr = rb.toArray();
        final int[] arrayrr3 = rb3.toArray();

        Assert.assertTrue(Arrays.equals(arrayrr, arrayrr3));
    }

    @Test
    public void ContainerFactory() {
        BitmapContainer bc1, bc2, bc3;
        ArrayContainer ac1, ac2, ac3;

        bc1 = new BitmapContainer();
        bc2 = new BitmapContainer();
        bc3 = new BitmapContainer();
        ac1 = new ArrayContainer();
        ac2 = new ArrayContainer();
        ac3 = new ArrayContainer();

        for (short i = 0; i < 5000; i++)
            bc1.add((short) (i * 70));
        for (short i = 0; i < 5000; i++)
            bc2.add((short) (i * 70));
        for (short i = 0; i < 5000; i++)
            bc3.add((short) (i * 70));

        for (short i = 0; i < 4000; i++)
            ac1.add((short) (i * 50));
        for (short i = 0; i < 4000; i++)
            ac2.add((short) (i * 50));
        for (short i = 0; i < 4000; i++)
            ac3.add((short) (i * 50));

        BitmapContainer rbc;

        rbc = ac1.clone().toBitmapContainer();
        Assert.assertTrue(validate(rbc, ac1));
        rbc = ac2.clone().toBitmapContainer();
        Assert.assertTrue(validate(rbc, ac2));
        rbc = ac3.clone().toBitmapContainer();
        Assert.assertTrue(validate(rbc, ac3));
    }

    @Test
    public void flipTest1() {
        final RoaringBitmap rb = new RoaringBitmap();

        rb.flip(100000, 200000); // in-place on empty bitmap
        final int rbcard = rb.getCardinality();
        Assert.assertEquals(100000, rbcard);

        final BitSet bs = new BitSet();
        for (int i = 100000; i < 200000; ++i)
            bs.set(i);
        Assert.assertTrue(equals(bs, rb));
    }

    @Test
    public void flipTest1A() {
        final RoaringBitmap rb = new RoaringBitmap();

        final RoaringBitmap rb1 = RoaringBitmap
                .flip(rb, 100000, 200000);
        final int rbcard = rb1.getCardinality();
        Assert.assertEquals(100000, rbcard);
        Assert.assertEquals(0, rb.getCardinality());

        final BitSet bs = new BitSet();
        Assert.assertTrue(equals(bs, rb)); // still empty?
        for (int i = 100000; i < 200000; ++i)
            bs.set(i);
        Assert.assertTrue(equals(bs, rb1));
    }

    @Test
    public void flipTest2() {
        final RoaringBitmap rb = new RoaringBitmap();

        rb.flip(100000, 100000);
        final int rbcard = rb.getCardinality();
        Assert.assertEquals(0, rbcard);

        final BitSet bs = new BitSet();
        Assert.assertTrue(equals(bs, rb));
    }

    @Test
    public void flipTest2A() {
        final RoaringBitmap rb = new RoaringBitmap();

        final RoaringBitmap rb1 = RoaringBitmap
                .flip(rb, 100000, 100000);
        rb.add(1); // will not affect rb1 (no shared container)
        final int rbcard = rb1.getCardinality();
        Assert.assertEquals(0, rbcard);
        Assert.assertEquals(1, rb.getCardinality());

        final BitSet bs = new BitSet();
        Assert.assertTrue(equals(bs, rb1));
        bs.set(1);
        Assert.assertTrue(equals(bs, rb));
    }

    @Test
    public void flipTest3() {
        final RoaringBitmap rb = new RoaringBitmap();

        rb.flip(100000, 200000); // got 100k-199999
        rb.flip(100000, 199991); // give back 100k-199990
        final int rbcard = rb.getCardinality();

        Assert.assertEquals(9, rbcard);

        final BitSet bs = new BitSet();
        for (int i = 199991; i < 200000; ++i)
            bs.set(i);

        Assert.assertTrue(equals(bs, rb));
    }

    @Test
    public void flipTest3A() {
        System.out.println("FlipTest3A");
        final RoaringBitmap rb = new RoaringBitmap();
        final RoaringBitmap rb1 = RoaringBitmap
                .flip(rb, 100000, 200000);
        final RoaringBitmap rb2 = RoaringBitmap.flip(rb1, 100000,
                199991);
        final int rbcard = rb2.getCardinality();

        Assert.assertEquals(9, rbcard);

        final BitSet bs = new BitSet();
        for (int i = 199991; i < 200000; ++i)
            bs.set(i);

        Assert.assertTrue(equals(bs, rb2));
    }

    @Test
    public void flipTest4() { // fits evenly on both ends
        System.out.println("FlipTest4");
        final RoaringBitmap rb = new RoaringBitmap();
        rb.flip(100000, 200000); // got 100k-199999
        rb.flip(65536, 4 * 65536);
        final int rbcard = rb.getCardinality();

        // 65536 to 99999 are 1s
        // 200000 to 262143 are 1s: total card

        Assert.assertEquals(96608, rbcard);

        final BitSet bs = new BitSet();
        for (int i = 65536; i < 100000; ++i)
            bs.set(i);
        for (int i = 200000; i < 262144; ++i)
            bs.set(i);

        Assert.assertTrue(equals(bs, rb));
    }

    @Test
    public void flipTest4A() {
        System.out.println("FlipTest4A");
        final RoaringBitmap rb = new RoaringBitmap();
        final RoaringBitmap rb1 = RoaringBitmap
                .flip(rb, 100000, 200000);
        final RoaringBitmap rb2 = RoaringBitmap.flip(rb1, 65536,
                4 * 65536);
        final int rbcard = rb2.getCardinality();

        Assert.assertEquals(96608, rbcard);

        final BitSet bs = new BitSet();
        for (int i = 65536; i < 100000; ++i)
            bs.set(i);
        for (int i = 200000; i < 262144; ++i)
            bs.set(i);

        Assert.assertTrue(equals(bs, rb2));
    }

    @Test
    public void flipTest5() { // fits evenly on small end, multiple
        // containers
        System.out.println("FlipTest5");
        final RoaringBitmap rb = new RoaringBitmap();
        rb.flip(100000, 132000);
        rb.flip(65536, 120000);
        final int rbcard = rb.getCardinality();

        // 65536 to 99999 are 1s
        // 120000 to 131999

        Assert.assertEquals(46464, rbcard);

        final BitSet bs = new BitSet();
        for (int i = 65536; i < 100000; ++i)
            bs.set(i);
        for (int i = 120000; i < 132000; ++i)
            bs.set(i);
        Assert.assertTrue(equals(bs, rb));
    }

    @Test
    public void flipTest5A() {
        System.out.println("FlipTest5A");
        final RoaringBitmap rb = new RoaringBitmap();
        final RoaringBitmap rb1 = RoaringBitmap
                .flip(rb, 100000, 132000);
        final RoaringBitmap rb2 = RoaringBitmap
                .flip(rb1, 65536, 120000);
        final int rbcard = rb2.getCardinality();

        Assert.assertEquals(46464, rbcard);

        final BitSet bs = new BitSet();
        for (int i = 65536; i < 100000; ++i)
            bs.set(i);
        for (int i = 120000; i < 132000; ++i)
            bs.set(i);
        Assert.assertTrue(equals(bs, rb2));
    }

    @Test
    public void flipTest6() { // fits evenly on big end, multiple containers
        System.out.println("FlipTest6");
        final RoaringBitmap rb = new RoaringBitmap();
        rb.flip(100000, 132000);
        rb.flip(99000, 2 * 65536);
        final int rbcard = rb.getCardinality();

        // 99000 to 99999 are 1000 1s
        // 131072 to 131999 are 928 1s

        Assert.assertEquals(1928, rbcard);

        final BitSet bs = new BitSet();
        for (int i = 99000; i < 100000; ++i)
            bs.set(i);
        for (int i = 2 * 65536; i < 132000; ++i)
            bs.set(i);
        Assert.assertTrue(equals(bs, rb));
    }

    @Test
    public void flipTest6A() {
        System.out.println("FlipTest6A");
        final RoaringBitmap rb = new RoaringBitmap();
        final RoaringBitmap rb1 = RoaringBitmap
                .flip(rb, 100000, 132000);
        final RoaringBitmap rb2 = RoaringBitmap.flip(rb1, 99000,
                2 * 65536);
        final int rbcard = rb2.getCardinality();

        Assert.assertEquals(1928, rbcard);

        final BitSet bs = new BitSet();
        for (int i = 99000; i < 100000; ++i)
            bs.set(i);
        for (int i = 2 * 65536; i < 132000; ++i)
            bs.set(i);
        Assert.assertTrue(equals(bs, rb2));
    }

    @Test
    public void flipTest7() { // within 1 word, first container
        System.out.println("FlipTest7");
        final RoaringBitmap rb = new RoaringBitmap();
        rb.flip(650, 132000);
        rb.flip(648, 651);
        final int rbcard = rb.getCardinality();

        // 648, 649, 651-131999

        Assert.assertEquals(132000 - 651 + 2, rbcard);

        final BitSet bs = new BitSet();
        bs.set(648);
        bs.set(649);
        for (int i = 651; i < 132000; ++i)
            bs.set(i);
        Assert.assertTrue(equals(bs, rb));
    }

    @Test
    public void flipTest7A() { // within 1 word, first container
        System.out.println("FlipTest7A");
        final RoaringBitmap rb = new RoaringBitmap();
        final RoaringBitmap rb1 = RoaringBitmap.flip(rb, 650, 132000);
        final RoaringBitmap rb2 = RoaringBitmap.flip(rb1, 648, 651);
        final int rbcard = rb2.getCardinality();

        // 648, 649, 651-131999

        Assert.assertEquals(132000 - 651 + 2, rbcard);

        final BitSet bs = new BitSet();
        bs.set(648);
        bs.set(649);
        for (int i = 651; i < 132000; ++i)
            bs.set(i);
        Assert.assertTrue(equals(bs, rb2));
    }

    @Test
    public void flipTestBig() {
        final int numCases = 1000;
        System.out.println("flipTestBig for " + numCases + " tests");
        final RoaringBitmap rb = new RoaringBitmap();
        final BitSet bs = new BitSet();
        final Random r = new Random(3333);
        int checkTime = 2;

        for (int i = 0; i < numCases; ++i) {
            final int start = r.nextInt(65536 * 20);
            int end = r.nextInt(65536 * 20);
            if (r.nextDouble() < 0.1)
                end = start + r.nextInt(100);
            rb.flip(start, end);
            if (start < end)
                bs.flip(start, end); // throws exception
            // otherwise
            // insert some more ANDs to keep things sparser
            if (r.nextDouble() < 0.2) {
                final RoaringBitmap mask = new RoaringBitmap();
                final BitSet mask1 = new BitSet();
                final int startM = r.nextInt(65536 * 20);
                final int endM = startM + 100000;
                mask.flip(startM, endM);
                mask1.flip(startM, endM);
                mask.flip(0, 65536 * 20 + 100000);
                mask1.flip(0, 65536 * 20 + 100000);
                rb.and(mask);
                bs.and(mask1);
            }
            // see if we can detect incorrectly shared containers
            if (r.nextDouble() < 0.1) {
                final RoaringBitmap irrelevant = RoaringBitmap
                        .flip(rb, 10, 100000);
                irrelevant.flip(5, 200000);
                irrelevant.flip(190000, 260000);
            }
            if (i > checkTime) {
                System.out.println("check after " + i
                        + ", card = " + rb.getCardinality());
                Assert.assertTrue(equals(bs, rb));
                checkTime *= 1.5;
            }
        }
    }

    @Test
    public void flipTestBigA() {
        final int numCases = 1000;
        System.out.println("flipTestBigA for " + numCases + " tests");
        final BitSet bs = new BitSet();
        final Random r = new Random(3333);
        int checkTime = 2;
        RoaringBitmap rb1 = new RoaringBitmap(), rb2 = null; // alternate
        // between
        // them

        for (int i = 0; i < numCases; ++i) {
            final int start = r.nextInt(65536 * 20);
            int end = r.nextInt(65536 * 20);
            if (r.nextDouble() < 0.1)
                end = start + r.nextInt(100);

            if ((i & 1) == 0) {
                rb2 = RoaringBitmap.flip(rb1, start, end);
                // tweak the other, catch bad sharing
                rb1.flip(r.nextInt(65536 * 20),
                        r.nextInt(65536 * 20));
            } else {
                rb1 = RoaringBitmap.flip(rb2, start, end);
                rb2.flip(r.nextInt(65536 * 20),
                        r.nextInt(65536 * 20));
            }

            if (start < end)
                bs.flip(start, end); // throws exception
            // otherwise
            // insert some more ANDs to keep things sparser
            if (r.nextDouble() < 0.2 && (i & 1) == 0) {
                final RoaringBitmap mask = new RoaringBitmap();
                final BitSet mask1 = new BitSet();
                final int startM = r.nextInt(65536 * 20);
                final int endM = startM + 100000;
                mask.flip(startM, endM);
                mask1.flip(startM, endM);
                mask.flip(0, 65536 * 20 + 100000);
                mask1.flip(0, 65536 * 20 + 100000);
                rb2.and(mask);
                bs.and(mask1);
            }

            if (i > checkTime) {
                System.out.println("check after " + i
                        + ", card = " + rb2.getCardinality());
                final RoaringBitmap rb = (i & 1) == 0 ? rb2
                        : rb1;
                final boolean status = equals(bs, rb);
                Assert.assertTrue(status);
                checkTime *= 1.5;
            }
        }
    }

    @Test
    public void ortest() {
        final RoaringBitmap rr = new RoaringBitmap();
        for (int k = 0; k < 4000; ++k) {
            rr.add(k);
        }
        rr.add(100000);
        rr.add(110000);
        final RoaringBitmap rr2 = new RoaringBitmap();
        for (int k = 0; k < 4000; ++k) {
            rr2.add(k);
        }

        final RoaringBitmap rror = RoaringBitmap.or(rr, rr2);

        final int[] array = rror.toArray();
        final int[] arrayrr = rr.toArray();

        Assert.assertTrue(Arrays.equals(array, arrayrr));

        rr.or(rr2);
        final int[] arrayirr = rr.toArray();
        Assert.assertTrue(Arrays.equals(array, arrayirr));

    }

    @Test
    public void ORtest() {
        final RoaringBitmap rr = new RoaringBitmap();
        for (int k = 4000; k < 4256; ++k)
            rr.add(k);
        for (int k = 65536; k < 65536 + 4000; ++k)
            rr.add(k);
        for (int k = 3 * 65536; k < 3 * 65536 + 9000; ++k)
            rr.add(k);
        for (int k = 4 * 65535; k < 4 * 65535 + 7000; ++k)
            rr.add(k);
        for (int k = 6 * 65535; k < 6 * 65535 + 10000; ++k)
            rr.add(k);
        for (int k = 8 * 65535; k < 8 * 65535 + 1000; ++k)
            rr.add(k);
        for (int k = 9 * 65535; k < 9 * 65535 + 30000; ++k)
            rr.add(k);

        final RoaringBitmap rr2 = new RoaringBitmap();
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
        final RoaringBitmap correct = RoaringBitmap.or(rr, rr2);
        rr.or(rr2);
        Assert.assertTrue(correct.equals(rr));
    }

    @Test
    public void ortest2() {
        final int[] arrayrr = new int[4000 + 4000 + 2];
        int pos = 0;
        final RoaringBitmap rr = new RoaringBitmap();
        for (int k = 0; k < 4000; ++k) {
            rr.add(k);
            arrayrr[pos++] = k;
        }
        rr.add(100000);
        rr.add(110000);
        final RoaringBitmap rr2 = new RoaringBitmap();
        for (int k = 4000; k < 8000; ++k) {
            rr2.add(k);
            arrayrr[pos++] = k;
        }

        arrayrr[pos++] = 100000;
        arrayrr[pos++] = 110000;

        final RoaringBitmap rror = RoaringBitmap.or(rr, rr2);

        final int[] arrayor = rror.toArray();

        Assert.assertTrue(Arrays.equals(arrayor, arrayrr));
    }

    @Test
    public void ortest3() {
        final HashSet<Integer> V1 = new HashSet<Integer>();
        final HashSet<Integer> V2 = new HashSet<Integer>();

        final RoaringBitmap rr = new RoaringBitmap();
        final RoaringBitmap rr2 = new RoaringBitmap();
        // For the first 65536: rr2 has a bitmap container, and rr has
        // an array container.
        // We will check the union between a BitmapCintainer and an
        // arrayContainer
        for (int k = 0; k < 4000; ++k) {
            rr2.add(k);
            V1.add(k);
        }
        for (int k = 3500; k < 4500; ++k) {
            rr.add(k);
            V1.add(k);
        }
        for (int k = 4000; k < 65000; ++k) {
            rr2.add(k);
            V1.add(k);
        }

        // In the second node of each roaring bitmap, we have two bitmap
        // containers.
        // So, we will check the union between two BitmapContainers
        for (int k = 65536; k < 65536 + 10000; ++k) {
            rr.add(k);
            V1.add(k);
        }

        for (int k = 65536; k < 65536 + 14000; ++k) {
            rr2.add(k);
            V1.add(k);
        }

        // In the 3rd node of each Roaring Bitmap, we have an
        // ArrayContainer, so, we will try the union between two
        // ArrayContainers.
        for (int k = 4 * 65535; k < 4 * 65535 + 1000; ++k) {
            rr.add(k);
            V1.add(k);
        }

        for (int k = 4 * 65535; k < 4 * 65535 + 800; ++k) {
            rr2.add(k);
            V1.add(k);
        }

        // For the rest, we will check if the union will take them in
        // the result
        for (int k = 6 * 65535; k < 6 * 65535 + 1000; ++k) {
            rr.add(k);
            V1.add(k);
        }

        for (int k = 7 * 65535; k < 7 * 65535 + 2000; ++k) {
            rr2.add(k);
            V1.add(k);
        }

        final RoaringBitmap rror = RoaringBitmap.or(rr, rr2);
        boolean valide = true;

        // Si tous les elements de rror sont dans V1 et que tous les
        // elements de
        // V1 sont dans rror(V2)
        // alors V1 == rror

        final Object[] tab = V1.toArray();
        final Vector<Integer> vector = new Vector<Integer>();
        for (Object aTab : tab)
            vector.add((Integer) aTab);

        for (final int i : rror.toArray()) {
            if (!vector.contains(new Integer(i))) {
                valide = false;
            }
            V2.add(i);
        }
        for (int i = 0; i < V1.size(); i++)
            if (!V2.contains(vector.elementAt(i))) {
                valide = false;
            }

        Assert.assertEquals(valide, true);
    }

    // tests for how range falls on container boundaries

    @Test
    public void ortest4() {
        final RoaringBitmap rb = new RoaringBitmap();
        final RoaringBitmap rb2 = new RoaringBitmap();

        for (int i = 0; i < 200000; i += 4)
            rb2.add(i);
        for (int i = 200000; i < 400000; i += 14)
            rb2.add(i);
        final int rb2card = rb2.getCardinality();

        // check or against an empty bitmap
        final RoaringBitmap orresult = RoaringBitmap.or(rb, rb2);
        final RoaringBitmap off = RoaringBitmap.or(rb2, rb);
        Assert.assertTrue(orresult.equals(off));

        Assert.assertEquals(rb2card, orresult.getCardinality());

        for (int i = 500000; i < 600000; i += 14)
            rb.add(i);
        for (int i = 200000; i < 400000; i += 3)
            rb2.add(i);
        // check or against an empty bitmap
        final RoaringBitmap orresult2 = RoaringBitmap.or(rb, rb2);
        Assert.assertEquals(rb2card, orresult.getCardinality());

        Assert.assertEquals(rb2.getCardinality() + rb.getCardinality(),
                orresult2.getCardinality());
        rb.or(rb2);
        Assert.assertTrue(rb.equals(orresult2));

    }

    @Test
    public void randomTest() {
        rTest(15);
        rTest(1024);
        rTest(4096);
        rTest(65536);
        rTest(65536 * 16);
    }

    @Test
    public void removeSpeedyArrayTest() {
        final RoaringBitmap rb = new RoaringBitmap();
        for (int i = 0; i < 10000; i++)
            rb.add(i);

        for (int i = 10000; i > 0; i++) {
            rb.highLowContainer.remove(Util.highbits(i));
            Assert.assertEquals(rb.contains(i), false);
        }

    }

    public void rTest(final int N) {
        System.out.println("rtest N=" + N);
        for (int gap = 1; gap <= 65536; gap *= 2) {
            final BitSet bs1 = new BitSet();
            final RoaringBitmap rb1 = new RoaringBitmap();
            for (int x = 0; x <= N; x += gap) {
                bs1.set(x);
                rb1.add(x);
            }
            if (bs1.cardinality() != rb1.getCardinality())
                throw new RuntimeException("different card");
            if (!equals(bs1, rb1))
                throw new RuntimeException("basic  bug");
            for (int offset = 1; offset <= gap; offset *= 2) {
                final BitSet bs2 = new BitSet();
                final RoaringBitmap rb2 = new RoaringBitmap();
                for (int x = 0; x <= N; x += gap) {
                    bs2.set(x + offset);
                    rb2.add(x + offset);
                }
                if (bs2.cardinality() != rb2.getCardinality())
                    throw new RuntimeException(
                            "different card");
                if (!equals(bs2, rb2))
                    throw new RuntimeException("basic  bug");

                BitSet clonebs1;
                // testing AND
                clonebs1 = (BitSet) bs1.clone();
                clonebs1.and(bs2);
                if (!equals(clonebs1,
                        RoaringBitmap.and(rb1, rb2)))
                    throw new RuntimeException("bug and");
                {
                    final RoaringBitmap t = rb1.clone();
                    t.and(rb2);
                    if (!equals(clonebs1, t))
                        throw new RuntimeException(
                                "bug inplace and");
                    if (!t.equals(RoaringBitmap.and(rb1, rb2))) {
                        System.out
                                .println(t.highLowContainer
                                        .getContainerAtIndex(
                                                0)
                                        .getClass()
                                        .getCanonicalName());
                        System.out
                                .println(RoaringBitmap
                                        .and(rb1, rb2).highLowContainer
                                        .getContainerAtIndex(
                                                0)
                                        .getClass()
                                        .getCanonicalName());

                        throw new RuntimeException(
                                "bug inplace and");
                    }
                }

                // testing OR
                clonebs1 = (BitSet) bs1.clone();
                clonebs1.or(bs2);

                if (!equals(clonebs1,RoaringBitmap.or(rb1, rb2)))
                    throw new RuntimeException("bug or");
                {
                    final RoaringBitmap t = rb1.clone();
                    t.or(rb2);
                    if (!equals(clonebs1, t))
                        throw new RuntimeException("bug or");
                    if (!t.equals(RoaringBitmap.or(rb1, rb2)))
                        throw new RuntimeException("bug or");
                    if (!t.toString().equals(RoaringBitmap.or(rb1, rb2).toString()))
                        throw new RuntimeException("bug or");

                }
                // testing XOR
                clonebs1 = (BitSet) bs1.clone();
                clonebs1.xor(bs2);
                if (!equals(clonebs1, RoaringBitmap.xor(rb1, rb2))) {
                    throw new RuntimeException("bug xor");
                }
                {
                    final RoaringBitmap t = rb1.clone();
                    t.xor(rb2);
                    if (!equals(clonebs1, t))
                        throw new RuntimeException("bug xor");
                    if (!t.equals(RoaringBitmap.xor(rb1, rb2)))
                        throw new RuntimeException("bug xor");
                }
                // testing NOTAND
                clonebs1 = (BitSet) bs1.clone();
                clonebs1.andNot(bs2);
                if (!equals(clonebs1, RoaringBitmap.andNot(rb1, rb2))) {
                    throw new RuntimeException("bug andnot");
                }
                clonebs1 = (BitSet) bs2.clone();
                clonebs1.andNot(bs1);
                if (!equals(clonebs1, RoaringBitmap.andNot(rb2, rb1))) {
                    throw new RuntimeException("bug andnot");
                }
                {
                    final RoaringBitmap t = rb2.clone();
                    t.andNot(rb1);
                    if (!equals(clonebs1, t)) {
                        throw new RuntimeException("bug inplace andnot");
                    }
                    final RoaringBitmap g = RoaringBitmap.andNot(rb2, rb1);
                    if (!equals(clonebs1, g)) {
                        throw new RuntimeException("bug andnot");
                    }
                    if (!t.equals(g))
                        throw new RuntimeException("bug");
                }
                clonebs1 = (BitSet) bs1.clone();
                clonebs1.andNot(bs2);
                if (!equals(clonebs1, RoaringBitmap.andNot(rb1, rb2))) {
                    throw new RuntimeException("bug andnot");
                }
                {
                    final RoaringBitmap t = rb1.clone();
                    t.andNot(rb2);
                    if (!equals(clonebs1, t)) {
                        throw new RuntimeException("bug andnot");
                    }
                    final RoaringBitmap g = RoaringBitmap.andNot(rb1, rb2);
                    if (!equals(clonebs1, g)) {
                        throw new RuntimeException("bug andnot");
                    }
                    if (!t.equals(g))
                        throw new RuntimeException("bug");
                }
            }
        }
    }

    @Test
    public void simplecardinalityTest() {
        final int N = 512;
        final int gap = 70;

        final RoaringBitmap rb = new RoaringBitmap();
        for (int k = 0; k < N; k++) {
            rb.add(k * gap);
            Assert.assertEquals(rb.getCardinality(), k + 1);
        }
        Assert.assertEquals(rb.getCardinality(), N);
        for (int k = 0; k < N; k++) {
            rb.add(k * gap);
            Assert.assertEquals(rb.getCardinality(), N);
        }

    }

    @Test
    public void testSerialization() throws IOException, ClassNotFoundException {
        final RoaringBitmap rr = new RoaringBitmap();
        for (int k = 65000; k < 2 * 65000; ++k)
            rr.add(k);
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        // Note: you could use a file output steam instead of
        // ByteArrayOutputStream
        final ObjectOutputStream oo = new ObjectOutputStream(bos);
        rr.writeExternal(oo);
        oo.close();
        final RoaringBitmap rrback = new RoaringBitmap();
        final ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        rrback.readExternal(new ObjectInputStream(bis));
        Assert.assertEquals(rr.getCardinality(), rrback.getCardinality());
        Assert.assertTrue(rr.equals(rrback));
    }


    @Test
    public void testSerialization2() throws IOException,
            ClassNotFoundException {
        final RoaringBitmap rr = new RoaringBitmap();
        for (int k = 200; k < 400; ++k)
            rr.add(k);
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        // Note: you could use a file output steam instead of
        // ByteArrayOutputStream
        final ObjectOutputStream oo = new ObjectOutputStream(bos);
        rr.writeExternal(oo);
        oo.close();
        final RoaringBitmap rrback = new RoaringBitmap();
        final ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        rrback.readExternal(new ObjectInputStream(bis));
        Assert.assertEquals(rr.getCardinality(),rrback.getCardinality());
        Assert.assertTrue(rr.equals(rrback));
    }

    @Test
    public void testSerialization3() throws IOException, ClassNotFoundException {
        final RoaringBitmap rr = new RoaringBitmap();
        for (int k = 65000; k < 2 * 65000; ++k)
            rr.add(k);
        rr.add(1444000);
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        // Note: you could use a file output steam instead of
        // ByteArrayOutputStream
        int howmuch = rr.serializedSizeInBytes();
        final DataOutputStream oo = new DataOutputStream(bos);
        rr.serialize(oo);
        oo.close();
        Assert.assertEquals(howmuch, bos.toByteArray().length);
        final RoaringBitmap rrback = new RoaringBitmap();
        final ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        rrback.deserialize(new DataInputStream(bis));
        Assert.assertEquals(rr.getCardinality(), rrback.getCardinality());
        Assert.assertTrue(rr.equals(rrback));
    }

    @Test
    public void testSerialization4() throws IOException, ClassNotFoundException {
      final RoaringBitmap rr = new RoaringBitmap();
      for (int k = 1; k <= 10000000; k+=10)
        rr.add(k);
      final ByteArrayOutputStream bos = new ByteArrayOutputStream();
      // Note: you could use a file output steam instead of
      // ByteArrayOutputStream
      int howmuch = rr.serializedSizeInBytes();
      final DataOutputStream oo = new DataOutputStream(bos);
      rr.serialize(oo);
      oo.close();
      Assert.assertEquals(howmuch, bos.toByteArray().length);
      final RoaringBitmap rrback = new RoaringBitmap();
      final ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
      rrback.deserialize(new DataInputStream(bis));
      Assert.assertEquals(rr.getCardinality(), rrback.getCardinality());
      Assert.assertTrue(rr.equals(rrback));
    }


    @Test
    public void XORtest() {
        final RoaringBitmap rr = new RoaringBitmap();
        for (int k = 4000; k < 4256; ++k)
            rr.add(k);
        for (int k = 65536; k < 65536 + 4000; ++k)
            rr.add(k);
        for (int k = 3 * 65536; k < 3 * 65536 + 9000; ++k)
            rr.add(k);
        for (int k = 4 * 65535; k < 4 * 65535 + 7000; ++k)
            rr.add(k);
        for (int k = 6 * 65535; k < 6 * 65535 + 10000; ++k)
            rr.add(k);
        for (int k = 8 * 65535; k < 8 * 65535 + 1000; ++k)
            rr.add(k);
        for (int k = 9 * 65535; k < 9 * 65535 + 30000; ++k)
            rr.add(k);

        final RoaringBitmap rr2 = new RoaringBitmap();
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
        final RoaringBitmap correct = RoaringBitmap.xor(rr, rr2);
        rr.xor(rr2);
        Assert.assertTrue(correct.equals(rr));
    }

    @Test
    public void xortest1() {
        final HashSet<Integer> V1 = new HashSet<Integer>();
        final HashSet<Integer> V2 = new HashSet<Integer>();

        final RoaringBitmap rr = new RoaringBitmap();
        final RoaringBitmap rr2 = new RoaringBitmap();
        // For the first 65536: rr2 has a bitmap container, and rr has
        // an array container.
        // We will check the union between a BitmapCintainer and an
        // arrayContainer
        for (int k = 0; k < 4000; ++k) {
            rr2.add(k);
            if (k < 3500)
                V1.add(k);
        }
        for (int k = 3500; k < 4500; ++k) {
            rr.add(k);
        }
        for (int k = 4000; k < 65000; ++k) {
            rr2.add(k);
            if (k >= 4500)
                V1.add(k);
        }

        // In the second node of each roaring bitmap, we have two bitmap
        // containers.
        // So, we will check the union between two BitmapContainers
        for (int k = 65536; k < 65536 + 30000; ++k) {
            rr.add(k);
        }

        for (int k = 65536; k < 65536 + 50000; ++k) {
            rr2.add(k);
            if (k >= 65536 + 30000)
                V1.add(k);
        }

        // In the 3rd node of each Roaring Bitmap, we have an
        // ArrayContainer. So, we will try the union between two
        // ArrayContainers.
        for (int k = 4 * 65535; k < 4 * 65535 + 1000; ++k) {
            rr.add(k);
            if (k >= 4 * 65535 + 800)
                V1.add(k);
        }

        for (int k = 4 * 65535; k < 4 * 65535 + 800; ++k) {
            rr2.add(k);
        }

        // For the rest, we will check if the union will take them in
        // the result
        for (int k = 6 * 65535; k < 6 * 65535 + 1000; ++k) {
            rr.add(k);
            V1.add(k);
        }

        for (int k = 7 * 65535; k < 7 * 65535 + 2000; ++k) {
            rr2.add(k);
            V1.add(k);
        }

        final RoaringBitmap rrxor = RoaringBitmap.xor(rr, rr2);
        boolean valide = true;

        // Si tous les elements de rror sont dans V1 et que tous les
        // elements de
        // V1 sont dans rror(V2)
        // alors V1 == rror
        final Object[] tab = V1.toArray();
        final Vector<Integer> vector = new Vector<Integer>();
        for (Object aTab : tab)
            vector.add((Integer) aTab);

        for (final int i : rrxor.toArray()) {
            if (!vector.contains(new Integer(i))) {
                valide = false;
            }
            V2.add(i);
        }
        for (int i = 0; i < V1.size(); i++)
            if (!V2.contains(vector.elementAt(i))) {
                valide = false;
            }

        Assert.assertEquals(valide, true);
    }

    @Test
    public void xortest4() {
        final RoaringBitmap rb = new RoaringBitmap();
        final RoaringBitmap rb2 = new RoaringBitmap();

        for (int i = 0; i < 200000; i += 4)
            rb2.add(i);
        for (int i = 200000; i < 400000; i += 14)
            rb2.add(i);
        final int rb2card = rb2.getCardinality();

        // check or against an empty bitmap
        final RoaringBitmap xorresult = RoaringBitmap.xor(rb, rb2);
        final RoaringBitmap off = RoaringBitmap.or(rb2, rb);
        Assert.assertTrue(xorresult.equals(off));

        Assert.assertEquals(rb2card, xorresult.getCardinality());

        for (int i = 500000; i < 600000; i += 14)
            rb.add(i);
        for (int i = 200000; i < 400000; i += 3)
            rb2.add(i);
        // check or against an empty bitmap
        final RoaringBitmap xorresult2 = RoaringBitmap.xor(rb, rb2);
        Assert.assertEquals(rb2card, xorresult.getCardinality());

        Assert.assertEquals(rb2.getCardinality() + rb.getCardinality(), xorresult2.getCardinality());
        rb.xor(rb2);
        Assert.assertTrue(xorresult2.equals(rb));

    }

    boolean validate(BitmapContainer bc, ArrayContainer ac) {
        // Checking the cardinalities of each container

        if (bc.getCardinality() != ac.getCardinality()) {
            System.out.println("cardinality differs");
            return false;
        }
        // Checking that the two containers contain the same values
        int counter = 0;

        int i = bc.nextSetBit(0);
        while (i >= 0) {
            ++counter;
            if (!ac.contains((short) i)) {
                System.out.println("content differs");
                System.out.println(bc);
                System.out.println(ac);
                return false;
            }
            i = bc.nextSetBit(i + 1);
        }

        // checking the cardinality of the BitmapContainer
        return counter == bc.getCardinality();
    }

    public static boolean equals(BitSet bs, RoaringBitmap rr) {
        final int[] a = new int[bs.cardinality()];
        int pos = 0;
        for (int x = bs.nextSetBit(0); x >= 0; x = bs.nextSetBit(x + 1))
            a[pos++] = x;
        return Arrays.equals(rr.toArray(), a);
    }
}