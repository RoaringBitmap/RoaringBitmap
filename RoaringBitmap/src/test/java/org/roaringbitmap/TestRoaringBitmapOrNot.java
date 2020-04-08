/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */
package org.roaringbitmap;

import org.junit.Assert;
import org.junit.Test;

/**
 * Generic testing of the roaring bitmaps
 */
public class TestRoaringBitmapOrNot {

  @Test
  public void orNot1() {
    final RoaringBitmap rb = new RoaringBitmap();
    final RoaringBitmap rb2 = new RoaringBitmap();

    rb.add(2);
    rb.add(1);
    rb.add(1 << 16); // 65 536
    rb.add(2 << 16); //131 072
    rb.add(3 << 16); //196 608

    rb2.add(1 << 16);// 65 536
    rb2.add(3 << 16);//196 608

    rb.orNot(rb2, (4 << 16) - 1);

    Assert.assertEquals((4 << 16) - 1, rb.getCardinality());

    final IntIterator iterator = rb.getIntIterator();

    for (int i = 0; i < (4 << 16) - 1; ++i) {
      Assert.assertTrue("Error on iteration " + i, iterator.hasNext());
      Assert.assertEquals(i, iterator.next());
    }
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void orNot2() {
    final RoaringBitmap rb = new RoaringBitmap();
    final RoaringBitmap rb2 = new RoaringBitmap();

    rb.add(0);
    rb.add(1 << 16); // 65 536
    rb.add(3 << 16); //196 608

    rb2.add((4 << 16) - 1); //262 143

    rb.orNot(rb2, 4 << 16);

    Assert.assertEquals((4 << 16) - 1, rb.getCardinality());

    final IntIterator iterator = rb.getIntIterator();

    for (int i = 0; i < (4 << 16) - 1; ++i) {
      Assert.assertTrue("Error on iteration " + i, iterator.hasNext());
      Assert.assertEquals(i, iterator.next());
    }
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void orNot3() {
    final RoaringBitmap rb = new RoaringBitmap();
    rb.add(2 << 16);

    final RoaringBitmap rb2 = new RoaringBitmap();
    rb2.add(1 << 14); //16 384
    rb2.add(3 << 16); //196 608

    rb.orNot(rb2, (5 << 16));
    Assert.assertEquals((5 << 16) - 2, rb.getCardinality());

    final IntIterator iterator = rb.getIntIterator();
    for (int i = 0; i < (5 << 16); ++i) {
      if ((i != (1 << 14)) && (i != (3 << 16))) {
        Assert.assertTrue("Error on iteration " + i, iterator.hasNext());
        Assert.assertEquals("Error on iteration " + i, i, iterator.next());
      }
    }
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void orNot4() {
    final RoaringBitmap rb = new RoaringBitmap();
    rb.add(1);

    final RoaringBitmap rb2 = new RoaringBitmap();
    rb2.add(3 << 16); //196 608

    rb.orNot(rb2, (2 << 16) + (2 << 14)); //131 072 + 32 768 = 163 840
    Assert.assertEquals((2 << 16) + (2 << 14), rb.getCardinality());

    final IntIterator iterator = rb.getIntIterator();
    for (int i = 0; i < (2 << 16) + (2 << 14); ++i) {
      Assert.assertTrue("Error on iteration " + i, iterator.hasNext());
      Assert.assertEquals("Error on iteration " + i, i, iterator.next());
    }
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void orNot5() {
    final RoaringBitmap rb = new RoaringBitmap();

    rb.add(1);
    rb.add(1 << 16); // 65 536
    rb.add(2 << 16); //131 072
    rb.add(3 << 16); //196 608

    final RoaringBitmap rb2 = new RoaringBitmap();

    rb.orNot(rb2, (5 << 16));
    Assert.assertEquals((5 << 16), rb.getCardinality());

    final IntIterator iterator = rb.getIntIterator();
    for (int i = 0; i < (5 << 16); ++i) {
      Assert.assertTrue("Error on iteration " + i, iterator.hasNext());
      Assert.assertEquals("Error on iteration " + i, i, iterator.next());
    }
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void orNot6() {
    final RoaringBitmap rb = new RoaringBitmap();

    rb.add(1);
    rb.add((1 << 16) - 1); // 65 535
    rb.add(1 << 16); // 65 536
    rb.add(2 << 16); //131 072
    rb.add(3 << 16); //196 608

    final RoaringBitmap rb2 = new RoaringBitmap();

    rb.orNot(rb2, (1 << 14));

    // {[0, 2^14], 65 535, 65 536, 131 072, 196 608}
    Assert.assertEquals((1 << 14) + 4, rb.getCardinality());

    final IntIterator iterator = rb.getIntIterator();
    for (int i = 0; i < (1 << 14); ++i) {
      Assert.assertTrue("Error on iteration " + i, iterator.hasNext());
      Assert.assertEquals("Error on iteration " + i, i, iterator.next());
    }

    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals((1 << 16) - 1, iterator.next());

    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(1 << 16, iterator.next());

    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(2 << 16, iterator.next());

    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(3 << 16, iterator.next());

    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void orNot7() {
    final RoaringBitmap rb = new RoaringBitmap();

    rb.add(1 << 16); // 65 536
    rb.add(2 << 16); //131 072
    rb.add(3 << 16); //196 608

    final RoaringBitmap rb2 = new RoaringBitmap();

    rb.orNot(rb2, (1 << 14));

    // {[0, 2^14], 65 536, 131 072, 196 608}
    Assert.assertEquals((1 << 14) + 3, rb.getCardinality());

    final IntIterator iterator = rb.getIntIterator();
    for (int i = 0; i < (1 << 14); ++i) {
      Assert.assertTrue("Error on iteration " + i, iterator.hasNext());
      Assert.assertEquals("Error on iteration " + i, i, iterator.next());
    }


    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(1 << 16, iterator.next());

    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(2 << 16, iterator.next());

    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(3 << 16, iterator.next());

    Assert.assertFalse(iterator.hasNext());
  }



  @Test
  public void orNot9() {
    final RoaringBitmap rb1 = new RoaringBitmap();

    rb1.add(1 << 16); // 65 536
    rb1.add(2 << 16); //131 072
    rb1.add(3 << 16); //196 608


    {
      final RoaringBitmap rb2 = new RoaringBitmap();
      final RoaringBitmap answer1 = RoaringBitmap.orNot(rb1, rb2, (1 << 14));

      // {[0, 2^14]}
      Assert.assertEquals(1 << 14, answer1.getCardinality());

      final IntIterator iterator1 = answer1.getIntIterator();
      for (int i = 0; i < (1 << 14); ++i) {
        Assert.assertTrue("Error on iteration " + i, iterator1.hasNext());
        Assert.assertEquals("Error on iteration " + i, i, iterator1.next());
      }
      Assert.assertFalse(iterator1.hasNext());
    }

    {
      final RoaringBitmap rb2 = new RoaringBitmap();
      final RoaringBitmap answer = RoaringBitmap.orNot(rb1, rb2, (2 << 16));

      // {[0, 2^16] | 196608}
      Assert.assertEquals((2 << 16) + 1, answer.getCardinality());

      final IntIterator iterator = answer.getIntIterator();
      for (int i = 0; i < (2 << 16) + 1; ++i) {
        Assert.assertTrue("Error on iteration " + i, iterator.hasNext());
        Assert.assertEquals("Error on iteration " + i, i, iterator.next());
      }
      Assert.assertFalse("Number of elements " + ((2 << 16) + 1) , iterator.hasNext());
    }


    {
      final RoaringBitmap rb2 = new RoaringBitmap();
      rb2.add((1 << 16) + (1 << 13));
      rb2.add((1 << 16) + (1 << 14));
      rb2.add((1 << 16) + (1 << 15));
      final RoaringBitmap answer = RoaringBitmap.orNot(rb1, rb2, (2 << 16));

      // {[0, 2^16] | 196608}
      Assert.assertEquals((2 << 16) - 2, answer.getCardinality());

      final IntIterator iterator = answer.getIntIterator();
      for (int i = 0; i < (2 << 16) + 1; ++i) {
        if ((i != (1 << 16) + (1 << 13)) && (i != (1 << 16) + (1 << 14)) && (i != (1 << 16) + (1 << 15))) {
          Assert.assertTrue("Error on iteration " + i, iterator.hasNext());
          Assert.assertEquals("Error on iteration " + i, i, iterator.next());
        }
      }
      Assert.assertFalse("Number of elements " + ((2 << 16) + 1) , iterator.hasNext());
    }

    {
      final RoaringBitmap rb2 = new RoaringBitmap();
      rb2.add(1 << 16);
      rb2.add(3 << 16);
      rb2.add(4 << 16);

      final RoaringBitmap answer = RoaringBitmap.orNot(rb1, rb2, (5 << 16));

      // {[0, 2^16]}
      Assert.assertEquals((5 << 16) - 1, answer.getCardinality());

      final IntIterator iterator = answer.getIntIterator();
      for (int i = 0; i < (5 << 16); ++i) {
        if (i != (4 << 16)) {
          Assert.assertTrue("Error on iteration " + i, iterator.hasNext());
          Assert.assertEquals("Error on iteration " + i, i, iterator.next());
        }
      }
      Assert.assertFalse("Number of elements " + (2 << 16) , iterator.hasNext());
    }

    {
      final RoaringBitmap rb2 = new RoaringBitmap();
      rb2.add(1 << 16);
      rb2.add(3 << 16);
      rb2.add(4 << 16);

      final RoaringBitmap answer = RoaringBitmap.orNot(rb2, rb1, (5 << 16));

      // {[0, 2^16]}
      Assert.assertEquals((5 << 16) - 1, answer.getCardinality());

      final IntIterator iterator = answer.getIntIterator();
      for (int i = 0; i < (5 << 16); ++i) {
        if (i != (2 << 16)) {
          Assert.assertTrue("Error on iteration " + i, iterator.hasNext());
          Assert.assertEquals("Error on iteration " + i, i, iterator.next());
        }
      }
      Assert.assertFalse("Number of elements " + (2 << 16) , iterator.hasNext());
    }
  }

  @Test
  public void orNot10() {
    final RoaringBitmap rb = new RoaringBitmap();
    final RoaringBitmap rb2 = new RoaringBitmap();

    rb.add(5);
    rb2.add(10);

    rb.orNot(rb2, 6);

    Assert.assertEquals(5, rb.last());
  }

  @Test
  public void orNot11() {
    final RoaringBitmap rb = new RoaringBitmap();
    final RoaringBitmap rb2 = new RoaringBitmap();

    rb.add((int) (65535L * 65536L + 65523));
    rb2.add((int) (65493L * 65536L + 65520));

    RoaringBitmap rb3 = RoaringBitmap.orNot(rb, rb2, 65535L * 65536L + 65524);

    Assert.assertEquals((int)(65535L * 65536L + 65523), rb3.last());
  }


}
