package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import java.util.Random;

public class TestRankIteratorsOfContainers {
  private void testContainerRanksOnNext(Container c) {
    PeekableCharRankIterator iterator = c.getCharRankIterator();
    while (iterator.hasNext()) {
      char bit = iterator.peekNext();
      int rank = iterator.peekNextRank();

      assertEquals(c.rank(bit), rank);

      iterator.next();
    }
  }

  private void testContainerRanksOnNextAsInt(Container c) {
    PeekableCharRankIterator iterator = c.getCharRankIterator();
    while (iterator.hasNext()) {
      char bit = iterator.peekNext();
      int rank = iterator.peekNextRank();

      assertEquals(c.rank(bit), rank);

      iterator.nextAsInt();
    }
  }

  private void testContainerRanksOnAdvance(Container c, int advance) {
    PeekableCharRankIterator iterator = c.getCharRankIterator();
    char bit;
    while (iterator.hasNext()) {
      bit = iterator.peekNext();
      int rank = iterator.peekNextRank();

      assertEquals(c.rank(bit), rank);

      if (((bit) + advance < 65536)) {
        iterator.advanceIfNeeded((char) (bit + advance));
      } else {
        iterator.next();
      }
    }
  }

  private void testContainerIterators(Container container) {
    testContainerRanksOnNext(container);
    testContainerRanksOnNextAsInt(container);
    for (int j = 1; j <= 16; ++j) {
      testContainerRanksOnAdvance(container, j);
      testContainerRanksOnAdvance(container, j * 3);
      testContainerRanksOnAdvance(container, j * 5);
      testContainerRanksOnAdvance(container, j * 7);
      testContainerRanksOnAdvance(container, j * 11);
      testContainerRanksOnAdvance(container, j * 64);
      testContainerRanksOnAdvance(container, j * 128);
      testContainerRanksOnAdvance(container, j * 256);
      testContainerRanksOnAdvance(container, j * 512);
      testContainerRanksOnAdvance(container, j * 1024);
    }
  }

  private void fillRandom(Container container, Random rnd) {
    Container empty = container;

    for (int i = 0; i < 1024; ++i) {
      container.add((char) rnd.nextInt(1 << 10));
    }

    for (int i = 0; i < 1024; ++i) {
      container.add((char) (8192 + rnd.nextInt(1 << 10)));
    }

    for (int i = 0; i < 1024; ++i) {
      container.add((char) (16384 + rnd.nextInt(1 << 10)));
    }

    assertSame(empty, container, "bad test -- container was changed");
  }

  private void fillRange(Container container, int begin, int end) {
    Container empty = container;

    container.iadd(begin, end);

    assertSame(empty, container, "bad test -- container was changed");
  }

  @Test
  public void testBitmapContainer1() {
    BitmapContainer container = new BitmapContainer();
    container.add((char) 123);
    container.add((char) 65535);

    testContainerRanksOnNext(container);
  }

  @Test
  public void testBitmapContainer2() {
    BitmapContainer container = new BitmapContainer();
    Random rnd = new Random(0);

    fillRandom(container, rnd);

    testContainerIterators(container);
  }

  @Test
  public void testBitmapContainer3() {
    BitmapContainer container = new BitmapContainer();
    fillRange(container, 0, 65535);

    testContainerIterators(container);
  }

  @Test
  public void testBitmapContainer4() {
    BitmapContainer container = new BitmapContainer();
    fillRange(container, 1024, 2048);
    fillRange(container, 8192 + 7, 24576 - 7);
    fillRange(container, 65534, 65535);

    testContainerIterators(container);
  }

  @Test
  public void testArrayContainer1() {
    ArrayContainer container = new ArrayContainer();
    container.add((char) 123);

    testContainerRanksOnNext(container);
  }

  @Test
  public void testArrayContainer2() {
    ArrayContainer container = new ArrayContainer();
    Random rnd = new Random(0);

    fillRandom(container, rnd);

    testContainerIterators(container);
  }

  @Test
  public void testArrayContainer3() {
    ArrayContainer container = new ArrayContainer();
    fillRange(container, 0, 1024);
    fillRange(container, 2048, 4096);
    fillRange(container, 65535 - 7, 65535 - 5);
  }

  @Test
  public void testRunContainer1() {
    RunContainer container = new RunContainer();
    container.add((char) 123);
    testContainerIterators(container);
  }

  @Test
  public void testRunContainer2() {
    RunContainer container = new RunContainer();
    Random rnd = new Random(0);

    fillRandom(container, rnd);
    testContainerIterators(container);
  }

  @Test
  public void testRunContainer3() {
    RunContainer container = new RunContainer();
    fillRange(container, 0, 1024);

    fillRange(container, 1024 + 3, 1024 + 5);
    fillRange(container, 1024 + 30, 1024 + 37);
    fillRange(container, 65535 - 7, 65535 - 5);
    testContainerIterators(container);
  }

  @Test
  public void testOverflow() {
    testContainerOverflow(new ArrayContainer(), false); // -- will be converted to BitmapContainer
    testContainerOverflow(new BitmapContainer(), true);
    testContainerOverflow(new RunContainer(), true);
  }

  private void testContainerOverflow(Container container, boolean checkSame) {
    Container c1 = container.iadd(0, 65536);

    if (checkSame) {
      assertSame(container, c1, "bad test -- container was changed");
    }

    PeekableCharRankIterator iterator = container.getCharRankIterator();
    while (iterator.hasNext()) {
      assertEquals((iterator.peekNext()) + 1, iterator.peekNextRank());
      iterator.next();
    }
  }

  /**
   * rank(Character.MAX_VALUE) must equal getCardinality() for every container type. This is the
   * path used by rangeCardinality when a query ends on a container boundary (issue #844).
   */
  @Test
  public void rankOfMaxEqualsCardinality() {
    Random rnd = new Random(42);

    ArrayContainer ac = new ArrayContainer();
    fillRandom(ac, rnd);
    assertEquals(ac.getCardinality(), ac.rank(Character.MAX_VALUE));
    ac.add(Character.MAX_VALUE);
    assertEquals(ac.getCardinality(), ac.rank(Character.MAX_VALUE));

    BitmapContainer bc = new BitmapContainer();
    fillRandom(bc, rnd);
    assertEquals(bc.getCardinality(), bc.rank(Character.MAX_VALUE));
    bc.add(Character.MAX_VALUE);
    assertEquals(bc.getCardinality(), bc.rank(Character.MAX_VALUE));

    // empty bitmap container
    assertEquals(0, new BitmapContainer().rank(Character.MAX_VALUE));

    // nearly full / full bitmap
    BitmapContainer almostFull = new BitmapContainer();
    fillRange(almostFull, 0, 65535);
    assertEquals(almostFull.getCardinality(), almostFull.rank(Character.MAX_VALUE));
    BitmapContainer full = new BitmapContainer();
    fillRange(full, 0, 65536);
    assertEquals(full.getCardinality(), full.rank(Character.MAX_VALUE));
    assertEquals(65536, full.rank(Character.MAX_VALUE));

    // lazy OR leaves cardinality invalid (-1); rank(MAX) must still be correct
    BitmapContainer lazy = new BitmapContainer();
    fillRange(lazy, 0, 10000);
    ArrayContainer other = new ArrayContainer();
    other.add((char) 20000);
    other.add(Character.MAX_VALUE);
    lazy.ilazyor(other);
    assertTrue(lazy.cardinality < 0, "precondition: lazy or invalidates cardinality");
    int expectedCard = lazy.rank((char) 0xFFFE) + 1; // 0xFFFE path does not repair card
    assertEquals(expectedCard, lazy.rank(Character.MAX_VALUE));
    // after rank(MAX), cardinality should have been repaired
    assertTrue(lazy.cardinality >= 0);
    assertEquals(lazy.getCardinality(), lazy.rank(Character.MAX_VALUE));

    RunContainer rc = new RunContainer();
    fillRandom(rc, rnd);
    assertEquals(rc.getCardinality(), rc.rank(Character.MAX_VALUE));
    fillRange(rc, 60000, 65536);
    assertEquals(rc.getCardinality(), rc.rank(Character.MAX_VALUE));

    // empty / single-point containers
    assertEquals(0, new ArrayContainer().rank(Character.MAX_VALUE));
    assertEquals(0, new RunContainer().rank(Character.MAX_VALUE));
    ArrayContainer single = new ArrayContainer();
    single.add(Character.MAX_VALUE);
    assertEquals(1, single.rank(Character.MAX_VALUE));
  }

  /** Full-container rangeCardinality must match getCardinality (issue #844). */
  @Test
  public void rangeCardinalityFullContainer() {
    RoaringBitmap rb = new RoaringBitmap();
    // one sparse container (array)
    for (int i = 0; i < 100; i++) {
      rb.add(i * 3);
    }
    // one dense container (bitmap)
    rb.add(1L << 16, (1L << 16) + 40000);
    // one run-friendly container
    rb.add(2L << 16, (2L << 16) + 65536);

    ContainerPointer p = rb.getContainerPointer();
    while (p.getContainer() != null) {
      long key = p.key() & 0xFFFFL;
      long start = key << 16;
      long end = start + (1L << 16);
      assertEquals(
          p.getContainer().getCardinality(),
          rb.rangeCardinality(start, end),
          "full-container range for key " + key);
      p.advance();
    }
  }
}
