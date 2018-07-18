package org.roaringbitmap;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class TestRankIteratorsOfContainers {
  private void testContainerRanksOnNext(Container c) {
    PeekableShortRankIterator iterator = c.getShortRankIterator();
    while (iterator.hasNext()) {
      short bit = iterator.peekNext();
      short rank = iterator.peekNextRank();

      Assert.assertEquals((short) c.rank(bit), rank);

      iterator.next();
    }
  }

  private void testContainerRanksOnNextAsInt(Container c) {
    PeekableShortRankIterator iterator = c.getShortRankIterator();
    while (iterator.hasNext()) {
      short bit = iterator.peekNext();
      short rank = iterator.peekNextRank();

      Assert.assertEquals((short) c.rank(bit), rank);

      iterator.nextAsInt();
    }
  }

  private void testContainerRanksOnAdvance(Container c, int advance) {
    PeekableShortRankIterator iterator = c.getShortRankIterator();
    short bit;
    while (iterator.hasNext()) {
      bit = iterator.peekNext();
      short rank = iterator.peekNextRank();

      Assert.assertEquals((short) c.rank(bit), rank);

      if ((Util.toIntUnsigned(bit) + advance < 65536)) {
        iterator.advanceIfNeeded((short) (bit + advance));
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
      container.add((short) rnd.nextInt(1 << 10));
    }

    for (int i = 0; i < 1024; ++i) {
      container.add((short) (8192 + rnd.nextInt(1 << 10)));
    }

    for (int i = 0; i < 1024; ++i) {
      container.add((short) (16384 + rnd.nextInt(1 << 10)));
    }

    Assert.assertSame("bad test -- container was changed", empty, container);
  }

  private void fillRange(Container container, int begin, int end) {
    Container empty = container;

    container.iadd(begin, end);

    Assert.assertSame("bad test -- container was changed", empty, container);
  }

  @Test
  public void testBitmapContainer1() {
    BitmapContainer container = new BitmapContainer();
    container.add((short) 123);
    container.add((short) 65535);

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
    container.add((short) 123);

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
    container.add((short) 123);
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
  }
}
