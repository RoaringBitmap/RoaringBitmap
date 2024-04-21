/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */
package org.roaringbitmap;


import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import static org.roaringbitmap.ValidationRangeConsumer.Value.ABSENT;
import static org.roaringbitmap.ValidationRangeConsumer.Value.PRESENT;


/**
 * Various tests on Container and its subclasses, ArrayContainer and BitmapContainer
 */
@SuppressWarnings({"static-method"})
public class TestContainer {

  private final static Class<?>[] CONTAINER_TYPES =
      new Class[] {ArrayContainer.class, BitmapContainer.class, RunContainer.class};

  @Test
  public void testNames() {
    assertEquals(new BitmapContainer().getContainerName(), Container.ContainerNames[0]);
    assertEquals(new ArrayContainer().getContainerName(), Container.ContainerNames[1]);
    assertEquals(new RunContainer().getContainerName(), Container.ContainerNames[2]);
  }
  
  public static boolean checkContent(Container c, char[] s) {
    CharIterator si = c.getCharIterator();
    int ctr = 0;
    boolean fail = false;
    while (si.hasNext()) {
      if (ctr == s.length) {
        fail = true;
        break;
      }
      if (si.next() != s[ctr]) {
        fail = true;
        break;
      }
      ++ctr;
    }
    if (ctr != s.length) {
      fail = true;
    }
    if (fail) {
      System.out.print("fail, found ");
      si = c.getCharIterator();
      while (si.hasNext()) {
        System.out.print(" " + si.next());
      }
      System.out.print("\n expected ");
      for (final char s1 : s) {
        System.out.print(" " + s1);
      }
      System.out.println();
    }
    return !fail;
  }

  public static Container makeContainer(char[] ss) {
    Container c = new ArrayContainer();
    for (final char s : ss) {
      c = c.add(s);
    }
    return c;
  }

  @Test
  public void and1() throws InstantiationException, IllegalAccessException {
    System.out.println("and1");
    for (Class<?> ct : CONTAINER_TYPES) {
      Container ac = (Container) ct.newInstance();
      ac.add((char) 1);
      ac.add((char) 3);
      ac.add((char) 5);
      ac.add((char) 50000);
      ac.add((char) 50001);
      for (Class<?> ct1 : CONTAINER_TYPES) {
        Container ac1 = (Container) ct1.newInstance();
        Container result = ac.and(ac1);
        assertTrue(checkContent(result, new char[] {}));
        assertEquals(0, result.getCardinality());
        assertEquals(0, ac.andCardinality(ac1));
        assertEquals(0, ac1.andCardinality(ac));
      }
    }
  }

  @Test
  public void and2() throws InstantiationException, IllegalAccessException {
    System.out.println("and2");
    for (Class<?> ct : CONTAINER_TYPES) {
      Container ac = (Container) ct.newInstance();
      ac.add((char) 1);
      for (Class<?> ct1 : CONTAINER_TYPES) {
        Container ac1 = (Container) ct1.newInstance();

        ac1.add((char) 1);
        ac1.add((char) 4);
        ac1.add((char) 5);
        ac1.add((char) 50000);
        ac1.add((char) 50002);
        ac1.add((char) 50003);
        ac1.add((char) 50004);

        Container result = ac.and(ac1);
        assertTrue(checkContent(result, new char[] {1}));
        assertEquals(result.getCardinality(), ac.andCardinality(ac1));
        assertEquals(result.getCardinality(), ac1.andCardinality(ac));
        assertEquals(1, ac1.andCardinality(ac));
      }
    }
  }

  @Test
  public void and3() throws InstantiationException, IllegalAccessException {
    System.out.println("and3");
    for (Class<?> ct : CONTAINER_TYPES) {
      Container ac = (Container) ct.newInstance();

      ac.add((char) 1);
      ac.add((char) 3);
      ac.add((char) 5);
      ac.add((char) 50000);
      ac.add((char) 50001);

      // array ends first

      for (Class<?> ct1 : CONTAINER_TYPES) {
        Container ac1 = (Container) ct1.newInstance();

        ac1.add((char) 1);
        ac1.add((char) 4);
        ac1.add((char) 5);
        ac1.add((char) 50000);
        ac1.add((char) 50002);
        ac1.add((char) 50003);
        ac1.add((char) 50004);

        Container result = ac.and(ac1);
        assertTrue(checkContent(result, new char[] {1, 5, (char) 50000}));
        assertEquals(result.getCardinality(), ac.andCardinality(ac1));
        assertEquals(result.getCardinality(), ac1.andCardinality(ac));
        assertEquals(3, ac1.andCardinality(ac));
      }
    }
  }

  @Test
  public void and4() throws InstantiationException, IllegalAccessException {
    System.out.println("and4");
    for (Class<?> ct : CONTAINER_TYPES) {
      Container ac = (Container) ct.newInstance();

      ac.add((char) 1);
      ac.add((char) 3);
      ac.add((char) 5);
      ac.add((char) 50000);
      ac.add((char) 50001);
      ac.add((char) 50011);

      // iterator ends first

      for (Class<?> ct1 : CONTAINER_TYPES) {
        Container ac1 = (Container) ct1.newInstance();

        ac1.add((char) 1);
        ac1.add((char) 4);
        ac1.add((char) 5);
        ac1.add((char) 50000);
        ac1.add((char) 50002);
        ac1.add((char) 50003);
        ac1.add((char) 50004);

        Container result = ac.and(ac1);
        assertTrue(checkContent(result, new char[] {1, 5, (char) 50000}));
        assertEquals(result.getCardinality(), ac.andCardinality(ac1));
        assertEquals(result.getCardinality(), ac1.andCardinality(ac));
        assertEquals(3, ac1.andCardinality(ac));
      }
    }
  }

  @Test
  public void and5() throws InstantiationException, IllegalAccessException {
    System.out.println("and5");
    for (Class<?> ct : CONTAINER_TYPES) {
      Container ac = (Container) ct.newInstance();

      ac.add((char) 1);
      ac.add((char) 3);
      ac.add((char) 5);
      ac.add((char) 50000);
      ac.add((char) 50001);

      // end together

      for (Class<?> ct1 : CONTAINER_TYPES) {
        Container ac1 = (Container) ct1.newInstance();

        ac1.add((char) 1);
        ac1.add((char) 4);
        ac1.add((char) 5);
        ac1.add((char) 50000);
        ac1.add((char) 50001);

        Container result = ac.and(ac1);
        assertTrue(checkContent(result, new char[] {1, 5, (char) 50000, (char) 50001}));
        assertEquals(result.getCardinality(), ac.andCardinality(ac1));
        assertEquals(result.getCardinality(), ac1.andCardinality(ac));
        assertEquals(4, ac1.andCardinality(ac));
      }
    }
  }

  @Test
  public void inotTest1() {
    // Array container, range is complete
    final char[] content = {1, 3, 5, 7, 9};
    Container c = makeContainer(content);
    c = c.inot(0, 65536);
    final char[] s = new char[65536 - content.length];
    int pos = 0;
    for (int i = 0; i < 65536; ++i) {
      if (Arrays.binarySearch(content, (char) i) < 0) {
        s[pos++] = (char) i;
      }
    }
    assertTrue(checkContent(c, s));
  }

  @Test
  public void inotTest10() {
    System.out.println("inotTest10");
    // Array container, inverting a range past any set bit
    final char[] content = new char[3];
    content[0] = 0;
    content[1] = 2;
    content[2] = 4;
    final Container c = makeContainer(content);
    final Container c1 = c.inot(65190, 65201);
    assertTrue(c1 instanceof ArrayContainer);
    assertEquals(14, c1.getCardinality());
    assertTrue(checkContent(c1,
        new char[] {0, 2, 4, (char) 65190, (char) 65191, (char) 65192, (char) 65193,
            (char) 65194, (char) 65195, (char) 65196, (char) 65197, (char) 65198,
            (char) 65199, (char) 65200}));
  }

  @Test
  public void inotTest2() {
    // Array and then Bitmap container, range is complete
    final char[] content = {1, 3, 5, 7, 9};
    Container c = makeContainer(content);
    c = c.inot(0, 65535);
    c = c.inot(0, 65535);
    assertTrue(checkContent(c, content));
  }

  @Test
  public void inotTest3() {
    // Bitmap to bitmap, full range

    Container c = new ArrayContainer();
    for (int i = 0; i < 65536; i += 2) {
      c = c.add((char) i);
    }

    c = c.inot(0, 65536);
    assertTrue(c.contains((char) 3) && !c.contains((char) 4));
    assertEquals(32768, c.getCardinality());
    c = c.inot(0, 65536);
    for (int i = 0; i < 65536; i += 2) {
      assertTrue(c.contains((char) i) && !c.contains((char) (i + 1)));
    }
  }

  @Test
  public void inotTest4() {
    // Array container, range is partial, result stays array
    final char[] content = {1, 3, 5, 7, 9};
    Container c = makeContainer(content);
    c = c.inot(4, 1000);
    assertTrue(c instanceof ArrayContainer);
    assertEquals(999 - 4 + 1 - 3 + 2, c.getCardinality());
    c = c.inot(4, 1000); // back
    assertTrue(checkContent(c, content));
  }

  @Test
  public void inotTest5() {
    System.out.println("inotTest5");
    // Bitmap container, range is partial, result stays bitmap
    final char[] content = new char[32768 - 5];
    content[0] = 0;
    content[1] = 2;
    content[2] = 4;
    content[3] = 6;
    content[4] = 8;
    for (int i = 10; i <= 32767; ++i) {
      content[i - 10 + 5] = (char) i;
    }
    Container c = makeContainer(content);
    c = c.inot(4, 1000);
    assertTrue(c instanceof BitmapContainer);
    assertEquals(31773, c.getCardinality());
    c = c.inot(4, 1000); // back, as a bitmap
    assertTrue(c instanceof BitmapContainer);
    assertTrue(checkContent(c, content));

  }

  @Test
  public void inotTest6() {
    System.out.println("inotTest6");
    // Bitmap container, range is partial and in one word, result
    // stays bitmap
    final char[] content = new char[32768 - 5];
    content[0] = 0;
    content[1] = 2;
    content[2] = 4;
    content[3] = 6;
    content[4] = 8;
    for (int i = 10; i <= 32767; ++i) {
      content[i - 10 + 5] = (char) i;
    }
    Container c = makeContainer(content);
    c = c.inot(4, 9);
    assertTrue(c instanceof BitmapContainer);
    assertEquals(32762, c.getCardinality());
    c = c.inot(4, 9); // back, as a bitmap
    assertTrue(c instanceof BitmapContainer);
    assertTrue(checkContent(c, content));
  }

  @Test
  public void inotTest7() {
    System.out.println("inotTest7");
    // Bitmap container, range is partial, result flips to array
    final char[] content = new char[32768 - 5];
    content[0] = 0;
    content[1] = 2;
    content[2] = 4;
    content[3] = 6;
    content[4] = 8;
    for (int i = 10; i <= 32767; ++i) {
      content[i - 10 + 5] = (char) i;
    }
    Container c = makeContainer(content);
    c = c.inot(5, 31001);
    if (c.getCardinality() <= ArrayContainer.DEFAULT_MAX_SIZE) {
      assertTrue(c instanceof ArrayContainer);
    } else {
      assertTrue(c instanceof BitmapContainer);
    }
    assertEquals(1773, c.getCardinality());
    c = c.inot(5, 31001); // back, as a bitmap
    if (c.getCardinality() <= ArrayContainer.DEFAULT_MAX_SIZE) {
      assertTrue(c instanceof ArrayContainer);
    } else {
      assertTrue(c instanceof BitmapContainer);
    }
    assertTrue(checkContent(c, content));
  }

  // case requiring contraction of ArrayContainer.
  @Test
  public void inotTest8() {
    System.out.println("inotTest8");
    // Array container
    final char[] content = new char[21];
    for (int i = 0; i < 18; ++i) {
      content[i] = (char) i;
    }
    content[18] = 21;
    content[19] = 22;
    content[20] = 23;

    Container c = makeContainer(content);
    c = c.inot(5, 22);
    assertTrue(c instanceof ArrayContainer);

    assertEquals(10, c.getCardinality());
    c = c.inot(5, 22); // back, as a bitmap
    assertTrue(c instanceof ArrayContainer);
    assertTrue(checkContent(c, content));
  }

  // mostly same tests, except for not. (check original unaffected)
  @Test
  public void notTest1() {
    // Array container, range is complete
    final char[] content = {1, 3, 5, 7, 9};
    final Container c = makeContainer(content);
    final Container c1 = c.not(0, 65536);
    final char[] s = new char[65536 - content.length];
    int pos = 0;
    for (int i = 0; i < 65536; ++i) {
      if (Arrays.binarySearch(content, (char) i) < 0) {
        s[pos++] = (char) i;
      }
    }
    assertTrue(checkContent(c1, s));
    assertTrue(checkContent(c, content));
  }

  @Test
  public void notTest10() {
    System.out.println("notTest10");
    // Array container, inverting a range past any set bit
    // attempting to recreate a bug (but bug required extra space
    // in the array with just the right junk in it.
    final char[] content = new char[40];
    for (int i = 244; i <= 283; ++i) {
      content[i - 244] = (char) i;
    }
    final Container c = makeContainer(content);
    final Container c1 = c.not(51413, 51471);
    assertTrue(c1 instanceof ArrayContainer);
    assertEquals(40 + 58, c1.getCardinality());
    final char[] rightAns = new char[98];
    for (int i = 244; i <= 283; ++i) {
      rightAns[i - 244] = (char) i;
    }
    for (int i = 51413; i <= 51470; ++i) {
      rightAns[i - 51413 + 40] = (char) i;
    }

    assertTrue(checkContent(c1, rightAns));
  }

  @Test
  public void notTest11() {
    System.out.println("notTest11");
    // Array container, inverting a range before any set bit
    // attempting to recreate a bug (but required extra space
    // in the array with the right junk in it.
    final char[] content = new char[40];
    for (int i = 244; i <= 283; ++i) {
      content[i - 244] = (char) i;
    }
    final Container c = makeContainer(content);
    final Container c1 = c.not(1, 59);
    assertTrue(c1 instanceof ArrayContainer);
    assertEquals(40 + 58, c1.getCardinality());
    final char[] rightAns = new char[98];
    for (int i = 1; i <= 58; ++i) {
      rightAns[i - 1] = (char) i;
    }
    for (int i = 244; i <= 283; ++i) {
      rightAns[i - 244 + 58] = (char) i;
    }

    assertTrue(checkContent(c1, rightAns));
  }

  @Test
  public void notTest2() {
    // Array and then Bitmap container, range is complete
    final char[] content = {1, 3, 5, 7, 9};
    final Container c = makeContainer(content);
    final Container c1 = c.not(0, 65535);
    final Container c2 = c1.not(0, 65535);
    assertTrue(checkContent(c2, content));
  }

  @Test
  public void notTest3() {
    // Bitmap to bitmap, full range

    Container c = new ArrayContainer();
    for (int i = 0; i < 65536; i += 2) {
      c = c.add((char) i);
    }

    final Container c1 = c.not(0, 65536);
    assertTrue(c1.contains((char) 3) && !c1.contains((char) 4));
    assertEquals(32768, c1.getCardinality());
    final Container c2 = c1.not(0, 65536);
    for (int i = 0; i < 65536; i += 2) {
      assertTrue(c2.contains((char) i) && !c2.contains((char) (i + 1)));
    }
  }

  @Test
  public void notTest4() {
    System.out.println("notTest4");
    // Array container, range is partial, result stays array
    final char[] content = {1, 3, 5, 7, 9};
    final Container c = makeContainer(content);
    final Container c1 = c.not(4, 1000);
    assertTrue(c1 instanceof ArrayContainer);
    assertEquals(999 - 4 + 1 - 3 + 2, c1.getCardinality());
    final Container c2 = c1.not(4, 1000); // back
    assertTrue(checkContent(c2, content));
  }

  @Test
  public void notTest5() {
    System.out.println("notTest5");
    // Bitmap container, range is partial, result stays bitmap
    final char[] content = new char[32768 - 5];
    content[0] = 0;
    content[1] = 2;
    content[2] = 4;
    content[3] = 6;
    content[4] = 8;
    for (int i = 10; i <= 32767; ++i) {
      content[i - 10 + 5] = (char) i;
    }
    final Container c = makeContainer(content);
    final Container c1 = c.not(4, 1000);
    assertTrue(c1 instanceof BitmapContainer);
    assertEquals(31773, c1.getCardinality());
    final Container c2 = c1.not(4, 1000); // back, as a bitmap
    assertTrue(c2 instanceof BitmapContainer);
    assertTrue(checkContent(c2, content));
  }

  @Test
  public void notTest6() {
    System.out.println("notTest6");
    // Bitmap container, range is partial and in one word, result
    // stays bitmap
    final char[] content = new char[32768 - 5];
    content[0] = 0;
    content[1] = 2;
    content[2] = 4;
    content[3] = 6;
    content[4] = 8;
    for (int i = 10; i <= 32767; ++i) {
      content[i - 10 + 5] = (char) i;
    }
    final Container c = makeContainer(content);
    final Container c1 = c.not(4, 9);
    assertTrue(c1 instanceof BitmapContainer);
    assertEquals(32762, c1.getCardinality());
    final Container c2 = c1.not(4, 9); // back, as a bitmap
    assertTrue(c2 instanceof BitmapContainer);
    assertTrue(checkContent(c2, content));
  }

  @Test
  public void notTest7() {
    System.out.println("notTest7");
    // Bitmap container, range is partial, result flips to array
    final char[] content = new char[32768 - 5];
    content[0] = 0;
    content[1] = 2;
    content[2] = 4;
    content[3] = 6;
    content[4] = 8;
    for (int i = 10; i <= 32767; ++i) {
      content[i - 10 + 5] = (char) i;
    }
    final Container c = makeContainer(content);
    final Container c1 = c.not(5, 31001);
    if (c1.getCardinality() <= ArrayContainer.DEFAULT_MAX_SIZE) {
      assertTrue(c1 instanceof ArrayContainer);
    } else {
      assertTrue(c1 instanceof BitmapContainer);
    }
    assertEquals(1773, c1.getCardinality());
    final Container c2 = c1.not(5, 31001); // back, as a bitmap
    if (c2.getCardinality() <= ArrayContainer.DEFAULT_MAX_SIZE) {
      assertTrue(c2 instanceof ArrayContainer);
    } else {
      assertTrue(c2 instanceof BitmapContainer);
    }
    assertTrue(checkContent(c2, content));
  }

  @Test
  public void notTest8() {
    System.out.println("notTest8");
    // Bitmap container, range is partial on the lower end
    final char[] content = new char[32768 - 5];
    content[0] = 0;
    content[1] = 2;
    content[2] = 4;
    content[3] = 6;
    content[4] = 8;
    for (int i = 10; i <= 32767; ++i) {
      content[i - 10 + 5] = (char) i;
    }
    final Container c = makeContainer(content);
    final Container c1 = c.not(4, 65536);
    assertTrue(c1 instanceof BitmapContainer);
    assertEquals(32773, c1.getCardinality());
    final Container c2 = c1.not(4, 65536); // back, as a bitmap
    assertTrue(c2 instanceof BitmapContainer);
    assertTrue(checkContent(c2, content));
  }

  @Test
  public void notTest9() {
    System.out.println("notTest9");
    // Bitmap container, range is partial on the upper end, not
    // single word
    final char[] content = new char[32768 - 5];
    content[0] = 0;
    content[1] = 2;
    content[2] = 4;
    content[3] = 6;
    content[4] = 8;
    for (int i = 10; i <= 32767; ++i) {
      content[i - 10 + 5] = (char) i;
    }
    final Container c = makeContainer(content);
    final Container c1 = c.not(0, 65201);
    assertTrue(c1 instanceof BitmapContainer);
    assertEquals(32438, c1.getCardinality());
    final Container c2 = c1.not(0, 65201); // back, as a bitmap
    assertTrue(c2 instanceof BitmapContainer);
    assertTrue(checkContent(c2, content));
  }



  @Test
  public void numberOfRuns() {
    char[] positions = {3, 4, 5, 10, 11, 13, 15, 62, 63, 64, 65};
    Container ac = new ArrayContainer();
    Container bc = new BitmapContainer();
    Container rc = new RunContainer();
    for (char position : positions) {
      ac.add(position);
      bc.add(position);
      rc.add(position);
    }
    assertEquals(rc.numberOfRuns(), ac.numberOfRuns());
    assertEquals(rc.numberOfRuns(), bc.numberOfRuns());
  }



  @Test
  public void numberOfRuns1() {
    System.out.println("numberOfRuns1");
    Random r = new Random(12345);

    for (double density = 0.001; density < 0.8; density *= 2) {

      ArrayList<Integer> values = new ArrayList<Integer>();
      for (int i = 0; i < 65536; ++i) {
        if (r.nextDouble() < density) {
          values.add(i);
        }
      }
      Integer[] positions = values.toArray(new Integer[0]);
      Container ac = new ArrayContainer(); // at high density becomes Bitmap
      BitmapContainer bc = new BitmapContainer();
      Container rc = new RunContainer();
      for (int position : positions) {
        ac = ac.add((char) position);
        bc.add((char) position);
        rc.add((char) position);
      }

      assertEquals(rc.numberOfRuns(), ac.numberOfRuns());
      assertEquals(rc.numberOfRuns(), bc.numberOfRuns());
      // a limit of 50k assures that the no early bail-out can be taken
      assertEquals(bc.numberOfRuns(),
          bc.numberOfRunsLowerBound(50000) + bc.numberOfRunsAdjustment());
      // inferior approaches to be removed in a future cleanup, now commented...
      // assertEquals(bc.numberOfRunsLowerBound(), bc.numberOfRunsLowerBoundUnrolled());
      // assertEquals(bc.numberOfRunsLowerBound(), bc.numberOfRunsLowerBoundUnrolled2());
      // assertEquals(bc.numberOfRunsAdjustment(), bc.numberOfRunsAdjustmentUnrolled());
    }
  }



  @Test
  public void or1() {
    System.out.println("or1");
    ArrayContainer ac = new ArrayContainer();
    ac.add((char) 1);
    ac.add((char) 3);
    ac.add((char) 5);
    ac.add((char) 50000);
    ac.add((char) 50001);

    ArrayContainer ac1 = new ArrayContainer(); // empty iterator
    Container result = ac.or(ac1.getCharIterator());
    assertTrue(checkContent(result, new char[] {1, 3, 5, (char) 50000, (char) 50001}));
  }


  @Test
  public void or2() {
    System.out.println("or2");
    ArrayContainer ac = new ArrayContainer();
    // empty array

    ArrayContainer ac1 = new ArrayContainer();
    ac1.add((char) 1);
    ac1.add((char) 4);
    ac1.add((char) 5);
    ac1.add((char) 50000);
    ac1.add((char) 50002);
    ac1.add((char) 50003);
    ac1.add((char) 50004);

    Container result = ac.or(ac1.getCharIterator());
    assertTrue(checkContent(result,
        new char[] {1, 4, 5, (char) 50000, (char) 50002, (char) 50003, (char) 50004}));
  }

  @Test
  public void or3() {
    System.out.println("or3");
    ArrayContainer ac = new ArrayContainer();
    ac.add((char) 1);
    ac.add((char) 3);
    ac.add((char) 5);
    ac.add((char) 50000);
    ac.add((char) 50001);

    // array ends first

    ArrayContainer ac1 = new ArrayContainer();
    ac1.add((char) 1);
    ac1.add((char) 4);
    ac1.add((char) 5);
    ac1.add((char) 50000);
    ac1.add((char) 50002);
    ac1.add((char) 50003);
    ac1.add((char) 50004);

    Container result = ac.or(ac1.getCharIterator());
    assertTrue(checkContent(result, new char[] {1, 3, 4, 5, (char) 50000, (char) 50001,
        (char) 50002, (char) 50003, (char) 50004}));
  }



  @Test
  public void or4() {
    System.out.println("or4");
    ArrayContainer ac = new ArrayContainer();
    ac.add((char) 1);
    ac.add((char) 3);
    ac.add((char) 5);
    ac.add((char) 50000);
    ac.add((char) 50001);
    ac.add((char) 50011);

    // iterator ends first

    ArrayContainer ac1 = new ArrayContainer();
    ac1.add((char) 1);
    ac1.add((char) 4);
    ac1.add((char) 5);
    ac1.add((char) 50000);
    ac1.add((char) 50002);
    ac1.add((char) 50003);
    ac1.add((char) 50004);


    Container result = ac.or(ac1.getCharIterator());
    assertTrue(checkContent(result, new char[] {1, 3, 4, 5, (char) 50000, (char) 50001,
        (char) 50002, (char) 50003, (char) 50004, (char) 50011}));
  }



  @Test
  public void or5() {
    System.out.println("or5");
    ArrayContainer ac = new ArrayContainer();
    ac.add((char) 1);
    ac.add((char) 3);
    ac.add((char) 5);
    ac.add((char) 50000);
    ac.add((char) 50001);

    // end together

    ArrayContainer ac1 = new ArrayContainer();
    ac1.add((char) 1);
    ac1.add((char) 4);
    ac1.add((char) 5);
    ac1.add((char) 50000);
    ac1.add((char) 50001);

    Container result = ac.or(ac1.getCharIterator());
    assertTrue(checkContent(result, new char[] {1, 3, 4, 5, (char) 50000, (char) 50001}));
  }

  @Test
  public void or6() {
    System.out.println("or6");
    RunContainer rc1 = new RunContainer();
    for (int i = 0; i < 6144; i += 6) {
      rc1.iadd(i, i+1);
    }

    RunContainer rc2 = new RunContainer();

    for (int i = 3; i < 6144; i += 6) {
      rc2.iadd(i, i+1);
    }

    Container result = rc1.or(rc2);
    assertTrue(result.getCardinality() < ArrayContainer.DEFAULT_MAX_SIZE);
    assertTrue(result instanceof ArrayContainer);
  }

  @Test
  public void testXorContainer() throws Exception {
    Container rc1 = new RunContainer(new char[] {10, 12, 90, 10}, 2);
    Container rc2 = new RunContainer(new char[]{1, 10, 40, 400, 900, 10}, 3);
    Container bc1 = new BitmapContainer().add(10, 20);
    Container bc2 = new BitmapContainer().add(21, 30);
    Container ac1 = new ArrayContainer(4, new char[] {10, 12, 90, 104});
    Container ac2 = new ArrayContainer(2, new char[]{1, 10, 40, 400, 900, 1910});
    for(Set<Container> test : Sets.powerSet(ImmutableSet.of(rc1, rc2, bc1, bc2, ac1, ac2))) {
      Iterator<Container> it = test.iterator();
      if(test.size() == 1) { // compare with self
        Container x = it.next();
        assertEquals(x.xor(x).getCardinality(), x.xorCardinality(x), x.getContainerName() + ": " + x);
      } else if(test.size() == 2) {
        Container x = it.next();
        Container y = it.next();
        assertEquals(
                x.xor(y).getCardinality(), x.xorCardinality(y), x.getContainerName() + " " + x + " " + y.getContainerName() + " " + y);
        assertEquals(y.xor(x).getCardinality(), y.xorCardinality(x), y.getContainerName() + " " + y + " " + x.getContainerName() + " " + x);
      }
    }
  }



  @Test
  public void rangeOfOnesTest1() {
    final Container c = Container.rangeOfOnes(4, 11); // sparse
    // assertTrue(c instanceof ArrayContainer);
    assertEquals(10 - 4 + 1, c.getCardinality());
    assertTrue(checkContent(c, new char[] {4, 5, 6, 7, 8, 9, 10}));
  }



  @Test
  public void rangeOfOnesTest2() {
    final Container c = Container.rangeOfOnes(1000, 35001); // dense
    // assertTrue(c instanceof BitmapContainer);
    assertEquals(35000 - 1000 + 1, c.getCardinality());
  }


  @Test
  public void rangeOfOnesTest2A() {
    final Container c = Container.rangeOfOnes(1000, 35001); // dense
    final char s[] = new char[35000 - 1000 + 1];
    for (int i = 1000; i <= 35000; ++i) {
      s[i - 1000] = (char) i;
    }
    assertTrue(checkContent(c, s));
  }


  @Test
  public void rangeOfOnesTest3() {
    // bdry cases
    Container.rangeOfOnes(1, ArrayContainer.DEFAULT_MAX_SIZE);
  }

  @Test
  public void rangeOfOnesTest4() {
    Container.rangeOfOnes(1, ArrayContainer.DEFAULT_MAX_SIZE + 2);
  }



  @Test
  public void testRunOptimize1() {
    ArrayContainer ac = new ArrayContainer();
    for (char s : new char[] {1, 2, 3, 4, 5, 6, 7, 8, 9, (char) 50000, (char) 50001}) {
      ac.add(s);
    }
    Container c = ac.runOptimize();
    assertTrue(c instanceof RunContainer);
    assertEquals(ac, c);
  }



  public void testRunOptimize1A() {
    ArrayContainer ac = new ArrayContainer();
    for (char s : new char[] {1, 2, 3, 4, 6, 8, 9, (char) 50000, (char) 50003}) {
      ac.add(s);
    }
    Container c = ac.runOptimize();
    assertTrue(c instanceof ArrayContainer);
    assertSame(ac, c);
  }


  @Test
  public void testRunOptimize2() {
    BitmapContainer bc = new BitmapContainer();
    for (int i = 0; i < 40000; ++i) {
      bc.add((char) i);
    }
    Container c = bc.runOptimize();
    assertTrue(c instanceof RunContainer);
    assertEquals(bc, c);
  }



  @Test
  public void testRunOptimize2A() {
    BitmapContainer bc = new BitmapContainer();
    for (int i = 0; i < 40000; i += 2) {
      bc.add((char) i);
    }
    Container c = bc.runOptimize();
    assertTrue(c instanceof BitmapContainer);
    assertSame(c, bc);
  }

  @Test
  public void testRunOptimize3() {
    RunContainer rc = new RunContainer();
    for (char s : new char[] {1, 2,3, 4, 5, 6, 7, 8, 9, (char) 50000, (char) 50001}) {
      rc.add(s);
    }
    Container c = rc.runOptimize();
    assertTrue(c instanceof RunContainer);
    assertSame(c, rc);
  }

  @Test
  public void testRunOptimize3A() {
    RunContainer rc = new RunContainer();
    for (char s : new char[] {1, 3, 5, 7, 9, 11, 17, 21, (char) 50000, (char) 50002}) {
      rc.add(s);
    }
    Container c = rc.runOptimize();
    assertTrue(c instanceof ArrayContainer);
    assertEquals(c, rc);
  }


  @Test
  public void testRunOptimize3B() {
    RunContainer rc = new RunContainer();
    for (char i = 100; i < 30000; i += 2) {
      rc.add(i);
    }
    Container c = rc.runOptimize();
    assertTrue(c instanceof BitmapContainer);
    assertEquals(c, rc);
  }

  @Test
  public void transitionTest() {
    Container c = new ArrayContainer();
    for (int i = 0; i < 4096; ++i) {
      c = c.add((char) i);
    }
    assertEquals(c.getCardinality(), 4096);
    assertTrue(c instanceof ArrayContainer);
    for (int i = 0; i < 4096; ++i) {
      c = c.add((char) i);
    }
    assertEquals(c.getCardinality(), 4096);
    assertTrue(c instanceof ArrayContainer);
    c = c.add((char) 4096);
    assertEquals(c.getCardinality(), 4097);
    assertTrue(c instanceof BitmapContainer);
    c = c.remove((char) 4096);
    assertEquals(c.getCardinality(), 4096);
    assertTrue(c instanceof ArrayContainer);
  }

  @Test
  public void xor1() {
    System.out.println("xor1");
    ArrayContainer ac = new ArrayContainer();
    ac.add((char) 1);
    ac.add((char) 3);
    ac.add((char) 5);
    ac.add((char) 50000);
    ac.add((char) 50001);

    ArrayContainer ac1 = new ArrayContainer(); // empty iterator
    Container result = ac.xor(ac1.getCharIterator());
    assertTrue(checkContent(result, new char[] {1, 3, 5, (char) 50000, (char) 50001}));
  }

  @Test
  public void xor2() {
    System.out.println("xor2");
    ArrayContainer ac = new ArrayContainer();
    // empty array

    ArrayContainer ac1 = new ArrayContainer();
    ac1.add((char) 1);
    ac1.add((char) 4);
    ac1.add((char) 5);
    ac1.add((char) 50000);
    ac1.add((char) 50002);
    ac1.add((char) 50003);
    ac1.add((char) 50004);

    Container result = ac.xor(ac1.getCharIterator());
    assertTrue(checkContent(result,
        new char[] {1, 4, 5, (char) 50000, (char) 50002, (char) 50003, (char) 50004}));
  }



  @Test
  public void xor3() {
    System.out.println("xor3");
    ArrayContainer ac = new ArrayContainer();
    ac.add((char) 1);
    ac.add((char) 3);
    ac.add((char) 5);
    ac.add((char) 50000);
    ac.add((char) 50001);

    // array ends first

    ArrayContainer ac1 = new ArrayContainer();
    ac1.add((char) 1);
    ac1.add((char) 4);
    ac1.add((char) 5);
    ac1.add((char) 50000);
    ac1.add((char) 50002);
    ac1.add((char) 50003);
    ac1.add((char) 50004);

    Container result = ac.xor(ac1.getCharIterator());
    assertTrue(checkContent(result,
        new char[] {3, 4, (char) 50001, (char) 50002, (char) 50003, (char) 50004}));
  }

  @Test
  public void testConsistentToString() {
    ArrayContainer ac = new ArrayContainer();
    BitmapContainer bc = new BitmapContainer();
    RunContainer rc = new RunContainer();
    for (char i : new char[]{0, 2, 17, Short.MAX_VALUE, (char)-3, (char)-1}) {
      ac.add(i);
      bc.add(i);
      rc.add(i);
    }
    String expected = "{0,2,17,32767,65533,65535}";

    assertEquals(expected, ac.toString());
    assertEquals(expected, bc.toString());
    String normalizedRCstr = rc.toString()
        .replaceAll("\\d+\\]\\[", "")
        .replace('[', '{')
        .replaceFirst(",\\d+\\]", "}");
    assertEquals(expected, normalizedRCstr);
  }

  @Test
  public void xor4() {
    System.out.println("xor4");
    ArrayContainer ac = new ArrayContainer();
    ac.add((char) 1);
    ac.add((char) 3);
    ac.add((char) 5);
    ac.add((char) 50000);
    ac.add((char) 50001);
    ac.add((char) 50011);

    // iterator ends first

    ArrayContainer ac1 = new ArrayContainer();
    ac1.add((char) 1);
    ac1.add((char) 4);
    ac1.add((char) 5);
    ac1.add((char) 50000);
    ac1.add((char) 50002);
    ac1.add((char) 50003);
    ac1.add((char) 50004);


    Container result = ac.xor(ac1.getCharIterator());
    assertTrue(checkContent(result, new char[] {3, 4, (char) 50001, (char) 50002, (char) 50003,
        (char) 50004, (char) 50011}));
  }



  @Test
  public void xor5() {
    System.out.println("xor5");
    ArrayContainer ac = new ArrayContainer();
    ac.add((char) 1);
    ac.add((char) 3);
    ac.add((char) 5);
    ac.add((char) 50000);
    ac.add((char) 50001);

    // end together

    ArrayContainer ac1 = new ArrayContainer();
    ac1.add((char) 1);
    ac1.add((char) 4);
    ac1.add((char) 5);
    ac1.add((char) 50000);
    ac1.add((char) 50001);

    Container result = ac.xor(ac1.getCharIterator());
    assertTrue(checkContent(result, new char[] {3, 4}));
  }

  private Container getContainerInstance(Class<?> ct) {
    try {
      return (Container) ct.getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      fail(e);
      throw new RuntimeException("unreachable code");
    }
  }

  private void testForAllMaterialization(char[] data) {
    for (Class<?> ct1 : CONTAINER_TYPES) {
      Container container = getContainerInstance(ct1);
      ValidationRangeConsumer.Value[] expected = new ValidationRangeConsumer.Value[Character.MAX_VALUE + 1];
      Arrays.fill(expected, ABSENT);
      for (char c : data) {
        container = container.add(c);
        expected[c] = PRESENT;
      }
      assertEquals(container.getCardinality(), data.length);
      ValidationRangeConsumer consumer = ValidationRangeConsumer.validate(expected);
      container.forAll(0, consumer);
      assertEquals(expected.length, consumer.getNumberOfValuesConsumed());
    }
  }

  private char[] allValues() {
    char[] allValues = new char[Character.MAX_VALUE + 1];
    IntStream.rangeClosed(0, Character.MAX_VALUE).forEach(i -> allValues[i] = (char) i);
    return allValues;
  }

  @Test
  public void forAll() {
    testForAllMaterialization(new char[0]);
    testForAllMaterialization(new char[]{0});
    testForAllMaterialization(new char[]{1});
    testForAllMaterialization(new char[]{Character.MAX_VALUE});
    testForAllMaterialization(new char[]{0, 2, 5, 7});
    testForAllMaterialization(new char[]{49, 63, 65, 32768, 3280});
    testForAllMaterialization(new char[]{0, Character.MAX_VALUE});
    testForAllMaterialization(new char[]{Character.MAX_VALUE - 1, Character.MAX_VALUE});
    testForAllMaterialization(new char[]{Character.MAX_VALUE - 1});
    testForAllMaterialization(allValues());
  }

  // start is inclusive
  private void testForAllFromMaterialization(char start, char[] data) {
    for (Class<?> ct1 : CONTAINER_TYPES) {
      Container container = getContainerInstance(ct1);
      ValidationRangeConsumer.Value[] expected = new ValidationRangeConsumer.Value[Character.MAX_VALUE + 1 - start];
      Arrays.fill(expected, ABSENT);
      for (char c : data) {
        container = container.add(c);
        int relativePos = c - start;
        if (relativePos >= 0) { // Otherwise it's out of range.
          expected[relativePos] = PRESENT;
        }
      }
      assertEquals(container.getCardinality(), data.length);
      ValidationRangeConsumer consumer = ValidationRangeConsumer.validate(expected);
      container.forAllFrom(start, consumer);
      assertEquals(expected.length, consumer.getNumberOfValuesConsumed());
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 50, 63, 64, 65, 32768, 3280, 65534, 65535})
  public void forAllFrom(int start) {
    testForAllFromMaterialization((char) start, new char[0]);
    testForAllFromMaterialization((char) start, new char[]{0});
    testForAllFromMaterialization((char) start, new char[]{1});
    testForAllFromMaterialization((char) start, new char[]{0, 2, 5, 7});
    testForAllFromMaterialization((char) start, new char[]{49, 63, 65, 32768, 3280});
    testForAllFromMaterialization((char) start, new char[]{0, Character.MAX_VALUE});
    testForAllFromMaterialization((char) start, new char[]{Character.MAX_VALUE - 1, Character.MAX_VALUE});
    testForAllFromMaterialization((char) start, new char[]{Character.MAX_VALUE - 1});
    testForAllFromMaterialization((char) start, allValues());
  }

  // end is exclusive
  private void testForAllUntilMaterialization(char end, char[] data) {
    for (Class<?> ct1 : CONTAINER_TYPES) {
      Container container = getContainerInstance(ct1);
      // End is an exclusive boundary, since there is `forAll` consume the entire container.
      ValidationRangeConsumer.Value[] expected = new ValidationRangeConsumer.Value[end];
      Arrays.fill(expected, ABSENT);
      for (char c : data) {
        container = container.add(c);
        if (c < end) { // Otherwise it's out of range.
          expected[c] = PRESENT;
        }
      }
      assertEquals(container.getCardinality(), data.length);
      ValidationRangeConsumer consumer = ValidationRangeConsumer.validate(expected);
      container.forAllUntil(0, end, consumer);
      assertEquals(expected.length, consumer.getNumberOfValuesConsumed());
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 50, 63, 64, 65, 32768, 3280, 65534, 65535})
  public void forAllUntil(int end) {
    testForAllUntilMaterialization((char) end, new char[0]);
    testForAllUntilMaterialization((char) end, new char[]{0});
    testForAllUntilMaterialization((char) end, new char[]{1});
    testForAllUntilMaterialization((char) end, new char[]{0, 2, 5, 7});
    testForAllUntilMaterialization((char) end, new char[]{49, 63, 65, 32768, 3280});
    testForAllUntilMaterialization((char) end, new char[]{0, Character.MAX_VALUE});
    testForAllUntilMaterialization((char) end, new char[]{Character.MAX_VALUE - 1, Character.MAX_VALUE});
    testForAllUntilMaterialization((char) end, new char[]{Character.MAX_VALUE - 1});
    testForAllUntilMaterialization((char) end, allValues());
  }

  // start is inclusive
  private void testForAllInRangeMaterialization(char start, char end, char[] data) {
    for (Class<?> ct1 : CONTAINER_TYPES) {
      if (start < end) {
        Container container = getContainerInstance(ct1);
        ValidationRangeConsumer.Value[] expected = new ValidationRangeConsumer.Value[end - start];
        Arrays.fill(expected, ABSENT);
        for (char c : data) {
          container = container.add(c);
          int relativePos = c - start;
          if (relativePos >= 0 && c < end) { // Otherwise it's out of range.
            expected[relativePos] = PRESENT;
          }
        }
        assertEquals(container.getCardinality(), data.length);
        ValidationRangeConsumer consumer = ValidationRangeConsumer.validate(expected);
        container.forAllInRange(start, end, consumer);
        assertEquals(expected.length, consumer.getNumberOfValuesConsumed());
      } else {
        final Container container = getContainerInstance(ct1);
        final ValidationRangeConsumer consumer = ValidationRangeConsumer.ofSize(0);
        assertThrows(IllegalArgumentException.class, () -> container.forAllInRange(start, end, consumer));
      }
    }
  }

  private static Stream<Arguments> provideArgsForAllInRange() {
    int[] baseArgs = IntStream.of(0, 1, 50, 63, 64, 65, 32768, 3280, 65534, 65535).toArray();
    List<Arguments> cartesianProduct = new ArrayList<>();
    for (int start : baseArgs) {
      for (int end : baseArgs) {
        if (start <= end) {
          cartesianProduct.add(Arguments.of(start, end));
        }
      }
    }
    return cartesianProduct.stream();
  }
  @ParameterizedTest
  @MethodSource("provideArgsForAllInRange")
  public void forAllInRange(int start, int end) {
    testForAllInRangeMaterialization((char) start, (char) end, new char[0]);
    testForAllInRangeMaterialization((char) start, (char) end, new char[]{0});
    testForAllInRangeMaterialization((char) start, (char) end, new char[]{1});
    testForAllInRangeMaterialization((char) start, (char) end, new char[]{0, 2, 5, 7});
    testForAllInRangeMaterialization((char) start, (char) end, new char[]{49, 63, 65, 32768, 3280});
    testForAllInRangeMaterialization((char) start, (char) end, new char[]{0, Character.MAX_VALUE});
    testForAllInRangeMaterialization((char) start, (char) end, new char[]{Character.MAX_VALUE - 1, Character.MAX_VALUE});
    testForAllInRangeMaterialization((char) start, (char) end, new char[]{Character.MAX_VALUE - 1});
    testForAllInRangeMaterialization((char) start, (char) end, allValues());
  }

  @Test
  public void debugMe() {
    char start = 65;
    char end = 32768;
    char[] data = new char[0];

    Container container = new BitmapContainer();

    ValidationRangeConsumer.Value[] expected = new ValidationRangeConsumer.Value[end - start];
    Arrays.fill(expected, ABSENT);
    for (char c : data) {
      container = container.add(c);
      int relativePos = c - start;
      if (relativePos >= 0 && c < end) { // Otherwise it's out of range.
        expected[relativePos] = PRESENT;
      }
    }
    assertEquals(container.getCardinality(), data.length);
    ValidationRangeConsumer consumer = ValidationRangeConsumer.validate(expected);
    container.forAllInRange(start, end, consumer);
    assertEquals(expected.length, consumer.getNumberOfValuesConsumed());
  }
}
