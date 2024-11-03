/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap.buffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.roaringbitmap.CharIterator;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Various tests on Container and its subclasses, ArrayContainer, RunContainer and BitmapContainer
 */
@SuppressWarnings({"static-method"})
@Execution(ExecutionMode.CONCURRENT)
public class TestContainer {

  @Test
  public void testNames() {
    assertEquals(
        new MappeableBitmapContainer().getContainerName(), MappeableContainer.ContainerNames[0]);
    assertEquals(
        new MappeableArrayContainer().getContainerName(), MappeableContainer.ContainerNames[1]);
    assertEquals(
        new MappeableRunContainer().getContainerName(), MappeableContainer.ContainerNames[2]);
  }

  public static boolean checkContent(MappeableContainer c, char[] s) {
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

  public static MappeableContainer makeContainer(char[] ss) {
    MappeableContainer c = new MappeableArrayContainer();
    for (final char s : ss) {
      c = c.add(s);
    }
    return c;
  }

  @Test
  public void inotTest1() {
    // Array container, range is complete
    final char[] content = {1, 3, 5, 7, 9};
    MappeableContainer c = makeContainer(content);
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
    final MappeableContainer c = makeContainer(content);
    final MappeableContainer c1 = c.inot(65190, 65201);
    assertTrue(c1 instanceof MappeableArrayContainer);
    assertEquals(14, c1.getCardinality());
    assertTrue(
        checkContent(
            c1,
            new char[] {
              0,
              2,
              4,
              (char) 65190,
              (char) 65191,
              (char) 65192,
              (char) 65193,
              (char) 65194,
              (char) 65195,
              (char) 65196,
              (char) 65197,
              (char) 65198,
              (char) 65199,
              (char) 65200
            }));
  }

  @Test
  public void inotTest10A() {
    System.out.println("inotTest10A");
    // Run container, inverting a range past any set bit
    final char[] content = new char[] {1, 2, 3, 4, 5};
    MappeableContainer c = makeContainer(content);
    c = c.runOptimize();
    assertTrue(c instanceof MappeableRunContainer);
    final MappeableContainer c1 = c.inot(65190, 65201);
    assertTrue(c1 instanceof MappeableRunContainer);
    assertEquals(16, c1.getCardinality());
    assertTrue(
        checkContent(
            c1,
            new char[] {
              1,
              2,
              3,
              4,
              5,
              (char) 65190,
              (char) 65191,
              (char) 65192,
              (char) 65193,
              (char) 65194,
              (char) 65195,
              (char) 65196,
              (char) 65197,
              (char) 65198,
              (char) 65199,
              (char) 65200
            }));
  }

  @Test
  public void inotTest1A() {
    // Run container, range is complete
    final char[] content = {1, 2, 3, 55, 56, 57};
    MappeableContainer c = makeContainer(content);
    c = c.runOptimize();
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
  public void inotTest2() {
    // Array and then Bitmap container, range is complete
    final char[] content = {1, 3, 5, 7, 9};
    MappeableContainer c = makeContainer(content);
    c = c.inot(0, 65535);
    c = c.inot(0, 65535);
    assertTrue(checkContent(c, content));
  }

  @Test
  public void inotTest2A() {
    // Run, range is complete
    final char[] content = {1, 2, 3, 8, 9, 10, 11, 12, 13, 14};
    MappeableContainer c = makeContainer(content);
    c = c.runOptimize();
    assertTrue(c instanceof MappeableRunContainer);
    c = c.inot(0, 65535);
    c = c.inot(0, 65535);
    assertTrue(c instanceof MappeableRunContainer);
    assertTrue(checkContent(c, content));
  }

  @Test
  public void inotTest3() {
    // Bitmap to bitmap, full range

    MappeableContainer c = new MappeableArrayContainer();
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
    MappeableContainer c = makeContainer(content);
    c = c.inot(4, 1000);
    assertTrue(c instanceof MappeableArrayContainer);
    assertEquals(999 - 4 + 1 - 3 + 2, c.getCardinality());
    c = c.inot(4, 1000); // back
    assertTrue(checkContent(c, content));
  }

  @Test
  public void inotTest4A() {
    // Run container, range is partial,
    final char[] content = {1, 2, 3, 5, 6, 7, 8};
    MappeableContainer c = makeContainer(content);
    c = c.runOptimize();
    c = c.inot(4, 1000);
    assertTrue(c instanceof MappeableRunContainer);
    assertEquals(995, c.getCardinality());
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
    MappeableContainer c = makeContainer(content);
    c = c.inot(4, 1000);
    assertTrue(c instanceof MappeableBitmapContainer);
    assertEquals(31773, c.getCardinality());
    c = c.inot(4, 1000); // back, as a bitmap
    assertTrue(c instanceof MappeableBitmapContainer);
    assertTrue(checkContent(c, content));
  }

  @Test
  public void inotTest5A() {
    System.out.println("inotTest5A");
    // Run container, range is partial, result stays run (repeats 4A somewhat)
    final char[] content = new char[32768 - 5];
    content[0] = 0;
    content[1] = 2;
    content[2] = 4;
    content[3] = 6;
    content[4] = 8;
    for (int i = 10; i <= 32767; ++i) {
      content[i - 10 + 5] = (char) i;
    }
    MappeableContainer c = makeContainer(content);
    c = c.runOptimize();
    c = c.inot(4, 1000);
    assertTrue(c instanceof MappeableRunContainer);
    assertEquals(31773, c.getCardinality());
    c = c.inot(4, 1000); // back, as a bitmap
    assertTrue(c instanceof MappeableRunContainer);
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
    MappeableContainer c = makeContainer(content);
    c = c.inot(4, 9);
    assertTrue(c instanceof MappeableBitmapContainer);
    assertEquals(32762, c.getCardinality());
    c = c.inot(4, 9); // back, as a bitmap
    assertTrue(c instanceof MappeableBitmapContainer);
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
    MappeableContainer c = makeContainer(content);
    c = c.inot(5, 31001);
    if (c.getCardinality() <= MappeableArrayContainer.DEFAULT_MAX_SIZE) {
      assertTrue(c instanceof MappeableArrayContainer);
    } else {
      assertTrue(c instanceof MappeableBitmapContainer);
    }
    assertEquals(1773, c.getCardinality());
    c = c.inot(5, 31001); // back, as a bitmap
    if (c.getCardinality() <= MappeableArrayContainer.DEFAULT_MAX_SIZE) {
      assertTrue(c instanceof MappeableArrayContainer);
    } else {
      assertTrue(c instanceof MappeableBitmapContainer);
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

    MappeableContainer c = makeContainer(content);
    c = c.inot(5, 22);
    assertTrue(c instanceof MappeableArrayContainer);

    assertEquals(10, c.getCardinality());
    c = c.inot(5, 22); // back, as a bitmap
    assertTrue(c instanceof MappeableArrayContainer);
    assertTrue(checkContent(c, content));
  }

  // mostly same tests, except for not. (check original unaffected)
  @Test
  public void notTest1() {
    // Array container, range is complete
    final char[] content = {1, 3, 5, 7, 9};
    final MappeableContainer c = makeContainer(content);
    final MappeableContainer c1 = c.not(0, 65536);
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
    final MappeableContainer c = makeContainer(content);
    final MappeableContainer c1 = c.not(51413, 51471);
    assertTrue(c1 instanceof MappeableArrayContainer);
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
    final MappeableContainer c = makeContainer(content);
    final MappeableContainer c1 = c.not(1, 59);
    assertTrue(c1 instanceof MappeableArrayContainer);
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
  public void notTest1A() {
    // Run container, range is complete
    final char[] content = {1, 2, 3, 6, 7, 8};
    MappeableContainer c = makeContainer(content);
    c = c.runOptimize();
    final MappeableContainer c1 = c.not(0, 65536);
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
  public void notTest2() {
    // Array and then Bitmap container, range is complete
    final char[] content = {1, 3, 5, 7, 9};
    final MappeableContainer c = makeContainer(content);
    final MappeableContainer c1 = c.not(0, 65535);
    final MappeableContainer c2 = c1.not(0, 65535);
    assertTrue(checkContent(c2, content));
  }

  @Test
  public void notTest2A() {
    // complete range
    final char[] content = {2, 3, 4, 7, 8};
    MappeableContainer c = makeContainer(content);
    c = c.runOptimize();
    final MappeableContainer c1 = c.not(0, 65535);
    final MappeableContainer c2 = c1.not(0, 65535);
    assertTrue(checkContent(c2, content));
  }

  @Test
  public void notTest3() {
    // Bitmap to bitmap, full range

    MappeableContainer c = new MappeableArrayContainer();
    for (int i = 0; i < 65536; i += 2) {
      c = c.add((char) i);
    }

    final MappeableContainer c1 = c.not(0, 65536);
    assertTrue(c1.contains((char) 3) && !c1.contains((char) 4));
    assertEquals(32768, c1.getCardinality());
    final MappeableContainer c2 = c1.not(0, 65536);
    for (int i = 0; i < 65536; i += 2) {
      assertTrue(c2.contains((char) i) && !c2.contains((char) (i + 1)));
    }
  }

  @Test
  public void notTest4() {
    System.out.println("notTest4");
    // Array container, range is partial, result stays array
    final char[] content = {1, 3, 5, 7, 9};
    final MappeableContainer c = makeContainer(content);
    final MappeableContainer c1 = c.not(4, 1000);
    assertTrue(c1 instanceof MappeableArrayContainer);
    assertEquals(999 - 4 + 1 - 3 + 2, c1.getCardinality());
    final MappeableContainer c2 = c1.not(4, 1000); // back
    assertTrue(checkContent(c2, content));
  }

  @Test
  public void notTest4A() {
    System.out.println("notTest4A");
    // Runcontainer version
    final char[] content = {1, 2, 3, 5, 6, 7, 8};
    MappeableContainer c = makeContainer(content);
    c = c.runOptimize();
    final MappeableContainer c1 = c.not(4, 1000);
    assertTrue(c1 instanceof MappeableRunContainer);
    assertEquals(995, c1.getCardinality());
    final MappeableContainer c2 = c1.not(4, 1000); // back
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
    final MappeableContainer c = makeContainer(content);
    final MappeableContainer c1 = c.not(4, 1000);
    assertTrue(c1 instanceof MappeableBitmapContainer);
    assertEquals(31773, c1.getCardinality());
    final MappeableContainer c2 = c1.not(4, 1000); // back, as a bitmap
    assertTrue(c2 instanceof MappeableBitmapContainer);
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
    final MappeableContainer c = makeContainer(content);
    final MappeableContainer c1 = c.not(4, 9);
    assertTrue(c1 instanceof MappeableBitmapContainer);
    assertEquals(32762, c1.getCardinality());
    final MappeableContainer c2 = c1.not(4, 9); // back, as a bitmap
    assertTrue(c2 instanceof MappeableBitmapContainer);
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
    final MappeableContainer c = makeContainer(content);
    final MappeableContainer c1 = c.not(5, 31001);
    if (c1.getCardinality() <= MappeableArrayContainer.DEFAULT_MAX_SIZE) {
      assertTrue(c1 instanceof MappeableArrayContainer);
    } else {
      assertTrue(c1 instanceof MappeableBitmapContainer);
    }
    assertEquals(1773, c1.getCardinality());
    final MappeableContainer c2 = c1.not(5, 31001); // back, as a bitmap
    if (c2.getCardinality() <= MappeableArrayContainer.DEFAULT_MAX_SIZE) {
      assertTrue(c2 instanceof MappeableArrayContainer);
    } else {
      assertTrue(c2 instanceof MappeableBitmapContainer);
    }
    assertTrue(checkContent(c2, content));
  }

  @Test
  public void notTest7A() {
    System.out.println("notTest7A");
    // Runcontainer version of notTest7
    final char[] content = new char[32768 - 5];
    content[0] = 0;
    content[1] = 2;
    content[2] = 4;
    content[3] = 6;
    content[4] = 8;
    for (int i = 10; i <= 32767; ++i) {
      content[i - 10 + 5] = (char) i;
    }
    MappeableContainer c = makeContainer(content);
    c = c.runOptimize();
    MappeableContainer c1 = c.not(5, 31001);
    assertTrue(c1 instanceof MappeableRunContainer);
    c1 = c1.runOptimize(); // should not change
    assertTrue(c1 instanceof MappeableRunContainer);
    assertEquals(1773, c1.getCardinality());
    final MappeableContainer c2 = c1.not(5, 31001); // back, as a bitmap
    assertTrue(c2 instanceof MappeableRunContainer);
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
    final MappeableContainer c = makeContainer(content);
    final MappeableContainer c1 = c.not(4, 65536);
    assertTrue(c1 instanceof MappeableBitmapContainer);
    assertEquals(32773, c1.getCardinality());
    final MappeableContainer c2 = c1.not(4, 65536); // back, as a bitmap
    assertTrue(c2 instanceof MappeableBitmapContainer);
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
    final MappeableContainer c = makeContainer(content);
    final MappeableContainer c1 = c.not(0, 65201);
    assertTrue(c1 instanceof MappeableBitmapContainer);
    assertEquals(32438, c1.getCardinality());
    final MappeableContainer c2 = c1.not(0, 65201); // back, as a bitmap
    assertTrue(c2 instanceof MappeableBitmapContainer);
    assertTrue(checkContent(c2, content));
  }

  @Test
  public void numberOfRuns() {
    char[] positions = {3, 4, 5, 10, 11, 13, 15, 62, 63, 64, 65};
    MappeableContainer ac = new MappeableArrayContainer();
    MappeableContainer bc = new MappeableArrayContainer();
    MappeableContainer rc = new MappeableRunContainer();

    for (char position : positions) {
      ac.add(position);
      bc.add(position);
      rc.add(position);
    }

    assertEquals(rc.numberOfRuns(), ac.numberOfRuns());
    assertEquals(rc.numberOfRuns(), bc.numberOfRuns());
  }

  @Test
  public void rangeOfOnesTest1() {
    final MappeableContainer c = MappeableContainer.rangeOfOnes(4, 11); // sparse
    // assertTrue(c instanceof MappeableArrayContainer);
    assertEquals(10 - 4 + 1, c.getCardinality());
    assertTrue(checkContent(c, new char[] {4, 5, 6, 7, 8, 9, 10}));
  }

  @Test
  public void rangeOfOnesTest1A() {
    MappeableContainer c = MappeableContainer.rangeOfOnes(4, 11); // sparse
    c = c.runOptimize();
    assertTrue(c instanceof MappeableRunContainer);
    assertTrue(checkContent(c, new char[] {4, 5, 6, 7, 8, 9, 10}));
  }

  @Test
  public void rangeOfOnesTest2() {
    final MappeableContainer c = MappeableContainer.rangeOfOnes(1000, 35001); // dense
    // assertTrue(c instanceof MappeableBitmapContainer);
    assertEquals(35000 - 1000 + 1, c.getCardinality());
  }

  @Test
  public void rangeOfOnesTest2A() {
    final MappeableContainer c = MappeableContainer.rangeOfOnes(1000, 35001); // dense
    final char s[] = new char[35000 - 1000 + 1];
    for (int i = 1000; i <= 35000; ++i) {
      s[i - 1000] = (char) i;
    }
    assertTrue(checkContent(c, s));
  }

  @Test
  public void rangeOfOnesTest2AA() {
    MappeableContainer c = MappeableContainer.rangeOfOnes(1000, 35001); // dense
    c = c.runOptimize();
    final char s[] = new char[35000 - 1000 + 1];
    for (int i = 1000; i <= 35000; ++i) {
      s[i - 1000] = (char) i;
    }
    assertTrue(checkContent(c, s));
  }

  @Test
  public void rangeOfOnesTest3() {
    // bdry cases
    MappeableContainer.rangeOfOnes(1, MappeableArrayContainer.DEFAULT_MAX_SIZE);
  }

  @Test
  public void rangeOfOnesTest4() {
    MappeableContainer.rangeOfOnes(1, MappeableArrayContainer.DEFAULT_MAX_SIZE + 2);
  }

  @Test
  public void transitionTest() {
    MappeableContainer c = new MappeableArrayContainer();
    for (int i = 0; i < 4096; ++i) {
      c = c.add((char) i);
    }
    assertEquals(c.getCardinality(), 4096);
    assertTrue(c instanceof MappeableArrayContainer);
    for (int i = 0; i < 4096; ++i) {
      c = c.add((char) i);
    }
    assertEquals(c.getCardinality(), 4096);
    assertTrue(c instanceof MappeableArrayContainer);
    c = c.add((char) 4096);
    assertEquals(c.getCardinality(), 4097);
    assertTrue(c instanceof MappeableBitmapContainer);
    c = c.remove((char) 4096);
    assertEquals(c.getCardinality(), 4096);
    assertTrue(c instanceof MappeableArrayContainer);
    c = c.runOptimize();
    assertEquals(c.getCardinality(), 4096);
    assertTrue(c instanceof MappeableRunContainer);
    c = c.inot(0, 4095); // just 4095 left
    c = c.runOptimize();
    assertEquals(c.getCardinality(), 1);
    assertTrue(c instanceof MappeableArrayContainer);
  }

  @Test
  public void testXorContainer() throws Exception {
    MappeableContainer rc1 = new MappeableRunContainer().add(2, 10).add(20, 40);
    MappeableContainer rc2 = new MappeableRunContainer().add(5, 11).add(13, 25);
    MappeableContainer bc1 = new MappeableBitmapContainer().add(10, 20);
    MappeableContainer bc2 = new MappeableBitmapContainer().add(21, 30);
    MappeableContainer ac1 = new MappeableArrayContainer().add((char) 3).add((char) 79);
    MappeableContainer ac2 =
        new MappeableArrayContainer().add((char) 45).add((char) 56).add((char) 109);
    for (Set<MappeableContainer> test :
        Sets.powerSet(ImmutableSet.of(rc1, rc2, bc1, bc2, ac1, ac2))) {
      Iterator<MappeableContainer> it = test.iterator();
      if (test.size() == 1) { // compare with self
        MappeableContainer x = it.next();
        assertEquals(x.xor(x).getCardinality(), x.xorCardinality(x));
      } else if (test.size() == 2) {
        MappeableContainer x = it.next();
        MappeableContainer y = it.next();
        assertEquals(x.xor(y).getCardinality(), x.xorCardinality(y));
        assertEquals(y.xor(x).getCardinality(), y.xorCardinality(x));
      }
    }
  }

  @Test
  public void testFirst_Array() {
    assertThrows(NoSuchElementException.class, () -> new MappeableArrayContainer().first());
  }

  @Test
  public void testLast_Array() {
    assertThrows(NoSuchElementException.class, () -> new MappeableArrayContainer().last());
  }

  @Test
  public void testFirst_Run() {
    assertThrows(NoSuchElementException.class, () -> new MappeableRunContainer().first());
  }

  @Test
  public void testLast_Run() {
    assertThrows(NoSuchElementException.class, () -> new MappeableRunContainer().last());
  }

  @Test
  public void testFirst_Bitmap() {
    assertThrows(NoSuchElementException.class, () -> new MappeableBitmapContainer().first());
  }

  @Test
  public void testLast_Bitmap() {
    assertThrows(NoSuchElementException.class, () -> new MappeableBitmapContainer().last());
  }

  @Test
  public void testFirstLast_Array() {
    testFirstLast(new MappeableArrayContainer());
  }

  @Test
  public void testFirstLast_Run() {
    testFirstLast(new MappeableRunContainer());
  }

  @Test
  public void testFirstLast_Bitmap() {
    testFirstLast(new MappeableBitmapContainer());
  }

  private void testFirstLast(MappeableContainer container) {
    final int firstInclusive = 1;
    int lastExclusive = firstInclusive;
    for (int i = 0; i < 1 << 15 / 10; ++i) {
      int newLastExclusive = lastExclusive + 10;
      container = container.add(lastExclusive, newLastExclusive);
      assertEquals(firstInclusive, container.first());
      assertEquals(newLastExclusive - 1, container.last());
      lastExclusive = newLastExclusive;
    }
  }

  @Test
  public void testConsistentToString() {
    MappeableArrayContainer ac = new MappeableArrayContainer();
    MappeableBitmapContainer bc = new MappeableBitmapContainer();
    MappeableRunContainer rc = new MappeableRunContainer();
    for (char i : new char[] {0, 2, 17, Short.MAX_VALUE, (char) -3, (char) -1}) {
      ac.add(i);
      bc.add(i);
      rc.add(i);
    }
    String expected = "{0,2,17,32767,65533,65535}";

    assertEquals(expected, ac.toString());
    assertEquals(expected, bc.toString());
    String normalizedRCstr =
        rc.toString().replaceAll("\\d+\\]\\[", "").replace('[', '{').replaceFirst(",\\d+\\]", "}");
    assertEquals(expected, normalizedRCstr);
  }
}
