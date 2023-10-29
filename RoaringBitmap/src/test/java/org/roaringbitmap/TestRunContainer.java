package org.roaringbitmap;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.roaringbitmap.ArrayContainer.DEFAULT_MAX_SIZE;
import static org.roaringbitmap.ValidationRangeConsumer.Value.ABSENT;
import static org.roaringbitmap.ValidationRangeConsumer.Value.PRESENT;

@Execution(ExecutionMode.CONCURRENT)
public class TestRunContainer {
  @Test
  public void testRunOpti() {
    RoaringBitmap mrb = new RoaringBitmap();
    for(int r = 0; r< 100000; r+=3 ) {
      mrb.add(r);
    }
    mrb.add(1000000);
    for(int r = 2000000; r < 3000000; ++r) {
      mrb.add(r);
    }
    RoaringBitmap m2 = mrb.clone();
    m2.runOptimize();
    IntIterator x = m2.getReverseIntIterator();
    int count = 0;
    while(x.hasNext()) {
      x.next();
      count++;
    }
    assertEquals(m2.getCardinality(), count);
    assertEquals(mrb.getCardinality(), count);
    assertTrue(m2.serializedSizeInBytes() < mrb.serializedSizeInBytes());
    assertEquals(m2, mrb);
  }

  public static Container fillMeUp(Container c, int[] values) {
    if (values.length == 0) {
      throw new RuntimeException("You are trying to create an empty bitmap! ");
    }
    for (int k = 0; k < values.length; ++k) {
      c = c.add((char) values[k]);
    }
    if (c.getCardinality() != values.length) {
      throw new RuntimeException("add failure");
    }
    return c;
  }

  /**
   * generates randomly N distinct integers from 0 to Max.
   */
  static int[] generateUniformHash(Random rand, int N, int Max) {

    if (N > Max) {
      throw new RuntimeException("not possible");
    }
    if (N > Max / 2) {
      return negate(generateUniformHash(rand, Max - N, Max), Max);
    }
    int[] ans = new int[N];
    HashSet<Integer> s = new HashSet<Integer>();
    while (s.size() < N) {
      s.add(rand.nextInt(Max));
    }
    Iterator<Integer> i = s.iterator();
    for (int k = 0; k < N; ++k) {
      ans[k] = i.next().intValue();
    }
    Arrays.sort(ans);
    return ans;
  }

  private static void getSetOfRunContainers(ArrayList<RunContainer> set,
      ArrayList<Container> setb) {
    RunContainer r1 = new RunContainer();
    r1 = (RunContainer) r1.iadd(0, (1 << 16));
    Container b1 = new ArrayContainer();
    b1 = b1.iadd(0, 1 << 16);
    assertEquals(r1, b1);

    set.add(r1);
    setb.add(b1);

    RunContainer r2 = new RunContainer();
    r2 = (RunContainer) r2.iadd(0, 4096);
    Container b2 = new ArrayContainer();
    b2 = b2.iadd(0, 4096);
    set.add(r2);
    setb.add(b2);
    assertEquals(r2, b2);

    RunContainer r3 = new RunContainer();
    Container b3 = new ArrayContainer();

    // mayhaps some of the 655536s were intended to be 65536s?? And later...
    for (int k = 0; k < 655536; k += 2) {
      r3 = (RunContainer) r3.add((char) k);
      b3 = b3.add((char) k);
    }
    assertEquals(r3, b3);
    set.add(r3);
    setb.add(b3);

    RunContainer r4 = new RunContainer();
    Container b4 = new ArrayContainer();
    for (int k = 0; k < 655536; k += 256) {
      r4 = (RunContainer) r4.add((char) k);
      b4 = b4.add((char) k);
    }
    assertEquals(r4, b4);
    set.add(r4);
    setb.add(b4);

    RunContainer r5 = new RunContainer();
    Container b5 = new ArrayContainer();
    for (int k = 0; k + 4096 < 65536; k += 4096) {
      r5 = (RunContainer) r5.iadd(k, k + 256);
      b5 = b5.iadd(k, k + 256);
    }
    assertEquals(r5, b5);
    set.add(r5);
    setb.add(b5);

    RunContainer r6 = new RunContainer();
    Container b6 = new ArrayContainer();
    for (int k = 0; k + 1 < 65536; k += 7) {
      r6 = (RunContainer) r6.iadd(k, k + 1);
      b6 = b6.iadd(k, k + 1);
    }
    assertEquals(r6, b6);
    set.add(r6);
    setb.add(b6);


    RunContainer r7 = new RunContainer();
    Container b7 = new ArrayContainer();
    for (int k = 0; k + 1 < 65536; k += 11) {
      r7 = (RunContainer) r7.iadd(k, k + 1);
      b7 = b7.iadd(k, k + 1);
    }
    assertEquals(r7, b7);
    set.add(r7);
    setb.add(b7);

  }

  /**
   * output all integers from the range [0,Max) that are not in the array
   */
  static int[] negate(int[] x, int Max) {
    int[] ans = new int[Max - x.length];
    int i = 0;
    int c = 0;
    for (int j = 0; j < x.length; ++j) {
      int v = x[j];
      for (; i < v; ++i) {
        ans[c++] = i;
      }
      ++i;
    }
    while (c < ans.length) {
      ans[c++] = i++;
    }
    return ans;
  }

  @Test
  public void addAndCompress() {
    RunContainer container = new RunContainer();
    container.add((char) 0);
    container.add((char) 99);
    container.add((char) 98);
    assertEquals(12, container.getSizeInBytes());
  }

  @Test
  public void addOutOfOrder() {
    RunContainer container = new RunContainer();
    container.add((char) 0);
    container.add((char) 2);
    container.add((char) 55);
    container.add((char) 1);
    assertEquals(4, container.getCardinality());
    assertTrue(container.contains((char) 0));
    assertTrue(container.contains((char) 1));
    assertTrue(container.contains((char) 2));
    assertTrue(container.contains((char) 55));
  }

  @Test
  public void addRange() {
    for (int i = 0; i < 100; ++i) {
      for (int j = 0; j < 100; ++j) {
        for (int k = 0; k < 50; ++k) {
          BitSet bs = new BitSet();
          RunContainer container = new RunContainer();
          for (int p = 0; p < i; ++p) {
            container.add((char) p);
            bs.set(p);
          }
          for (int p = 0; p < j; ++p) {
            container.add((char) (99 - p));
            bs.set(99 - p);
          }
          Container newContainer = container.add(49 - k, 50 + k);
          bs.set(49 - k, 50 + k);
          assertNotSame(container, newContainer);
          assertEquals(bs.cardinality(), newContainer.getCardinality());

          int nb_runs = 1;
          int lastIndex = bs.nextSetBit(0);
          for (int p = bs.nextSetBit(0); p >= 0; p = bs.nextSetBit(p + 1)) {
            if (p - lastIndex > 1) {
              nb_runs++;
            }
            lastIndex = p;
            assertTrue(newContainer.contains((char) p));
          }
          assertEquals(nb_runs * 4 + 4, newContainer.getSizeInBytes());
        }
      }
    }
  }

  @Test
  public void addRangeAndFuseWithNextValueLength() {
    RunContainer container = new RunContainer();
    for (char i = 10; i < 20; ++i) {
      container.add(i);
    }
    for (char i = 21; i < 30; ++i) {
      container.add(i);
    }
    Container newContainer = container.add(15, 21);
    assertNotSame(container, newContainer);
    assertEquals(20, newContainer.getCardinality());
    for (char i = 10; i < 30; ++i) {
      assertTrue(newContainer.contains(i));
    }
    assertEquals(8, newContainer.getSizeInBytes());
  }

  @Test
  public void addRangeAndFuseWithPreviousValueLength() {
    RunContainer container = new RunContainer();
    for (char i = 10; i < 20; ++i) {
      container.add(i);
    }
    Container newContainer = container.add(20, 30);
    assertNotSame(container, newContainer);
    assertEquals(20, newContainer.getCardinality());
    for (char i = 10; i < 30; ++i) {
      assertTrue(newContainer.contains(i));
    }
    assertEquals(8, newContainer.getSizeInBytes());
  }

  @Test
  public void addRangeOnEmptyContainer() {
    RunContainer container = new RunContainer();
    Container newContainer = container.add(10, 100);
    assertNotSame(container, newContainer);
    assertEquals(90, newContainer.getCardinality());
    for (char i = 10; i < 100; ++i) {
      assertTrue(newContainer.contains(i));
    }
  }

  @Test
  public void addRangeOnNonEmptyContainer() {
    RunContainer container = new RunContainer();
    container.add((char) 1);
    container.add((char) 256);
    Container newContainer = container.add(10, 100);
    assertNotSame(container, newContainer);
    assertEquals(92, newContainer.getCardinality());
    assertTrue(newContainer.contains((char) 1));
    assertTrue(newContainer.contains((char) 256));
    for (char i = 10; i < 100; ++i) {
      assertTrue(newContainer.contains(i));
    }
  }

  @Test
  public void addRangeOnNonEmptyContainerAndFuse() {
    RunContainer container = new RunContainer();
    for (char i = 1; i < 20; ++i) {
      container.add(i);
    }
    for (char i = 90; i < 120; ++i) {
      container.add(i);
    }
    Container newContainer = container.add(10, 100);
    assertNotSame(container, newContainer);
    assertEquals(119, newContainer.getCardinality());
    for (char i = 1; i < 120; ++i) {
      assertTrue(newContainer.contains(i));
    }
  }

  @Test
  public void addRangeWithinSetBounds() {
    RunContainer container = new RunContainer();
    container.add((char) 10);
    container.add((char) 99);
    Container newContainer = container.add(10, 100);
    assertNotSame(container, newContainer);
    assertEquals(90, newContainer.getCardinality());
    for (char i = 10; i < 100; ++i) {
      assertTrue(newContainer.contains(i));
    }
  }

  @Test
  public void addRangeWithinSetBoundsAndFuse() {
    RunContainer container = new RunContainer();
    container.add((char) 1);
    container.add((char) 10);
    container.add((char) 55);
    container.add((char) 99);
    container.add((char) 150);
    Container newContainer = container.add(10, 100);
    assertNotSame(container, newContainer);
    assertEquals(92, newContainer.getCardinality());
    for (char i = 10; i < 100; ++i) {
      assertTrue(newContainer.contains(i));
    }
  }

  @Test
  public void andNot() {
    Container bc = new BitmapContainer();
    Container rc = new RunContainer();
    for (int k = 0; k < 2 * DEFAULT_MAX_SIZE; ++k) {
      bc = bc.add((char) (k * 10));
      rc = rc.add((char) (k * 10 + 3));
    }
    Container result = rc.andNot(bc);
    assertEquals(rc, result);
  }

  @Test
  public void andNot1() {
    Container bc = new BitmapContainer();
    Container rc = new RunContainer();
    rc.add((char) 1);
    Container result = rc.andNot(bc);
    assertEquals(1, result.getCardinality());
    assertTrue(result.contains((char) 1));
  }

  @Test
  public void andNot2() {
    Container bc = new BitmapContainer();
    Container rc = new RunContainer();
    bc.add((char) 1);
    Container result = rc.andNot(bc);
    assertEquals(0, result.getCardinality());
  }

  @Test
  public void andNotTest1() {
    // this test uses a bitmap container that will be too sparse- okay?
    Container bc = new BitmapContainer();
    Container rc = new RunContainer();
    for (int k = 0; k < 100; ++k) {
      bc = bc.add((char) (k * 10));
      bc = bc.add((char) (k * 10 + 3));

      rc = rc.add((char) (k * 10 + 5));
      rc = rc.add((char) (k * 10 + 3));
    }
    Container intersectionNOT = rc.andNot(bc);
    assertEquals(100, intersectionNOT.getCardinality());
    for (int k = 0; k < 100; ++k) {
      assertTrue(intersectionNOT.contains((char) (k * 10 + 5)), " missing k=" + k);
    }
    assertEquals(200, bc.getCardinality());
    assertEquals(200, rc.getCardinality());
  }

  @Test
  public void andNotTest2() {
    System.out.println("andNotTest2");
    Container ac = new ArrayContainer();
    Container rc = new RunContainer();
    for (int k = 0; k < 100; ++k) {
      ac = ac.add((char) (k * 10));
      ac = ac.add((char) (k * 10 + 3));

      rc = rc.add((char) (k * 10 + 5));
      rc = rc.add((char) (k * 10 + 3));
    }
    Container intersectionNOT = rc.andNot(ac);
    assertEquals(100, intersectionNOT.getCardinality());
    for (int k = 0; k < 100; ++k) {
      assertTrue(intersectionNOT.contains((char) (k * 10 + 5)), " missing k=" + k);
    }
    assertEquals(200, ac.getCardinality());
    assertEquals(200, rc.getCardinality());
  }

  @Test
  public void basic() {
    RunContainer x = new RunContainer();
    for (int k = 0; k < (1 << 16); ++k) {
      assertFalse(x.contains((char) k));
    }
    for (int k = 0; k < (1 << 16); ++k) {
      assertFalse(x.contains((char) k));
      x = (RunContainer) x.add((char) k);
      assertEquals(k + 1, x.getCardinality());
      assertTrue(x.contains((char) k));
    }
    for (int k = 0; k < (1 << 16); ++k) {
      assertTrue(x.contains((char) k));
    }
    for (int k = 0; k < (1 << 16); ++k) {
      assertTrue(x.contains((char) k));
      x = (RunContainer) x.remove((char) k);
      assertFalse(x.contains((char) k));
      assertEquals(k + 1, (1 << 16) - x.getCardinality());
    }
    for (int k = 0; k < (1 << 16); ++k) {
      assertFalse(x.contains((char) k));
      x = (RunContainer) x.add((char) k);
      assertEquals(k + 1, x.getCardinality());
      assertTrue(x.contains((char) k));
    }
    for (int k = (1 << 16) - 1; k >= 0; --k) {
      assertTrue(x.contains((char) k));
      x = (RunContainer) x.remove((char) k);
      assertFalse(x.contains((char) k));
      assertEquals(k, x.getCardinality());
    }
    for (int k = 0; k < (1 << 16); ++k) {
      assertFalse(x.contains((char) k));
      x = (RunContainer) x.add((char) k);
      assertEquals(k + 1, x.getCardinality());
      assertTrue(x.contains((char) k));
    }
    for (int k = 0; k < (1 << 16); ++k) {
      RunContainer copy = (RunContainer) x.clone();
      copy = (RunContainer) copy.remove((char) k);
      assertEquals(copy.getCardinality() + 1, x.getCardinality());
      copy = (RunContainer) copy.add((char) k);
      assertEquals(copy.getCardinality(), x.getCardinality());
      assertEquals(copy, x);
      assertEquals(x, copy);
      copy.trim();
      assertEquals(copy, x);
      assertEquals(x, copy);
    }
  }

  @Test
  public void basic2() {
    RunContainer x = new RunContainer();
    int a = 33;
    int b = 50000;
    for (int k = a; k < b; ++k) {
      x = (RunContainer) x.add((char) k);
    }
    for (int k = 0; k < (1 << 16); ++k) {
      if (x.contains((char) k)) {
        RunContainer copy = (RunContainer) x.clone();
        copy = (RunContainer) copy.remove((char) k);
        copy = (RunContainer) copy.add((char) k);
        assertEquals(copy.getCardinality(), x.getCardinality());
        assertEquals(copy, x);
        assertEquals(x, copy);
        x.trim();
        assertEquals(copy, x);
        assertEquals(x, copy);

      } else {
        RunContainer copy = (RunContainer) x.clone();
        copy = (RunContainer) copy.add((char) k);
        assertEquals(copy.getCardinality(), x.getCardinality() + 1);
      }
    }
  }

  @Test
  public void basictri() {
    RunContainer x = new RunContainer();
    for (int k = 0; k < (1 << 16); ++k) {
      assertFalse(x.contains((char) k));
    }
    for (int k = 0; k < (1 << 16); ++k) {
      assertFalse(x.contains((char) k));
      x = (RunContainer) x.add((char) k);
      x.trim();
      assertEquals(k + 1, x.getCardinality());
      assertTrue(x.contains((char) k));
    }
    for (int k = 0; k < (1 << 16); ++k) {
      assertTrue(x.contains((char) k));
    }
    for (int k = 0; k < (1 << 16); ++k) {
      assertTrue(x.contains((char) k));
      x = (RunContainer) x.remove((char) k);
      x.trim();
      assertFalse(x.contains((char) k));
      assertEquals(k + 1, (1 << 16) - x.getCardinality());
    }
    for (int k = 0; k < (1 << 16); ++k) {
      assertFalse(x.contains((char) k));
      x = (RunContainer) x.add((char) k);
      x.trim();
      assertEquals(k + 1, x.getCardinality());
      assertTrue(x.contains((char) k));
    }
    for (int k = (1 << 16) - 1; k >= 0; --k) {
      assertTrue(x.contains((char) k));
      x = (RunContainer) x.remove((char) k);
      x.trim();
      assertFalse(x.contains((char) k));
      assertEquals(k, x.getCardinality());
    }
    for (int k = 0; k < (1 << 16); ++k) {
      assertFalse(x.contains((char) k));
      x = (RunContainer) x.add((char) k);
      x.trim();
      assertEquals(k + 1, x.getCardinality());
      assertTrue(x.contains((char) k));
    }
    for (int k = 0; k < (1 << 16); ++k) {
      RunContainer copy = (RunContainer) x.clone();
      copy.trim();
      copy = (RunContainer) copy.remove((char) k);
      copy = (RunContainer) copy.add((char) k);
      assertEquals(copy.getCardinality(), x.getCardinality());
      assertEquals(copy, x);
      assertEquals(x, copy);
      copy.trim();
      assertEquals(copy, x);
      assertEquals(x, copy);
    }
  }

  @Test
  public void clear() {
    Container rc = new RunContainer();
    rc.add((char) 1);
    assertEquals(1, rc.getCardinality());
    rc.clear();
    assertEquals(0, rc.getCardinality());
  }

  @Test
  public void equalTest1() {
    Container ac = new ArrayContainer();
    Container ar = new RunContainer();
    for (int k = 0; k < 100; ++k) {
      ac = ac.add((char) (k * 10));
      ar = ar.add((char) (k * 10));
    }
    assertEquals(ac, ar);
  }

  @Test
  public void equalTest2() {
    Container ac = new ArrayContainer();
    Container ar = new RunContainer();
    for (int k = 0; k < 10000; ++k) {
      ac = ac.add((char) k);
      ar = ar.add((char) k);
    }
    assertEquals(ac, ar);
  }


  @Test
  public void fillLeastSignificantBits() {
    Container rc = new RunContainer();
    rc.add((char) 1);
    rc.add((char) 3);
    rc.add((char) 12);
    int[] array = new int[4];
    rc.fillLeastSignificant16bits(array, 1, 0);
    assertEquals(0, array[0]);
    assertEquals(1, array[1]);
    assertEquals(3, array[2]);
    assertEquals(12, array[3]);
  }



  @Test
  public void flip() {
    RunContainer rc = new RunContainer();
    rc.flip((char) 1);
    assertTrue(rc.contains((char) 1));
    rc.flip((char) 1);
    assertFalse(rc.contains((char) 1));
  }


  @Test
  public void iaddInvalidRange1() {
    assertThrows(IllegalArgumentException.class, () -> {
      Container rc = new RunContainer();
      rc.iadd(10, 9);
    });
  }



  @Test
  public void iaddInvalidRange2() {
    assertThrows(IllegalArgumentException.class, () -> {
      Container rc = new RunContainer();
      rc.iadd(0, 1 << 20);
    });
  }

  @Test
  public void iaddRange() {
    for (int i = 0; i < 100; ++i) {
      for (int j = 0; j < 100; ++j) {
        for (int k = 0; k < 50; ++k) {
          BitSet bs = new BitSet();
          RunContainer container = new RunContainer();
          for (int p = 0; p < i; ++p) {
            container.add((char) p);
            bs.set(p);
          }
          for (int p = 0; p < j; ++p) {
            container.add((char) (99 - p));
            bs.set(99 - p);
          }
          container.iadd(49 - k, 50 + k);
          bs.set(49 - k, 50 + k);
          assertEquals(bs.cardinality(), container.getCardinality());

          int nb_runs = 1;
          int lastIndex = bs.nextSetBit(0);
          for (int p = bs.nextSetBit(0); p >= 0; p = bs.nextSetBit(p + 1)) {
            if (p - lastIndex > 1) {
              nb_runs++;
            }
            lastIndex = p;
            assertTrue(container.contains((char) p));
          }
          assertEquals(nb_runs * 4 + 4, container.getSizeInBytes());
        }
      }
    }
  }

  @Test
  public void iaddRange1() {
    Container rc = new RunContainer();
    for (char k = 0; k < 10; ++k) {
      rc.add(k);
    }
    for (char k = 20; k < 30; ++k) {
      rc.add(k);
    }
    for (char k = 40; k < 50; ++k) {
      rc.add(k);
    }
    rc.iadd(5, 21);
    assertEquals(40, rc.getCardinality());
    for (char k = 0; k < 30; ++k) {
      assertTrue(rc.contains(k));
    }
    for (char k = 40; k < 50; ++k) {
      assertTrue(rc.contains(k));
    }
    assertEquals(12, rc.getSizeInBytes());
  }

  @Test
  public void iaddRange10() {
    Container rc = new RunContainer();
    for (char k = 0; k < 10; ++k) {
      rc.add(k);
    }
    for (char k = 20; k < 30; ++k) {
      rc.add(k);
    }
    rc.iadd(15, 35);
    assertEquals(30, rc.getCardinality());
    for (char k = 0; k < 10; ++k) {
      assertTrue(rc.contains(k));
    }
    for (char k = 15; k < 35; ++k) {
      assertTrue(rc.contains(k));
    }
    assertEquals(12, rc.getSizeInBytes());
  }

  @Test
  public void iaddRange11() {
    Container rc = new RunContainer();
    for (char k = 5; k < 10; ++k) {
      rc.add(k);
    }
    for (char k = 20; k < 30; ++k) {
      rc.add(k);
    }
    rc.iadd(0, 20);
    assertEquals(30, rc.getCardinality());
    for (char k = 0; k < 30; ++k) {
      assertTrue(rc.contains(k));
    }
    assertEquals(8, rc.getSizeInBytes());
  }

  @Test
  public void iaddRange12() {
    Container rc = new RunContainer();
    for (char k = 5; k < 10; ++k) {
      rc.add(k);
    }
    for (char k = 20; k < 30; ++k) {
      rc.add(k);
    }
    rc.iadd(0, 35);
    assertEquals(35, rc.getCardinality());
    for (char k = 0; k < 35; ++k) {
      assertTrue(rc.contains(k));
    }
    assertEquals(8, rc.getSizeInBytes());
  }

  @Test
  public void iaddRange2() {
    Container rc = new RunContainer();
    for (char k = 0; k < 10; ++k) {
      rc.add(k);
    }
    for (char k = 20; k < 30; ++k) {
      rc.add(k);
    }
    for (char k = 40; k < 50; ++k) {
      rc.add(k);
    }
    rc.iadd(0, 26);
    assertEquals(40, rc.getCardinality());
    for (char k = 0; k < 30; ++k) {
      assertTrue(rc.contains(k));
    }
    for (char k = 40; k < 50; ++k) {
      assertTrue(rc.contains(k));
    }
    assertEquals(12, rc.getSizeInBytes());
  }

  @Test
  public void iaddRange3() {
    Container rc = new RunContainer();
    for (char k = 0; k < 10; ++k) {
      rc.add(k);
    }
    for (char k = 20; k < 30; ++k) {
      rc.add(k);
    }
    for (char k = 40; k < 50; ++k) {
      rc.add(k);
    }
    rc.iadd(0, 20);
    assertEquals(40, rc.getCardinality());
    for (char k = 0; k < 30; ++k) {
      assertTrue(rc.contains(k));
    }
    for (char k = 40; k < 50; ++k) {
      assertTrue(rc.contains(k));
    }
    assertEquals(12, rc.getSizeInBytes());
  }

  @Test
  public void iaddRange4() {
    Container rc = new RunContainer();
    for (char k = 0; k < 10; ++k) {
      rc.add(k);
    }
    for (char k = 20; k < 30; ++k) {
      rc.add(k);
    }
    for (char k = 40; k < 50; ++k) {
      rc.add(k);
    }
    rc.iadd(10, 21);
    assertEquals(40, rc.getCardinality());
    for (char k = 0; k < 30; ++k) {
      assertTrue(rc.contains(k));
    }
    for (char k = 40; k < 50; ++k) {
      assertTrue(rc.contains(k));
    }
    assertEquals(12, rc.getSizeInBytes());
  }

  @Test
  public void iaddRange5() {
    Container rc = new RunContainer();
    for (char k = 0; k < 10; ++k) {
      rc.add(k);
    }
    for (char k = 20; k < 30; ++k) {
      rc.add(k);
    }
    for (char k = 40; k < 50; ++k) {
      rc.add(k);
    }
    rc.iadd(15, 21);
    assertEquals(35, rc.getCardinality());
    for (char k = 0; k < 10; ++k) {
      assertTrue(rc.contains(k));
    }
    for (char k = 15; k < 30; ++k) {
      assertTrue(rc.contains(k));
    }
    for (char k = 40; k < 50; ++k) {
      assertTrue(rc.contains(k));
    }
    assertEquals(16, rc.getSizeInBytes());
  }

  @Test
  public void iaddRange6() {
    Container rc = new RunContainer();
    for (char k = 5; k < 10; ++k) {
      rc.add(k);
    }
    for (char k = 20; k < 30; ++k) {
      rc.add(k);
    }
    for (char k = 40; k < 50; ++k) {
      rc.add(k);
    }
    rc.iadd(0, 21);
    assertEquals(40, rc.getCardinality());
    for (char k = 0; k < 30; ++k) {
      assertTrue(rc.contains(k));
    }
    for (char k = 40; k < 50; ++k) {
      assertTrue(rc.contains(k));
    }
    assertEquals(12, rc.getSizeInBytes());
  }

  @Test
  public void iaddRange7() {
    Container rc = new RunContainer();
    for (char k = 0; k < 10; ++k) {
      rc.add(k);
    }
    for (char k = 20; k < 30; ++k) {
      rc.add(k);
    }
    for (char k = 40; k < 50; ++k) {
      rc.add(k);
    }
    rc.iadd(15, 25);
    assertEquals(35, rc.getCardinality());
    for (char k = 0; k < 10; ++k) {
      assertTrue(rc.contains(k));
    }
    for (char k = 15; k < 30; ++k) {
      assertTrue(rc.contains(k));
    }
    for (char k = 40; k < 50; ++k) {
      assertTrue(rc.contains(k));
    }
    assertEquals(16, rc.getSizeInBytes());
  }


  @Test
  public void iaddRange8() {
    Container rc = new RunContainer();
    for (char k = 0; k < 10; ++k) {
      rc.add(k);
    }
    for (char k = 20; k < 30; ++k) {
      rc.add(k);
    }
    for (char k = 40; k < 50; ++k) {
      rc.add(k);
    }
    rc.iadd(15, 40);
    assertEquals(45, rc.getCardinality());
    for (char k = 0; k < 10; ++k) {
      assertTrue(rc.contains(k));
    }
    for (char k = 15; k < 50; ++k) {
      assertTrue(rc.contains(k));
    }
    assertEquals(12, rc.getSizeInBytes());
  }



  @Test
  public void iaddRangeAndFuseWithPreviousValueLength() {
    RunContainer container = new RunContainer();
    for (char i = 10; i < 20; ++i) {
      container.add(i);
    }
    container.iadd(20, 30);
    assertEquals(20, container.getCardinality());
    for (char i = 10; i < 30; ++i) {
      assertTrue(container.contains(i));
    }
    assertEquals(8, container.getSizeInBytes());
  }


  @Test
  public void iaddRangeOnNonEmptyContainerAndFuse() {
    RunContainer container = new RunContainer();
    for (char i = 1; i < 20; ++i) {
      container.add(i);
    }
    for (char i = 90; i < 120; ++i) {
      container.add(i);
    }
    container.iadd(10, 100);
    assertEquals(119, container.getCardinality());
    for (char i = 1; i < 120; ++i) {
      assertTrue(container.contains(i));
    }
  }


  @Test
  public void iaddRangeWithinSetBounds() {
    RunContainer container = new RunContainer();
    container.add((char) 10);
    container.add((char) 99);
    container.iadd(10, 100);
    assertEquals(90, container.getCardinality());
    for (char i = 10; i < 100; ++i) {
      assertTrue(container.contains(i));
    }
  }

  @Test
  public void inot1() {
    RunContainer container = new RunContainer();
    container.add((char) 0);
    container.add((char) 2);
    container.add((char) 55);
    container.add((char) 64);
    container.add((char) 256);

    Container result = container.inot(64, 64); // empty range
    assertSame(container, result);
    assertEquals(5, container.getCardinality());
  }

  @Test
  public void inot10() {
    RunContainer container = new RunContainer();
    container.add((char) 300);
    container.add((char) 500);
    container.add((char) 501);
    container.add((char) 502);
    container.add((char) 503);
    container.add((char) 504);
    container.add((char) 505);

    // second run begins inside the range but extends outside
    Container result = container.inot(498, 504);

    assertEquals(5, result.getCardinality());
    for (char i : new char[] {300, 498, 499, 504, 505}) {
      assertTrue(result.contains(i));
    }
  }

  @Test
  public void inot11() {
    RunContainer container = new RunContainer();
    container.add((char) 300);

    container.add((char) 500);
    container.add((char) 501);
    container.add((char) 502);

    container.add((char) 504);

    container.add((char) 510);

    // second run entirely inside range, third run entirely inside range, 4th run entirely outside
    Container result = container.inot(498, 507);

    assertEquals(7, result.getCardinality());
    for (char i : new char[] {300, 498, 499, 503, 505, 506, 510}) {
      assertTrue(result.contains(i));
    }
  }

  @Test
  public void inot12() {
    RunContainer container = new RunContainer();
    container.add((char) 300);

    container.add((char) 500);
    container.add((char) 501);
    container.add((char) 502);

    container.add((char) 504);

    container.add((char) 510);
    container.add((char) 511);

    // second run crosses into range, third run entirely inside range, 4th crosses outside
    Container result = container.inot(501, 511);

    assertEquals(9, result.getCardinality());
    for (char i : new char[] {300, 500, 503, 505, 506, 507, 508, 509, 511}) {
      assertTrue(result.contains(i));
    }
  }

  @Test
  public void inot12A() {
    RunContainer container = new RunContainer();
    container.add((char) 300);
    container.add((char) 301);

    // first run crosses into range
    Container result = container.inot(301, 303);

    assertEquals(2, result.getCardinality());
    for (char i : new char[] {300, 302}) {
      assertTrue(result.contains(i));
    }
  }



  @Test
  public void inot13() {
    RunContainer container = new RunContainer();
    // check for off-by-1 errors that might affect length 1 runs

    for (int i = 100; i < 120; i += 3) {
      container.add((char) i);
    }

    // second run crosses into range, third run entirely inside range, 4th crosses outside
    Container result = container.inot(110, 115);

    assertEquals(10, result.getCardinality());
    for (char i : new char[] {100, 103, 106, 109, 110, 111, 113, 114, 115, 118}) {
      assertTrue(result.contains(i));
    }
  }

  @Test
  public void inot14() {
    inot14once(10, 1);
    inot14once(10, 10);
    inot14once(1000, 100);
    for (int i = 1; i <= 100; ++i) {
      if (i % 10 == 0) {
        System.out.println("inot 14 attempt " + i);
      }
      inot14once(50000, 100);
    }
  }

  private void inot14once(int num, int rangeSize) {
    RunContainer container = new RunContainer();
    Random generator = new Random(1234);
    BitSet checker = new BitSet();
    for (int i = 0; i < num; ++i) {
      int val = (int) (generator.nextDouble() * 65536);
      checker.set(val);
      container.add((char) val);
    }

    int rangeStart = (int) generator.nextDouble() * (65536 - rangeSize);
    int rangeEnd = rangeStart + rangeSize;

    // this test is not checking runcontainer flip if "add" has converted
    // a runcontainer to an array or bitmap container. Flag this as requiring thought, if it happens
    assertTrue(container instanceof RunContainer);

    Container result = container.inot(rangeStart, rangeEnd);
    checker.flip(rangeStart, rangeEnd);

    // esnsure they agree on each possible bit
    for (int i = 0; i < 65536; ++i) {
      assertFalse(result.contains((char) i) ^ checker.get(i));
    }

  }

  @Test
  public void inot15() {
    RunContainer container = new RunContainer();
    for (int i = 0; i < 20000; ++i) {
      container.add((char) i);
    }

    for (int i = 40000; i < 60000; ++i) {
      container.add((char) i);
    }

    Container result = container.inot(15000, 25000);

    // this result should stay as a run container (same one)
    assertSame(container, result);
  }

  @Test
  public void inot2() {
    RunContainer container = new RunContainer();
    container.add((char) 0);
    container.add((char) 2);
    container.add((char) 55);
    container.add((char) 64);
    container.add((char) 256);

    Container result = container.inot(64, 66);
    assertEquals(5, result.getCardinality());
    for (char i : new char[] {0, 2, 55, 65, 256}) {
      assertTrue(result.contains(i));
    }
  }

  @Test
  public void inot3() {
    RunContainer container = new RunContainer();
    // applied to a run-less container
    Container result = container.inot(64, 68);
    assertEquals(4, result.getCardinality());
    for (char i : new char[] {64, 65, 66, 67}) {
      assertTrue(result.contains(i));
    }
  }



  @Test
  public void inot4() {
    RunContainer container = new RunContainer();
    container.add((char) 0);
    container.add((char) 2);
    container.add((char) 55);
    container.add((char) 64);
    container.add((char) 256);

    // all runs are before the range
    Container result = container.inot(300, 303);
    assertEquals(8, result.getCardinality());
    for (char i : new char[] {0, 2, 55, 64, 256, 300, 301, 302}) {
      assertTrue(result.contains(i));
    }
  }


  @Test
  public void inot5() {
    RunContainer container = new RunContainer();
    container.add((char) 500);
    container.add((char) 502);
    container.add((char) 555);
    container.add((char) 564);
    container.add((char) 756);

    // all runs are after the range
    Container result = container.inot(300, 303);
    assertEquals(8, result.getCardinality());
    for (char i : new char[] {500, 502, 555, 564, 756, 300, 301, 302}) {
      assertTrue(result.contains(i));
    }
  }

  @Test
  public void inot6() {
    RunContainer container = new RunContainer();
    container.add((char) 500);
    container.add((char) 501);
    container.add((char) 502);
    container.add((char) 503);

    // one run is strictly within the range
    Container result = container.inot(499, 505);
    assertEquals(2, result.getCardinality());
    for (char i : new char[] {499, 504}) {
      assertTrue(result.contains(i));
    }
  }


  @Test
  public void inot7() {
    RunContainer container = new RunContainer();
    container.add((char) 500);
    container.add((char) 501);
    container.add((char) 502);
    container.add((char) 503);
    container.add((char) 504);
    container.add((char) 505);


    // one run, spans the range
    Container result = container.inot(502, 504);

    assertEquals(4, result.getCardinality());
    for (char i : new char[] {500, 501, 504, 505}) {
      assertTrue(result.contains(i));
    }
  }

  @Test
  public void inot8() {
    RunContainer container = new RunContainer();
    container.add((char) 300);
    container.add((char) 500);
    container.add((char) 501);
    container.add((char) 502);
    container.add((char) 503);
    container.add((char) 504);
    container.add((char) 505);

    // second run, spans the range
    Container result = container.inot(502, 504);

    assertEquals(5, result.getCardinality());
    for (char i : new char[] {300, 500, 501, 504, 505}) {
      assertTrue(result.contains(i));
    }
  }

  @Test
  public void inot9() {
    RunContainer container = new RunContainer();
    container.add((char) 500);
    container.add((char) 501);
    container.add((char) 502);
    container.add((char) 503);
    container.add((char) 504);
    container.add((char) 505);

    // first run, begins inside the range but extends outside
    Container result = container.inot(498, 504);

    assertEquals(4, result.getCardinality());
    for (char i : new char[] {498, 499, 504, 505}) {
      assertTrue(result.contains(i));
    }
  }

  @Test
  public void intersectionTest1() {
    Container ac = new ArrayContainer();
    Container rc = new RunContainer();
    for (int k = 0; k < 100; ++k) {
      ac = ac.add((char) (k * 10));
      rc = rc.add((char) (k * 10));
    }
    assertEquals(ac, ac.and(rc));
    assertEquals(ac, rc.and(ac));
  }

  @Test
  public void intersectionTest2() {
    Container ac = new ArrayContainer();
    Container rc = new RunContainer();
    for (int k = 0; k < 10000; ++k) {
      ac = ac.add((char) k);
      rc = rc.add((char) k);
    }
    assertEquals(ac, ac.and(rc));
    assertEquals(ac, rc.and(ac));
  }

  @Test
  public void intersectionTest3() {
    Container ac = new ArrayContainer();
    Container rc = new RunContainer();
    for (int k = 0; k < 100; ++k) {
      ac = ac.add((char) k);
      rc = rc.add((char) (k + 100));
    }
    assertEquals(0, rc.and(ac).getCardinality());
  }

  @Test
  public void intersectionTest4() {
    Container bc = new BitmapContainer();
    Container rc = new RunContainer();
    for (int k = 0; k < 100; ++k) {
      bc = bc.add((char) (k * 10));
      bc = bc.add((char) (k * 10 + 3));

      rc = rc.add((char) (k * 10 + 5));
      rc = rc.add((char) (k * 10 + 3));
    }
    Container intersection = rc.and(bc);
    assertEquals(100, intersection.getCardinality());
    for (int k = 0; k < 100; ++k) {
      assertTrue(intersection.contains((char) (k * 10 + 3)));
    }
    assertEquals(200, bc.getCardinality());
    assertEquals(200, rc.getCardinality());
  }

  @Test
  public void ior() {
    Container rc1 = new RunContainer();
    Container rc2 = new RunContainer();
    rc1.iadd(0, 128);
    rc2.iadd(128, 256);
    rc1.ior(rc2);
    assertEquals(256, rc1.getCardinality());
  }

  @Test
  public void ior2() {
    Container rc = new RunContainer();
    Container ac = new ArrayContainer();
    rc.iadd(0, 128);
    rc.iadd(256, 512);
    ac.iadd(128, 256);
    rc.ior(ac);
    assertEquals(512, rc.getCardinality());
  }
  @Test
  public void iorNot() {
    Container rc1 = new RunContainer();
    Container rc2 = new ArrayContainer();

    rc1.iadd(257, 258);
    rc2.iadd(128, 256);
    rc1 = rc1.iorNot(rc2, 258);
    assertEquals(130, rc1.getCardinality());

    PeekableCharIterator iterator = rc1.getCharIterator();
    for (int i = 0; i < 128; i++) {
      assertTrue(iterator.hasNext());
      assertEquals(i, iterator.next());
    }
    assertTrue(iterator.hasNext());
    assertEquals(256, iterator.next());

    assertTrue(iterator.hasNext());
    assertEquals(257, iterator.next());

    assertFalse(iterator.hasNext());
  }

  @Test
  public void iorNot2() {
    Container rc1 = new RunContainer();
    Container rc2 = new ArrayContainer();
    rc2.iadd(128, 256).iadd(257, 260);
    rc1 = rc1.iorNot(rc2, 261);
    assertEquals(130, rc1.getCardinality());

    PeekableCharIterator iterator = rc1.getCharIterator();
    for (int i = 0; i < 128; i++) {
      assertTrue(iterator.hasNext());
      assertEquals(i, iterator.next());
    }
    assertTrue(iterator.hasNext());
    assertEquals(256, iterator.next());

    assertTrue(iterator.hasNext());
    assertEquals(260, iterator.next());

    assertFalse(iterator.hasNext());
  }

  @Test
  public void iorNot3() {
    Container rc1 = new RunContainer();
    Container rc2 = new BitmapContainer();

    rc1.iadd(257, 258);
    rc2.iadd(128, 256);
    rc1 = rc1.iorNot(rc2, 258);
    assertEquals(130, rc1.getCardinality());

    PeekableCharIterator iterator = rc1.getCharIterator();
    for (int i = 0; i < 128; i++) {
      assertTrue(iterator.hasNext());
      assertEquals(i, iterator.next());
    }
    assertTrue(iterator.hasNext());
    assertEquals(256, iterator.next());

    assertTrue(iterator.hasNext());
    assertEquals(257, iterator.next());

    assertFalse(iterator.hasNext());
  }

  @Test
  public void iorNot4() {
    Container rc1 = new RunContainer();
    Container rc2 = new RunContainer();

    rc1.iadd(257, 258);
    rc2.iadd(128, 256);
    rc1 = rc1.iorNot(rc2, 258);
    assertEquals(130, rc1.getCardinality());

    PeekableCharIterator iterator = rc1.getCharIterator();
    for (int i = 0; i < 128; i++) {
      assertTrue(iterator.hasNext());
      assertEquals(i, iterator.next());
    }
    assertTrue(iterator.hasNext());
    assertEquals(256, iterator.next());

    assertTrue(iterator.hasNext());
    assertEquals(257, iterator.next());

    assertFalse(iterator.hasNext());
  }

  @Test
  public void orNot() {
    final Container rc1 = new RunContainer();

    {
      Container rc2 = new ArrayContainer();
      rc2.iadd(128, 256);
      Container res = rc1.orNot(rc2, 257);
      assertEquals(129, res.getCardinality());

      PeekableCharIterator iterator = res.getCharIterator();
      for (int i = 0; i < 128; i++) {
        assertTrue(iterator.hasNext());
        assertEquals(i, iterator.next());
      }
      assertTrue(iterator.hasNext());
      assertEquals(256, iterator.next());

      assertFalse(iterator.hasNext());
    }

    {
      Container rc2 = new BitmapContainer();
      rc2.iadd(128, 256);
      Container res = rc1.orNot(rc2, 257);
      assertEquals(129, res.getCardinality());

      PeekableCharIterator iterator = res.getCharIterator();
      for (int i = 0; i < 128; i++) {
        assertTrue(iterator.hasNext());
        assertEquals(i, iterator.next());
      }
      assertTrue(iterator.hasNext());
      assertEquals(256, iterator.next());

      assertFalse(iterator.hasNext());
    }

    {
      Container rc2 = new RunContainer();
      rc2.iadd(128, 256);
      Container res = rc1.orNot(rc2, 257);
      assertEquals(129, res.getCardinality());

      PeekableCharIterator iterator = res.getCharIterator();
      for (int i = 0; i < 128; i++) {
        assertTrue(iterator.hasNext());
        assertEquals(i, iterator.next());
      }
      assertTrue(iterator.hasNext());
      assertEquals(256, iterator.next());

      assertFalse(iterator.hasNext());
    }

  }

  @Test
  public void orNot2() {
    Container rc1 = new RunContainer();
    Container rc2 = new ArrayContainer();
    rc2.iadd(128, 256).iadd(257, 260);
    rc1 = rc1.orNot(rc2, 261);
    assertEquals(130, rc1.getCardinality());

    PeekableCharIterator iterator = rc1.getCharIterator();
    for (int i = 0; i < 128; i++) {
      assertTrue(iterator.hasNext());
      assertEquals(i, iterator.next());
    }
    assertTrue(iterator.hasNext());
    assertEquals(256, iterator.next());

    assertTrue(iterator.hasNext());
    assertEquals(260, iterator.next());

    assertFalse(iterator.hasNext());
  }

  @Test
  public void iremove1() {
    Container rc = new RunContainer();
    rc.add((char) 1);
    rc.iremove(1, 2);
    assertEquals(0, rc.getCardinality());
  }

  @Test
  public void iremove10() {
    Container rc = new RunContainer();
    rc.iadd(5, 10);
    rc.iadd(20, 30);
    rc.iremove(0, 25);
    assertEquals(5, rc.getCardinality());
    for (char k = 25; k < 30; ++k) {
      assertTrue(rc.contains(k));
    }
    assertEquals(8, rc.getSizeInBytes());
  }

  @Test
  public void iremove11() {
    Container rc = new RunContainer();
    rc.iadd(5, 10);
    rc.iadd(20, 30);
    rc.iremove(0, 35);
    assertEquals(0, rc.getCardinality());
  }

  @Test
  public void iremove12() {
    Container rc = new RunContainer();
    rc.add((char) 0);
    rc.add((char) 10);
    rc.iremove(0, 11);
    assertEquals(0, rc.getCardinality());
  }

  @Test
  public void iremove13() {
    Container rc = new RunContainer();
    rc.iadd(0, 10);
    rc.iadd(20, 30);
    rc.iremove(5, 25);
    assertEquals(10, rc.getCardinality());
    for (char k = 0; k < 5; ++k) {
      assertTrue(rc.contains(k));
    }
    for (char k = 25; k < 30; ++k) {
      assertTrue(rc.contains(k));
    }
    assertEquals(12, rc.getSizeInBytes());
  }

  @Test
  public void iremove14() {
    Container rc = new RunContainer();
    rc.iadd(0, 10);
    rc.iadd(20, 30);
    rc.iremove(5, 31);
    assertEquals(5, rc.getCardinality());
    for (char k = 0; k < 5; ++k) {
      assertTrue(rc.contains(k));
    }
    assertEquals(8, rc.getSizeInBytes());
  }

  @Test
  public void iremove15() {
    Container rc = new RunContainer();
    rc.iadd(0, 5);
    rc.iadd(20, 30);
    rc.iremove(5, 25);
    assertEquals(10, rc.getCardinality());
    for (char k = 0; k < 5; ++k) {
      assertTrue(rc.contains(k));
    }
    for (char k = 25; k < 30; ++k) {
      assertTrue(rc.contains(k));
    }
    assertEquals(12, rc.getSizeInBytes());
  }

  @Test
  public void iremove16() {
    Container rc = new RunContainer();
    rc.iadd(0, 5);
    rc.iadd(20, 30);
    rc.iremove(5, 31);
    assertEquals(5, rc.getCardinality());
    for (char k = 0; k < 5; ++k) {
      assertTrue(rc.contains(k));
    }
    assertEquals(8, rc.getSizeInBytes());
  }

  @Test
  public void iremove17() {
    Container rc = new RunContainer();
    rc.iadd(37543, 65536);
    rc.iremove(9795, 65536);
    assertEquals(0, rc.getCardinality());
  }

  @Test
  public void iremove18() {
    Container rc = new RunContainer();
    rc.iadd(100, 200);
    rc.iadd(300, 400);
    rc.iadd(37543, 65000);
    rc.iremove(300, 65000); // start at beginning of run, end at end of another run
    assertEquals(100, rc.getCardinality());
  }

  @Test
  public void iremove19() {
    Container rc = new RunContainer();
    rc.iadd(100, 200);
    rc.iadd(300, 400);
    rc.iadd(64000, 65000);
    rc.iremove(350, 64000); // start midway through run, end at the start of next
    // got 100..199, 300..349, 64000..64999
    assertEquals(1150, rc.getCardinality());
  }

  @Test
  public void iremove2() {
    Container rc = new RunContainer();
    rc.iadd(0, 10);
    rc.iadd(20, 30);
    rc.iremove(0, 21);
    assertEquals(9, rc.getCardinality());
    for (char k = 21; k < 30; ++k) {
      assertTrue(rc.contains(k));
    }
    assertEquals(8, rc.getSizeInBytes());
  }

  @Test
  public void iremove20() {
    Container rc = new RunContainer();
    rc.iadd(100, 200);
    rc.iadd(300, 400);
    rc.iadd(64000, 65000);
    rc.iremove(350, 64001); // start midway through run, end at the start of next
    // got 100..199, 300..349, 64001..64999
    assertEquals(1149, rc.getCardinality());
  }

  @Test
  public void iremove3() {
    Container rc = new RunContainer();
    rc.iadd(0, 10);
    rc.iadd(20, 30);
    rc.iadd(40, 50);
    rc.iremove(0, 21);
    assertEquals(19, rc.getCardinality());
    for (char k = 21; k < 30; ++k) {
      assertTrue(rc.contains(k));
    }
    for (char k = 40; k < 50; ++k) {
      assertTrue(rc.contains(k));
    }
    assertEquals(12, rc.getSizeInBytes());
  }

  @Test
  public void iremove4() {
    Container rc = new RunContainer();
    rc.iadd(0, 10);
    rc.iremove(0, 5);
    assertEquals(5, rc.getCardinality());
    for (char k = 5; k < 10; ++k) {
      assertTrue(rc.contains(k));
    }
    assertEquals(8, rc.getSizeInBytes());
  }

  @Test
  public void iremove5() {
    Container rc = new RunContainer();
    rc.iadd(0, 10);
    rc.iadd(20, 30);
    rc.iremove(0, 31);
    assertEquals(0, rc.getCardinality());
  }

  @Test
  public void iremove6() {
    Container rc = new RunContainer();
    rc.iadd(0, 10);
    rc.iadd(20, 30);
    rc.iremove(0, 25);
    assertEquals(5, rc.getCardinality());
    for (char k = 25; k < 30; ++k) {
      assertTrue(rc.contains(k));
    }
    assertEquals(8, rc.getSizeInBytes());
  }

  @Test
  public void iremove7() {
    Container rc = new RunContainer();
    rc.iadd(0, 10);
    rc.iremove(0, 15);
    assertEquals(0, rc.getCardinality());
  }

  @Test
  public void iremove8() {
    Container rc = new RunContainer();
    rc.iadd(0, 10);
    rc.iadd(20, 30);
    rc.iremove(5, 21);
    assertEquals(14, rc.getCardinality());
    for (char k = 0; k < 5; ++k) {
      assertTrue(rc.contains(k));
    }
    for (char k = 21; k < 30; ++k) {
      assertTrue(rc.contains(k));
    }
    assertEquals(12, rc.getSizeInBytes());
  }

  @Test
  public void iremove9() {
    Container rc = new RunContainer();
    rc.iadd(0, 10);
    rc.iadd(20, 30);
    rc.iremove(15, 21);
    assertEquals(19, rc.getCardinality());
    for (char k = 0; k < 10; ++k) {
      assertTrue(rc.contains(k));
    }
    for (char k = 21; k < 30; ++k) {
      assertTrue(rc.contains(k));
    }
    assertEquals(12, rc.getSizeInBytes());
  }

  @Test
  public void iremoveInvalidRange1() {
    assertThrows(IllegalArgumentException.class, () -> {
      Container rc = new RunContainer();
      rc.iremove(10, 9);
    });
  }

  @Test
  public void iremoveInvalidRange2() {
    assertThrows(IllegalArgumentException.class, () -> {
      Container rc = new RunContainer();
      rc.remove(0, 1 << 20);
    });
  }

  @Test
  public void iremoveRange() {
    for (int i = 0; i < 100; ++i) {
      for (int j = 0; j < 100; ++j) {
        for (int k = 0; k < 50; ++k) {
          BitSet bs = new BitSet();
          RunContainer container = new RunContainer();
          for (int p = 0; p < i; ++p) {
            container.add((char) p);
            bs.set(p);
          }
          for (int p = 0; p < j; ++p) {
            container.add((char) (99 - p));
            bs.set(99 - p);
          }
          container.iremove(49 - k, 50 + k);
          bs.clear(49 - k, 50 + k);
          assertEquals(bs.cardinality(), container.getCardinality());

          int nb_runs = bs.isEmpty() ? 0 : 1;
          int lastIndex = bs.nextSetBit(0);
          for (int p = bs.nextSetBit(0); p >= 0; p = bs.nextSetBit(p + 1)) {
            if (p - lastIndex > 1) {
              nb_runs++;
            }
            lastIndex = p;
            assertTrue(container.contains((char) p));
          }
          assertEquals(nb_runs * 4 + 4, container.getSizeInBytes());
        }
      }
    }
  }

  @Test
  public void iremoveEmptyRange() {
    RunContainer container = new RunContainer();
    assertEquals(0, container.getCardinality());
    container.iremove(0,0);
    assertEquals(0, container.getCardinality());
  }

  @Test
  public void iterator() {
    RunContainer x = new RunContainer();
    for (int k = 0; k < 100; ++k) {
      for (int j = 0; j < k; ++j) {
        x = (RunContainer) x.add((char) (k * 100 + j));
      }
    }
    CharIterator i = x.getCharIterator();
    for (int k = 0; k < 100; ++k) {
      for (int j = 0; j < k; ++j) {
        assertTrue(i.hasNext());
        assertEquals(i.next(), (char) (k * 100 + j));
      }
    }
    assertFalse(i.hasNext());
  }

  @Test
  public void limit() {
    RunContainer container = new RunContainer();
    container.add((char) 0);
    container.add((char) 2);
    container.add((char) 55);
    container.add((char) 64);
    container.add((char) 256);
    Container limit = container.limit(1024);
    assertNotSame(container, limit);
    assertEquals(container, limit);
    limit = container.limit(3);
    assertNotSame(container, limit);
    assertEquals(3, limit.getCardinality());
    assertTrue(limit.contains((char) 0));
    assertTrue(limit.contains((char) 2));
    assertTrue(limit.contains((char) 55));
  }

  @Test
  public void longbacksimpleIterator() {
    RunContainer x = new RunContainer();
    for (int k = 0; k < (1 << 16); ++k) {
      x = (RunContainer) x.add((char) k);
    }
    CharIterator i = x.getReverseCharIterator();
    for (int k = (1 << 16) - 1; k >= 0; --k) {
      assertTrue(i.hasNext());
      assertEquals(i.next(), (char) k);
    }
    assertFalse(i.hasNext());
  }

  @Test
  public void longcsimpleIterator() {
    RunContainer x = new RunContainer();
    for (int k = 0; k < (1 << 16); ++k) {
      x = (RunContainer) x.add((char) k);
    }
    Iterator<Character> i = x.iterator();
    for (int k = 0; k < (1 << 16); ++k) {
      assertTrue(i.hasNext());
      assertEquals(i.next().charValue(), (char) k);
    }
    assertFalse(i.hasNext());
  }

  @Test
  public void longsimpleIterator() {
    RunContainer x = new RunContainer();
    for (int k = 0; k < (1 << 16); ++k) {
      x = (RunContainer) x.add((char) (k));
    }
    CharIterator i = x.getCharIterator();
    for (int k = 0; k < (1 << 16); ++k) {
      assertTrue(i.hasNext());
      assertEquals(i.next(), (char) k);
    }
    assertFalse(i.hasNext());
  }

  @Test
  public void not1() {
    RunContainer container = new RunContainer();
    container.add((char) 0);
    container.add((char) 2);
    container.add((char) 55);
    container.add((char) 64);
    container.add((char) 256);

    Container result = container.not(64, 64); // empty range
    assertNotSame(container, result);
    assertEquals(container, result);
  }

  @Test
  public void not10() {
    RunContainer container = new RunContainer();
    container.add((char) 300);
    container.add((char) 500);
    container.add((char) 501);
    container.add((char) 502);
    container.add((char) 503);
    container.add((char) 504);
    container.add((char) 505);

    // second run begins inside the range but extends outside
    Container result = container.not(498, 504);

    assertEquals(5, result.getCardinality());
    for (char i : new char[] {300, 498, 499, 504, 505}) {
      assertTrue(result.contains(i));
    }
  }

  @Test
  public void not11() {
    RunContainer container = new RunContainer();
    container.add((char) 300);

    container.add((char) 500);
    container.add((char) 501);
    container.add((char) 502);

    container.add((char) 504);

    container.add((char) 510);

    // second run entirely inside range, third run entirely inside range, 4th run entirely outside
    Container result = container.not(498, 507);

    assertEquals(7, result.getCardinality());
    for (char i : new char[] {300, 498, 499, 503, 505, 506, 510}) {
      assertTrue(result.contains(i));
    }
  }


  @Test
  public void not12() {
    RunContainer container = new RunContainer();
    container.add((char) 300);

    container.add((char) 500);
    container.add((char) 501);
    container.add((char) 502);

    container.add((char) 504);

    container.add((char) 510);
    container.add((char) 511);

    // second run crosses into range, third run entirely inside range, 4th crosses outside
    Container result = container.not(501, 511);

    assertEquals(9, result.getCardinality());
    for (char i : new char[] {300, 500, 503, 505, 506, 507, 508, 509, 511}) {
      assertTrue(result.contains(i));
    }
  }


  @Test
  public void not12A() {
    RunContainer container = new RunContainer();
    container.add((char) 300);
    container.add((char) 301);

    // first run crosses into range
    Container result = container.not(301, 303);

    assertEquals(2, result.getCardinality());
    for (char i : new char[] {300, 302}) {
      assertTrue(result.contains(i));
    }
  }

  @Test
  public void not13() {
    RunContainer container = new RunContainer();
    // check for off-by-1 errors that might affect length 1 runs

    for (int i = 100; i < 120; i += 3) {
      container.add((char) i);
    }

    // second run crosses into range, third run entirely inside range, 4th crosses outside
    Container result = container.not(110, 115);

    assertEquals(10, result.getCardinality());
    for (char i : new char[] {100, 103, 106, 109, 110, 111, 113, 114, 115, 118}) {
      assertTrue(result.contains(i));
    }
  }


  @Test
  public void not14() {
    not14once(10, 1);
    not14once(10, 10);
    not14once(1000, 100);

    for (int i = 1; i <= 100; ++i) {
      if (i % 10 == 0) {
        System.out.println("not 14 attempt " + i);
      }
      not14once(50000, 100);
    }
  }


  private void not14once(int num, int rangeSize) {
    RunContainer container = new RunContainer();
    BitSet checker = new BitSet();
    Random generator = new Random(1234);
    for (int i = 0; i < num; ++i) {
      int val = (int) (generator.nextDouble() * 65536);
      checker.set(val);
      container.add((char) val);
    }

    int rangeStart = (int) generator.nextDouble() * (65536 - rangeSize);
    int rangeEnd = rangeStart + rangeSize;

    assertTrue(container instanceof RunContainer);

    Container result = container.not(rangeStart, rangeEnd);
    checker.flip(rangeStart, rangeEnd);

    // esnsure they agree on each possible bit
    for (int i = 0; i < 65536; ++i) {
      assertFalse(result.contains((char) i) ^ checker.get(i));
    }

  }


  @Test
  public void not15() {
    RunContainer container = new RunContainer();
    for (int i = 0; i < 20000; ++i) {
      container.add((char) i);
    }

    for (int i = 40000; i < 60000; ++i) {
      container.add((char) i);
    }

    Container result = container.not(15000, 25000);

    // this result should stay as a run container.
    assertTrue(result instanceof RunContainer);
  }


  @Test
  public void not2() {
    RunContainer container = new RunContainer();
    container.add((char) 0);
    container.add((char) 2);
    container.add((char) 55);
    container.add((char) 64);
    container.add((char) 256);

    Container result = container.not(64, 66);
    assertEquals(5, result.getCardinality());
    for (char i : new char[] {0, 2, 55, 65, 256}) {
      assertTrue(result.contains(i));
    }
  }


  @Test
  public void not3() {
    RunContainer container = new RunContainer();
    // applied to a run-less container
    Container result = container.not(64, 68);
    assertEquals(4, result.getCardinality());
    for (char i : new char[] {64, 65, 66, 67}) {
      assertTrue(result.contains(i));
    }
  }

  @Test
  public void not4() {
    RunContainer container = new RunContainer();
    container.add((char) 0);
    container.add((char) 2);
    container.add((char) 55);
    container.add((char) 64);
    container.add((char) 256);

    // all runs are before the range
    Container result = container.not(300, 303);
    assertEquals(8, result.getCardinality());
    for (char i : new char[] {0, 2, 55, 64, 256, 300, 301, 302}) {
      assertTrue(result.contains(i));
    }
  }


  @Test
  public void not5() {
    RunContainer container = new RunContainer();
    container.add((char) 500);
    container.add((char) 502);
    container.add((char) 555);
    container.add((char) 564);
    container.add((char) 756);

    // all runs are after the range
    Container result = container.not(300, 303);
    assertEquals(8, result.getCardinality());
    for (char i : new char[] {500, 502, 555, 564, 756, 300, 301, 302}) {
      assertTrue(result.contains(i));
    }
  }



  @Test
  public void not6() {
    RunContainer container = new RunContainer();
    container.add((char) 500);
    container.add((char) 501);
    container.add((char) 502);
    container.add((char) 503);

    // one run is strictly within the range
    Container result = container.not(499, 505);
    assertEquals(2, result.getCardinality());
    for (char i : new char[] {499, 504}) {
      assertTrue(result.contains(i));
    }
  }

  @Test
  public void not7() {
    RunContainer container = new RunContainer();
    container.add((char) 500);
    container.add((char) 501);
    container.add((char) 502);
    container.add((char) 503);
    container.add((char) 504);
    container.add((char) 505);


    // one run, spans the range
    Container result = container.not(502, 504);

    assertEquals(4, result.getCardinality());
    for (char i : new char[] {500, 501, 504, 505}) {
      assertTrue(result.contains(i));
    }
  }



  @Test
  public void not8() {
    RunContainer container = new RunContainer();
    container.add((char) 300);
    container.add((char) 500);
    container.add((char) 501);
    container.add((char) 502);
    container.add((char) 503);
    container.add((char) 504);
    container.add((char) 505);

    // second run, spans the range
    Container result = container.not(502, 504);

    assertEquals(5, result.getCardinality());
    for (char i : new char[] {300, 500, 501, 504, 505}) {
      assertTrue(result.contains(i));
    }
  }



  @Test
  public void not9() {
    RunContainer container = new RunContainer();
    container.add((char) 500);
    container.add((char) 501);
    container.add((char) 502);
    container.add((char) 503);
    container.add((char) 504);
    container.add((char) 505);

    // first run, begins inside the range but extends outside
    Container result = container.not(498, 504);

    assertEquals(4, result.getCardinality());
    for (char i : new char[] {498, 499, 504, 505}) {
      assertTrue(result.contains(i));
    }
  }



  @Test
  public void randomFun() {
    final int bitsetperword1 = 32;
    final int bitsetperword2 = 63;

    Container rc1, rc2, ac1, ac2;
    Random rand = new Random(0);
    final int max = 1 << 16;
    final int howmanywords = (1 << 16) / 64;
    int[] values1 = generateUniformHash(rand, bitsetperword1 * howmanywords, max);
    int[] values2 = generateUniformHash(rand, bitsetperword2 * howmanywords, max);


    rc1 = new RunContainer();
    rc1 = fillMeUp(rc1, values1);

    rc2 = new RunContainer();
    rc2 = fillMeUp(rc2, values2);

    ac1 = new ArrayContainer();
    ac1 = fillMeUp(ac1, values1);

    ac2 = new ArrayContainer();
    ac2 = fillMeUp(ac2, values2);

    if (!rc1.equals(ac1)) {
      throw new RuntimeException("first containers do not match");
    }

    if (!rc2.equals(ac2)) {
      throw new RuntimeException("second containers do not match");
    }

    if (!rc1.or(rc2).equals(ac1.or(ac2))) {
      throw new RuntimeException("ors do not match");
    }
    if (!rc1.and(rc2).equals(ac1.and(ac2))) {
      throw new RuntimeException("ands do not match");
    }
    if (!rc1.andNot(rc2).equals(ac1.andNot(ac2))) {
      throw new RuntimeException("andnots do not match");
    }
    if (!rc2.andNot(rc1).equals(ac2.andNot(ac1))) {
      throw new RuntimeException("andnots do not match");
    }
    if (!rc1.xor(rc2).equals(ac1.xor(ac2))) {
      throw new RuntimeException("xors do not match");
    }

  }

  @Test
  public void rank() {
    RunContainer container = new RunContainer();
    container.add((char) 0);
    container.add((char) 2);
    container.add((char) 55);
    container.add((char) 64);
    container.add((char) 256);
    assertEquals(1, container.rank((char) 0));
    assertEquals(2, container.rank((char) 10));
    assertEquals(4, container.rank((char) 128));
    assertEquals(5, container.rank((char) 1024));
  }

  @Test
  public void charRangeRank() {
    Container container = new RunContainer();
    container = container.add(16, 32);
    assertTrue(container instanceof RunContainer);
    // results in correct value: 16
    // assertEquals(16, container.toBitmapContainer().rank((char) 32));
    assertEquals(16, container.rank((char) 32));
  }


  @Test
  public void remove() {
    Container rc = new RunContainer();
    rc.add((char) 1);
    Container newContainer = rc.remove(1, 2);
    assertEquals(0, newContainer.getCardinality());
  }



  @Test
  public void RunContainerArg_ArrayAND() {
    boolean atLeastOneArray = false;
    ArrayList<RunContainer> set = new ArrayList<RunContainer>();
    ArrayList<Container> setb = new ArrayList<Container>();
    getSetOfRunContainers(set, setb);
    for (int k = 0; k < set.size(); ++k) {
      for (int l = 0; l < set.size(); ++l) {
        assertEquals(set.get(k), setb.get(k));
        assertEquals(set.get(l), setb.get(l));
        Container thisContainer = setb.get(k);
        if (thisContainer instanceof BitmapContainer) {
          ; // continue;
        } else {
          atLeastOneArray = true;
        }
        Container c1 = thisContainer.and(set.get(l));
        Container c2 = setb.get(k).and(setb.get(l));
        assertEquals(c1, c2);
      }
    }
    assertTrue(atLeastOneArray);
  }



  @Test
  public void RunContainerArg_ArrayANDNOT() {
    boolean atLeastOneArray = false;
    ArrayList<RunContainer> set = new ArrayList<RunContainer>();
    ArrayList<Container> setb = new ArrayList<Container>();
    getSetOfRunContainers(set, setb);
    for (int k = 0; k < set.size(); ++k) {
      for (int l = 0; l < set.size(); ++l) {
        assertEquals(set.get(k), setb.get(k));
        assertEquals(set.get(l), setb.get(l));
        Container thisContainer = setb.get(k);
        if (thisContainer instanceof BitmapContainer) {
          // continue;
        } else {
          atLeastOneArray = true;
        }

        Container c1 = thisContainer.andNot(set.get(l));
        Container c2 = setb.get(k).andNot(setb.get(l));

        assertEquals(c1, c2);
      }
    }
    assertTrue(atLeastOneArray);
  }

  @Test
  public void RunContainerArg_ArrayANDNOT2() {
    ArrayContainer ac = new ArrayContainer(12, new char[]{0, 2, 4, 8, 10, 15, 16, 48, 50, 61, 80, (char)-2});
    RunContainer rc = new RunContainer(new char[]{7, 3, 17, 2, 20, 3, 30, 3, 36, 6, 60, 5, (char)-3, 2}, 7);
    assertEquals(new ArrayContainer(8, new char[]{0, 2, 4, 15, 16, 48, 50, 80}), ac.andNot(rc));
  }

  @Test
  public void FullRunContainerArg_ArrayANDNOT2() {
    ArrayContainer ac = new ArrayContainer(1, new char[]{3});
    Container rc = RunContainer.full();
    assertEquals(new ArrayContainer(), ac.andNot(rc));
  }

  @Test
  public void RunContainerArg_ArrayANDNOT3() {
    ArrayContainer ac = new ArrayContainer(1, new char[]{5});
    Container rc = new RunContainer(new char[]{3, 10}, 1);
    assertEquals(new ArrayContainer(), ac.andNot(rc));
  }

  @Test
  public void RunContainerArg_ArrayOR() {
    boolean atLeastOneArray = false;
    ArrayList<RunContainer> set = new ArrayList<RunContainer>();
    ArrayList<Container> setb = new ArrayList<Container>();
    getSetOfRunContainers(set, setb);
    for (int k = 0; k < set.size(); ++k) {
      for (int l = 0; l < set.size(); ++l) {
        assertEquals(set.get(k), setb.get(k));
        assertEquals(set.get(l), setb.get(l));
        Container thisContainer = setb.get(k);
        // BitmapContainers are tested separately, but why not test some more?
        if (thisContainer instanceof BitmapContainer) {
          ; // continue;
        } else {
          atLeastOneArray = true;
        }

        Container c1 = thisContainer.or(set.get(l));
        Container c2 = setb.get(k).or(setb.get(l));
        assertEquals(c1, c2);
      }
    }
    assertTrue(atLeastOneArray);
  }

  @Test
  public void RunContainerArg_ArrayXOR() {
    boolean atLeastOneArray = false;
    ArrayList<RunContainer> set = new ArrayList<RunContainer>();
    ArrayList<Container> setb = new ArrayList<Container>();
    getSetOfRunContainers(set, setb);
    for (int k = 0; k < set.size(); ++k) {
      for (int l = 0; l < set.size(); ++l) {
        assertEquals(set.get(k), setb.get(k));
        assertEquals(set.get(l), setb.get(l));
        Container thisContainer = setb.get(k);
        if (thisContainer instanceof BitmapContainer) {
          ; // continue;
        } else {
          atLeastOneArray = true;
        }

        Container c1 = thisContainer.xor(set.get(l));
        Container c2 = setb.get(k).xor(setb.get(l));
        assertEquals(c1, c2);
      }
    }
    assertTrue(atLeastOneArray);
  }


  @Test
  public void RunContainerFromBitmap() {
    Container rc = new RunContainer();
    Container bc = new BitmapContainer();

    rc = rc.add((char) 2);
    bc = bc.add((char) 2);
    rc = rc.add((char) 3);
    bc = bc.add((char) 3);
    rc = rc.add((char) 4);
    bc = bc.add((char) 4);
    rc = rc.add((char) 17);
    bc = bc.add((char) 17);
    for (int i = 192; i < 500; ++i) {
      rc = rc.add((char) i);
      bc = bc.add((char) i);
    }
    rc = rc.add((char) 1700);
    bc = bc.add((char) 1700);
    rc = rc.add((char) 1701);
    bc = bc.add((char) 1701);

    // cases depending on whether we have largest item.
    // this test: no, we don't get near largest word

    RunContainer rc2 = new RunContainer((BitmapContainer) bc, ((RunContainer) rc).nbrruns);
    assertEquals(rc, rc2);
  }


  @Test
  public void RunContainerFromBitmap1() {
    Container rc = new RunContainer();
    Container bc = new BitmapContainer();


    rc = rc.add((char) 2);
    bc = bc.add((char) 2);
    rc = rc.add((char) 3);
    bc = bc.add((char) 3);
    rc = rc.add((char) 4);
    bc = bc.add((char) 4);
    rc = rc.add((char) 17);
    bc = bc.add((char) 17);
    for (int i = 192; i < 500; ++i) {
      rc = rc.add((char) i);
      bc = bc.add((char) i);
    }
    rc = rc.add((char) 1700);
    bc = bc.add((char) 1700);
    rc = rc.add((char) 1701);
    bc = bc.add((char) 1701);

    // cases depending on whether we have largest item.
    // this test: we have a 1 in the largest word but not at end
    rc = rc.add((char) 65530);
    bc = bc.add((char) 65530);

    RunContainer rc2 = new RunContainer((BitmapContainer) bc, ((RunContainer) rc).nbrruns);
    assertEquals(rc, rc2);
  }


  @Test
  public void RunContainerFromBitmap2() {
    Container rc = new RunContainer();
    Container bc = new BitmapContainer();

    rc = rc.add((char) 2);
    bc = bc.add((char) 2);
    rc = rc.add((char) 3);
    bc = bc.add((char) 3);
    rc = rc.add((char) 4);
    bc = bc.add((char) 4);
    rc = rc.add((char) 17);
    bc = bc.add((char) 17);
    for (int i = 192; i < 500; ++i) {
      rc = rc.add((char) i);
      bc = bc.add((char) i);
    }
    rc = rc.add((char) 1700);
    bc = bc.add((char) 1700);
    rc = rc.add((char) 1701);
    bc = bc.add((char) 1701);

    // cases depending on whether we have largest item.
    // this test: we have a 1 in the largest word and at end
    rc = rc.add((char) 65530);
    bc = bc.add((char) 65530);
    rc = rc.add((char) 65535);
    bc = bc.add((char) 65535);


    RunContainer rc2 = new RunContainer((BitmapContainer) bc, ((RunContainer) rc).nbrruns);
    assertEquals(rc, rc2);
  }


  @Test
  public void RunContainerFromBitmap3() {
    Container rc = new RunContainer();
    Container bc = new BitmapContainer();

    rc = rc.add((char) 2);
    bc = bc.add((char) 2);
    rc = rc.add((char) 3);
    bc = bc.add((char) 3);
    rc = rc.add((char) 4);
    bc = bc.add((char) 4);
    rc = rc.add((char) 17);
    bc = bc.add((char) 17);
    for (int i = 192; i < 500; ++i) {
      rc = rc.add((char) i);
      bc = bc.add((char) i);
    }
    rc = rc.add((char) 1700);
    bc = bc.add((char) 1700);
    rc = rc.add((char) 1701);
    bc = bc.add((char) 1701);
    // cases depending on whether we have largest item.
    // this test: we have a lot of 1s in a run at the end

    for (int i = 65000; i < 65535; ++i) {
      rc = rc.add((char) i);
      bc = bc.add((char) i);
    }

    RunContainer rc2 = new RunContainer((BitmapContainer) bc, ((RunContainer) rc).nbrruns);
    assertEquals(rc, rc2);
  }

  @Test
  public void RunContainerVSRunContainerAND() {
    ArrayList<RunContainer> set = new ArrayList<RunContainer>();
    ArrayList<Container> setb = new ArrayList<Container>();
    getSetOfRunContainers(set, setb);
    for (int k = 0; k < set.size(); ++k) {
      for (int l = 0; l < set.size(); ++l) {
        assertEquals(set.get(k), setb.get(k));
        assertEquals(set.get(l), setb.get(l));
        Container c1 = set.get(k).and(set.get(l));
        Container c2 = setb.get(k).and(setb.get(l));
        assertEquals(c1, c2);
      }
    }
  }


  @Test
  public void RunContainerVSRunContainerANDNOT() {
    ArrayList<RunContainer> set = new ArrayList<RunContainer>();
    ArrayList<Container> setb = new ArrayList<Container>();
    getSetOfRunContainers(set, setb);
    for (int k = 0; k < set.size(); ++k) {
      for (int l = 0; l < set.size(); ++l) {
        assertEquals(set.get(k), setb.get(k));
        assertEquals(set.get(l), setb.get(l));
        Container c1 = set.get(k).andNot(set.get(l));
        Container c2 = setb.get(k).andNot(setb.get(l));
        assertEquals(c1, c2);
      }
    }
  }



  @Test
  public void RunContainerVSRunContainerOR() {
    ArrayList<RunContainer> set = new ArrayList<RunContainer>();
    ArrayList<Container> setb = new ArrayList<Container>();
    getSetOfRunContainers(set, setb);
    for (int k = 0; k < set.size(); ++k) {
      for (int l = 0; l < set.size(); ++l) {
        assertEquals(set.get(k), setb.get(k));
        assertEquals(set.get(l), setb.get(l));
        Container c1 = set.get(k).or(set.get(l));
        Container c2 = setb.get(k).or(setb.get(l));
        assertEquals(c1, c2);
      }
    }
  }

  @Test
  public void RunContainerVSRunContainerXOR() {
    ArrayList<RunContainer> set = new ArrayList<RunContainer>();
    ArrayList<Container> setb = new ArrayList<Container>();
    getSetOfRunContainers(set, setb);
    for (int k = 0; k < set.size(); ++k) {
      for (int l = 0; l < set.size(); ++l) {
        assertEquals(set.get(k), setb.get(k));
        assertEquals(set.get(l), setb.get(l));
        Container c1 = set.get(k).xor(set.get(l));
        Container c2 = setb.get(k).xor(setb.get(l));
        assertEquals(c1, c2);
      }
    }
  }



  @Test
  public void safeor() {
    Container rc1 = new RunContainer();
    Container rc2 = new RunContainer();
    for (int i = 0; i < 100; ++i) {
      rc1 = rc1.iadd(i * 4, (i + 1) * 4 - 1);
      rc2 = rc2.iadd(i * 4 + 10000, (i + 1) * 4 - 1 + 10000);
    }
    Container x = rc1.or(rc2);
    rc1.ior(rc2);
    if (!rc1.equals(x)) {
      throw new RuntimeException("bug");
    }
  }

  @Test
  public void orFullToRunContainer() {
    Container rc = Container.rangeOfOnes(0, 1 << 15);
    Container half = new BitmapContainer(1 << 15, 1 << 16);
    assertTrue(rc instanceof RunContainer);
    Container result = rc.or(half);
    assertEquals(1 << 16, result.getCardinality());
    assertTrue(result instanceof RunContainer);
  }

  @Test
  public void orFullToRunContainer2() {
    Container rc = Container.rangeOfOnes((1 << 10) - 200, 1 << 16);
    Container half = new ArrayContainer(0, 1 << 10);
    assertTrue(rc instanceof RunContainer);
    Container result = rc.or(half);
    assertEquals(1 << 16, result.getCardinality());
    assertTrue(result instanceof RunContainer);
  }

  @Test
  public void orFullToRunContainer3() {
    Container rc = Container.rangeOfOnes(0, 1 << 15);
    Container half = Container.rangeOfOnes((1 << 15) - 200, 1 << 16);
    assertTrue(rc instanceof RunContainer);
    Container result = rc.or(half);
    assertEquals(1 << 16, result.getCardinality());
    assertTrue(result instanceof RunContainer);
  }

  @Test
  public void safeSerialization() throws Exception {
    RunContainer container = new RunContainer();
    container.add((char) 0);
    container.add((char) 2);
    container.add((char) 55);
    container.add((char) 64);
    container.add((char) 256);

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(bos);
    out.writeObject(container);

    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    ObjectInputStream in = new ObjectInputStream(bis);
    RunContainer newContainer = (RunContainer) in.readObject();
    assertEquals(container, newContainer);
    assertEquals(container.serializedSizeInBytes(), newContainer.serializedSizeInBytes());
  }



  @Test
  public void select() {
    RunContainer container = new RunContainer();
    container.add((char) 0);
    container.add((char) 2);
    container.add((char) 55);
    container.add((char) 64);
    container.add((char) 256);
    assertEquals(0, container.select(0));
    assertEquals(2, container.select(1));
    assertEquals(55, container.select(2));
    assertEquals(64, container.select(3));
    assertEquals(256, container.select(4));
  }

  @Test
  public void select2() {
    assertThrows(IllegalArgumentException.class, () -> {
      RunContainer container = new RunContainer();
      container.add((char) 0);
      container.add((char) 3);
      container.add((char) 118);
      container.select(666);
    });
  }

  @Test
  public void simpleIterator() {
    RunContainer x = new RunContainer();
    for (int k = 0; k < 100; ++k) {
      x = (RunContainer) x.add((char) (k));
    }
    CharIterator i = x.getCharIterator();
    for (int k = 0; k < 100; ++k) {
      assertTrue(i.hasNext());
      assertEquals(i.next(), (char) k);
    }
    assertFalse(i.hasNext());
  }


  @Test
  public void testAndNot() {
    int[] array1 = {39173, 39174, 39175, 39176, 39177, 39178, 39179, 39180, 39181, 39182, 39183,
        39184, 39185, 39186, 39187, 39188};
    int[] array2 = {14205};
    RoaringBitmap rb1 = RoaringBitmap.bitmapOf(array1);
    rb1.runOptimize();
    RoaringBitmap rb2 = RoaringBitmap.bitmapOf(array2);
    RoaringBitmap answer = RoaringBitmap.andNot(rb1, rb2);
    assertEquals(answer.getCardinality(), array1.length);
  }



  @Test
  public void testRoaringWithOptimize() {
    // create the same bitmap over and over again, with optimizing it
    final Set<RoaringBitmap> setWithOptimize = new HashSet<RoaringBitmap>();
    final int max = 1000;
    for (int i = 0; i < max; i++) {
      final RoaringBitmap bitmapWithOptimize = new RoaringBitmap();
      bitmapWithOptimize.add(1);
      bitmapWithOptimize.add(2);
      bitmapWithOptimize.add(3);
      bitmapWithOptimize.add(4);
      bitmapWithOptimize.runOptimize();
      setWithOptimize.add(bitmapWithOptimize);
    }
    assertEquals(1, setWithOptimize.size());
  }


  @Test
  public void testRoaringWithoutOptimize() {
    // create the same bitmap over and over again, without optimizing it
    final Set<RoaringBitmap> setWithoutOptimize = new HashSet<RoaringBitmap>();
    final int max = 1000;
    for (int i = 0; i < max; i++) {
      final RoaringBitmap bitmapWithoutOptimize = new RoaringBitmap();
      bitmapWithoutOptimize.add(1);
      bitmapWithoutOptimize.add(2);
      bitmapWithoutOptimize.add(3);
      bitmapWithoutOptimize.add(4);
      setWithoutOptimize.add(bitmapWithoutOptimize);
    }
    assertEquals(1, setWithoutOptimize.size());
  }

  @Test
  public void toBitmapOrArrayContainer() {
    RunContainer rc = new RunContainer();
    rc.iadd(0, DEFAULT_MAX_SIZE / 2);
    Container ac = rc.toBitmapOrArrayContainer(rc.getCardinality());
    assertTrue(ac instanceof ArrayContainer);
    assertEquals(DEFAULT_MAX_SIZE / 2, ac.getCardinality());
    for (char k = 0; k < DEFAULT_MAX_SIZE / 2; ++k) {
      assertTrue(ac.contains(k));
    }
    rc.iadd(DEFAULT_MAX_SIZE / 2, 2 * DEFAULT_MAX_SIZE);
    Container bc = rc.toBitmapOrArrayContainer(rc.getCardinality());
    assertTrue(bc instanceof BitmapContainer);
    assertEquals(2 * DEFAULT_MAX_SIZE, bc.getCardinality());
    for (char k = 0; k < 2 * DEFAULT_MAX_SIZE; ++k) {
      assertTrue(bc.contains(k));
    }
  }

  @Test
  public void union() {
    Container bc = new BitmapContainer();
    Container rc = new RunContainer();
    for (int k = 0; k < 100; ++k) {
      bc = bc.add((char) (k * 10));
      rc = rc.add((char) (k * 10 + 3));
    }
    Container union = rc.or(bc);
    assertEquals(200, union.getCardinality());
    for (int k = 0; k < 100; ++k) {
      assertTrue(union.contains((char) (k * 10)));
      assertTrue(union.contains((char) (k * 10 + 3)));
    }
    assertEquals(100, bc.getCardinality());
    assertEquals(100, rc.getCardinality());
  }


  @Test
  public void union2() {
    System.out.println("union2");
    ArrayContainer ac = new ArrayContainer();
    RunContainer rc = new RunContainer();
    for (int k = 0; k < 100; ++k) {
      ac = (ArrayContainer) ac.add((char) (k * 10));
      rc = (RunContainer) rc.add((char) (k * 10 + 3));
    }
    Container union = rc.or(ac);
    assertEquals(200, union.getCardinality());
    for (int k = 0; k < 100; ++k) {
      assertTrue(union.contains((char) (k * 10)));
      assertTrue(union.contains((char) (k * 10 + 3)));
    }
    assertEquals(100, ac.getCardinality());
    assertEquals(100, rc.getCardinality());
  }


  @Test
  public void xor() {
    Container bc = new BitmapContainer();
    Container rc = new RunContainer();
    for (int k = 0; k < 2 * DEFAULT_MAX_SIZE; ++k) {
      bc = bc.add((char) (k * 10));
      bc = bc.add((char) (k * 10 + 1));
      rc = rc.add((char) (k * 10));
      rc = rc.add((char) (k * 10 + 3));
    }
    Container result = rc.xor(bc);
    assertEquals(4 * DEFAULT_MAX_SIZE, result.getCardinality());
    for (int k = 0; k < 2 * DEFAULT_MAX_SIZE; ++k) {
      assertTrue(result.contains((char) (k * 10 + 1)));
      assertTrue(result.contains((char) (k * 10 + 3)));
    }
    assertEquals(4 * DEFAULT_MAX_SIZE, bc.getCardinality());
    assertEquals(4 * DEFAULT_MAX_SIZE, rc.getCardinality());
  }


  @Test
  public void xor_array() {
    Container bc = new ArrayContainer();
    Container rc = new RunContainer();
    for (int k = 0; k < 2 * DEFAULT_MAX_SIZE; ++k) {
      bc = bc.add((char) (k * 10));
      bc = bc.add((char) (k * 10 + 1));
      rc = rc.add((char) (k * 10));
      rc = rc.add((char) (k * 10 + 3));
    }
    Container result = rc.xor(bc);
    assertEquals(4 * DEFAULT_MAX_SIZE, result.getCardinality());
    for (int k = 0; k < 2 * DEFAULT_MAX_SIZE; ++k) {
      assertTrue(result.contains((char) (k * 10 + 1)));
      assertTrue(result.contains((char) (k * 10 + 3)));
    }
    assertEquals(4 * DEFAULT_MAX_SIZE, bc.getCardinality());
    assertEquals(4 * DEFAULT_MAX_SIZE, rc.getCardinality());
  }



  @Test
  public void xor_array_largecase_runcontainer_best() {
    Container bc = new ArrayContainer();
    Container rc = new RunContainer();
    for (int k = 0; k < 60; ++k) {
      for (int j = 0; j < 99; ++j) {
        rc = rc.add((char) (k * 100 + j)); // most efficiently stored as runs
        bc = bc.add((char) (k * 100 + 98)).add((char) (k * 100 + 99));
      }
    }

    // size ordering preference for rc: run, bitmap, array

    assertTrue(bc instanceof ArrayContainer);
    assertTrue(rc instanceof RunContainer);
    int rcSize = rc.getCardinality();
    int bcSize = bc.getCardinality();

    Container result = rc.xor(bc);

    // input containers should not change (just check card)
    assertEquals(rcSize, rc.getCardinality());
    assertEquals(bcSize, bc.getCardinality());

    // each group of 60, we gain the missing 99th value but lose the 98th. Net wash
    assertEquals(rcSize, result.getCardinality());

    // a runcontainer would be, space-wise, best
    // but the code may (and does) opt to produce a bitmap

    // assertTrue( result instanceof RunContainer);

    for (int k = 0; k < 60; ++k) {
      for (int j = 0; j < 98; ++j) {
        assertTrue(result.contains((char) (k * 100 + j)));
      }
      assertTrue(result.contains((char) (k * 100 + 99)));
    }
  }


  @Test
  public void xor_array_mediumcase() {
    Container bc = new ArrayContainer();
    Container rc = new RunContainer();
    for (int k = 0; k < DEFAULT_MAX_SIZE / 6; ++k) {
      rc = rc.add((char) (k * 10)); // most efficiently stored as runs
      rc = rc.add((char) (k * 10 + 1));
      rc = rc.add((char) (k * 10 + 2));
    }

    for (int k = 0; k < DEFAULT_MAX_SIZE / 12; ++k) {
      bc = bc.add((char) (k * 10));
    }

    // size ordering preference for rc: run, array, bitmap

    assertTrue(bc instanceof ArrayContainer);
    assertTrue(rc instanceof RunContainer);
    int rcSize = rc.getCardinality();
    int bcSize = bc.getCardinality();

    Container result = rc.xor(bc);

    // input containers should not change (just check card)
    assertEquals(rcSize, rc.getCardinality());
    assertEquals(bcSize, bc.getCardinality());

    assertEquals(rcSize - bcSize, result.getCardinality());

    // The result really ought to be a runcontainer, by its size
    // however, as of test writing, the implementation
    // will have converted the result to an array container.
    // This is suboptimal, storagewise, but arguably not an error

    // assertTrue( result instanceof RunContainer);

    for (int k = 0; k < DEFAULT_MAX_SIZE / 12; ++k) {
      assertTrue(result.contains((char) (k * 10 + 1)));
      assertTrue(result.contains((char) (k * 10 + 2)));
    }

    for (int k = DEFAULT_MAX_SIZE / 12; k < DEFAULT_MAX_SIZE / 6; ++k) {
      assertTrue(result.contains((char) (k * 10 + 1)));
      assertTrue(result.contains((char) (k * 10 + 2)));
    }
  }


  @Test
  public void xor_array_smallcase() {
    Container bc = new ArrayContainer();
    Container rc = new RunContainer();
    for (int k = 0; k < DEFAULT_MAX_SIZE / 3; ++k) {
      rc = rc.add((char) (k * 10)); // most efficiently stored as runs
      rc = rc.add((char) (k * 10 + 1));
      rc = rc.add((char) (k * 10 + 2));
      rc = rc.add((char) (k * 10 + 3));
      rc = rc.add((char) (k * 10 + 4));
    }

    // very small array.
    bc = bc.add((char) 1).add((char) 2).add((char) 3).add((char) 4).add((char) 5);

    assertTrue(bc instanceof ArrayContainer);
    assertTrue(rc instanceof RunContainer);
    int rcSize = rc.getCardinality();
    int bcSize = bc.getCardinality();


    Container result = rc.xor(bc);

    // input containers should not change (just check card)
    assertEquals(rcSize, rc.getCardinality());
    assertEquals(bcSize, bc.getCardinality());

    assertEquals(rcSize - 3, result.getCardinality());
    assertTrue(result.contains((char) 5));
    assertTrue(result.contains((char) 0));


    for (int k = 1; k < DEFAULT_MAX_SIZE / 3; ++k) {
      for (int i = 0; i < 5; ++i) {
        assertTrue(result.contains((char) (k * 10 + i)));
      }
    }
  }

  @Test
  public void xor1() {
    Container bc = new BitmapContainer();
    Container rc = new RunContainer();
    rc.add((char) 1);
    Container result = rc.xor(bc);
    assertEquals(1, result.getCardinality());
    assertTrue(result.contains((char) 1));
  }

  @Test
  public void xor1a() {
    Container bc = new ArrayContainer();
    Container rc = new RunContainer();
    rc.add((char) 1);
    Container result = rc.xor(bc);
    assertEquals(1, result.getCardinality());
    assertTrue(result.contains((char) 1));
  }


  @Test
  public void xor2() {
    Container bc = new BitmapContainer();
    Container rc = new RunContainer();
    bc.add((char) 1);
    Container result = rc.xor(bc);
    assertEquals(1, result.getCardinality());
    assertTrue(result.contains((char) 1));
  }


  @Test
  public void xor2a() {
    Container bc = new ArrayContainer();
    Container rc = new RunContainer();
    bc.add((char) 1);
    Container result = rc.xor(bc);
    assertEquals(1, result.getCardinality());
    assertTrue(result.contains((char) 1));
  }


  @Test
  public void xor3() {
    Container bc = new BitmapContainer();
    Container rc = new RunContainer();
    rc.add((char) 1);
    bc.add((char) 1);
    Container result = rc.xor(bc);
    assertEquals(0, result.getCardinality());
  }



  @Test
  public void xor3a() {
    Container bc = new ArrayContainer();
    Container rc = new RunContainer();
    rc.add((char) 1);
    bc.add((char) 1);
    Container result = rc.xor(bc);
    assertEquals(0, result.getCardinality());
  }



  @Test
  public void xor4() {
    Container bc = new ArrayContainer();
    Container rc = new RunContainer();
    Container answer = new ArrayContainer();
    answer = answer.add(28203, 28214);
    rc = rc.add(28203, 28214);
    int[] data = {17739, 17740, 17945, 19077, 19278, 19407};
    for (int x : data) {
      answer = answer.add((char) x);
      bc = bc.add((char) x);
    }
    Container result = rc.xor(bc);
    assertEquals(answer, result);
  }

  @Test
  public void xor5() {
    Container rc1 = new RunContainer();
    Container rc2 = new RunContainer();
    rc2.iadd(1, 13);
    assertEquals(rc2, rc1.xor(rc2));
    assertEquals(rc2, rc2.xor(rc1));
  }

  @Test
  public void intersects1() {
    Container ac = new ArrayContainer();
    ac = ac.add((char) 1);
    ac = ac.add((char) 7);
    ac = ac.add((char) 13);
    ac = ac.add((char) 666);

    Container rc = new RunContainer();

    assertFalse(rc.intersects(ac));
    assertFalse(ac.intersects(rc));

    rc = rc.add((char) 1000);
    assertFalse(rc.intersects(ac));
    assertFalse(ac.intersects(rc));

    rc = rc.remove((char) 1000);
    rc = rc.add(100,200);
    rc = rc.add(300,500);
    assertFalse(rc.intersects(ac));
    assertFalse(ac.intersects(rc));

    rc = rc.add(500,1000);
    assertTrue(rc.intersects(ac));
    assertTrue(ac.intersects(rc));
  }

  @Test
  public void intersects2() {
    Container rc1 = new RunContainer();
    Container rc2 = new RunContainer();

    assertFalse(rc1.intersects(rc2));

    rc1 = rc1.add(10, 50);
    rc2 = rc2.add(100, 500);
    assertFalse(rc1.intersects(rc2));

    rc1 = rc1.add(60, 70);
    assertFalse(rc1.intersects(rc2));

    rc1 = rc1.add(600, 700);
    rc2 = rc2.add(800, 900);
    assertFalse(rc1.intersects(rc2));

    rc2 = rc2.add(30, 40);
    assertTrue(rc1.intersects(rc2));
  }

  @Test
  public void intersects3() {
    Container rc = new RunContainer();
    Container bc = new BitmapContainer();

    rc = rc.add(10, 50);
    bc = bc.add(100, 500);
    assertFalse(rc.intersects(bc));
  }

  @Test
  public void constructor1() {
    assertThrows(RuntimeException.class, () -> new RunContainer(new char[]{1, 2, 10, 3}, 5));
  }

  @Test
  public void ensureCapacity() {
    RunContainer rc = new RunContainer();
    rc.add((char) 13);
    assertTrue(rc.contains((char) 13));

    rc.ensureCapacity(10);
    assertTrue(rc.contains((char) 13));
  }

  @Test
  public void testToString() {
    Container rc = new RunContainer(32200, 35000);
    rc.add((char)-1);
    assertEquals("[32200,34999][65535,65535]", rc.toString());
  }

  @Test
  public void lazyIOR() {
    Container rc = new RunContainer();
    Container ac = new ArrayContainer();

    ac = ac.add(0, 1);
    rc = rc.lazyIOR(ac);
    assertEquals(1, rc.getCardinality());

    rc = new RunContainer();
    rc = rc.add(0, 13);
    rc = rc.lazyIOR(ac);
    assertEquals(13, rc.getCardinality());

    rc = new RunContainer();
    rc = rc.add(0, 1<<16);
    rc = rc.lazyIOR(ac);
    assertEquals(1<<16, rc.getCardinality());
  }

  @Test
  public void lazyOR() {
    Container rc = new RunContainer();
    Container ac = new ArrayContainer();

    ac = ac.add(0, 1);
    rc = rc.lazyOR(ac);
    assertEquals(1, rc.getCardinality());

    rc = new RunContainer();
    rc = rc.add(0, 13);
    rc = rc.lazyOR(ac);
    assertEquals(13, rc.getCardinality());

    rc = new RunContainer();
    rc = rc.add(0, 1<<16);
    rc = rc.lazyOR(ac);
    assertEquals(1<<16, rc.getCardinality());
  }

  @Test
  public void testLazyORFull() {
    Container rc = Container.rangeOfOnes(0, 1 << 15);
    BitmapContainer bc2 = new BitmapContainer(3210, 1 << 16);
    Container rbc = rc.lazyOR(bc2);
    assertEquals(-1, rbc.getCardinality());
    Container repaired = rbc.repairAfterLazy();
    assertEquals(1 << 16, repaired.getCardinality());
    assertTrue(repaired instanceof RunContainer);
  }

  @Test
  public void testLazyORFull2() {
    Container rc = Container.rangeOfOnes((1 << 10) - 200, 1 << 16);
    ArrayContainer ac = new ArrayContainer(0, 1 << 10);
    Container rbc = rc.lazyOR(ac);
    assertEquals(1 << 16, rbc.getCardinality());
    assertTrue(rbc instanceof RunContainer);
  }

  @Test
  public void testLazyORFull3() {
    Container rc = Container.rangeOfOnes(0, 1 << 15);
    Container rc2 = Container.rangeOfOnes(1 << 15, 1 << 16);
    Container result = rc.lazyOR(rc2);
    Container iresult = rc.lazyIOR(rc2);
    assertEquals(1 << 16, result.getCardinality());
    assertEquals(1 << 16, iresult.getCardinality());
    assertTrue(result instanceof RunContainer);
    assertTrue(iresult instanceof RunContainer);
  }

  @Test
  public void testRangeCardinality() {
    BitmapContainer bc = TestBitmapContainer.generateContainer((char) 100, (char) 10000, 5);
    RunContainer rc = new RunContainer(new char[]{7, 300, 400, 900, 1400, 2200}, 3);
    Container result = rc.or(bc);
    assertEquals(8677, result.getCardinality());
  }

  @Test
  public void testRangeCardinality2() {
    BitmapContainer bc = TestBitmapContainer.generateContainer((char) 100, (char) 10000, 5);
    bc.add((char)22345); //important case to have greater element than run container
    bc.add((char)Short.MAX_VALUE);
    RunContainer rc = new RunContainer(new char[]{7, 300, 400, 900, 1400, 18000}, 3);
    assertTrue(rc.getCardinality() > ArrayContainer.DEFAULT_MAX_SIZE);
    Container result = rc.andNot(bc);
    assertEquals(11437, result.getCardinality());
  }

  @Test
  public void testRangeCardinality3() {
    BitmapContainer bc = TestBitmapContainer.generateContainer((char) 100, (char) 10000, 5);
    RunContainer rc = new RunContainer(new char[]{7, 300, 400, 900, 1400, 5200}, 3);
    BitmapContainer result = (BitmapContainer) rc.and(bc);
    assertEquals(5046, result.getCardinality());
  }

  @Test
  public void testRangeCardinality4() {
    BitmapContainer bc = TestBitmapContainer.generateContainer((char) 100, (char) 10000, 5);
    RunContainer rc = new RunContainer(new char[]{7, 300, 400, 900, 1400, 2200}, 3);
    BitmapContainer result = (BitmapContainer) rc.xor(bc);
    assertEquals(6031, result.getCardinality());
  }

  @Test
  public void testFirst_Empty() {
    assertThrows(NoSuchElementException.class, () -> new RunContainer().first());
  }

  @Test
  public void testLast_Empty() {
    assertThrows(NoSuchElementException.class, () -> new RunContainer().last());
  }

  @Test
  public void testFirstLast() {
    Container rc = new RunContainer();
    final int firstInclusive = 1;
    int lastExclusive = firstInclusive;
    for (int i = 0; i < 1 << 16 - 10; ++i) {
      int newLastExclusive = lastExclusive + 10;
      rc = rc.add(lastExclusive, newLastExclusive);
      assertEquals(firstInclusive, rc.first());
      assertEquals(newLastExclusive - 1, rc.last());
      lastExclusive = newLastExclusive;
    }
  }

  @Test
  public void testFirstUnsigned() {
    RoaringBitmap roaringWithRun = new RoaringBitmap();
    roaringWithRun.add(32768L, 65536); // (1 << 15) to (1 << 16).
    assertEquals(roaringWithRun.first(), 32768);
  }

  @Test
  public void testContainsBitmapContainer_EmptyContainsEmpty() {
    Container rc = new RunContainer();
    Container subset = new BitmapContainer();
    assertTrue(rc.contains(subset));
  }

  @Test
  public void testContainsBitmapContainer_IncludeProperSubset() {
    Container rc = new RunContainer().add(0,10);
    Container subset = new BitmapContainer().add(0,9);
    assertTrue(rc.contains(subset));
  }


  @Test
  public void testContainsBitmapContainer_IncludeProperSubsetDifferentStart() {
    Container rc = new RunContainer().add(0,10);
    Container subset = new BitmapContainer().add(1,9);
    assertTrue(rc.contains(subset));
  }

  @Test
  public void testContainsBitmapContainer_ExcludeShiftedSet() {
    Container rc = new RunContainer().add(0,10);
    Container subset = new BitmapContainer().add(2,12);
    assertFalse(rc.contains(subset));
  }

  @Test
  public void testContainsBitmapContainer_IncludeSelf() {
    Container rc = new RunContainer().add(0,10);
    Container subset = new BitmapContainer().add(0,10);
    assertTrue(rc.contains(subset));
  }

  @Test
  public void testContainsBitmapContainer_ExcludeSuperSet() {
    Container rc = new RunContainer().add(0,10);
    Container superset = new BitmapContainer().add(0,20);
    assertFalse(rc.contains(superset));
  }

  @Test
  public void testContainsBitmapContainer_ExcludeDisJointSet() {
    Container rc = new RunContainer().add(0,10);
    Container disjoint = new BitmapContainer().add(20, 40);
    assertFalse(rc.contains(disjoint));
    assertFalse(disjoint.contains(rc));
  }

  @Test
  public void testContainsRunContainer_EmptyContainsEmpty() {
    Container rc = new RunContainer();
    Container subset = new RunContainer();
    assertTrue(rc.contains(subset));
  }

  @Test
  public void testContainsRunContainer_IncludeProperSubset() {
    Container rc = new RunContainer().add(0,10);
    Container subset = new RunContainer().add(0,9);
    assertTrue(rc.contains(subset));
  }

  @Test
  public void testContainsRunContainer_IncludeSelf() {
    Container rc = new RunContainer().add(0,10);
    Container subset = new RunContainer().add(0,10);
    assertTrue(rc.contains(subset));
  }

  @Test
  public void testContainsRunContainer_ExcludeSuperSet() {
    Container rc = new RunContainer().add(0,10);
    Container superset = new RunContainer().add(0,20);
    assertFalse(rc.contains(superset));
  }

  @Test
  public void testContainsRunContainer_IncludeProperSubsetDifferentStart() {
    Container rc = new RunContainer().add(0,10);
    Container subset = new RunContainer().add(1,9);
    assertTrue(rc.contains(subset));
  }

  @Test
  public void testContainsRunContainer_ExcludeShiftedSet() {
    Container rc = new RunContainer().add(0,10);
    Container subset = new RunContainer().add(2,12);
    assertFalse(rc.contains(subset));
  }

  @Test
  public void testContainsRunContainer_ExcludeDisJointSet() {
    Container rc = new RunContainer().add(0,10);
    Container disjoint = new RunContainer().add(20, 40);
    assertFalse(rc.contains(disjoint));
    assertFalse(disjoint.contains(rc));
  }

  @Test
  public void testContainsArrayContainer_EmptyContainsEmpty() {
    Container rc = new RunContainer();
    Container subset = new ArrayContainer();
    assertTrue(rc.contains(subset));
  }

  @Test
  public void testContainsArrayContainer_IncludeProperSubset() {
    Container rc = new RunContainer().add(0,10);
    Container subset = new ArrayContainer().add(0,9);
    assertTrue(rc.contains(subset));
  }

  @Test
  public void testContainsArrayContainer_IncludeProperSubsetDifferentStart() {
    Container rc = new RunContainer().add(0,10);
    Container subset = new ArrayContainer().add(2,9);
    assertTrue(rc.contains(subset));
  }

  @Test
  public void testContainsArrayContainer_ExcludeShiftedSet() {
    Container rc = new RunContainer().add(0,10);
    Container shifted = new ArrayContainer().add(2,12);
    assertFalse(rc.contains(shifted));
  }

  @Test
  public void testContainsArrayContainer_IncludeSelf() {
    Container rc = new RunContainer().add(0,10);
    Container subset = new ArrayContainer().add(0,10);
    assertTrue(rc.contains(subset));
  }

  @Test
  public void testContainsArrayContainer_ExcludeSuperSet() {
    Container rc = new RunContainer().add(0,10);
    Container superset = new ArrayContainer().add(0,20);
    assertFalse(rc.contains(superset));
  }

  @Test
  public void testContainsArrayContainer_ExcludeDisJointSet() {
    Container rc = new RunContainer().add(0, 10);
    Container disjoint = new ArrayContainer().add(20, 40);
    assertFalse(rc.contains(disjoint));
    assertFalse(disjoint.contains(rc));

    disjoint = new ArrayContainer().add((char)512);
    assertFalse(rc.contains(disjoint));
    assertFalse(disjoint.contains(rc));

    rc = rc.add(12,14).add(16,18).add(20,22);
    assertFalse(rc.contains(disjoint));
    assertFalse(disjoint.contains(rc));

    rc.trim();
    assertFalse(rc.contains(disjoint));
    assertFalse(disjoint.contains(rc));



  }

  @Test
  public void testEqualsArrayContainer_Equal() {
    Container rc = new RunContainer().add(0, 10);
    Container ac = new ArrayContainer().add(0, 10);
    assertEquals(rc, ac);
    assertEquals(ac, rc);
  }

  @Test
  public void testEqualsArrayContainer_NotEqual_ArrayLarger() {
    Container rc = new RunContainer().add(0, 10);
    Container ac = new ArrayContainer().add(0, 11);
    assertNotEquals(rc, ac);
    assertNotEquals(ac, rc);
  }

  @Test
  public void testEqualsArrayContainer_NotEqual_ArraySmaller() {
    Container rc = new RunContainer().add(0, 10);
    Container ac = new ArrayContainer().add(0, 9);
    assertNotEquals(rc, ac);
    assertNotEquals(ac, rc);
  }

  @Test
  public void testEqualsArrayContainer_NotEqual_ArrayShifted() {
    Container rc = new RunContainer().add(0, 10);
    Container ac = new ArrayContainer().add(1, 11);
    assertNotEquals(rc, ac);
    assertNotEquals(ac, rc);
  }

  @Test
  public void testEqualsArrayContainer_NotEqual_ArrayDiscontiguous() {
    Container rc = new RunContainer().add(0, 10);
    Container ac = new ArrayContainer().add(0, 11);
    ac.flip((char)9);
    assertNotEquals(rc, ac);
    assertNotEquals(ac, rc);
  }

  @Test
  public void testEquals_FullRunContainerWithArrayContainer() {
    Container full = new RunContainer().add(0, 1 << 16);
    assertNotEquals(full, new ArrayContainer().add(0, 10));
  }

  @Test
  public void testFullConstructor() {
    assertTrue(RunContainer.full().isFull());
  }

  @Test
  public void testRangeConstructor() {
    RunContainer c = new RunContainer(0, 1 << 16);
    assertTrue(c.isFull());
    assertEquals(65536, c.getCardinality());
  }

  @Test
  public void testRangeConstructor2() {
    RunContainer c = new RunContainer(17, 1000);
    assertEquals(983, c.getCardinality());
  }

  @Test
  public void testRangeConstructor3() {
    RunContainer a = new RunContainer(17, 45679);
    RunContainer b = new RunContainer();
    b.iadd(17, 45679);
    assertEquals(a, b);
  }

  @Test
  public void testRangeConstructor4() {
    RunContainer c = new RunContainer(0, 45679);
    assertEquals(45679, c.getCardinality());
  }

  @Test
  public void testSimpleCardinality() {
    RunContainer c = new RunContainer();
    c.add((char) 1);
    c.add((char) 17);
    assertEquals(2, c.getCardinality());
  }

  @Test
  public void testIntersectsWithRange() {
    Container container = new RunContainer().add(0, 10);
    assertTrue(container.intersects(0, 1));
    assertTrue(container.intersects(0, 101));
    assertTrue(container.intersects(0, 1 << 16));
    assertFalse(container.intersects(11, 1 << 16));
  }


  @Test
  public void testIntersectsWithRangeUnsigned() {
    Container container = new RunContainer().add(lower16Bits(-50), lower16Bits(-10));
    assertFalse(container.intersects(0, 1));
    assertTrue(container.intersects(0, lower16Bits(-40)));
    assertFalse(container.intersects(lower16Bits(-100), lower16Bits(-55)));
    //assertFalse(container.intersects(-9, 1 << 16)); // forbidden
    assertTrue(container.intersects(11, 1 << 16));
  }


  @Test
  public void testIntersectsWithRangeManyRuns() {
    Container container = new RunContainer().add(0, 10).add(lower16Bits(-50), lower16Bits(-10));
    assertTrue(container.intersects(0, 1));
    assertTrue(container.intersects(0, 101));
    assertTrue(container.intersects(0, lower16Bits(-1)));
    assertTrue(container.intersects(11, lower16Bits(-1)));
    assertTrue(container.intersects(0, lower16Bits(-40)));
    assertFalse(container.intersects(lower16Bits(-100), lower16Bits(-55)));
    assertFalse(container.intersects(lower16Bits(-9), lower16Bits(-1)));
    assertTrue(container.intersects(11, 1 << 16));
  }

  @Test
  public void testContainsFull() {
    assertTrue(RunContainer.full().contains(0, 1 << 16));
    assertFalse(RunContainer.full().flip((char)(1 << 15)).contains(0, 1 << 16));
  }

  @Test
  public void testContainsRange() {
    Container rc = new RunContainer().add(1, 100).add(5000, 10000);
    assertFalse(rc.contains(0, 100));
    assertFalse(rc.contains(0, 100000));
    assertTrue(rc.contains(1, 100));
    assertTrue(rc.contains(1, 99));
    assertTrue(rc.contains(2, 100));
    assertTrue(rc.contains(5000, 10000));
    assertTrue(rc.contains(5000, 9999));
    assertTrue(rc.contains(5001, 9999));
    assertTrue(rc.contains(5001, 10000));
    assertFalse(rc.contains(100, 5000));
    assertFalse(rc.contains(50, 5000));
    assertFalse(rc.contains(4000, 6000));
    assertFalse(rc.contains(10001, 20000));
  }

  @Test
  public void testContainsRange3() {
    Container rc = new RunContainer().add(1, 100)
            .add(300, 300)
            .add(400, 500)
            .add(502, 600)
            .add(700, 10000);
    assertFalse(rc.contains(0, 100));
    assertFalse(rc.contains(500, 600));
    assertFalse(rc.contains(501, 600));
    assertTrue(rc.contains(502, 600));
    assertFalse(rc.contains(600, 700));
    assertTrue(rc.contains(9999, 10000));
    assertFalse(rc.contains(9999, 10001));
  }

  @Test
  public void testNextValue() {
    RunContainer container = new RunContainer(new char[] { 64, 64 }, 1);
    assertEquals(64, container.nextValue((char)0));
    assertEquals(64, container.nextValue((char)64));
    assertEquals(65, container.nextValue((char)65));
    assertEquals(128, container.nextValue((char)128));
    assertEquals(-1, container.nextValue((char)129));
  }

  @Test
  public void testNextValueBetweenRuns() {
    RunContainer container = new RunContainer(new char[] { 64, 64, 256, 64 }, 2);
    assertEquals(64, container.nextValue((char)0));
    assertEquals(64, container.nextValue((char)64));
    assertEquals(65, container.nextValue((char)65));
    assertEquals(128, container.nextValue((char)128));
    assertEquals(256, container.nextValue((char)129));
    assertEquals(-1, container.nextValue((char)512));
  }

  @Test
  public void testNextValue2() {
    RunContainer container = new RunContainer(new char[] { 64, 64, 200, 300, 5000, 200 }, 3);
    assertEquals(64, container.nextValue((char)0));
    assertEquals(64, container.nextValue((char)63));
    assertEquals(64, container.nextValue((char)64));
    assertEquals(65, container.nextValue((char)65));
    assertEquals(128, container.nextValue((char)128));
    assertEquals(200, container.nextValue((char)129));
    assertEquals(200, container.nextValue((char)199));
    assertEquals(200, container.nextValue((char)200));
    assertEquals(250, container.nextValue((char)250));
    assertEquals(5000, container.nextValue((char)2500));
    assertEquals(5000, container.nextValue((char)5000));
    assertEquals(5200, container.nextValue((char)5200));
    assertEquals(-1, container.nextValue((char)5201));
  }

  @Test
  public void testPreviousValue1() {
    RunContainer container = new RunContainer(new char[] { 64, 64 }, 1);
    assertEquals(-1, container.previousValue((char)0));
    assertEquals(-1, container.previousValue((char)63));
    assertEquals(64, container.previousValue((char)64));
    assertEquals(65, container.previousValue((char)65));
    assertEquals(128, container.previousValue((char)128));
    assertEquals(128, container.previousValue((char)129));
  }

  @Test
  public void testPreviousValue2() {
    RunContainer container = new RunContainer(new char[] { 64, 64, 200, 300, 5000, 200 }, 3);
    assertEquals(-1, container.previousValue((char)0));
    assertEquals(-1, container.previousValue((char)63));
    assertEquals(64, container.previousValue((char)64));
    assertEquals(65, container.previousValue((char)65));
    assertEquals(128, container.previousValue((char)128));
    assertEquals(128, container.previousValue((char)129));
    assertEquals(128, container.previousValue((char)199));
    assertEquals(200, container.previousValue((char)200));
    assertEquals(250, container.previousValue((char)250));
    assertEquals(500, container.previousValue((char)2500));
    assertEquals(5000, container.previousValue((char)5000));
    assertEquals(5200, container.previousValue((char)5200));
  }

  @Test
  public void testPreviousValueUnsigned() {
    RunContainer container = new RunContainer(new char[] { (char)((1 << 15) | 5), (char)0, (char)((1 << 15) | 7), (char)0}, 2);
    assertEquals(-1, container.previousValue((char)((1 << 15) | 4)));
    assertEquals(((1 << 15) | 5), container.previousValue((char)((1 << 15) | 5)));
    assertEquals(((1 << 15) | 5), container.previousValue((char)((1 << 15) | 6)));
    assertEquals(((1 << 15) | 7), container.previousValue((char)((1 << 15) | 7)));
    assertEquals(((1 << 15) | 7), container.previousValue((char)((1 << 15) | 8)));
  }

  @Test
  public void testNextValueUnsigned() {
    RunContainer container = new RunContainer(new char[] { (char)((1 << 15) | 5), (char)0, (char)((1 << 15) | 7), (char)0}, 2);
    assertEquals(((1 << 15) | 5), container.nextValue((char)((1 << 15) | 4)));
    assertEquals(((1 << 15) | 5), container.nextValue((char)((1 << 15) | 5)));
    assertEquals(((1 << 15) | 7), container.nextValue((char)((1 << 15) | 6)));
    assertEquals(((1 << 15) | 7), container.nextValue((char)((1 << 15) | 7)));
    assertEquals(-1, container.nextValue((char)((1 << 15) | 8)));
  }

  @Test
  public void testPreviousAbsentValue1() {
    Container container = new RunContainer().iadd(64, 129);
    assertEquals(0, container.previousAbsentValue((char)0));
    assertEquals(63, container.previousAbsentValue((char)63));
    assertEquals(63, container.previousAbsentValue((char)64));
    assertEquals(63, container.previousAbsentValue((char)65));
    assertEquals(63, container.previousAbsentValue((char)128));
    assertEquals(129, container.previousAbsentValue((char)129));
  }

  @Test
  public void testPreviousAbsentValue2() {
    Container container = new RunContainer().iadd(64, 129).iadd(200, 501).iadd(5000, 5201);
    assertEquals(0, container.previousAbsentValue((char)0));
    assertEquals(63, container.previousAbsentValue((char)63));
    assertEquals(63, container.previousAbsentValue((char)64));
    assertEquals(63, container.previousAbsentValue((char)65));
    assertEquals(63, container.previousAbsentValue((char)128));
    assertEquals(129, container.previousAbsentValue((char)129));
    assertEquals(199, container.previousAbsentValue((char)199));
    assertEquals(199, container.previousAbsentValue((char)200));
    assertEquals(199, container.previousAbsentValue((char)250));
    assertEquals(2500, container.previousAbsentValue((char)2500));
    assertEquals(4999, container.previousAbsentValue((char)5000));
    assertEquals(4999, container.previousAbsentValue((char)5200));
  }

  @Test
  public void testPreviousAbsentValueEmpty() {
    RunContainer container = new RunContainer();
    for (int i = 0; i < 1000; i++) {
      assertEquals(i, container.previousAbsentValue((char)i));
    }
  }

  @Test
  public void testPreviousAbsentValueSparse() {
    RunContainer container = new RunContainer(new char[] { 10, 0, 20, 0, 30, 0}, 3);
    assertEquals(9, container.previousAbsentValue((char)9));
    assertEquals(9, container.previousAbsentValue((char)10));
    assertEquals(11, container.previousAbsentValue((char)11));
    assertEquals(21, container.previousAbsentValue((char)21));
    assertEquals(29, container.previousAbsentValue((char)30));
  }

  @Test
  public void testPreviousAbsentEvenBits() {
    char[] evenBits = new char[1 << 15];
    for (int i = 0; i < 1 << 15; i += 2) {
      evenBits[i] = (char) i;
      evenBits[i + 1] = 0;
    }

    RunContainer container = new RunContainer(evenBits, 1 << 14);
    for (int i = 0; i < 1 << 10; i+=2) {
      assertEquals(i - 1, container.previousAbsentValue((char)i));
      assertEquals(i + 1, container.previousAbsentValue((char)(i+1)));
    }
  }

  @Test
  public void testPreviousAbsentValueUnsigned() {
    RunContainer container = new RunContainer(new char[] { (char)((1 << 15) | 5), 0, (char)((1 << 15) | 7), 0}, 2);
    assertEquals(((1 << 15) | 4), container.previousAbsentValue((char)((1 << 15) | 4)));
    assertEquals(((1 << 15) | 4), container.previousAbsentValue((char)((1 << 15) | 5)));
    assertEquals(((1 << 15) | 6), container.previousAbsentValue((char)((1 << 15) | 6)));
    assertEquals(((1 << 15) | 6), container.previousAbsentValue((char)((1 << 15) | 7)));
    assertEquals(((1 << 15) | 8), container.previousAbsentValue((char)((1 << 15) | 8)));
  }


  @Test
  public void testNextAbsentValue1() {
    Container container = new RunContainer().iadd(64, 129);
    assertEquals(0, container.nextAbsentValue((char)0));
    assertEquals(63, container.nextAbsentValue((char)63));
    assertEquals(129, container.nextAbsentValue((char)64));
    assertEquals(129, container.nextAbsentValue((char)65));
    assertEquals(129, container.nextAbsentValue((char)128));
    assertEquals(129, container.nextAbsentValue((char)129));
  }

  @Test
  public void testNextAbsentValue2() {
    Container container = new RunContainer().iadd(64, 129).iadd(200, 501).iadd(5000, 5201);
    assertEquals(0, container.nextAbsentValue((char)0));
    assertEquals(63, container.nextAbsentValue((char)63));
    assertEquals(129, container.nextAbsentValue((char)64));
    assertEquals(129, container.nextAbsentValue((char)65));
    assertEquals(129, container.nextAbsentValue((char)128));
    assertEquals(129, container.nextAbsentValue((char)129));
    assertEquals(199, container.nextAbsentValue((char)199));
    assertEquals(501, container.nextAbsentValue((char)200));
    assertEquals(501, container.nextAbsentValue((char)250));
    assertEquals(2500, container.nextAbsentValue((char)2500));
    assertEquals(5201, container.nextAbsentValue((char)5000));
    assertEquals(5201, container.nextAbsentValue((char)5200));
  }

  @Test
  public void testNextAbsentValueEmpty() {
    RunContainer container = new RunContainer();
    for (int i = 0; i < 1000; i++) {
      assertEquals(i, container.nextAbsentValue((char)i));
    }
  }

  @Test
  public void testNextAbsentValueSparse() {
    Container container = new RunContainer(new char[] { 10, 0, 20, 0, 30, 0}, 3);
    assertEquals(9, container.nextAbsentValue((char)9));
    assertEquals(11, container.nextAbsentValue((char)10));
    assertEquals(11, container.nextAbsentValue((char)11));
    assertEquals(21, container.nextAbsentValue((char)21));
    assertEquals(31, container.nextAbsentValue((char)30));
  }

  @Test
  public void testNextAbsentEvenBits() {
    char[] evenBits = new char[1 << 15];
    for (int i = 0; i < 1 << 15; i += 2) {
      evenBits[i] = (char) i;
      evenBits[i + 1] = 0;
    }

    RunContainer container = new RunContainer(evenBits, 1 << 14);
    for (int i = 0; i < 1 << 10; i+=2) {
      assertEquals(i + 1, container.nextAbsentValue((char)i));
      assertEquals(i + 1, container.nextAbsentValue((char)(i+1)));
    }
  }

  @Test
  public void testNextAbsentValueUnsigned() {
    RunContainer container = new RunContainer(new char[] { (char)((1 << 15) | 5), 0, (char)((1 << 15) | 7), 0}, 2);
    assertEquals(((1 << 15) | 4), container.nextAbsentValue((char)((1 << 15) | 4)));
    assertEquals(((1 << 15) | 6), container.nextAbsentValue((char)((1 << 15) | 5)));
    assertEquals(((1 << 15) | 6), container.nextAbsentValue((char)((1 << 15) | 6)));
    assertEquals(((1 << 15) | 8), container.nextAbsentValue((char)((1 << 15) | 7)));
    assertEquals(((1 << 15) | 8), container.nextAbsentValue((char)((1 << 15) | 8)));
  }

  @Test
  public void testContains() {
    RunContainer rc = new RunContainer(new char[]{23, 24}, 1);
    assertFalse(rc.contains(48, 49));
  }

  @Test
  public void testIntersects() {
    RunContainer rc = new RunContainer(new char[]{41, 15, 215, 0, 217, 2790, 3065, 170, 3269, 422, 3733, 43, 3833, 16, 3852, 7, 3662, 3, 3901, 2}, 10);
    assertFalse(rc.intersects(57, 215));
  }

  @Test
  public void testRangeConsumer() {
    char[] entries = new char[] {3, 4, 7, 8, 10, 65530, 65534, 65535};
    RunContainer container = new RunContainer();
    container.iadd(3, 5);
    container.iadd(7, 9);
    container.add((char) 10);
    container.add((char) 65530);
    container.iadd(65534, 65536);

    ValidationRangeConsumer consumer = ValidationRangeConsumer.validate(new ValidationRangeConsumer.Value[] {
        ABSENT, ABSENT, ABSENT, PRESENT, PRESENT, ABSENT, ABSENT, PRESENT, PRESENT, ABSENT, PRESENT
    });
    container.forAllUntil(0, (char) 11, consumer);
    assertEquals(11, consumer.getNumberOfValuesConsumed());

    ValidationRangeConsumer consumer2 = ValidationRangeConsumer.validate(new ValidationRangeConsumer.Value[] {
        PRESENT, ABSENT, ABSENT, PRESENT, PRESENT
    });
    container.forAllInRange((char) 4, (char) 9, consumer2);
    assertEquals(5, consumer2.getNumberOfValuesConsumed());

    ValidationRangeConsumer consumer3 = ValidationRangeConsumer.validate(new ValidationRangeConsumer.Value[] {
        PRESENT, ABSENT, ABSENT, ABSENT, PRESENT, PRESENT
    });
    container.forAllFrom((char) 65530, consumer3);
    assertEquals(6, consumer3.getNumberOfValuesConsumed());

    ValidationRangeConsumer consumer4 = ValidationRangeConsumer.ofSize(BitmapContainer.MAX_CAPACITY);
    container.forAll(0, consumer4);
    consumer4.assertAllAbsentExcept(entries, 0);

    ValidationRangeConsumer consumer5 = ValidationRangeConsumer.ofSize(2 * BitmapContainer.MAX_CAPACITY);
    consumer5.acceptAllAbsent(0, BitmapContainer.MAX_CAPACITY);
    container.forAll(BitmapContainer.MAX_CAPACITY, consumer5);
    consumer5.assertAllAbsentExcept(entries, BitmapContainer.MAX_CAPACITY);

    // Completely Empty
    container = new RunContainer();
    ValidationRangeConsumer consumer6 = ValidationRangeConsumer.ofSize(BitmapContainer.MAX_CAPACITY);
    container.forAll(0, consumer6);
    consumer6.assertAllAbsent();

    // Completely Full
    container = new RunContainer();
    container.iadd(0, BitmapContainer.MAX_CAPACITY);
    ValidationRangeConsumer consumer7 = ValidationRangeConsumer.ofSize(BitmapContainer.MAX_CAPACITY);
    container.forAll(0, consumer7);
    consumer7.assertAllPresent();

    int middle = BitmapContainer.MAX_CAPACITY / 2;
    ValidationRangeConsumer consumer8 = ValidationRangeConsumer.ofSize(middle);
    container.forAllFrom((char) middle, consumer8);
    consumer8.assertAllPresent();

    ValidationRangeConsumer consumer9 = ValidationRangeConsumer.ofSize(middle);
    container.forAllUntil(0, (char) middle, consumer9);
    consumer9.assertAllPresent();

    int quarter = middle / 2;
    ValidationRangeConsumer consumer10 = ValidationRangeConsumer.ofSize(middle);
    container.forAllInRange((char) quarter, (char) (middle + quarter), consumer10);
    consumer10.assertAllPresent();
  }

  private static int lower16Bits(int x) {
    return ((char)x) & 0xFFFF;
  }
}
