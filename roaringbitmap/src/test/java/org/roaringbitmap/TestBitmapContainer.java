/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Random;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.*;

public class TestBitmapContainer {
  private static BitmapContainer emptyContainer() {
    return new BitmapContainer(new long[1], 0);
  }

  static BitmapContainer generateContainer(short min, short max, int sample) {
    BitmapContainer bc = new BitmapContainer();
    for (int i = min; i < max; i++) {
      if (i % sample != 0) bc.add((short) i);
    }
    return bc;
  }

  @Test
  public void testToString() {
    BitmapContainer bc2 = new BitmapContainer(5, 15);
    bc2.add((short) -19);
    bc2.add((short) -3);
    assertEquals("{5,6,7,8,9,10,11,12,13,14,65517,65533}", bc2.toString());
  }

  @Test  
  public void testXOR() {
    BitmapContainer bc = new BitmapContainer(100,10000);
    BitmapContainer bc2 = new BitmapContainer();
    BitmapContainer bc3 = new BitmapContainer();

    for(int i = 100; i < 10000; ++i) {
      if((i%2 ) == 0)
        bc2 = (BitmapContainer) bc2.add((short) i);
        else bc3 = (BitmapContainer) bc3.add((short) i);
    }
    bc = (BitmapContainer) bc.ixor(bc2);
    assertTrue(bc.ixor(bc3).getCardinality() == 0);
  }
  
  @Test  
  public void testANDNOT() {
    BitmapContainer bc = new BitmapContainer(100,10000);
    BitmapContainer bc2 = new BitmapContainer();
    BitmapContainer bc3 = new BitmapContainer();

    for(int i = 100; i < 10000; ++i) {
      if((i%2 ) == 0)
        bc2 = (BitmapContainer) bc2.add((short) i);
        else bc3 = (BitmapContainer) bc3.add((short) i);
    }
    RunContainer rc = new RunContainer();
    rc.iadd(0, 1<<16);
    bc = (BitmapContainer) bc.iand(rc);
    bc = (BitmapContainer) bc.iandNot(bc2);
    assertTrue(bc.equals(bc3));
    assertTrue(bc.hashCode() == bc3.hashCode());
    assertTrue(bc.iandNot(bc3).getCardinality() == 0);
    bc3.clear();
    assertTrue(bc3.getCardinality() == 0);
  }
  

  @Test  
  public void testAND() {
    BitmapContainer bc = new BitmapContainer(100,10000);
    BitmapContainer bc2 = new BitmapContainer();
    BitmapContainer bc3 = new BitmapContainer();

    for(int i = 100; i < 10000; ++i) {
      if((i%2 ) == 0)
        bc2 = (BitmapContainer) bc2.add((short) i);
        else bc3 = (BitmapContainer) bc3.add((short) i);
    }
    bc = (BitmapContainer) bc.iand(bc2);
    assertTrue(bc.equals(bc2));    
    assertTrue(bc.iand(bc3).getCardinality() == 0);    
  }

  

  @Test  
  public void testOR() {
    BitmapContainer bc = new BitmapContainer(100,10000);
    BitmapContainer bc2 = new BitmapContainer();
    BitmapContainer bc3 = new BitmapContainer();

    for(int i = 100; i < 10000; ++i) {
      if((i%2 ) == 0)
        bc2 = (BitmapContainer) bc2.add((short) i);
        else bc3 = (BitmapContainer) bc3.add((short) i);
    }
    bc2 = (BitmapContainer) bc2.ior(bc3);
    assertTrue(bc.equals(bc2));        
    bc2 = (BitmapContainer) bc2.ior(bc);
    assertTrue(bc.equals(bc2));       
    RunContainer rc = new RunContainer();
    rc.iadd(0, 1<<16);
    assertEquals(0, bc.iandNot(rc).getCardinality());
  }

  @Test
  public void testLazyORFull() {
    BitmapContainer bc = new BitmapContainer(0, 1 << 15);
    BitmapContainer bc2 = new BitmapContainer(3210, 1 << 16);
    Container result = bc.lazyor(bc2);
    Container iresult = bc.ilazyor(bc2);
    assertEquals(-1, result.getCardinality());
    assertEquals(-1, iresult.getCardinality());
    Container repaired = result.repairAfterLazy();
    Container irepaired = iresult.repairAfterLazy();
    assertEquals(1 << 16, repaired.getCardinality());
    assertEquals(1 << 16, irepaired.getCardinality());
    assertThat(repaired, instanceOf(RunContainer.class));
    assertThat(irepaired, instanceOf(RunContainer.class));
  }

  @Test
  public void testLazyORFull2() {
    BitmapContainer bc = new BitmapContainer((1 << 10) - 200, 1 << 16);
    ArrayContainer ac = new ArrayContainer(0, 1 << 10);
    Container result = bc.lazyor(ac);
    Container iresult = bc.ilazyor(ac);
    assertEquals(-1, result.getCardinality());
    assertEquals(-1, iresult.getCardinality());
    Container repaired = result.repairAfterLazy();
    Container irepaired = iresult.repairAfterLazy();
    assertEquals(1 << 16, repaired.getCardinality());
    assertEquals(1 << 16, irepaired.getCardinality());
    assertThat(repaired, instanceOf(RunContainer.class));
    assertThat(irepaired, instanceOf(RunContainer.class));
  }

  @Test
  public void testLazyORFull3() {
    BitmapContainer bc = new BitmapContainer(0, 1 << 15);
    Container rc = Container.rangeOfOnes(1 << 15, 1 << 16);
    Container result = bc.lazyor((RunContainer) rc);
    Container iresult = bc.ilazyor((RunContainer) rc);
    assertEquals(-1, result.getCardinality());
    assertEquals(-1, iresult.getCardinality());
    Container repaired = result.repairAfterLazy();
    Container irepaired = iresult.repairAfterLazy();
    assertEquals(1 << 16, repaired.getCardinality());
    assertEquals(1 << 16, irepaired.getCardinality());
    assertThat(repaired, instanceOf(RunContainer.class));
    assertThat(irepaired, instanceOf(RunContainer.class));
  }

  @Test
  public void runConstructorForBitmap() {
    System.out.println("runConstructorForBitmap");
    for(int start = 0; start <= (1<<16); start += 4096 ) {
      for(int end = start; end <= (1<<16); end += 4096 ) {
        BitmapContainer bc = new BitmapContainer(start,end);
        BitmapContainer bc2 = new BitmapContainer();
        BitmapContainer bc3 = (BitmapContainer) bc2.add(start,end);
        bc2 = (BitmapContainer) bc2.iadd(start,end);
        assertEquals(bc.getCardinality(), end-start);
        assertEquals(bc2.getCardinality(), end-start);
        assertEquals(bc, bc2);
        assertEquals(bc, bc3);
        assertEquals(0,bc2.remove(start, end).getCardinality());
        assertEquals(bc2.getCardinality(), end-start);
        assertEquals(0,bc2.not(start, end).getCardinality());
      }  
    }
  }

  @Test
  public void runConstructorForBitmap2() {
    System.out.println("runConstructorForBitmap2");
    for(int start = 0; start <= (1<<16); start += 63 ) {
      for(int end = start; end <= (1<<16); end += 63 ) {
        BitmapContainer bc = new BitmapContainer(start,end);
        BitmapContainer bc2 = new BitmapContainer();
        BitmapContainer bc3 = (BitmapContainer) bc2.add(start,end);
        bc2 = (BitmapContainer) bc2.iadd(start,end);
        assertEquals(bc.getCardinality(), end-start);
        assertEquals(bc2.getCardinality(), end-start);
        assertEquals(bc, bc2);
        assertEquals(bc, bc3);
        assertEquals(0,bc2.remove(start, end).getCardinality());
        assertEquals(bc2.getCardinality(), end-start);
        assertEquals(0,bc2.not(start, end).getCardinality());
      }  
    }
  }

  @Test
  public void testRangeCardinality() {
    BitmapContainer bc = generateContainer((short)100, (short)10000, 5);
    bc = (BitmapContainer) bc.add(200, 2000);
    assertEquals(8280, bc.cardinality);
  }

  @Test
  public void testRangeCardinality2() {
    BitmapContainer bc = generateContainer((short)100, (short)10000, 5);
    bc.iadd(200, 2000);
    assertEquals(8280, bc.cardinality);
  }

  @Test
  public void testRangeCardinality3() {
    BitmapContainer bc = generateContainer((short)100, (short)10000, 5);
    RunContainer rc = new RunContainer(new short[]{7, 300, 400, 900, 1400, 2200}, 3);
    bc.ior(rc);
    assertEquals(8677, bc.cardinality);
  }

  @Test
  public void testRangeCardinality4() {
    BitmapContainer bc = generateContainer((short)100, (short)10000, 5);
    RunContainer rc = new RunContainer(new short[]{7, 300, 400, 900, 1400, 2200}, 3);
    bc = (BitmapContainer) bc.andNot(rc);
    assertEquals(5274, bc.cardinality);
  }

  @Test
  public void testRangeCardinality5() {
    BitmapContainer bc = generateContainer((short)100, (short)10000, 5);
    RunContainer rc = new RunContainer(new short[]{7, 300, 400, 900, 1400, 2200}, 3);
    bc.iandNot(rc);
    assertEquals(5274, bc.cardinality);
  }

  @Test
  public void testRangeCardinality6() {
    BitmapContainer bc = generateContainer((short)100, (short)10000, 5);
    RunContainer rc = new RunContainer(new short[]{7, 300, 400, 900, 1400, 5200}, 3);
    bc = (BitmapContainer) bc.iand(rc);
    assertEquals(5046, bc.cardinality);
  }

  @Test
  public void testRangeCardinality7() {
    BitmapContainer bc = generateContainer((short)100, (short)10000, 5);
    RunContainer rc = new RunContainer(new short[]{7, 300, 400, 900, 1400, 2200}, 3);
    bc.ixor(rc);
    assertEquals(6031, bc.cardinality);
  }

  @Test
  public void numberOfRunsLowerBound1() {
    System.out.println("numberOfRunsLowerBound1");
    Random r = new Random(12345);

    for (double density = 0.001; density < 0.8; density *= 2) {

      ArrayList<Integer> values = new ArrayList<Integer>();
      for (int i = 0; i < 65536; ++i) {
        if (r.nextDouble() < density) {
          values.add(i);
        }
      }
      Integer[] positions = values.toArray(new Integer[0]);
      BitmapContainer bc = new BitmapContainer();

      for (int position : positions) {
        bc.add((short) position);
      }

      assertTrue(bc.numberOfRunsLowerBound(1) > 1);
      assertTrue(bc.numberOfRunsLowerBound(100) <= bc.numberOfRuns());

      // a big parameter like 100000 ensures that the full lower bound
      // is taken


      assertTrue(bc.numberOfRunsLowerBound(100000) <= bc.numberOfRuns());
      assertEquals(bc.numberOfRuns(),
          bc.numberOfRunsLowerBound(100000) + bc.numberOfRunsAdjustment());

      /*
       * the unrolled guys are commented out, did not help performance and slated for removal
       * soon...
       * 
       * assertTrue(bc.numberOfRunsLowerBoundUnrolled2(1) > 1);
       * assertTrue(bc.numberOfRunsLowerBoundUnrolled2(100) <= bc.numberOfRuns());
       * 
       * assertEquals(bc.numberOfRunsLowerBound(100000),
       * bc.numberOfRunsLowerBoundUnrolled2(100000));
       */
    }

  }

  @Test(expected = ArrayIndexOutOfBoundsException.class)
  public void testNextTooLarge() {
    emptyContainer().nextSetBit(Short.MAX_VALUE + 1);
  }

  @Test(expected = ArrayIndexOutOfBoundsException.class)
  public void testNextTooSmall() {
    emptyContainer().nextSetBit(-1);
  }

  @Test(expected = ArrayIndexOutOfBoundsException.class)
  public void testPreviousTooLarge() {
    emptyContainer().prevSetBit(Short.MAX_VALUE + 1);
  }


  @Test(expected = ArrayIndexOutOfBoundsException.class)
  public void testPreviousTooSmall() {
    emptyContainer().prevSetBit(-1);
  }


  @Test(expected = IllegalArgumentException.class)
  public void addInvalidRange() {
    Container bc = new BitmapContainer();
    bc.add(13,1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void iaddInvalidRange() {
    Container bc = new BitmapContainer();
    bc.iadd(13,1);
  }

  @Test
  public void roundtrip() throws Exception {
    Container bc = new BitmapContainer();
    bc = bc.add(1, 5);
    final ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (ObjectOutputStream oo = new ObjectOutputStream(bos)) {
      bc.writeExternal(oo);
    }
    Container bc2 = new BitmapContainer();
    final ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    bc2.readExternal(new ObjectInputStream(bis));

    assertEquals(4, bc2.getCardinality());
    for (int i = 1; i < 5; i++) {
      assertTrue(bc2.contains((short) i));
    }
  }

  @Test
  public void iorRun() {
    Container bc = new BitmapContainer();
    bc = bc.add(1, 5);
    Container rc = new RunContainer();
    rc = rc.add(4, 10);
    bc.ior(rc);
    assertEquals(9, bc.getCardinality());
    for (int i = 1; i < 10; i++) {
      assertTrue(bc.contains((short) i));
    }
  }

  @Test
  public void orFullToRunContainer() {
    BitmapContainer bc = new BitmapContainer(0, 1 << 15);
    BitmapContainer half = new BitmapContainer(1 << 15, 1 << 16);
    Container result = bc.or(half);
    assertEquals(1 << 16, result.getCardinality());
    assertThat(result, instanceOf(RunContainer.class));
  }

  @Test
  public void orFullToRunContainer2() {
    BitmapContainer bc = new BitmapContainer(0, 1 << 15);
    ArrayContainer half = new ArrayContainer(1 << 15, 1 << 16);
    Container result = bc.or(half);
    assertEquals(1 << 16, result.getCardinality());
    assertThat(result, instanceOf(RunContainer.class));
  }

  @Test
  public void orFullToRunContainer3() {
    BitmapContainer bc = new BitmapContainer(0, 1 << 15);
    BitmapContainer bc2 = new BitmapContainer(3210, 1 << 16);
    Container result = bc.or(bc2);
    Container iresult = bc.ior(bc2);
    assertEquals(1 << 16, result.getCardinality());
    assertEquals(1 << 16, iresult.getCardinality());
    assertThat(result, instanceOf(RunContainer.class));
    assertThat(iresult, instanceOf(RunContainer.class));
  }

  @Test
  public void orFullToRunContainer4() {
    BitmapContainer bc = new BitmapContainer(0, 1 << 15);
    Container bc2 = Container.rangeOfOnes(3210, 1 << 16);
    Container iresult = bc.ior(bc2);
    assertEquals(1 << 16, iresult.getCardinality());
    assertThat(iresult, instanceOf(RunContainer.class));
  }

  @Test
  public void iremoveEmptyRange() {
    Container bc = new BitmapContainer();
    bc = bc.iremove(1,1);
    assertEquals(0, bc.getCardinality());
  }

  @Test(expected = IllegalArgumentException.class)
  public void iremoveInvalidRange() {
    Container ac = new BitmapContainer();
    ac.iremove(13,1);
  }

  @Test
  public void iremove() {
    Container bc = new BitmapContainer();
    bc = bc.add(1,10);
    bc = bc.iremove(5,10);
    assertEquals(4, bc.getCardinality());
    for (int i = 1; i < 5; i++) {
      assertTrue(bc.contains((short) i));
    }
  }

  @Test
  public void iremove2() {
    Container bc = new BitmapContainer();
    bc = bc.add(1,8092);
    bc = bc.iremove(1,10);
    assertEquals(8082, bc.getCardinality());
    for (int i = 10; i < 8092; i++) {
      assertTrue(bc.contains((short) i));
    }
  }

  @Test
  public void ixorRun() {
    Container bc = new BitmapContainer();
    bc = bc.add(1,10);
    Container rc = new RunContainer();
    rc = rc.add(5, 15);
    bc = bc.ixor(rc);
    assertEquals(9, bc.getCardinality());
    for (int i = 1; i < 5; i++) {
      assertTrue(bc.contains((short) i));
    }
    for (int i = 10; i < 15; i++) {
      assertTrue(bc.contains((short) i));
    }
  }

  @Test
  public void ixorRun2() {
    Container bc = new BitmapContainer();
    bc = bc.add(1,8092);
    Container rc = new RunContainer();
    rc = rc.add(1, 10);
    bc = bc.ixor(rc);
    assertEquals(8082, bc.getCardinality());
    for (int i = 10; i < 8092; i++) {
      assertTrue(bc.contains((short) i));
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void selectInvalidPosition() {
    Container bc = new BitmapContainer();
    bc = bc.add(1,13);
    bc.select(100);
  }

  @Test(expected = IllegalArgumentException.class)
  public void removeInvalidRange() {
    Container ac = new BitmapContainer();
    ac.remove(13,1);
  }

  @Test
  public void remove() {
    Container bc = new BitmapContainer();
    bc = bc.add(1,8092);
    bc = bc.remove(1,10);
    assertEquals(8082, bc.getCardinality());
    for (int i = 10; i < 8092; i++) {
      assertTrue(bc.contains((short) i));
    }
  }

  @Test
  public void iandRun() {
    Container bc = new BitmapContainer();
    bc = bc.add(0,8092);
    Container rc = new RunContainer();
    rc = rc.add(1, 10);
    bc = bc.iand(rc);
    assertEquals(9, bc.getCardinality());
    for (int i = 1; i < 10; i++) {
      assertTrue(bc.contains((short) i));
    }
  }

  @Test(expected = NoSuchElementException.class)
  public void testFirst_Empty() {
    new BitmapContainer().first();
  }

  @Test(expected = NoSuchElementException.class)
  public void testLast_Empty() {
    new BitmapContainer().last();
  }

  @Test
  public void testFirstLast() {
    Container rc = new ArrayContainer();
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
  public void testContainsBitmapContainer_EmptyContainsEmpty() {
    Container bc = new BitmapContainer();
    Container subset = new BitmapContainer();
    assertTrue(bc.contains(subset));
  }

  @Test
  public void testContainsBitmapContainer_IncludeProperSubset() {
    Container bc = new BitmapContainer().add(0,10);
    Container subset = new BitmapContainer().add(0,9);
    assertTrue(bc.contains(subset));
  }

  @Test
  public void testContainsBitmapContainer_IncludeSelf() {
    Container bc = new BitmapContainer().add(0,10);
    Container subset = new BitmapContainer().add(0,10);
    assertTrue(bc.contains(subset));
  }

  @Test
  public void testContainsBitmapContainer_ExcludeSuperSet() {
    Container bc = new BitmapContainer().add(0,10);
    Container superset = new BitmapContainer().add(0,20);
    assertFalse(bc.contains(superset));
  }

  @Test
  public void testContainsBitmapContainer_IncludeProperSubsetDifferentStart() {
    Container bc = new BitmapContainer().add(0,10);
    Container subset = new RunContainer().add(2,9);
    assertTrue(bc.contains(subset));
  }

  @Test
  public void testContainsBitmapContainer_ExcludeShiftedSet() {
    Container bc = new BitmapContainer().add(0,10);
    Container shifted = new BitmapContainer().add(2,12);
    assertFalse(bc.contains(shifted));
  }

  @Test
  public void testContainsBitmapContainer_ExcludeDisJointSet() {
    Container bc = new BitmapContainer().add(0,10);
    Container disjoint = new BitmapContainer().add(20, 40);
    assertFalse(bc.contains(disjoint));
    assertFalse(disjoint.contains(bc));
  }

  @Test
  public void testContainsRunContainer_EmptyContainsEmpty() {
    Container bc = new BitmapContainer();
    Container subset = new BitmapContainer();
    assertTrue(bc.contains(subset));
  }

  @Test
  public void testContainsRunContainer_IncludeProperSubset() {
    Container bc = new BitmapContainer().add(0,10);
    Container subset = new RunContainer().add(0,9);
    assertTrue(bc.contains(subset));
  }

  @Test
  public void testContainsRunContainer_IncludeSelf() {
    Container bc = new BitmapContainer().add(0,10);
    Container subset = new RunContainer().add(0,10);
    assertTrue(bc.contains(subset));
  }

  @Test
  public void testContainsRunContainer_ExcludeSuperSet() {
    Container bc = new BitmapContainer().add(0,10);
    Container superset = new RunContainer().add(0,20);
    assertFalse(bc.contains(superset));
  }

  @Test
  public void testContainsRunContainer_IncludeProperSubsetDifferentStart() {
    Container bc = new BitmapContainer().add(0,10);
    Container subset = new RunContainer().add(2,9);
    assertTrue(bc.contains(subset));
  }

  @Test
  public void testContainsRunContainer_ExcludeShiftedSet() {
    Container bc = new BitmapContainer().add(0,10);
    Container shifted = new RunContainer().add(2,12);
    assertFalse(bc.contains(shifted));
  }

  @Test
  public void testContainsRunContainer_ExcludeDisJointSet() {
    Container bc = new BitmapContainer().add(0,10);
    Container disjoint = new RunContainer().add(20, 40);
    assertFalse(bc.contains(disjoint));
    assertFalse(disjoint.contains(bc));
  }

  @Test
  public void testContainsArrayContainer_EmptyContainsEmpty() {
    Container bc = new BitmapContainer();
    Container subset = new ArrayContainer();
    assertTrue(bc.contains(subset));
  }

  @Test
  public void testContainsArrayContainer_IncludeProperSubset() {
    Container bc = new BitmapContainer().add(0,10);
    Container subset = new ArrayContainer().add(0,9);
    assertTrue(bc.contains(subset));
  }

  @Test
  public void testContainsArrayContainer_IncludeSelf() {
    Container bc = new BitmapContainer().add(0,10);
    Container subset = new ArrayContainer().add(0,10);
    assertTrue(bc.contains(subset));
  }

  @Test
  public void testContainsArrayContainer_ExcludeSuperSet() {
    Container bc = new BitmapContainer().add(0,10);
    Container superset = new ArrayContainer().add(0,20);
    assertFalse(bc.contains(superset));
  }

  @Test
  public void testContainsArrayContainer_IncludeProperSubsetDifferentStart() {
    Container bc = new BitmapContainer().add(0,10);
    Container subset = new ArrayContainer().add(2,9);
    assertTrue(bc.contains(subset));
  }

  @Test
  public void testContainsArrayContainer_ExcludeShiftedSet() {
    Container bc = new BitmapContainer().add(0,10);
    Container shifted = new ArrayContainer().add(2,12);
    assertFalse(bc.contains(shifted));
  }

  @Test
  public void testContainsArrayContainer_ExcludeDisJointSet() {
    Container bc = new BitmapContainer().add(0, 10);
    Container disjoint = new ArrayContainer().add(20, 40);
    assertFalse(bc.contains(disjoint));
    assertFalse(disjoint.contains(bc));
  }

  @Test
  public void testIntersectsWithRange() {
    Container container = new BitmapContainer().add(0, 10);
    assertTrue(container.intersects(0, 1));
    assertTrue(container.intersects(0, 101));
    assertTrue(container.intersects(0, 1 << 16));
    assertFalse(container.intersects(11, lower16Bits(-1)));
  }


  @Test
  public void testIntersectsWithRangeHitScan() {
    Container container = new BitmapContainer().add(0, 10)
            .add(500, 512).add(lower16Bits(-50), lower16Bits(-10));
    assertTrue(container.intersects(0, 1));
    assertTrue(container.intersects(0, 101));
    assertTrue(container.intersects(0, 1 << 16));
    assertTrue(container.intersects(11, 1 << 16));
    assertTrue(container.intersects(501, 511));
  }


  @Test
  public void testIntersectsWithRangeUnsigned() {
    Container container = new BitmapContainer().add(lower16Bits(-50), lower16Bits(-10));
    assertFalse(container.intersects(0, 1));
    assertTrue(container.intersects(0, lower16Bits(-40)));
    assertFalse(container.intersects(lower16Bits(-100), lower16Bits(-55)));
    assertFalse(container.intersects(lower16Bits(-9), lower16Bits(-1)));
    assertTrue(container.intersects(11, (short)-1));
  }

  @Test
  public void testIntersectsAtEndWord() {
    Container container = new BitmapContainer().add(lower16Bits(-500), lower16Bits(-10));
    assertTrue(container.intersects(lower16Bits(-50), lower16Bits(-10)));
    assertTrue(container.intersects(lower16Bits(-400), lower16Bits(-11)));
    assertTrue(container.intersects(lower16Bits(-11), lower16Bits(-1)));
    assertFalse(container.intersects(lower16Bits(-10), lower16Bits(-1)));
  }


  @Test
  public void testIntersectsAtEndWord2() {
    Container container = new BitmapContainer().add(lower16Bits(500), lower16Bits(-500));
    assertTrue(container.intersects(lower16Bits(-650), lower16Bits(-500)));
    assertTrue(container.intersects(lower16Bits(-501), lower16Bits(-1)));
    assertFalse(container.intersects(lower16Bits(-500), lower16Bits(-1)));
    assertFalse(container.intersects(lower16Bits(-499), 1 << 16));
  }

  @Test
  public void testContainsRangeSingleWord() {
    long[] bitmap = oddBits();
    bitmap[10] = -1L;
    int cardinality = 32 + 1 << 15;
    BitmapContainer container = new BitmapContainer(bitmap, cardinality);
    assertTrue(container.contains(0, 1));
    assertTrue(container.contains(64 * 10, 64 * 11));
    assertFalse(container.contains(64 * 10, 2 + 64 * 11));
    assertTrue(container.contains(1 + 64 * 10, (64 * 11) - 1));
  }

  @Test
  public void testContainsRangeMultiWord() {
    long[] bitmap = oddBits();
    bitmap[10] = -1L;
    bitmap[11] = -1L;
    bitmap[12] |= ((1L << 32) - 1);
    int cardinality = 32 + 32 + 16 + 1 << 15;
    BitmapContainer container = new BitmapContainer(bitmap, cardinality);
    assertTrue(container.contains(0, 1));
    assertFalse(container.contains(64 * 10, (64 * 13) - 30));
    assertTrue(container.contains(64 * 10, (64 * 13) - 31));
    assertTrue(container.contains(1 + 64 * 10, (64 * 13) - 32));
    assertTrue(container.contains(64 * 10, 64 * 12));
    assertFalse(container.contains(64 * 10, 2 + 64 * 13));
  }


  @Test
  public void testContainsRangeSubWord() {
    long[] bitmap = oddBits();
    bitmap[bitmap.length - 1] = ~((1L << 63) | 1L);
    int cardinality = 32 + 32 + 16 + 1 << 15;
    BitmapContainer container = new BitmapContainer(bitmap, cardinality);
    assertFalse(container.contains(64 * 1023, 64 * 1024));
    assertFalse(container.contains(64 * 1023, 64 * 1024 - 1));
    assertTrue(container.contains(1 + 64 * 1023, 64 * 1024 - 1));
    assertTrue(container.contains(1 + 64 * 1023, 64 * 1024 - 2));
    assertFalse(container.contains(64 * 1023, 64 * 1023 + 2));
    assertTrue(container.contains(64 * 1023 + 1, 64 * 1023 + 2));
  }

  private static long[] oddBits() {
    long[] bitmap = new long[1 << 10];
    Arrays.fill(bitmap, 0x5555555555555555L);
    return bitmap;
  }

  private static int lower16Bits(int x) {
    return ((short)x) & 0xFFFF;
  }
}
