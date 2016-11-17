/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap.buffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.roaringbitmap.buffer.MappeableBitmapContainer.MAX_CAPACITY;
import static org.roaringbitmap.buffer.TestMappeableArrayContainer.newArrayContainer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import org.junit.Test;
import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.ShortIterator;

import static org.hamcrest.CoreMatchers.instanceOf;


public class TestMappeableBitmapContainer {

  private static MappeableBitmapContainer emptyContainer() {
    return new MappeableBitmapContainer(0, LongBuffer.allocate(1));
  }
  
  @Test
  public void testToString() {
    MappeableBitmapContainer bc2 = new MappeableBitmapContainer();
    bc2.add((short)1);
    String s = bc2.toString();
    assertTrue(s.equals("{1}"));
  }
  
  @Test  
  public void testXOR() {
    MappeableBitmapContainer bc = new MappeableBitmapContainer(100,10000);
    MappeableBitmapContainer bc2 = new MappeableBitmapContainer();
    MappeableBitmapContainer bc3 = new MappeableBitmapContainer();

    for(int i = 100; i < 10000; ++i) {
      if((i%2 ) == 0)
        bc2 = (MappeableBitmapContainer) bc2.add((short) i);
        else bc3 = (MappeableBitmapContainer) bc3.add((short) i);
    }
    bc = (MappeableBitmapContainer) bc.ixor(bc2);
    assertTrue(bc.ixor(bc3).getCardinality() == 0);
  }
  
  @Test  
  public void testANDNOT() {
    MappeableBitmapContainer bc = new MappeableBitmapContainer(100,10000);
    MappeableBitmapContainer bc2 = new MappeableBitmapContainer();
    MappeableBitmapContainer bc3 = new MappeableBitmapContainer();

    for(int i = 100; i < 10000; ++i) {
      if((i%2 ) == 0)
        bc2 = (MappeableBitmapContainer) bc2.add((short) i);
        else bc3 = (MappeableBitmapContainer) bc3.add((short) i);
    }
    bc = (MappeableBitmapContainer) bc.iandNot(bc2);
    assertTrue(bc.equals(bc3));
    assertTrue(bc.hashCode() == bc3.hashCode());
    assertTrue(bc.iandNot(bc3).getCardinality() == 0);
    bc3.clear();
    assertTrue(bc3.getCardinality() == 0);
  }
  

  @Test  
  public void testAND() {
    MappeableBitmapContainer bc = new MappeableBitmapContainer(100,10000);
    MappeableBitmapContainer bc2 = new MappeableBitmapContainer();
    MappeableBitmapContainer bc3 = new MappeableBitmapContainer();
    

    for(int i = 100; i < 10000; ++i) {
      if((i%2 ) == 0)
        bc2 = (MappeableBitmapContainer) bc2.add((short) i);
        else bc3 = (MappeableBitmapContainer) bc3.add((short) i);
    }
    MappeableRunContainer rc = new MappeableRunContainer();
    rc.iadd(0, 1<<16);
    bc = (MappeableBitmapContainer) bc.iand(rc);
    bc = (MappeableBitmapContainer) bc.iand(bc2);
    assertTrue(bc.equals(bc2));    
    assertTrue(bc.iand(bc3).getCardinality() == 0);    
  }

  

  @Test  
  public void testOR() {
    MappeableBitmapContainer bc = new MappeableBitmapContainer(100,10000);
    MappeableBitmapContainer bc2 = new MappeableBitmapContainer();
    MappeableBitmapContainer bc3 = new MappeableBitmapContainer();

    for(int i = 100; i < 10000; ++i) {
      if((i%2 ) == 0)
        bc2 = (MappeableBitmapContainer) bc2.add((short) i);
        else bc3 = (MappeableBitmapContainer) bc3.add((short) i);
    }
    bc2 = (MappeableBitmapContainer) bc2.ior(bc3);
    assertTrue(bc.equals(bc2));        
    bc2 = (MappeableBitmapContainer) bc2.ior(bc);
    assertTrue(bc.equals(bc2));       
    MappeableRunContainer rc = new MappeableRunContainer();
    rc.iadd(0, 1<<16);
    assertTrue(bc.iandNot(rc).getCardinality() == 0);      
  }

  private static void removeArray(MappeableBitmapContainer bc) {
    LongBuffer array = ByteBuffer.allocateDirect((1<<16)/8).asLongBuffer();
    for(int k = 0; k < bc.bitmap.limit(); ++k)
      array.put(k, bc.bitmap.get(k));
    bc.bitmap = array;
  }

  @Test  
  public void testXORNoArray() {
    MappeableBitmapContainer bc = new MappeableBitmapContainer(100,10000);
    MappeableBitmapContainer bc2 = new MappeableBitmapContainer();
    MappeableBitmapContainer bc3 = new MappeableBitmapContainer();

    for(int i = 100; i < 10000; ++i) {
      if((i%2 ) == 0)
        bc2 = (MappeableBitmapContainer) bc2.add((short) i);
        else bc3 = (MappeableBitmapContainer) bc3.add((short) i);
    }
    removeArray(bc2);
    removeArray(bc3);
    bc = (MappeableBitmapContainer) bc.ixor(bc2);
    assertTrue(bc.ixor(bc3).getCardinality() == 0);
  }
  
  @Test  
  public void testANDNOTNoArray() {
    MappeableBitmapContainer bc = new MappeableBitmapContainer(100,10000);
    MappeableBitmapContainer bc2 = new MappeableBitmapContainer();
    MappeableBitmapContainer bc3 = new MappeableBitmapContainer();

    for(int i = 100; i < 10000; ++i) {
      if((i%2 ) == 0)
        bc2 = (MappeableBitmapContainer) bc2.add((short) i);
        else bc3 = (MappeableBitmapContainer) bc3.add((short) i);
    }
    removeArray(bc2);
    removeArray(bc3);
    bc = (MappeableBitmapContainer) bc.iandNot(bc2);
    assertTrue(bc.equals(bc3));
  }
  

  @Test  
  public void testANDNoArray() {
    MappeableBitmapContainer bc = new MappeableBitmapContainer(100,10000);
    MappeableBitmapContainer bc2 = new MappeableBitmapContainer();
    MappeableBitmapContainer bc3 = new MappeableBitmapContainer();

    for(int i = 100; i < 10000; ++i) {
      if((i%2 ) == 0)
        bc2 = (MappeableBitmapContainer) bc2.add((short) i);
        else bc3 = (MappeableBitmapContainer) bc3.add((short) i);
    }
    removeArray(bc);
    removeArray(bc2);
    removeArray(bc3);
    bc = (MappeableBitmapContainer) bc.iand(bc2);
    assertTrue(bc.equals(bc2));
    removeArray(bc);    
    assertTrue(bc.iand(bc3).getCardinality() == 0);    
  }

  

  @Test  
  public void testORNoArray() {
    MappeableBitmapContainer bc = new MappeableBitmapContainer(100,10000);
    MappeableBitmapContainer bc2 = new MappeableBitmapContainer();
    MappeableBitmapContainer bc3 = new MappeableBitmapContainer();

    for(int i = 100; i < 10000; ++i) {
      if((i%2 ) == 0)
        bc2 = (MappeableBitmapContainer) bc2.add((short) i);
        else bc3 = (MappeableBitmapContainer) bc3.add((short) i);
    }
    removeArray(bc);
    removeArray(bc3);
    bc2 = (MappeableBitmapContainer) bc2.ior(bc3);
    assertTrue(bc.equals(bc2));
    bc2 = (MappeableBitmapContainer) bc2.ior(bc);
    assertTrue(bc.equals(bc2));       
  }
  
  @Test
  public void runConstructorForBitmap() {
    System.out.println("runConstructorForBitmap");
    for(int start = 0; start <= (1<<16); start += 4096 ) {
      for(int end = start; end <= (1<<16); end += 4096 ) {
        LongBuffer array = ByteBuffer.allocateDirect((1<<16)/8).asLongBuffer();
        if(array.hasArray()) throw new RuntimeException("unexpected.");
        MappeableBitmapContainer bc = new MappeableBitmapContainer(start,end);
        MappeableBitmapContainer bc2 = new MappeableBitmapContainer(array,0);
        MappeableBitmapContainer bc3 = (MappeableBitmapContainer) bc2.add(start,end);
        bc2.iadd(start,end);
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
        LongBuffer array = ByteBuffer.allocateDirect((1<<16)/8).asLongBuffer();
        MappeableBitmapContainer bc = new MappeableBitmapContainer(start,end);
        MappeableBitmapContainer bc2 = new MappeableBitmapContainer(array,0);
        MappeableBitmapContainer bc3 = (MappeableBitmapContainer) bc2.add(start,end);
        bc2.iadd(start,end);
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

 
  @Test(expected = IndexOutOfBoundsException.class)
  public void testNextTooLarge() {
    emptyContainer().nextSetBit(Short.MAX_VALUE + 1);
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testNextTooSmall() {
    emptyContainer().nextSetBit(-1);
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testPreviousTooLarge() {
    emptyContainer().prevSetBit(Short.MAX_VALUE + 1);
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testPreviousTooSmall() {
    emptyContainer().prevSetBit(-1);
  }

  @Test(expected = RuntimeException.class)
  public void addInvalidRange() {
    MappeableBitmapContainer bc = new MappeableBitmapContainer();
    bc.add(10,1);
  }

  @Test(expected = RuntimeException.class)
  public void iaddInvalidRange() {
    MappeableBitmapContainer bc = new MappeableBitmapContainer();
    bc.iadd(10,1);
  }

  @Test
  public void iand() {
    MappeableBitmapContainer bc = new MappeableBitmapContainer();
    MappeableRunContainer rc = new MappeableRunContainer();
    bc.iadd(1,13);
    rc.iadd(5,27);
    MappeableContainer result = bc.iand(rc);
    assertEquals(8, result.getCardinality());
    for (short i = 5; i < 13; i++) {
      assertTrue(result.contains(i));
    }
  }

  @Test
  public void ior() {
    MappeableBitmapContainer bc = new MappeableBitmapContainer();
    MappeableRunContainer rc = new MappeableRunContainer();
    bc.iadd(1,13);
    rc.iadd(5,27);
    MappeableContainer result = bc.ior(rc);
    assertEquals(26, result.getCardinality());
    for (short i = 1; i < 27; i++) {
      assertTrue(result.contains(i));
    }
  }

  @Test
  public void iremoveEmptyRange() {
    MappeableBitmapContainer bc = new MappeableBitmapContainer();
    bc.iremove(1,1);
    assertEquals(0, bc.getCardinality());
  }

  @Test(expected = IllegalArgumentException.class)
  public void iremoveInvalidRange() {
    MappeableBitmapContainer bc = new MappeableBitmapContainer();
    bc.iremove(13,1);
  }

  @Test
  public void iremove() {
    MappeableBitmapContainer bc = new MappeableBitmapContainer();
    bc.iremove(1,13);
    assertEquals(0, bc.getCardinality());
  }

  @Test
  public void iremove2() {
    MappeableContainer bc = new MappeableBitmapContainer();
    bc = bc.add(1, 8192);
    bc.iremove(1, 10);
    assertEquals(8182, bc.getCardinality());
    for (short i = 10; i < 8192; i++) {
      assertTrue(bc.contains(i));
    }
  }

  @Test
  public void numberOfRuns() {
    MappeableContainer bc = new MappeableBitmapContainer();
    bc = bc.add(1,13);
    bc = bc.add(19,27);
    assertEquals(2, bc.numberOfRuns());
  }

  @Test
  public void numberOfRuns2() {
    LongBuffer buffer = LongBuffer.allocate(MAX_CAPACITY / 64);
    buffer.put(~8L);
    MappeableContainer bc = new MappeableBitmapContainer(buffer.asReadOnlyBuffer(), 64);
    assertEquals(2, bc.numberOfRuns());
  }

  @Test(expected = IllegalArgumentException.class)
  public void selectInvalidPosition() {
    MappeableContainer bc = new MappeableBitmapContainer();
    bc = bc.add(1,13);
    bc.select(100);
  }

  @Test
  public void select() {
    LongBuffer buffer = LongBuffer.allocate(MAX_CAPACITY / 64);
    buffer.put(~0L);
    buffer.put(~0L);
    MappeableContainer bc = new MappeableBitmapContainer(buffer.asReadOnlyBuffer(), 64);
    assertEquals(100, bc.select(100));
  }

  @Test
  public void reverseShortIterator() {
    MappeableBitmapContainer bc = new MappeableBitmapContainer();
    bc = (MappeableBitmapContainer) bc.iadd(1,13);
    bc = (MappeableBitmapContainer) bc.iadd(10017,10029);
    ShortIterator iterator = new ReverseMappeableBitmapContainerShortIterator(bc);
    for (int i = 10028; i >= 10017; i--) {
      assertTrue(iterator.hasNext());
      assertEquals(i, iterator.next());
    }
    for (int i = 12; i >= 1; i--) {
      assertTrue(iterator.hasNext());
      assertEquals(i, iterator.next());
    }
    assertFalse(iterator.hasNext());
  }

  @Test
  public void andNotArray() {
    LongBuffer buffer = LongBuffer.allocate(MAX_CAPACITY / 64);
    buffer.put(~0L);
    MappeableContainer bc = new MappeableBitmapContainer(buffer.asReadOnlyBuffer(), 64);
    MappeableContainer ac = new MappeableArrayContainer();
    ac = ac.add(32, 64);
    bc = bc.andNot(ac);
    assertEquals(32, bc.getCardinality());
    for (short i = 0; i < 32; i++) {
      assertTrue(bc.contains(i));
    }
  }

  @Test
  public void andNotBitmap() {
    LongBuffer buffer = LongBuffer.allocate(MAX_CAPACITY / 64);
    buffer.put(~0L);
    MappeableContainer bc = new MappeableBitmapContainer(buffer.asReadOnlyBuffer(), 64);
    MappeableContainer bc2 = new MappeableBitmapContainer();
    bc2 = bc2.add(32, 64);
    bc = bc.andNot(bc2);
    assertEquals(32, bc.getCardinality());
    for (short i = 0; i < 32; i++) {
      assertTrue(bc.contains(i));
    }
  }

  @Test
  public void intersectsArray() {
    MappeableContainer bc = new MappeableBitmapContainer();
    bc = bc.add(1, 13);
    MappeableContainer ac = newArrayContainer(5, 10);
    assertTrue(bc.intersects(ac));
  }

  @Test
  public void intersectsBitmap() {
    LongBuffer buffer = LongBuffer.allocate(MAX_CAPACITY / 64);
    buffer.put(~0L);
    LongBuffer buffer2 = LongBuffer.allocate(MAX_CAPACITY / 64);
    buffer2.put(~1L);
    MappeableContainer bc = new MappeableBitmapContainer(buffer.asReadOnlyBuffer(), 64);
    MappeableContainer bc2 = new MappeableBitmapContainer(buffer2.asReadOnlyBuffer(), 64);
    assertTrue(bc.intersects(bc2));
  }

  @Test
  public void iorArray() {
    MappeableContainer bc = new MappeableBitmapContainer();
    bc = bc.add(1, 13);
    MappeableContainer ac = newArrayContainer(5, 15);
    bc = bc.ior(ac);
    assertEquals(14, bc.getCardinality());
    for (short i = 1; i < 15; i++) {
      assertTrue(bc.contains(i));
    }
  }

  @Test
  public void orArray() {
    MappeableContainer bc = new MappeableBitmapContainer();
    bc = bc.add(1, 13);
    MappeableContainer ac = newArrayContainer(5, 15);
    bc = bc.or(ac);
    assertEquals(14, bc.getCardinality());
    for (short i = 1; i < 15; i++) {
      assertTrue(bc.contains(i));
    }
  }

  @Test
  public void xorArray() {
    LongBuffer buffer = LongBuffer.allocate(MAX_CAPACITY / 64);
    buffer.put(~0L);
    MappeableContainer bc = new MappeableBitmapContainer(buffer.asReadOnlyBuffer(), 64);
    MappeableContainer ac = newArrayContainer(5, 15);
    bc = bc.xor(ac);
    assertEquals(54, bc.getCardinality());
    for (short i = 0; i < 5; i++) {
      assertTrue(bc.contains(i));
    }
    for (short i = 15; i < 64; i++) {
      assertTrue(bc.contains(i));
    }
  }

  @Test
  public void xorBitmap() {
    LongBuffer buffer = LongBuffer.allocate(MAX_CAPACITY / 64);
    buffer.put(~0L);
    MappeableContainer bc = new MappeableBitmapContainer(buffer.asReadOnlyBuffer(), 64);
    MappeableContainer bc2 = new MappeableBitmapContainer();
    bc2 = bc2.add(10, 64);
    bc = bc.xor(bc2);
    assertEquals(10, bc.getCardinality());
    for (short i = 0; i < 10; i++) {
      assertTrue(bc.contains(i));
    }
  }

  @Test
  public void xorBitmap2() {
    LongBuffer buffer = LongBuffer.allocate(MAX_CAPACITY / 64);
    for (int i = 0; i < 128; i++) {
      buffer.put(~0L);
    }
    MappeableContainer bc = new MappeableBitmapContainer(buffer.asReadOnlyBuffer(), 8192);
    MappeableContainer bc2 = new MappeableBitmapContainer();
    bc2 = bc2.add(5000, 8192);
    bc = bc.xor(bc2);
    assertEquals(5000, bc.getCardinality());
    for (short i = 0; i < 5000; i++) {
      assertTrue(bc.contains(i));
    }
  }

  @Test
  public void foreach() {
    LongBuffer buffer = LongBuffer.allocate(MAX_CAPACITY / 64);
    buffer.put(~0L);
    MappeableContainer bc = new MappeableBitmapContainer(buffer.asReadOnlyBuffer(), 64);
    bc.forEach((short) 0, new IntConsumer() {
      int expected = 0;

      @Override
      public void accept(int value) {
        assertEquals(value, expected++);
      }
    });
  }

  @Test
  public void roundtrip() throws Exception {
    LongBuffer buffer = LongBuffer.allocate(MAX_CAPACITY / 64);
    buffer.put(~0L);
    MappeableContainer bc = new MappeableBitmapContainer(buffer.asReadOnlyBuffer(), 64);
    final ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (ObjectOutputStream oo = new ObjectOutputStream(bos)) {
      bc.writeExternal(oo);
    }
    MappeableContainer bc2 = new MappeableBitmapContainer();
    final ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    bc2.readExternal(new ObjectInputStream(bis));

    assertEquals(64, bc2.getCardinality());
    for (int i = 0; i < 64; i++) {
      assertTrue(bc2.contains((short) i));
    }
  }

  @Test
  public void orFullToRunContainer() {
    MappeableBitmapContainer bc = new MappeableBitmapContainer(0, 1 << 15);
    MappeableBitmapContainer half = new MappeableBitmapContainer(1 << 15, 1 << 16);
    MappeableContainer result = bc.or(half);
    assertEquals(1 << 16, result.getCardinality());
    assertThat(result, instanceOf(MappeableRunContainer.class));
  }

  @Test
  public void orFullToRunContainer2() {
    MappeableBitmapContainer bc = new MappeableBitmapContainer(0, 1 << 15);
    MappeableArrayContainer half = new MappeableArrayContainer(1 << 15, 1 << 16);
    MappeableContainer result = bc.or(half);
    assertEquals(1 << 16, result.getCardinality());
    assertThat(result, instanceOf(MappeableRunContainer.class));
  }

  @Test
  public void testLazyORFull() {
    MappeableBitmapContainer bc = new MappeableBitmapContainer(0, 1 << 15);
    MappeableBitmapContainer bc2 = new MappeableBitmapContainer(3210, 1 << 16);
    MappeableContainer result = bc.lazyor(bc2);
    MappeableContainer iresult = bc.ilazyor(bc2);
    assertEquals(-1, result.getCardinality());
    assertEquals(-1, iresult.getCardinality());
    MappeableContainer repaired = result.repairAfterLazy();
    MappeableContainer irepaired = iresult.repairAfterLazy();
    assertEquals(1 << 16, repaired.getCardinality());
    assertEquals(1 << 16, irepaired.getCardinality());
    assertThat(repaired, instanceOf(MappeableRunContainer.class));
    assertThat(irepaired, instanceOf(MappeableRunContainer.class));
  }

  @Test
  public void orFullToRunContainer4() {
    MappeableBitmapContainer bc = new MappeableBitmapContainer(0, 1 << 15);
    MappeableContainer bc2 = MappeableContainer.rangeOfOnes(3210, 1 << 16);
    MappeableContainer iresult = bc.ior(bc2);
    assertEquals(1 << 16, iresult.getCardinality());
    assertThat(iresult, instanceOf(MappeableRunContainer.class));
  }

  @Test
  public void testLazyORFull2() {
    MappeableBitmapContainer bc = new MappeableBitmapContainer((1 << 10) - 200, 1 << 16);
    MappeableArrayContainer ac = new MappeableArrayContainer(0, 1 << 10);
    MappeableContainer result = bc.lazyor(ac);
    MappeableContainer iresult = bc.ilazyor(ac);
    assertEquals(-1, result.getCardinality());
    assertEquals(-1, iresult.getCardinality());
    MappeableContainer repaired = result.repairAfterLazy();
    MappeableContainer irepaired = iresult.repairAfterLazy();
    assertEquals(1 << 16, repaired.getCardinality());
    assertEquals(1 << 16, irepaired.getCardinality());
    assertThat(repaired, instanceOf(MappeableRunContainer.class));
    assertThat(irepaired, instanceOf(MappeableRunContainer.class));
  }

  @Test
  public void testLazyORFull3() {
    MappeableBitmapContainer bc = new MappeableBitmapContainer(0, 1 << 15);
    MappeableContainer rc = MappeableContainer.rangeOfOnes(1 << 15, 1 << 16);
    MappeableContainer result = bc.lazyor((MappeableRunContainer) rc);
    MappeableContainer iresult = bc.ilazyor((MappeableRunContainer) rc);
    assertEquals(-1, result.getCardinality());
    assertEquals(-1, iresult.getCardinality());
    MappeableContainer repaired = result.repairAfterLazy();
    MappeableContainer irepaired = iresult.repairAfterLazy();
    assertEquals(1 << 16, repaired.getCardinality());
    assertEquals(1 << 16, irepaired.getCardinality());
    assertThat(repaired, instanceOf(MappeableRunContainer.class));
    assertThat(irepaired, instanceOf(MappeableRunContainer.class));
  }

}
