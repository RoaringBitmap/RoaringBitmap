/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap.buffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import org.junit.Test;
import org.roaringbitmap.ShortIterator;


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
  public void numberOfRuns() {
    MappeableContainer bc = new MappeableBitmapContainer();
    bc = bc.add(1,13);
    bc = bc.add(19,27);
    assertEquals(2, bc.numberOfRuns());
  }

  @Test(expected = IllegalArgumentException.class)
  public void selectInvalidPosition() {
    MappeableContainer bc = new MappeableBitmapContainer();
    bc = bc.add(1,13);
    bc.select(100);
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

}
