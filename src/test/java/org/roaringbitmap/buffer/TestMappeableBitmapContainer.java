/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap.buffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import org.junit.Test;

public class TestMappeableBitmapContainer {

  private static MappeableBitmapContainer emptyContainer() {
    return new MappeableBitmapContainer(0, LongBuffer.allocate(1));
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
}
