package org.roaringbitmap;

import org.junit.Test;

import java.io.*;
import java.util.BitSet;
import java.util.Iterator;

import static org.junit.Assert.*;

public class TestRunContainer {

    @Test
    public void intersectionTest1() {
        Container ac = new ArrayContainer();
        Container ar = new RunContainer();
        for(int k = 0; k<100; ++k) {
            ac = ac.add((short) (k*10));
            ar = ar.add((short) (k*10));
        }
        assertEquals(ac, ac.and(ar));
        assertEquals(ac, ar.and(ac));        
    }
    
    @Test
    public void intersectionTest2() {
        Container ac = new ArrayContainer();
        Container ar = new RunContainer();
        for(int k = 0; k<10000; ++k) {
            ac = ac.add((short) k);
            ar = ar.add((short) k);
        }
        assertEquals(ac, ac.and(ar));
        assertEquals(ac, ar.and(ac));        
    }

    
    @Test
    public void equalTest1() {
        Container ac = new ArrayContainer();
        Container ar = new RunContainer();
        for(int k = 0; k<100; ++k) {
            ac = ac.add((short) (k*10));
            ar = ar.add((short) (k*10));
        }
        assertEquals(ac, ar);
    }

    @Test
    public void equalTest2() {
        Container ac = new ArrayContainer();
        Container ar = new RunContainer();
        for(int k = 0; k<10000; ++k) {
            ac = ac.add((short) k);
            ar = ar.add((short) k);
        }
        assertEquals(ac, ar);
    }

    
    @Test
    public void addRange() {
        for(int i = 0; i < 100; ++i) {
            for(int j = 0; j < 100; ++j) {
                for(int k = 0; k < 50; ++k) {
                    BitSet bs = new BitSet();
                    RunContainer container = new RunContainer();
                    for(int p = 0; p < i; ++p) {
                        container.add((short) p);
                        bs.set(p);
                    }
                    for(int p = 0; p < j; ++p) {
                        container.add((short) (99-p));
                        bs.set(99 - p);
                    }
                    Container newContainer = container.add(49 - k, 50 + k);
                    bs.set(49 - k, 50 + k);
                    assertNotSame(container, newContainer);
                    assertEquals(bs.cardinality(), newContainer.getCardinality());

                    int nb_runs = 1;
                    int lastIndex = bs.nextSetBit(0);
                    for (int p = bs.nextSetBit(0); p >= 0; p = bs.nextSetBit(p+1)) {
                        if(p - lastIndex > 1) {
                            nb_runs++;
                        }
                        lastIndex = p;
                        assertTrue(newContainer.contains((short) p));
                    }
                    assertEquals(nb_runs*4+4, newContainer.getSizeInBytes());
                }
            }
        }
    }


    @Test
    public void iaddRange() {
        for(int i = 0; i < 100; ++i) {
            for(int j = 0; j < 100; ++j) {
                for(int k = 0; k < 50; ++k) {
                    BitSet bs = new BitSet();
                    RunContainer container = new RunContainer();
                    for(int p = 0; p < i; ++p) {
                        container.add((short) p);
                        bs.set(p);
                    }
                    for(int p = 0; p < j; ++p) {
                        container.add((short) (99-p));
                        bs.set(99 - p);
                    }
                    Container newContainer = container.iadd(49 - k, 50 + k);
                    bs.set(49 - k, 50 + k);
                    assertNotSame(container, newContainer);
                    assertEquals(bs.cardinality(), newContainer.getCardinality());

                    int nb_runs = 1;
                    int lastIndex = bs.nextSetBit(0);
                    for (int p = bs.nextSetBit(0); p >= 0; p = bs.nextSetBit(p+1)) {
                        if(p - lastIndex > 1) {
                            nb_runs++;
                        }
                        lastIndex = p;
                        assertTrue(newContainer.contains((short) p));
                    }
                    assertEquals(nb_runs*4+4, newContainer.getSizeInBytes());
                }
            }
        }
    }
    @Test
    public void addRangeAndFuseWithPreviousValueLength() {
        RunContainer container = new RunContainer();
        for(short i = 10; i < 20; ++i) {
            container.add(i);
        }
        Container newContainer = container.add(20, 30);
        assertNotSame(container, newContainer);
        assertEquals(20, newContainer.getCardinality());
        for(short i = 10; i < 30; ++i) {
            assertTrue(newContainer.contains(i));
        }
        assertEquals(8, newContainer.getSizeInBytes());
    }
    @Test
    public void iaddRangeAndFuseWithPreviousValueLength() {
        RunContainer container = new RunContainer();
        for(short i = 10; i < 20; ++i) {
            container.add(i);
        }
        Container newContainer = container.iadd(20, 30);
        assertNotSame(container, newContainer);
        assertEquals(20, newContainer.getCardinality());
        for(short i = 10; i < 30; ++i) {
            assertTrue(newContainer.contains(i));
        }
        assertEquals(8, newContainer.getSizeInBytes());
    }
    @Test
    public void addRangeAndFuseWithNextValueLength() {
        RunContainer container = new RunContainer();
        for(short i = 10; i < 20; ++i) {
            container.add(i);
        }
        for(short i = 21; i < 30; ++i) {
            container.add(i);
        }
        Container newContainer = container.add(15, 21);
        assertNotSame(container, newContainer);
        assertEquals(20, newContainer.getCardinality());
        for(short i = 10; i < 30; ++i) {
            assertTrue(newContainer.contains(i));
        }
        assertEquals(8, newContainer.getSizeInBytes());
    }

    @Test
    public void addRangeOnNonEmptyContainerAndFuse() {
        RunContainer container = new RunContainer();
        for(short i = 1; i < 20; ++i) {
            container.add(i);
        }
        for(short i = 90; i < 120; ++i) {
            container.add(i);
        }
        Container newContainer = container.add(10, 100);
        assertNotSame(container, newContainer);
        assertEquals(119, newContainer.getCardinality());
        for(short i = 1; i < 120; ++i) {
            assertTrue(newContainer.contains(i));
        }
    }

    @Test
    public void iaddRangeOnNonEmptyContainerAndFuse() {
        RunContainer container = new RunContainer();
        for(short i = 1; i < 20; ++i) {
            container.add(i);
        }
        for(short i = 90; i < 120; ++i) {
            container.add(i);
        }
        Container newContainer = container.iadd(10, 100);
        assertNotSame(container, newContainer);
        assertEquals(119, newContainer.getCardinality());
        for(short i = 1; i < 120; ++i) {
            assertTrue(newContainer.contains(i));
        }
    }
    @Test
    public void addRangeOnNonEmptyContainer() {
        RunContainer container = new RunContainer();
        container.add((short) 1);
        container.add((short) 256);
        Container newContainer = container.add(10, 100);
        assertNotSame(container, newContainer);
        assertEquals(92, newContainer.getCardinality());
        assertTrue(newContainer.contains((short) 1));
        assertTrue(newContainer.contains((short) 256));
        for(short i = 10; i < 100; ++i) {
            assertTrue(newContainer.contains(i));
        }
    }

    @Test
    public void addRangeOnEmptyContainer() {
        RunContainer container = new RunContainer();
        Container newContainer = container.add(10, 100);
        assertNotSame(container, newContainer);
        assertEquals(90, newContainer.getCardinality());
        for(short i = 10; i < 100; ++i) {
            assertTrue(newContainer.contains(i));
        }
    }

    @Test
    public void addRangeWithinSetBoundsAndFuse() {
        RunContainer container = new RunContainer();
        container.add((short) 1);
        container.add((short) 10);
        container.add((short) 55);
        container.add((short) 99);
        container.add((short) 150);
        Container newContainer = container.add(10, 100);
        assertNotSame(container, newContainer);
        assertEquals(92, newContainer.getCardinality());
        for(short i = 10; i < 100; ++i) {
            assertTrue(newContainer.contains(i));
        }
    }

    @Test
    public void iaddRangeWithinSetBounds() {
        RunContainer container = new RunContainer();
        container.add((short) 10);
        container.add((short) 99);
        Container newContainer = container.iadd(10, 100);
        assertNotSame(container, newContainer);
        assertEquals(90, newContainer.getCardinality());
        for(short i = 10; i < 100; ++i) {
            assertTrue(newContainer.contains(i));
        }
    }

    @Test
    public void addRangeWithinSetBounds() {
        RunContainer container = new RunContainer();
        container.add((short) 10);
        container.add((short) 99);
        Container newContainer = container.add(10, 100);
        System.out.println(newContainer.getCardinality());
        assertNotSame(container, newContainer);
        assertEquals(90, newContainer.getCardinality());
        for(short i = 10; i < 100; ++i) {
            assertTrue(newContainer.contains(i));
        }
    }

    @Test
    public void addAndCompress() {
        RunContainer container  = new RunContainer();
        container.add((short) 0);
        container.add((short) 99);
        container.add((short) 98);
        assertEquals(12, container.getSizeInBytes());
    }

    @Test
    public void addOutOfOrder() {
        RunContainer container  = new RunContainer();
        container.add((short) 0);
        container.add((short) 2);
        container.add((short) 55);
        container.add((short) 1);
        assertEquals(4, container.getCardinality());
        assertTrue(container.contains((short) 0));
        assertTrue(container.contains((short) 1));
        assertTrue(container.contains((short) 2));
        assertTrue(container.contains((short) 55));
    }

    @Test
    public void limit() {
        RunContainer container  = new RunContainer();
        container.add((short) 0);
        container.add((short) 2);
        container.add((short) 55);
        container.add((short) 64);
        container.add((short) 256);
        Container limit = container.limit(1024);
        assertNotSame(container, limit);
        assertEquals(container, limit);
        limit = container.limit(3);
        assertNotSame(container, limit);
        assertEquals(3, limit.getCardinality());
        assertTrue(limit.contains((short) 0));
        assertTrue(limit.contains((short) 2));
        assertTrue(limit.contains((short) 55));
    }

    @Test
    public void rank() {
        RunContainer container  = new RunContainer();
        container.add((short) 0);
        container.add((short) 2);
        container.add((short) 55);
        container.add((short) 64);
        container.add((short) 256);
        assertEquals(1, container.rank((short) 0));
        assertEquals(2, container.rank((short) 10));
        assertEquals(4, container.rank((short) 128));
        assertEquals(5, container.rank((short) 1024));
    }

    @Test
    public void select() {
        RunContainer container  = new RunContainer();
        container.add((short) 0);
        container.add((short) 2);
        container.add((short) 55);
        container.add((short) 64);
        container.add((short) 256);
        assertEquals(0, container.select(0));
        assertEquals(2, container.select(1));
        assertEquals(55, container.select(2));
        assertEquals(64, container.select(3));
        assertEquals(256, container.select(4));
    }

    @Test
    public void safeSerialization() throws Exception {
        RunContainer container  = new RunContainer();
        container.add((short) 0);
        container.add((short) 2);
        container.add((short) 55);
        container.add((short) 64);
        container.add((short) 256);

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
    public void basic() {
        RunContainer x = new RunContainer();
        for(int k = 0; k < (1<<16);++k) {
            assertFalse(x.contains((short)k));
        }
        for(int k = 0; k < (1<<16);++k) {
            assertFalse(x.contains((short)k));
            x = (RunContainer)x.add((short)k);
            assertEquals(k+1,x.getCardinality());
            assertTrue(x.contains((short)k));
        }
        for(int k = 0; k < (1<<16);++k) {
            assertTrue(x.contains((short)k));
        }
        for(int k = 0; k < (1<<16);++k) {
            assertTrue(x.contains((short)k));
            x = (RunContainer)x.remove((short)k);
            assertFalse(x.contains((short)k));
            assertEquals(k+1,(1<<16)-x.getCardinality());
        }
        for(int k = 0; k < (1<<16);++k) {
            assertFalse(x.contains((short)k));
            x = (RunContainer) x.add((short)k);
            assertEquals(k+1,x.getCardinality());
            assertTrue(x.contains((short)k));
        }
        for(int k = (1<<16)-1; k >= 0 ;--k) {
            assertTrue(x.contains((short)k));
            x = (RunContainer) x.remove((short)k);
            assertFalse(x.contains((short)k));
            assertEquals(k,x.getCardinality());
        }
        for(int k = 0; k < (1<<16);++k) {
            assertFalse(x.contains((short)k));
            x = (RunContainer) x.add((short)k);
            assertEquals(k+1,x.getCardinality());
            assertTrue(x.contains((short)k));
        }
        for(int k = 0; k < (1<<16);++k) {
            RunContainer copy = (RunContainer) x.clone();
            copy = (RunContainer) copy.remove((short) k);
            assertEquals(copy.getCardinality() + 1,x.getCardinality());
            copy = (RunContainer) copy.add((short) k);
            assertEquals(copy.getCardinality(),x.getCardinality());
            assertTrue(copy.equals(x));
            assertTrue(x.equals(copy));
            copy.trim();
            assertTrue(copy.equals(x));
            assertTrue(x.equals(copy));
        }        
    }
    @Test
    public void basictri() {
        RunContainer x = new RunContainer();
        for(int k = 0; k < (1<<16);++k) {
            assertFalse(x.contains((short)k));
        }
        for(int k = 0; k < (1<<16);++k) {
            assertFalse(x.contains((short)k));
            x = (RunContainer)x.add((short)k);
            x.trim();
            assertEquals(k+1,x.getCardinality());
            assertTrue(x.contains((short)k));
        }
        for(int k = 0; k < (1<<16);++k) {
            assertTrue(x.contains((short)k));
        }
        for(int k = 0; k < (1<<16);++k) {
            assertTrue(x.contains((short)k));
            x = (RunContainer)x.remove((short)k);
            x.trim();
            assertFalse(x.contains((short)k));
            assertEquals(k+1,(1<<16)-x.getCardinality());
        }
        for(int k = 0; k < (1<<16);++k) {
            assertFalse(x.contains((short)k));
            x = (RunContainer) x.add((short)k);
            x.trim();
            assertEquals(k+1,x.getCardinality());
            assertTrue(x.contains((short)k));
        }
        for(int k = (1<<16)-1; k >= 0 ;--k) {
            assertTrue(x.contains((short)k));
            x = (RunContainer) x.remove((short)k);
            x.trim();
            assertFalse(x.contains((short)k));
            assertEquals(k,x.getCardinality());
        }
        for(int k = 0; k < (1<<16);++k) {
            assertFalse(x.contains((short)k));
            x = (RunContainer) x.add((short)k);
            x.trim();
            assertEquals(k+1,x.getCardinality());
            assertTrue(x.contains((short)k));
        }
        for(int k = 0; k < (1<<16);++k) {
            RunContainer copy = (RunContainer) x.clone();
            copy.trim();
            copy = (RunContainer) copy.remove((short) k);
            copy = (RunContainer) copy.add((short) k);
            assertEquals(copy.getCardinality(),x.getCardinality());
            assertTrue(copy.equals(x));
            assertTrue(x.equals(copy));
            copy.trim();
            assertTrue(copy.equals(x));
            assertTrue(x.equals(copy));
        }        
    }

    @Test
    public void simpleIterator() {
        RunContainer x = new RunContainer();
        for(int k = 0; k < 100;++k) {
              x = (RunContainer)x.add((short)(k));
        }
        ShortIterator i = x.getShortIterator();
        for(int k = 0; k < 100;++k) {
                assertTrue(i.hasNext());
                assertEquals(i.next(), (short)k);
        }
        assertFalse(i.hasNext());
    }

    @Test
    public void longsimpleIterator() {
        RunContainer x = new RunContainer();
        for(int k = 0; k < (1<<16);++k) {
              x = (RunContainer)x.add((short)(k));
        }
        ShortIterator i = x.getShortIterator();
        for(int k = 0; k < (1<<16);++k) {
                assertTrue(i.hasNext());
                assertEquals(i.next(), (short)k);
        }
        assertFalse(i.hasNext());
    }
    
    @Test
    public void longbacksimpleIterator() {
        RunContainer x = new RunContainer();
        for(int k = 0; k < (1<<16);++k) {
              x = (RunContainer)x.add((short)k);
        }
        ShortIterator i = x.getReverseShortIterator();
        for(int k = (1<<16)-1; k >=0 ;--k) {
                assertTrue(i.hasNext());
                assertEquals(i.next(), (short)k);
        }
        assertFalse(i.hasNext());
    }

    @Test
    public void longcsimpleIterator() {
        RunContainer x = new RunContainer();
        for(int k = 0; k < (1<<16);++k) {
              x = (RunContainer)x.add((short)k);
        }
        Iterator i = x.iterator();
        for(int k = 0; k < (1<<16);++k) {
                assertTrue(i.hasNext());
                assertEquals(i.next(), (short)k);
        }
        assertFalse(i.hasNext());
    }
    
    @Test
    public void iterator() {
        RunContainer x = new RunContainer();
        for(int k = 0; k < 100;++k) {
            for(int j = 0; j < k; ++j)
              x = (RunContainer)x.add((short)(k*100+j));
        }
        ShortIterator i = x.getShortIterator();
        for(int k = 0; k < 100;++k) {
            for(int j = 0; j < k; ++j) {
                assertTrue(i.hasNext());
                assertEquals(i.next(), (short)(k*100+j));
            }
        }
        assertFalse(i.hasNext());        
    }

    @Test
    public void basic2() {
        RunContainer x = new RunContainer();
        int a = 33;
        int b = 50000;
        for(int k = a; k < b;++k) {
            x = (RunContainer)x.add((short)k);
        }
        for(int k = 0; k < (1<<16);++k) {
            if(x.contains((short) k)) {
                RunContainer copy = (RunContainer) x.clone();
                copy = (RunContainer) copy.remove((short) k);
                copy = (RunContainer) copy.add((short) k);
                assertEquals(copy.getCardinality(),x.getCardinality());
                assertTrue(copy.equals(x));
                assertTrue(x.equals(copy));      
                x.trim();
                assertTrue(copy.equals(x));
                assertTrue(x.equals(copy));      
                
            } else {
                RunContainer copy = (RunContainer) x.clone();
                copy = (RunContainer) copy.add((short) k);
                assertEquals(copy.getCardinality(),x.getCardinality() + 1);
            }
        }
    }
    
}
