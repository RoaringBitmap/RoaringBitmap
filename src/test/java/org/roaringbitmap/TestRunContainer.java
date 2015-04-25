package org.roaringbitmap;

import org.junit.Test;

import java.io.*;
import java.util.Iterator;

import static org.junit.Assert.*;

public class TestRunContainer {

    @Test
    public void safeSerialization() throws IOException {
        RunContainer container  = new RunContainer();
        container.add((short) 0);
        container.add((short) 2);
        container.add((short) 55);
        container.add((short) 64);
        container.add((short) (1<<8));

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        container.serialize(new DataOutputStream(bos));

        RunContainer newContainer= new RunContainer();
        byte[] bout = bos.toByteArray();
        newContainer.deserialize(new DataInputStream(new ByteArrayInputStream(bout)));
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
