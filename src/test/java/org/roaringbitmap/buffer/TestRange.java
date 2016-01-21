package org.roaringbitmap.buffer;

import static org.junit.Assert.*;

import java.util.BitSet;
import java.util.Random;

import org.junit.Test;


public class TestRange {

    @Test
    public void testRangeRemovalWithEightBitsAndRunlengthEncoding1() {
        final MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
        bitmap.add(1); // index 1
        bitmap.add(2);
        bitmap.add(3);
        bitmap.add(4);
        bitmap.add(5);
        bitmap.add(7); // index 7

        bitmap.runOptimize();
        // should remove from index 0 to 7
        bitmap.remove(0, 8);

        assertTrue(bitmap.isEmpty());
    }

    @Test
    public void testRangeRemovalWithEightBitsAndRunlengthEncoding2() {
        final MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
        bitmap.add(1); // index 1
        bitmap.add(2);
        bitmap.add(3);
        bitmap.add(4);
        bitmap.add(5);
        bitmap.add(7); // index 7

        bitmap.runOptimize();
        bitmap.removeRunCompression();
        assertFalse(bitmap.hasRunCompression());
        // should remove from index 0 to 7
        bitmap.remove(0, 8);
        assertTrue(bitmap.isEmpty());
    }
    @Test
    public void testRangeRemoval() {
        final MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
        bitmap.add(1);
        assertTrue((bitmap.getCardinality() == 1)&& bitmap.contains(1));
        bitmap.runOptimize();
        assertTrue((bitmap.getCardinality() == 1)&& bitmap.contains(1));
        bitmap.removeRunCompression();
        assertTrue((bitmap.getCardinality() == 1)&& bitmap.contains(1));
        bitmap.remove(0,1); // should do nothing
        assertTrue((bitmap.getCardinality() == 1)&& bitmap.contains(1));        bitmap.remove(1,2);
        bitmap.remove(1,2); // should clear [1,2)
        assertTrue(bitmap.isEmpty());
    }
    
    @Test
    public void testRemovalWithAddedAndRemovedRunOptimization() {
        // with runlength encoding
        testRangeRemovalWithRandomBits(true);
    }

    @Test
    public void testRemovalWithoutAddedAndRemovedRunOptimization() {
        // without runlength encoding
        testRangeRemovalWithRandomBits(false);
    }

    private void testRangeRemovalWithRandomBits(boolean withRunCompression) {
        final int iterations = 4096;
        final int bits = 8;

        for (int i = 0; i < iterations; i++) {
            final BitSet bitset = new BitSet(bits);
            final MutableRoaringBitmap bitmap = new MutableRoaringBitmap();

            // check, if empty
            assertTrue(bitset.isEmpty());
            assertTrue(bitmap.isEmpty());

            // fill with random bits
            final int added = fillWithRandomBits(bitmap, bitset, bits);

            // check for cardinalities and if not empty
            assertTrue(added > 0 ? !bitmap.isEmpty() : bitmap.isEmpty());
            assertTrue(added > 0 ? !bitset.isEmpty() : bitset.isEmpty());
            assertEquals(added, bitmap.getCardinality());
            assertEquals(added, bitset.cardinality());

            // apply runtime compression or not
            if (withRunCompression) {
                bitmap.runOptimize();
                bitmap.removeRunCompression();
            }

            // clear and check bitmap, if really empty
            bitmap.remove(0, bits);          
            assertEquals("fails with bits: "+bitset, 0, bitmap.getCardinality());
            assertTrue(bitmap.isEmpty());

            // clear java's bitset
            bitset.clear(0, bits);
            assertEquals(0, bitset.cardinality());
            assertTrue(bitset.isEmpty());
        }
    }

    private static int fillWithRandomBits(final MutableRoaringBitmap bitmap, final BitSet bitset, final int bits) {
        int added = 0;
        Random r = new Random(1011);
        for (int j = 0; j < bits; j++) {
            if (r.nextBoolean()) {
                added++;
                bitmap.add(j);
                bitset.set(j);
            }
        }
        return added;
    }



    
    @Test
    public void testFlipRanges()  {
        int N = 256;
        for(int end = 1; end < N; ++end ) {
            for(int start = 0; start< end; ++start) {
                MutableRoaringBitmap bs1 = new MutableRoaringBitmap();
                for(int k = start; k < end; ++k) {
                    bs1.flip(k);
                }
                MutableRoaringBitmap bs2 = new MutableRoaringBitmap();
                bs2.flip(start, end);
                assertEquals(bs2.getCardinality(), end-start);
                assertEquals(bs1, bs2);
            }
        }
    }

    @Test
    public void testSetRanges() {
        int N = 256;
        for(int end = 1; end < N; ++end ) {
            for(int start = 0; start< end; ++start) {
                MutableRoaringBitmap bs1 = new MutableRoaringBitmap();
                for(int k = start; k < end; ++k) {
                    bs1.add(k);
                }
                MutableRoaringBitmap bs2 = new MutableRoaringBitmap();
                bs2.add(start, end);
                assertEquals(bs1, bs2);
            }
        }
    }
    

    @Test
    public void testClearRanges()  {
        int N = 16;
        for(int end = 1; end < N; ++end ) {
            for(int start = 0; start< end; ++start) {
                MutableRoaringBitmap bs1 = new MutableRoaringBitmap();
                bs1.add(0, N);
                for(int k = start; k < end; ++k) {
                    bs1.remove(k);
                }
                MutableRoaringBitmap bs2 = new MutableRoaringBitmap();
                bs2.add(0, N);
                bs2.remove(start, end);
                assertEquals(bs1, bs2);
            }
        }
    }


    @Test
    public void testStaticSetRanges() {
        int N = 256;
        for(int end = 1; end < N; ++end ) {
            for(int start = 0; start< end; ++start) {
                MutableRoaringBitmap bs1 = new MutableRoaringBitmap();
                for(int k = start; k < end; ++k) {
                    bs1.add(k);
                }
                MutableRoaringBitmap bs2 = new MutableRoaringBitmap();
                bs2 = MutableRoaringBitmap.add(bs2,start, end);
                assertEquals(bs1, bs2);
            }
        }
    }
    

    @Test
    public void testStaticClearRanges()  {
        int N = 16;
        for(int end = 1; end < N; ++end ) {
            for(int start = 0; start< end; ++start) {
                MutableRoaringBitmap bs1 = new MutableRoaringBitmap();
                bs1.add(0, N);
                for(int k = start; k < end; ++k) {
                    bs1.remove(k);
                }
                MutableRoaringBitmap bs2 = new MutableRoaringBitmap();
                bs2 = MutableRoaringBitmap.add(bs2, 0, N);
                bs2 = MutableRoaringBitmap.remove(bs2,start, end);
                assertEquals(bs1, bs2);
            }
        }
    }
    
    @Test
    public void setTest1() {
        final MutableRoaringBitmap rb = new MutableRoaringBitmap();

        rb.add(100000, 200000); // in-place on empty bitmap
        final int rbcard = rb.getCardinality();
        assertEquals(100000, rbcard);

        final BitSet bs = new BitSet();
        for (int i = 100000; i < 200000; ++i)
            bs.set(i);
        assertTrue(TestRoaringBitmap.equals(bs, rb));
    }
    
    @Test
    public void setTest1A() {
        final MutableRoaringBitmap rb = new MutableRoaringBitmap();

        final MutableRoaringBitmap rb1 = MutableRoaringBitmap
                .add(rb, 100000, 200000);
        final int rbcard = rb1.getCardinality();
        assertEquals(100000, rbcard);
        assertEquals(0, rb.getCardinality());

        final BitSet bs = new BitSet();
        assertTrue(TestRoaringBitmap.equals(bs, rb)); // still empty?
        for (int i = 100000; i < 200000; ++i)
            bs.set(i);
        assertTrue(TestRoaringBitmap.equals(bs, rb1));
    }
    
    @Test
    public void setTest2() {
        final MutableRoaringBitmap rb = new MutableRoaringBitmap();

        rb.add(100000, 100000);
        final int rbcard = rb.getCardinality();
        assertEquals(0, rbcard);

        final BitSet bs = new BitSet();
        assertTrue(TestRoaringBitmap.equals(bs, rb));
    }

    @Test
    public void setTest2A() {
        final MutableRoaringBitmap rb = new MutableRoaringBitmap();
        final MutableRoaringBitmap rb1 = MutableRoaringBitmap
                .add(rb, 100000, 100000);
        rb.add(1); // will not affect rb1 (no shared container)
        final int rbcard = rb1.getCardinality();
        assertEquals(0, rbcard);
        assertEquals(1, rb.getCardinality());

        final BitSet bs = new BitSet();
        assertTrue(TestRoaringBitmap.equals(bs, rb1));
        bs.set(1);
        assertTrue(TestRoaringBitmap.equals(bs, rb));
    }
    
    @Test
    public void setTest3() {
        final MutableRoaringBitmap rb = new MutableRoaringBitmap();

        rb.add(0, 65536);
        final int rbcard = rb.getCardinality();

        assertEquals(65536, rbcard);

        final BitSet bs = new BitSet();
        for (int i = 0; i < 65536; ++i)
            bs.set(i);

        assertTrue(TestRoaringBitmap.equals(bs, rb));
    }
    
    @Test
    public void setTest3A() {
        final MutableRoaringBitmap rb = new MutableRoaringBitmap();
        final MutableRoaringBitmap rb1 = MutableRoaringBitmap
                .add(rb, 100000, 200000);
        final MutableRoaringBitmap rb2 = MutableRoaringBitmap.add(rb1, 500000,
                600000);
        final int rbcard = rb2.getCardinality();

        assertEquals(200000, rbcard);

        final BitSet bs = new BitSet();
        for (int i = 100000; i < 200000; ++i)
            bs.set(i);
        for (int i = 500000; i < 600000; ++i)
            bs.set(i);
        assertTrue(TestRoaringBitmap.equals(bs, rb2));
    }
    
    @Test
    public void setTest4() { 
        final MutableRoaringBitmap rb = new MutableRoaringBitmap();
        rb.add(100000, 200000);
        rb.add(65536, 4 * 65536);
        final int rbcard = rb.getCardinality();        
            
        assertEquals(196608, rbcard);

        final BitSet bs = new BitSet();
        for (int i = 65536; i < 4 * 65536; ++i)
            bs.set(i);

        assertTrue(TestRoaringBitmap.equals(bs, rb));
    }
    
    @Test
    public void setTest4A() {
        final MutableRoaringBitmap rb = new MutableRoaringBitmap();
        final MutableRoaringBitmap rb1 = MutableRoaringBitmap
                .add(rb, 100000, 200000);
        final MutableRoaringBitmap rb2 = MutableRoaringBitmap.add(rb1, 65536, 4 * 65536);
        final int rbcard = rb2.getCardinality();
        
        assertEquals(196608, rbcard);

        final BitSet bs = new BitSet();
        for (int i = 65536; i < 4*65536; ++i)
            bs.set(i);

        assertTrue(TestRoaringBitmap.equals(bs, rb2));
    }
    
    @Test
    public void setTest5() { 
        final MutableRoaringBitmap rb = new MutableRoaringBitmap();
        rb.add(500, 65536*3+500);
        rb.add(65536, 65536*3);
            
        final int rbcard = rb.getCardinality();
        
        assertEquals(196608, rbcard);

        final BitSet bs = new BitSet();
        for (int i = 500; i < 65536*3+500; ++i)
            bs.set(i);
        assertTrue(TestRoaringBitmap.equals(bs, rb));
    }
    
    @Test
    public void setTest5A() {
        final MutableRoaringBitmap rb = new MutableRoaringBitmap();
        final MutableRoaringBitmap rb1 = MutableRoaringBitmap
                .add(rb, 100000, 500000);  
        final MutableRoaringBitmap rb2 = MutableRoaringBitmap
                .add(rb1, 65536, 120000);
                final int rbcard = rb2.getCardinality();

        assertEquals(434464, rbcard);

        BitSet bs = new BitSet();
        for (int i = 65536; i < 500000; ++i)
            bs.set(i);
        assertTrue(TestRoaringBitmap.equals(bs, rb2));
    }
    
    @Test
    public void setTestArrayContainer() {
        final MutableRoaringBitmap rb = new MutableRoaringBitmap();
        rb.add(500, 3000);  
        rb.add(65536, 66000);
        final int rbcard = rb.getCardinality();

        assertEquals(2964, rbcard);

        BitSet bs = new BitSet();
        for (int i = 500; i < 3000; ++i)
            bs.set(i);
        for (int i = 65536; i < 66000; ++i)
            bs.set(i);
        assertTrue(TestRoaringBitmap.equals(bs, rb));
    }
    
    @Test
    public void setTestArrayContainerA() {
        final MutableRoaringBitmap rb = new MutableRoaringBitmap();
        final MutableRoaringBitmap rb1 = MutableRoaringBitmap
                .add(rb, 500, 3000);  
        final MutableRoaringBitmap rb2 = MutableRoaringBitmap
                .add(rb1, 65536, 66000);
                final int rbcard = rb2.getCardinality();

        assertEquals(2964, rbcard);

        BitSet bs = new BitSet();
        for (int i = 500; i < 3000; ++i)
            bs.set(i);
        for (int i = 65536; i < 66000; ++i)
            bs.set(i);
        assertTrue(TestRoaringBitmap.equals(bs, rb2));
    }
    
    @Test
    public void setTestSinglePonitsA() {
        final MutableRoaringBitmap rb = new MutableRoaringBitmap();
        final MutableRoaringBitmap rb1 = MutableRoaringBitmap
                .add(rb, 500, 501);  
        final MutableRoaringBitmap rb2 = MutableRoaringBitmap
                .add(rb1, 65536, 65537);
                final int rbcard = rb2.getCardinality();

        assertEquals(2, rbcard);

        BitSet bs = new BitSet();
        bs.set(500);
        bs.set(65536);
        assertTrue(TestRoaringBitmap.equals(bs, rb2));
    }
    
    @Test
    public void setTestSinglePonits() {
        final MutableRoaringBitmap rb = new MutableRoaringBitmap();
        rb.add(500, 501);  
        rb.add(65536, 65537);
        final int rbcard = rb.getCardinality();

        assertEquals(2, rbcard);

        BitSet bs = new BitSet();
        bs.set(500);
        bs.set(65536);
        assertTrue(TestRoaringBitmap.equals(bs, rb));
    }
    
    @Test
    public void setTest6() { // fits evenly on big end, multiple containers
        final MutableRoaringBitmap rb = new MutableRoaringBitmap();
        rb.add(100000, 132000);
        rb.add(3*65536, 4 * 65536);
        final int rbcard = rb.getCardinality();

        assertEquals(97536, rbcard);

        final BitSet bs = new BitSet();
        for (int i = 100000; i < 132000; ++i)
            bs.set(i);
        for (int i = 3*65536; i < 4 * 65536; ++i)
            bs.set(i);
        assertTrue(TestRoaringBitmap.equals(bs, rb));
    }
    
    @Test
    public void setTest6A() {
        final MutableRoaringBitmap rb = new MutableRoaringBitmap();
        final MutableRoaringBitmap rb1 = MutableRoaringBitmap
                .add(rb, 100000, 132000);
        final MutableRoaringBitmap rb2 = MutableRoaringBitmap.add(rb1, 3*65536,
                4 * 65536);
        final int rbcard = rb2.getCardinality();

        assertEquals(97536, rbcard);

        final BitSet bs = new BitSet();
        for (int i = 100000; i < 132000; ++i)
            bs.set(i);
        for (int i = 3 * 65536; i < 4 * 65536; ++i)
            bs.set(i);
        assertTrue(TestRoaringBitmap.equals(bs, rb2));
    }
    
    @Test
    public void setTest7() { 
        final MutableRoaringBitmap rb = new MutableRoaringBitmap();
        rb.add(10, 50);
        rb.add(1, 9);
        rb.add(130, 185);
        rb.add(6407, 6460);
        rb.add(325, 380);
        rb.add((65536*3)+3, (65536*3)+60);
        rb.add(65536*3+195, 65536*3+245);
        final int rbcard = rb.getCardinality();         

        assertEquals(318, rbcard);

        final BitSet bs = new BitSet();
        for (int i = 10; i < 50; ++i)
            bs.set(i);
        for (int i = 1; i < 9; ++i)
            bs.set(i);
        for (int i = 130; i < 185; ++i)
            bs.set(i);
        for (int i = 325; i < 380; ++i)
            bs.set(i);
        for (int i = 6407; i < 6460; ++i)
            bs.set(i);
        for (int i = 65536*3+3; i < 65536*3+60; ++i)
            bs.set(i);
        for (int i = 65536*3+195; i < 65536*3+245; ++i)
            bs.set(i);
        assertTrue(TestRoaringBitmap.equals(bs, rb));
    }
    
    @Test
    public void setTest8() {
        final MutableRoaringBitmap rb = new MutableRoaringBitmap();
        for(int i=0; i<5; i++)
            for(int j=0; j<1024; j++)
                rb.add(i*(1<<16)+j*64+2, i*(1<<16)+j*64+63);
         
        final int rbcard = rb.getCardinality();         

        assertEquals(312320, rbcard);

        final BitSet bs = new BitSet();
        for(int i=0; i<5; i++)
            for(int j=0; j<1024; j++)
                bs.set(i*(1<<16)+j*64+2, i*(1<<16)+j*64+63);
        
        assertTrue(TestRoaringBitmap.equals(bs, rb));
    }
    
    @Test
    public void setTest7A() { 
        final MutableRoaringBitmap rb = new MutableRoaringBitmap();
        final BitSet bs = new BitSet();
        assertTrue(TestRoaringBitmap.equals(bs, rb));

        final MutableRoaringBitmap rb1 = MutableRoaringBitmap.add(rb,10, 50);
        bs.set(10, 50);
        rb.add(10,50);
        assertTrue(rb1.equals(rb));
        assertTrue(TestRoaringBitmap.equals(bs, rb1));

        MutableRoaringBitmap rb2 = MutableRoaringBitmap.add(rb1,130, 185);
        bs.set(130, 185);
        rb.add(130, 185);
        assertTrue(rb2.equals(rb));
        assertTrue(TestRoaringBitmap.equals(bs, rb2));
        
        MutableRoaringBitmap rb3 = MutableRoaringBitmap.add(rb2,6407, 6460);
        bs.set(6407, 6460);
        assertTrue(TestRoaringBitmap.equals(bs, rb3));
        rb2.add(6407, 6460);
        assertTrue(rb2.equals(rb3));

        rb3 = MutableRoaringBitmap.add(rb3,(65536*3)+3, (65536*3)+60);
        rb2.add((65536*3)+3, (65536*3)+60);
        bs.set((65536*3)+3, (65536*3)+60);
        assertTrue(rb2.equals(rb3));
        assertTrue(TestRoaringBitmap.equals(bs, rb3));

        
        rb3 = MutableRoaringBitmap.add(rb3,65536*3+195, 65536*3+245);
        bs.set(65536*3+195, 65536*3+245);
        rb2.add(65536*3+195, 65536*3+245);
        assertTrue(rb2.equals(rb3));
        assertTrue(TestRoaringBitmap.equals(bs, rb3));

        final int rbcard = rb3.getCardinality();            

        assertEquals(255, rbcard);
        
        // now removing
        
        
        rb3 = MutableRoaringBitmap.remove(rb3,65536*3+195, 65536*3+245);
        bs.clear(65536*3+195, 65536*3+245);
        rb2.remove(65536*3+195, 65536*3+245);

        assertTrue(rb2.equals(rb3));
        assertTrue(TestRoaringBitmap.equals(bs, rb3));

        rb3 = MutableRoaringBitmap.remove(rb3,(65536*3)+3, (65536*3)+60);
        bs.clear((65536*3)+3, (65536*3)+60);
        rb2.remove((65536*3)+3, (65536*3)+60);

        assertTrue(rb2.equals(rb3));
        assertTrue(TestRoaringBitmap.equals(bs, rb3));

        rb3 = MutableRoaringBitmap.remove(rb3,6407, 6460);
        bs.clear(6407, 6460);
        rb2.remove(6407, 6460);
    
        assertTrue(rb2.equals(rb3));
        assertTrue(TestRoaringBitmap.equals(bs, rb3));

        
        rb2 = MutableRoaringBitmap.remove(rb1,130, 185);
        bs.clear(130, 185);
        rb.remove(130, 185);
        assertTrue(rb2.equals(rb));
        assertTrue(TestRoaringBitmap.equals(bs, rb2));

        
    }
    
    @Test
    public void setRemoveTest1() {
        final BitSet bs = new BitSet();
        MutableRoaringBitmap rb = new MutableRoaringBitmap();
        bs.set(0,1000000);
        rb.add(0,1000000);
        rb.remove(43022,392542);
        bs.clear(43022,392542);
        assertTrue(TestRoaringBitmap.equals(bs, rb));
    }

    @Test
    public void setRemoveTest2() {
        final BitSet bs = new BitSet();
        MutableRoaringBitmap rb = new MutableRoaringBitmap();
        bs.set(43022,392542);
        rb.add(43022,392542);
        rb.remove(43022,392542);
        bs.clear(43022,392542);
        assertTrue(TestRoaringBitmap.equals(bs, rb));
    }
    
    @Test
    public void doubleadd() {
        MutableRoaringBitmap rb = new MutableRoaringBitmap();
        rb.add(65533, 65536);
        rb.add(65530, 65536);
        BitSet bs = new BitSet();
        bs.set(65530, 65536);
        assertTrue(TestRoaringBitmap.equals(bs, rb));
        rb.remove(65530, 65536);
        assertTrue(rb.getCardinality() == 0);
    }
    

    @Test
    public void rangeAddRemoveBig() {
        final int numCases = 5000;
        MutableRoaringBitmap rbstatic = new MutableRoaringBitmap();
        final MutableRoaringBitmap rbinplace = new MutableRoaringBitmap();
        final BitSet bs = new BitSet();
        final Random r = new Random(3333);
        int start, end;
        for (int i = 0; i < numCases; ++i) {
            start = r.nextInt(65536 * 20);
            end = r.nextInt(65536 * 20);
            if(start > end) {
                int tmp = start;
                start = end;
                end = tmp;
            }
            rbinplace.add(start,end);
            rbstatic = MutableRoaringBitmap.add(rbstatic,start,end);
            bs.set(start,end);

            //
            start = r.nextInt(65536 * 20);
            end = r.nextInt(65536 * 20);
            if(start > end) {
                int tmp = start;
                start = end;
                end = tmp;
            }
            rbinplace.remove(start,end);
            rbstatic = MutableRoaringBitmap.remove(rbstatic,start,end);
            bs.clear(start,end);

            //
            start = r.nextInt(20) * 65536 ;
            end = r.nextInt(65536 * 20);
            if(start > end) {
                int tmp = start;
                start = end;
                end = tmp;
            }
            rbinplace.add(start,end);
            rbstatic = MutableRoaringBitmap.add(rbstatic,start,end);
            bs.set(start,end);

            //
            start = r.nextInt(65536 * 20) ;
            end = r.nextInt(20) * 65536;
            if(start > end) {
                int tmp = start;
                start = end;
                end = tmp;
            }
            rbinplace.add(start,end);
            rbstatic = MutableRoaringBitmap.add(rbstatic,start,end);
            bs.set(start,end);
            //
            start = r.nextInt(20) * 65536 ;
            end = r.nextInt(65536 * 20);
            if(start > end) {
                int tmp = start;
                start = end;
                end = tmp;
            }
            rbinplace.remove(start,end);
            rbstatic = MutableRoaringBitmap.remove(rbstatic,start,end);
            bs.clear(start,end);

            //
            start = r.nextInt(65536 * 20) ;
            end = r.nextInt(20) * 65536;
            if(start > end) {
                int tmp = start;
                start = end;
                end = tmp;
            }
            rbinplace.remove(start,end);
            rbstatic = MutableRoaringBitmap.remove(rbstatic,start,end);
            bs.clear(start,end);
        }
        assertTrue(TestRoaringBitmap.equals(bs, rbstatic));
        assertTrue(TestRoaringBitmap.equals(bs, rbinplace));
        assertTrue(rbinplace.equals(rbstatic));
    }
    

}
