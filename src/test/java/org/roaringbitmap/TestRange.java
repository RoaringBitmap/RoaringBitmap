package org.roaringbitmap;



import static org.junit.Assert.*;

import java.util.BitSet;
import java.util.Random;

import org.junit.Test;



public class TestRange {
	

	
	@Test
	public void testFlipRanges()  {
		int N = 256;
		for(int end = 1; end < N; ++end ) {
			for(int start = 0; start< end; ++start) {
				RoaringBitmap bs1 = new RoaringBitmap();
				for(int k = start; k < end; ++k) {
					bs1.flip(k);
				}
				RoaringBitmap bs2 = new RoaringBitmap();
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
				RoaringBitmap bs1 = new RoaringBitmap();
				for(int k = start; k < end; ++k) {
					bs1.add(k);
				}
				RoaringBitmap bs2 = new RoaringBitmap();
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
				RoaringBitmap bs1 = new RoaringBitmap();
				bs1.add(0, N);
				for(int k = start; k < end; ++k) {
					bs1.remove(k);
				}
				RoaringBitmap bs2 = new RoaringBitmap();
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
                RoaringBitmap bs1 = new RoaringBitmap();
                for(int k = start; k < end; ++k) {
                    bs1.add(k);
                }
                RoaringBitmap bs2 = new RoaringBitmap();
                bs2 = RoaringBitmap.add(bs2,start, end);
                assertEquals(bs1, bs2);
            }
        }
    }
    

    @Test
    public void testStaticClearRanges()  {
        int N = 16;
        for(int end = 1; end < N; ++end ) {
            for(int start = 0; start< end; ++start) {
                RoaringBitmap bs1 = new RoaringBitmap();
                bs1.add(0, N);
                for(int k = start; k < end; ++k) {
                    bs1.remove(k);
                }
                RoaringBitmap bs2 = new RoaringBitmap();
                bs2 = RoaringBitmap.add(bs2, 0, N);
                bs2 = RoaringBitmap.remove(bs2,start, end);
                assertEquals(bs1, bs2);
            }
        }
    }
    
    @Test
    public void setTest1() {
        final RoaringBitmap rb = new RoaringBitmap();

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
        final RoaringBitmap rb = new RoaringBitmap();

        final RoaringBitmap rb1 = RoaringBitmap
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
        final RoaringBitmap rb = new RoaringBitmap();

        rb.add(100000, 100000);
        final int rbcard = rb.getCardinality();
        assertEquals(0, rbcard);

        final BitSet bs = new BitSet();
        assertTrue(TestRoaringBitmap.equals(bs, rb));
    }

    @Test
    public void setTest2A() {
        final RoaringBitmap rb = new RoaringBitmap();
        final RoaringBitmap rb1 = RoaringBitmap
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
        final RoaringBitmap rb = new RoaringBitmap();

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
        final RoaringBitmap rb = new RoaringBitmap();
        final RoaringBitmap rb1 = RoaringBitmap
                .add(rb, 100000, 200000);
        final RoaringBitmap rb2 = RoaringBitmap.add(rb1, 500000,
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
        final RoaringBitmap rb = new RoaringBitmap();
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
        final RoaringBitmap rb = new RoaringBitmap();
        final RoaringBitmap rb1 = RoaringBitmap
                .add(rb, 100000, 200000);
        final RoaringBitmap rb2 = RoaringBitmap.add(rb1, 65536, 4 * 65536);
        final int rbcard = rb2.getCardinality();
        
        assertEquals(196608, rbcard);

        final BitSet bs = new BitSet();
        for (int i = 65536; i < 4*65536; ++i)
            bs.set(i);

        assertTrue(TestRoaringBitmap.equals(bs, rb2));
    }
    
    @Test
    public void setTest5() { 
        final RoaringBitmap rb = new RoaringBitmap();
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
        final RoaringBitmap rb = new RoaringBitmap();
        final RoaringBitmap rb1 = RoaringBitmap
                .add(rb, 100000, 500000);  
        final RoaringBitmap rb2 = RoaringBitmap
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
        final RoaringBitmap rb = new RoaringBitmap();
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
        final RoaringBitmap rb = new RoaringBitmap();
        final RoaringBitmap rb1 = RoaringBitmap
                .add(rb, 500, 3000);  
        final RoaringBitmap rb2 = RoaringBitmap
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
        final RoaringBitmap rb = new RoaringBitmap();
        final RoaringBitmap rb1 = RoaringBitmap
                .add(rb, 500, 501);  
        final RoaringBitmap rb2 = RoaringBitmap
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
        final RoaringBitmap rb = new RoaringBitmap();
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
        final RoaringBitmap rb = new RoaringBitmap();
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
        final RoaringBitmap rb = new RoaringBitmap();
        final RoaringBitmap rb1 = RoaringBitmap
                .add(rb, 100000, 132000);
        final RoaringBitmap rb2 = RoaringBitmap.add(rb1, 3*65536,
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
        final RoaringBitmap rb = new RoaringBitmap();
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
        final RoaringBitmap rb = new RoaringBitmap();
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
        final RoaringBitmap rb = new RoaringBitmap();
        final BitSet bs = new BitSet();
        assertTrue(TestRoaringBitmap.equals(bs, rb));

        final RoaringBitmap rb1 = RoaringBitmap.add(rb,10, 50);
        bs.set(10, 50);
        rb.add(10,50);
        assertTrue(rb1.equals(rb));
        assertTrue(TestRoaringBitmap.equals(bs, rb1));

        RoaringBitmap rb2 = RoaringBitmap.add(rb1,130, 185);
        bs.set(130, 185);
        rb.add(130, 185);
        assertTrue(rb2.equals(rb));
        assertTrue(TestRoaringBitmap.equals(bs, rb2));
        
        RoaringBitmap rb3 = RoaringBitmap.add(rb2,6407, 6460);
        bs.set(6407, 6460);
        assertTrue(TestRoaringBitmap.equals(bs, rb3));
        rb2.add(6407, 6460);
        assertTrue(rb2.equals(rb3));

        rb3 = RoaringBitmap.add(rb3,(65536*3)+3, (65536*3)+60);
        rb2.add((65536*3)+3, (65536*3)+60);
        bs.set((65536*3)+3, (65536*3)+60);
        assertTrue(rb2.equals(rb3));
        assertTrue(TestRoaringBitmap.equals(bs, rb3));

        
        rb3 = RoaringBitmap.add(rb3,65536*3+195, 65536*3+245);
        bs.set(65536*3+195, 65536*3+245);
        rb2.add(65536*3+195, 65536*3+245);
        assertTrue(rb2.equals(rb3));
        assertTrue(TestRoaringBitmap.equals(bs, rb3));

        final int rbcard = rb3.getCardinality();            

        assertEquals(255, rbcard);
        
        // now removing
        
        
        rb3 = RoaringBitmap.remove(rb3,65536*3+195, 65536*3+245);
        bs.clear(65536*3+195, 65536*3+245);
        rb2.remove(65536*3+195, 65536*3+245);

        assertTrue(rb2.equals(rb3));
        assertTrue(TestRoaringBitmap.equals(bs, rb3));

        rb3 = RoaringBitmap.remove(rb3,(65536*3)+3, (65536*3)+60);
        bs.clear((65536*3)+3, (65536*3)+60);
        rb2.remove((65536*3)+3, (65536*3)+60);

        assertTrue(rb2.equals(rb3));
        assertTrue(TestRoaringBitmap.equals(bs, rb3));

        rb3 = RoaringBitmap.remove(rb3,6407, 6460);
        bs.clear(6407, 6460);
        rb2.remove(6407, 6460);
    
        assertTrue(rb2.equals(rb3));
        assertTrue(TestRoaringBitmap.equals(bs, rb3));

        
        rb2 = RoaringBitmap.remove(rb1,130, 185);
        bs.clear(130, 185);
        rb.remove(130, 185);
        assertTrue(rb2.equals(rb));
        assertTrue(TestRoaringBitmap.equals(bs, rb2));

        
    }
    
    @Test
    public void setRemoveTest1() {
        final BitSet bs = new BitSet();
        RoaringBitmap rb = new RoaringBitmap();
        bs.set(0,1000000);
        rb.add(0,1000000);
        rb.remove(43022,392542);
        bs.clear(43022,392542);
        assertTrue(TestRoaringBitmap.equals(bs, rb));
    }

    @Test
    public void setRemoveTest2() {
        final BitSet bs = new BitSet();
        RoaringBitmap rb = new RoaringBitmap();
        bs.set(43022,392542);
        rb.add(43022,392542);
        rb.remove(43022,392542);
        bs.clear(43022,392542);
        assertTrue(TestRoaringBitmap.equals(bs, rb));
    }
    
    @Test
    public void doubleadd() {
        System.out.println(Util.toIntUnsigned((short)-1752));
        System.out.println(Util.toIntUnsigned((short)-1));
        System.out.println((short)61565);
        RoaringBitmap rb = new RoaringBitmap();
        rb.add(65533, 65536);
        rb.add(65530, 65536);
        System.out.println(rb);
        BitSet bs = new BitSet();
        bs.set(65530, 65536);
        assertTrue(TestRoaringBitmap.equals(bs, rb));
        rb.remove(65530, 65536);
        assertTrue(rb.getCardinality() == 0);
    }
    

    @Test
    public void rangeAddRemoveBig() {
        final int numCases = 5000;
        RoaringBitmap rbstatic = new RoaringBitmap();
        final RoaringBitmap rbinplace = new RoaringBitmap();
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
            rbstatic = RoaringBitmap.add(rbstatic,start,end);
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
            rbstatic = RoaringBitmap.remove(rbstatic,start,end);
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
            rbstatic = RoaringBitmap.add(rbstatic,start,end);
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
            rbstatic = RoaringBitmap.add(rbstatic,start,end);
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
            rbstatic = RoaringBitmap.remove(rbstatic,start,end);
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
            rbstatic = RoaringBitmap.remove(rbstatic,start,end);
            bs.clear(start,end);
        }
        assertTrue(TestRoaringBitmap.equals(bs, rbstatic));
        assertTrue(TestRoaringBitmap.equals(bs, rbinplace));
        assertTrue(rbinplace.equals(rbstatic));
    }
    

    
}
