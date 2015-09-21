package org.roaringbitmap;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Random;
import java.util.Iterator;

import org.junit.BeforeClass;
import org.junit.Test;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import static org.junit.Assert.*;

public class TestSerialization {
    static RoaringBitmap bitmap_a;

    static RoaringBitmap bitmap_a1;

    static RoaringBitmap bitmap_b = new RoaringBitmap();

    static MutableRoaringBitmap bitmap_ar;

    static MutableRoaringBitmap bitmap_br = new MutableRoaringBitmap();

    static ByteBuffer outbb; 

    static ByteBuffer presoutbb; 

    @Test
    public void testDeserialize() throws IOException {
        presoutbb.rewind();
        ByteBufferBackedInputStream in = new ByteBufferBackedInputStream(presoutbb);
        bitmap_b.deserialize(new DataInputStream(in));
    }

    @Test
    public void testMutableDeserializeMutable() throws IOException {
        presoutbb.rewind();
        ByteBufferBackedInputStream in = new ByteBufferBackedInputStream(presoutbb);
        bitmap_br.deserialize(new DataInputStream(in));
    }

    
    @Test
    public void testSerialize() throws IOException {
        outbb.rewind();
        ByteBufferBackedOutputStream out = new ByteBufferBackedOutputStream(outbb);
        bitmap_a.serialize(new DataOutputStream(out));
    }

    
    @Test
    public void testMutableSerialize()  throws IOException {
        System.out.println("testMutableSerialize");
        outbb.rewind();
        ByteBufferBackedOutputStream out = new ByteBufferBackedOutputStream(outbb);
        System.out.println("bitmap_ar is "+bitmap_ar.getClass().getName());
        bitmap_ar.serialize(new DataOutputStream(out));
    }



    @Test
    public void testMutableBuilding() {
        // did we build a mutable equal to the regular one?
        int cksum1 = 0, cksum2 = 0;
        for (int x : bitmap_a) cksum1 += x;
        for (int x: bitmap_ar) cksum2 += x;
        assertEquals(cksum1,cksum2);
    }


    @Test
    public void testMutableBuildingBySerialization() throws IOException {
        presoutbb.rewind();
        ByteBufferBackedInputStream in = new ByteBufferBackedInputStream(presoutbb);
        MutableRoaringBitmap mrb = new MutableRoaringBitmap();
        mrb.deserialize(new DataInputStream(in));
        int cksum1 = 0, cksum2 = 0;
        for (int x : bitmap_a) cksum1 += x;
        for (int x: mrb) cksum2 += x;
        assertEquals(cksum1,cksum2);
    }

    @Test
    public void testImmutableBuildingBySerialization() {
        presoutbb.rewind();
        ImmutableRoaringBitmap imrb = new ImmutableRoaringBitmap(presoutbb);
        int cksum1 = 0, cksum2 = 0, count1=0, count2=0;
        for (int x : bitmap_a) {  // or bitmap_a1 for a version without run
            cksum1 += x; ++count1;
        }
        for (int x: imrb) {cksum2 += x; ++count2;}

        Iterator<Integer> it1, it2;
        it1 = bitmap_a.iterator();
        //it1 = bitmap_a1.iterator();
        it2 = imrb.iterator();
        int blabcount=0;
        int valcount=0;
        while (it1.hasNext() && it2.hasNext()) {
            ++valcount;
            int val1 = it1.next(), val2 = it2.next();
            if (val1 != val2) {
                    if (++blabcount < 10)
                        System.out.println("disagree on "+valcount+" nonmatching values are "+val1+" "+val2);
            }
        }
        System.out.println("there were "+blabcount+" diffs");
        if (it1.hasNext() != it2.hasNext()) 
            System.out.println("one ran out earlier");

        assertEquals(count1,count2);
        assertEquals(cksum1,cksum2);
    }


    @Test
    public void testImmutableBuildingBySerializationSimple() {
     System.out.println("testImmutableBuildingBySerializationSimple ");
     ByteBuffer bb1; 
     MutableRoaringBitmap bm1 = new MutableRoaringBitmap();
        for (int k=20; k < 30; ++k) {  // runcontainer would be best
            bm1.add(k);
        }
        bm1.runOptimize();  
       
        bb1 = ByteBuffer.allocate(bitmap_a.serializedSizeInBytes());
        ByteBufferBackedOutputStream out = new ByteBufferBackedOutputStream(bb1);
        try {
           bm1.serialize(new DataOutputStream(out));
        } catch(Exception e) {
            e.printStackTrace();
        }
        bb1.flip();
        ImmutableRoaringBitmap imrb = new ImmutableRoaringBitmap(bb1);
        int cksum1 = 0, cksum2 = 0, count1=0, count2=0;
        for (int x : bm1) {
            cksum1 += x; ++count1;
        }

        for (int x: imrb) {cksum2 += x; ++count2;}
    
        assertEquals(count1,count2);
        assertEquals(cksum1,cksum2);
    }





    @BeforeClass
    public static void init() throws IOException {
        final int[] data = takeSortedAndDistinct(new Random(0xcb000a2b9b5bdfb6l), 100000);
        bitmap_a = RoaringBitmap.bitmapOf(data);
        bitmap_ar = MutableRoaringBitmap.bitmapOf(data);
        bitmap_a1 = RoaringBitmap.bitmapOf(data);
      
        for(int k = 100000; k < 200000; ++k) {
            bitmap_a.add(3 * k);   // bitmap density and too many little runs
            bitmap_ar.add(3 * k);
            bitmap_a1.add(3*k);
        }

        for (int k=700000; k < 800000; ++k) {  // runcontainer would be best
            bitmap_a.add(k);
            bitmap_ar.add(k);
            bitmap_a1.add(k);
        }

        bitmap_a.runOptimize();  // mix of all 3 container kinds
        bitmap_ar.runOptimize(); // must stay in sync with bitmap_a
        /* There is potentially some "slop" betweeen the size estimates used for
           RoaringBitmaps and MutableRoaringBitmaps, so it is risky to assume that
           they will both *always* agree whether to run encode a container.  Nevertheless
           testMutableSerialize effectively does that, by using the serialized size 
           of one as the output buffer size for the other. */
        // do not runoptimize bitmap_a1
       
        outbb = ByteBuffer.allocate(bitmap_a.serializedSizeInBytes());
        presoutbb = ByteBuffer.allocate(bitmap_a.serializedSizeInBytes());
        ByteBufferBackedOutputStream out = new ByteBufferBackedOutputStream(presoutbb);
        try {
           bitmap_a.serialize(new DataOutputStream(out));
        } catch(Exception e) {
            e.printStackTrace();
        }
        presoutbb.flip();

    }
    
    private static int[] takeSortedAndDistinct(Random source, int count) {

        LinkedHashSet<Integer> ints = new LinkedHashSet<Integer>(count);

        for (int size = 0; size < count; size++) {
           int next;
           do {
              next = Math.abs(source.nextInt());
           } while (!ints.add(next));
        }

        int[] unboxed = toArray(ints);
        Arrays.sort(unboxed);
        return unboxed;
     }

     private static int[] toArray(LinkedHashSet<Integer> integers) {
        int[] ints = new int[integers.size()];
        int i = 0;
        for (Integer n : integers) {
           ints[i++] = n;
        }
        return ints;
     }

    
  @Test
  public void testRunSerializationDeserialization() throws java.io.IOException {
      final int[] data = takeSortedAndDistinct(new Random(07734), 100000);
      RoaringBitmap bitmap_a = RoaringBitmap.bitmapOf(data);
      RoaringBitmap bitmap_ar = RoaringBitmap.bitmapOf(data);
      
        for(int k = 100000; k < 200000; ++k) {
            bitmap_a.add(3 * k);   // bitmap density and too many little runs
            bitmap_ar.add(3 * k);
        }

        for (int k=700000; k < 800000; ++k) {  //  will choose a runcontainer on this
            bitmap_a.add(k);
            bitmap_ar.add(k);
        }
      
        bitmap_a.runOptimize();  // mix of all 3 container kinds

        ByteBuffer outbuf = ByteBuffer.allocate(bitmap_a.serializedSizeInBytes());
        ByteBufferBackedOutputStream out = new ByteBufferBackedOutputStream(outbuf);
        try {
           bitmap_a.serialize(new DataOutputStream(out));
        } catch(Exception e) {
            e.printStackTrace();
        }
        outbuf.flip();

        RoaringBitmap bitmap_c = new RoaringBitmap();

        ByteBufferBackedInputStream in = new ByteBufferBackedInputStream(outbuf);
        bitmap_c.deserialize(new DataInputStream(in));

        assertEquals(bitmap_a, bitmap_c);
  }


    
  @Test
  public void testMutableRunSerializationBasicDeserialization() throws java.io.IOException {
      final int[] data = takeSortedAndDistinct(new Random(07734), 100000);
      RoaringBitmap bitmap_a = RoaringBitmap.bitmapOf(data);
      MutableRoaringBitmap bitmap_ar = MutableRoaringBitmap.bitmapOf(data);
      
        for(int k = 100000; k < 200000; ++k) {
            bitmap_a.add(3 * k);   // bitmap density and too many little runs
            bitmap_ar.add(3 * k);
        }

        for (int k=700000; k < 800000; ++k) {  //  will choose a runcontainer on this
            bitmap_a.add(k);
            bitmap_ar.add(k);
        }
      
        bitmap_a.runOptimize();  // mix of all 3 container kinds
        bitmap_ar.runOptimize();

        ByteBuffer outbuf = ByteBuffer.allocate(bitmap_a.serializedSizeInBytes());
        ByteBufferBackedOutputStream out = new ByteBufferBackedOutputStream(outbuf);
        try {
           bitmap_a.serialize(new DataOutputStream(out));
        } catch(Exception e) {
            e.printStackTrace();
        }
        outbuf.flip();

        RoaringBitmap bitmap_c = new RoaringBitmap();

        ByteBufferBackedInputStream in = new ByteBufferBackedInputStream(outbuf);
        bitmap_c.deserialize(new DataInputStream(in));

        assertEquals(bitmap_a, bitmap_c);
  }





   
}





class ByteBufferBackedInputStream extends InputStream {
  
  ByteBuffer buf;
  ByteBufferBackedInputStream( ByteBuffer buf){
    this.buf = buf;
  }
  
  @Override
  public int read() throws IOException {
    if (!buf.hasRemaining()) {
      return -1;
    }
    return 0xFF & buf.get();
  }
  
  @Override
  public int read(byte[] bytes) throws IOException {
      int len = Math.min(bytes.length, buf.remaining());
      buf.get(bytes, 0, len);
      return len;
  }
  
  @Override
  public long skip(long n) {
      int len = Math.min((int)n, buf.remaining());
      buf.position(buf.position() + (int)n);
      return len;
  }
  
  @Override
  public int available() throws IOException {
      return buf.remaining();
  }
  
  @Override
  public boolean markSupported() {
      return false;
  }
      
  @Override
  public int read(byte[] bytes, int off, int len) throws IOException {
    len = Math.min(len, buf.remaining());
    buf.get(bytes, off, len);
    return len;
  }
}

class ByteBufferBackedOutputStream extends OutputStream{
  ByteBuffer buf;
  ByteBufferBackedOutputStream( ByteBuffer buf){
    this.buf = buf;
  }
  
  @Override
  public synchronized void write(int b) throws IOException {
    buf.put((byte) b);
  }

  @Override
  public synchronized void write(byte[] bytes) throws IOException {
    buf.put(bytes);
  }
  
  @Override
  public synchronized void write(byte[] bytes, int off, int len) throws IOException {
    buf.put(bytes, off, len);
  }
  
}

