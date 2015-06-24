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

import org.junit.BeforeClass;
import org.junit.Test;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import static org.junit.Assert.*;

public class TestSerialization {
    static RoaringBitmap bitmap_a;

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
        System.out.println(presoutbb.limit());
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
        int cksum1 = 0, cksum2 = 0;
        for (int x : bitmap_a) cksum1 += x;
        for (int x: bitmap_ar) cksum2 += x;
        assertEquals(cksum1,cksum2);
    }

    @BeforeClass
    public static void init() throws IOException {
        final int[] data = takeSortedAndDistinct(new Random(0xcb000a2b9b5bdfb6l), 100000);
        bitmap_a = RoaringBitmap.bitmapOf(data);
        bitmap_ar = MutableRoaringBitmap.bitmapOf(data);
        for(int k = 100000; k < 200000; ++k) {
            bitmap_a.add(3 * k);   // bitmap density and too many little runs
            bitmap_ar.add(3 * k);
        }

        for (int k=700000; k < 800000; ++k) {  // runcontainer would be best
            bitmap_a.add(k);
            bitmap_ar.add(k);
        }

        bitmap_a.runOptimize();  // mix of all 3 container kinds
        bitmap_ar.runOptimize(); // must stay in sync with bitmap_a
        /* There is potentially some "slop" betweeen the size estimates used for
           RoaringBitmaps and MutableRoaringBitmaps, so it is risky to assume that
           they will both *always* agree whether to run encode a container.  Nevertheless
           testMutableSerialize effectively does that, by using the serialized size 
           of one as the output buffer size for the other. */
       
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
   
}





class ByteBufferBackedInputStream extends InputStream {
  
  ByteBuffer buf;
  ByteBufferBackedInputStream( ByteBuffer buf){
    this.buf = buf;
  }
  public int read() throws IOException {
    if (!buf.hasRemaining()) {
      return -1;
    }
    return 0xFF & buf.get();
  }
  public int read(byte[] bytes) throws IOException {
      int len = Math.min(bytes.length, buf.remaining());
      buf.get(bytes, 0, len);
      return len;
   }
  
  public long skip(long n) {
      int len = Math.min((int)n, buf.remaining());
      buf.position(buf.position() + (int)n);
      return len;
  }
  
  public int available() throws IOException {
      return buf.remaining();
  }
  
  public boolean markSupported() {
      return false;
  }
      
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
  public synchronized void write(int b) throws IOException {
    buf.put((byte) b);
  }

  public synchronized void write(byte[] bytes) throws IOException {
    buf.put(bytes);
  }
  
  public synchronized void write(byte[] bytes, int off, int len) throws IOException {
    buf.put(bytes, off, len);
  }
  
}

