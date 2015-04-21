package org.roaringbitmap.iteration;

import org.openjdk.jmh.annotations.*;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.TimeUnit;


@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class SerializationBenchmark {


   @BenchmarkMode(Mode.AverageTime)
   @Benchmark
   public int testDeserialize(BenchmarkState benchmarkState) throws IOException {
       benchmarkState.presoutbb.rewind();
       ByteBufferBackedInputStream in = new ByteBufferBackedInputStream(benchmarkState.presoutbb);
       benchmarkState.bitmap_b.deserialize(new DataInputStream(in));
       return benchmarkState.presoutbb.limit();
   }

   @BenchmarkMode(Mode.AverageTime)
   @Benchmark
   public int testMutableDeserializeMutable(BenchmarkState benchmarkState) throws IOException {
       benchmarkState.presoutbb.rewind();
       ByteBufferBackedInputStream in = new ByteBufferBackedInputStream(benchmarkState.presoutbb);
       benchmarkState.bitmap_br.deserialize(new DataInputStream(in));
       return benchmarkState.presoutbb.limit();
   }

   
   @BenchmarkMode(Mode.AverageTime)
   @Benchmark
   public int testSerialize(BenchmarkState benchmarkState) throws IOException {
       benchmarkState.outbb.rewind();
       ByteBufferBackedOutputStream out = new ByteBufferBackedOutputStream(benchmarkState.outbb);
       benchmarkState.bitmap_a.serialize(new DataOutputStream(out));
       return benchmarkState.outbb.limit();
   }

   
   @BenchmarkMode(Mode.AverageTime)
   @Benchmark
   public int testMutableSerialize(BenchmarkState benchmarkState)  throws IOException {
       benchmarkState.outbb.rewind();
       ByteBufferBackedOutputStream out = new ByteBufferBackedOutputStream(benchmarkState.outbb);
       benchmarkState.bitmap_ar.serialize(new DataOutputStream(out));
       return benchmarkState.outbb.limit();
   }

   
   @State(Scope.Benchmark)
   public static class BenchmarkState {

      final RoaringBitmap bitmap_a;

      final RoaringBitmap bitmap_b = new RoaringBitmap();

      final MutableRoaringBitmap bitmap_ar;

      final MutableRoaringBitmap bitmap_br = new MutableRoaringBitmap();

      final ByteBuffer outbb; 

      final ByteBuffer presoutbb; 

      
      public BenchmarkState() {

         final int[] data = takeSortedAndDistinct(new Random(0xcb000a2b9b5bdfb6l), 100000);
         bitmap_a = RoaringBitmap.bitmapOf(data);
         bitmap_ar = MutableRoaringBitmap.bitmapOf(data);
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

      private int[] takeSortedAndDistinct(Random source, int count) {

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

      private int[] toArray(LinkedHashSet<Integer> integers) {
         int[] ints = new int[integers.size()];
         int i = 0;
         for (Integer n : integers) {
            ints[i++] = n;
         }
         return ints;
      }
   }

}



class ByteBufferBackedInputStream extends InputStream {
  
  ByteBuffer buf;
  ByteBufferBackedInputStream( ByteBuffer buf){
    this.buf = buf;
  }
  public synchronized int read() throws IOException {
    if (!buf.hasRemaining()) {
      return -1;
    }
    return buf.get();
  }
  public synchronized int read(byte[] bytes, int off, int len) throws IOException {
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

  public synchronized void write(byte[] bytes, int off, int len) throws IOException {
    buf.put(bytes, off, len);
  }
  
}
