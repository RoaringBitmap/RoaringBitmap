package org.roaringbitmap.runcontainer;


import it.uniroma3.mat.extendedset.intset.ConciseSet;
import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.roaringbitmap.ZipRealDataRetriever;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class MappedRunContainerRealDataBenchmarkIterate {

    static ConciseSet toConcise(int[] dat) {
        ConciseSet ans = new ConciseSet();
        for (int i : dat) {
            ans.add(i);
        }
        return ans;
    }

    @Benchmark
    public int iterate_RoaringWithRun(BenchmarkState benchmarkState) {
        int total = 0;
        for (int k = 0; k < benchmarkState.mrc.size(); ++k) {
            ImmutableRoaringBitmap rb = benchmarkState.mrc.get(k);
            org.roaringbitmap.IntIterator i = rb.getIntIterator();
            while(i.hasNext())
                total += i.next();
        }
        if(total != benchmarkState.expectedvalue)
            throw new RuntimeException("bug");
        return total;
    }


    @Benchmark
    public int iterate_Roaring(BenchmarkState benchmarkState) {
        int total = 0;
        for (int k = 0; k < benchmarkState.mac.size(); ++k) {
            ImmutableRoaringBitmap rb = benchmarkState.mac.get(k);
            org.roaringbitmap.IntIterator i = rb.getIntIterator();
            while(i.hasNext())
                total += i.next();
        }
        if(total != benchmarkState.expectedvalue)
            throw new RuntimeException("bug");
        return total;
    }
    
    
    @Benchmark
    public int iterate_Concise(BenchmarkState benchmarkState) {
        int total = 0;
        for (int k = 0; k < benchmarkState.cc.size(); ++k) {
            ImmutableConciseSet cs = benchmarkState.cc.get(k);
            it.uniroma3.mat.extendedset.intset.IntSet.IntIterator i = cs.iterator();
            while(i.hasNext())
                total += i.next();
        }
        if(total != benchmarkState.expectedvalue)
            throw new RuntimeException("bug");
        return total;
    }


    @State(Scope.Benchmark)
    public static class BenchmarkState {
        @Param ({// putting the data sets in alpha. order
            "census-income", "census1881",
            "dimension_008", "dimension_003", 
            "dimension_033", "uscensus2000", 
            "weather_sept_85", "wikileaks-noquotes"
        })
        String dataset;
        public int expectedvalue = 0;

        List<ImmutableRoaringBitmap> mrc = new ArrayList<ImmutableRoaringBitmap>();
        List<ImmutableRoaringBitmap> mac = new ArrayList<ImmutableRoaringBitmap>();
        List<ImmutableConciseSet> cc = new ArrayList<ImmutableConciseSet>();

        public BenchmarkState() {
        }
        
        
        public List<ImmutableRoaringBitmap> convertToImmutableRoaring(List<MutableRoaringBitmap> source) throws IOException {
            System.out.println("Setting up memory-mapped file. (Can take some time.)");
            File tmpfile = File.createTempFile("roaring", "bin");
            tmpfile.deleteOnExit();
            final FileOutputStream fos = new FileOutputStream(tmpfile);
            final DataOutputStream dos = new DataOutputStream(fos);
            
            for(MutableRoaringBitmap rb1 : source)
                rb1.serialize(dos);
            
            final long totalcount = fos.getChannel().position();
            System.out.println("[roaring] Wrote " + totalcount / 1024 + " KB");
            dos.close();
            final RandomAccessFile memoryMappedFile = new RandomAccessFile(tmpfile, "r");
            ByteBuffer out = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, totalcount);
            ArrayList<ImmutableRoaringBitmap> answer = new ArrayList<ImmutableRoaringBitmap>(source.size());
            while(out.position()< out.limit()) {
                    final ByteBuffer bb = out.slice();
                    ImmutableRoaringBitmap newbitmap = new ImmutableRoaringBitmap(bb);       
                    answer.add(newbitmap);
                    out.position(out.position() + newbitmap.serializedSizeInBytes());
            }
            memoryMappedFile.close();
            return answer;
        }
        
        public List<ImmutableConciseSet> convertToImmutableConcise(List<ConciseSet> source) throws IOException {
            System.out.println("Setting up memory-mapped file. (Can take some time.)");
            File tmpfile = File.createTempFile("concise", "bin");
            tmpfile.deleteOnExit();
            final FileOutputStream fos = new FileOutputStream(tmpfile);
            final DataOutputStream dos = new DataOutputStream(fos);
            int[] sizes = new int[source.size()];
            int pos = 0;
            for(ConciseSet cc : source) {
                byte[] data = ImmutableConciseSet.newImmutableFromMutable(cc).toBytes();
                sizes[pos++] = data.length;
                fos.write(data);
            }
            final long totalcount = fos.getChannel().position();
            System.out.println("[concise] Wrote " + totalcount / 1024 + " KB");
            dos.close();
            RandomAccessFile  memoryMappedFile = new RandomAccessFile(tmpfile, "r");
            ByteBuffer out = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, totalcount);
            ArrayList<ImmutableConciseSet> answer = new ArrayList<ImmutableConciseSet>(source.size());
            while(out.position() < out.limit()) {
                    byte[] olddata = ImmutableConciseSet.newImmutableFromMutable(source.get(answer.size())).toBytes();
                    final ByteBuffer bb = out.slice();
                    bb.limit(sizes[answer.size()]);
                    ImmutableConciseSet newbitmap = new ImmutableConciseSet(bb);
                    byte[] newdata = newbitmap.toBytes();
                    if(!Arrays.equals(olddata, newdata))
                       throw new RuntimeException("bad concise serialization");
                    answer.add(newbitmap);
                    out.position(out.position() + bb.limit());
            }
            memoryMappedFile.close();
            return answer;
        }

                
                
        @Setup
        public void setup() throws Exception {
            ZipRealDataRetriever dataRetriever = new ZipRealDataRetriever(dataset);
            System.out.println();
            System.out.println("Loading files from " + dataRetriever.getName());
            ArrayList<MutableRoaringBitmap> tmpac = new ArrayList<MutableRoaringBitmap>();
            ArrayList<MutableRoaringBitmap> tmprc = new ArrayList<MutableRoaringBitmap>();
            ArrayList<ConciseSet> tmpcc = new ArrayList<ConciseSet>();

            for (int[] data : dataRetriever.fetchBitPositions()) {
                MutableRoaringBitmap mbasic = MutableRoaringBitmap.bitmapOf(data);
                MutableRoaringBitmap mopti = mbasic.clone();
                mopti.runOptimize();
                ConciseSet concise = toConcise(data);
                tmpac.add(mbasic);
                tmprc.add(mopti);
                tmpcc.add(concise);
            }
            mrc = convertToImmutableRoaring(tmprc);
            mac = convertToImmutableRoaring(tmpac);
            cc = convertToImmutableConcise(tmpcc);
            if((mrc.size() != mac.size()) || (mac.size() != cc.size()))
                throw new RuntimeException("number of bitmaps do not match.");
            expectedvalue = 0;
            for (int k = 0; k < mrc.size(); ++k) {
                ImmutableRoaringBitmap rb = mrc.get(k);
                org.roaringbitmap.IntIterator i = rb.getIntIterator();
                while(i.hasNext())
                    expectedvalue += i.next();
            }

        }

    }

}
