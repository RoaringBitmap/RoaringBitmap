package org.roaringbitmap.buffer;

import static org.roaringbitmap.buffer.BenchmarkConsumers.CONSOLE_CONSUMER;
import static org.roaringbitmap.buffer.BenchmarkConsumers.H2_CONSUMER;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;

public class TestBenchmarkBasic {
    
    @Rule
    public TestRule benchmarkRun = new BenchmarkRule(CONSOLE_CONSUMER, H2_CONSUMER);

    @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 3)
    @Test
    public void bigunion() throws Exception {
        for (int k = 1; k < N; k += 10) {
            MutableRoaringBitmap bitmapor = BufferFastAggregation.horizontal_or(Arrays.copyOf(ewah, k + 1));
            bogus += bitmapor.getCardinality();
        }
    }

    @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 3)
    @Test
    public void bigunionbuf() throws Exception {
        for (int k = 1; k < N; k += 10) {
            MutableRoaringBitmap bitmapor = BufferFastAggregation.horizontal_or(Arrays.copyOf(ewahbuf, k + 1));
            bogus += bitmapor.getCardinality();
        }
    }


    @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 3)
    @Test
    public void toarray() throws Exception {
        for (int k = 1; k < N * 100; ++k) {
            bogus += ewah[k % N].toArray().length;
        }
    }

    @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 3)
    @Test
    public void toarraybuf() throws Exception {
        for (int k = 1; k < N * 100; ++k) {
            bogus += ewahbuf[k % N].toArray().length;
        }
    }


    @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 3)
    @Test
    public void cardinality() throws Exception {
        for (int k = 1; k < N * 100; ++k) {
            bogus += ewah[k % N].getCardinality();
        }
    }

    @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 3)
    @Test
    public void cardinalitybuf() throws Exception {
        for (int k = 1; k < N * 100; ++k) {
            bogus += ewahbuf[k % N].getCardinality();
        }
    }


    @BeforeClass
    public static void prepare() throws IOException {
        for (int k = 0; k < N; ++k) {
            ewah[k] = new MutableRoaringBitmap();
            for (int x = 0; x < M; ++x) {
                ewah[k].add(x * (N - k + 2));
            }
            ewah[k].trim();
            ewahbuf[k] = convertToMappedBitmap(ewah[k]);
        }
    }

    @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 3)
    @Test
    public void createBitmapOrdered() {
        long besttime = Long.MAX_VALUE;
        MutableRoaringBitmap r = new MutableRoaringBitmap();
        long bef = System.nanoTime();
        for (int k = 0; k < 65536; k++) {
            r.add(k * 32);
        }
        long aft = System.nanoTime();
        if(besttime > aft - bef) besttime = aft-bef;
    }
    
    @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 3)
    @Test
    public void createBitmapUnordered() {
        long besttime = Long.MAX_VALUE;
        MutableRoaringBitmap r = new MutableRoaringBitmap();
        long bef = System.nanoTime();
        for (int k = 65536 - 1; k >= 0; k--) {
            r.add(k * 32);
        }
        long aft = System.nanoTime();
        if (besttime > aft - bef)
            besttime = aft - bef;
    }

    public static ImmutableRoaringBitmap convertToMappedBitmap(MutableRoaringBitmap orig) throws IOException {
        File tmpfile = File.createTempFile("roaring", ".bin");
        tmpfile.deleteOnExit();
        final FileOutputStream fos = new FileOutputStream(tmpfile);
        orig.serialize(new DataOutputStream(fos));
        long totalcount = fos.getChannel().position();
        fos.close();
        RandomAccessFile memoryMappedFile = new RandomAccessFile(tmpfile, "r");
        ByteBuffer bb = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, totalcount);
        return new ImmutableRoaringBitmap(bb);
    }

    static int N = 1000;
    static int M = 1000;

    static MutableRoaringBitmap[] ewah = new MutableRoaringBitmap[N];
    static ImmutableRoaringBitmap[] ewahbuf = new ImmutableRoaringBitmap[N];

    public static int bogus = 0;
}
