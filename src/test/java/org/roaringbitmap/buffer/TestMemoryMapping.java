/*
 * (c) Daniel Lemire, Owen Kaser, Samy Chambi, Jon Alvarado, Rory Graves, Björn Sperber
 * Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap.buffer;



import org.junit.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.roaringbitmap.IntIterator;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

@SuppressWarnings({"javadoc", "static-method"})
public class TestMemoryMapping {


    @Test
    public void basic() {
        for (int k = 0; k < mappedbitmaps.size(); ++k) {
            Assert.assertTrue(mappedbitmaps.get(k).equals(rambitmaps.get(k)));
        }
    }
    @Test
    public void testIterator() {
        for (int k = 0; k < mappedbitmaps.size(); ++k) {
            MutableRoaringBitmap copy0 = mappedbitmaps.get(k).toMutableRoaringBitmap();
            Assert.assertTrue(copy0.equals(mappedbitmaps.get(k)));
            MutableRoaringBitmap copy1 = new MutableRoaringBitmap();
            for(int x : mappedbitmaps.get(k)) {
                copy1.add(x);
            }    
            Assert.assertTrue(copy1.equals(mappedbitmaps.get(k)));
            MutableRoaringBitmap copy2 = new MutableRoaringBitmap();
            IntIterator i = mappedbitmaps.get(k).getIntIterator();
            while(i.hasNext()) {
                copy2.add(i.next());
            }
            Assert.assertTrue(copy2.equals(mappedbitmaps.get(k)));
        }

        MutableRoaringBitmap rb = new MutableRoaringBitmap();
        for(int k = 0; k<4000;++k) rb.add(k);
        for(int k = 0; k<1000;++k) rb.add(k*100);
        MutableRoaringBitmap copy1 = new MutableRoaringBitmap();
        for(int x : rb) {
            copy1.add(x);
        }
        Assert.assertTrue(copy1.equals(rb));
        MutableRoaringBitmap copy2 = new MutableRoaringBitmap();
        IntIterator i = rb.getIntIterator();
        while(i.hasNext()) {
            copy2.add(i.next());
        }
        Assert.assertTrue(copy2.equals(rb));
    }


    @Test
    public void complements() {
        System.out.println("testing complements");
        for (int k = 0; k < mappedbitmaps.size() - 1; k += 4) {
            final MutableRoaringBitmap rb = ImmutableRoaringBitmap.andNot(
                    mappedbitmaps.get(k), mappedbitmaps.get(k + 1));
            final MutableRoaringBitmap rbram = ImmutableRoaringBitmap
                    .andNot(rambitmaps.get(k), rambitmaps.get(k + 1));
            Assert.assertTrue(rb.equals(rbram));
        }
    }

    @Test
    public void intersections() {
        System.out.println("testing intersections");
        for (int k = 0; k + 1 < mappedbitmaps.size() ; k += 2) {
            final MutableRoaringBitmap rb = ImmutableRoaringBitmap.and(mappedbitmaps.get(k), mappedbitmaps.get(k+1));
            final MutableRoaringBitmap rbram = ImmutableRoaringBitmap.and(rambitmaps.get(k), rambitmaps.get(k+1));
            Assert.assertTrue(rb.equals(rbram));
        }

        for (int k = 0; k < mappedbitmaps.size() - 4; k += 4) {
        	final MutableRoaringBitmap rb = BufferFastAggregation.and(
                    mappedbitmaps.get(k), mappedbitmaps.get(k + 1),
                    mappedbitmaps.get(k + 3),
                    mappedbitmaps.get(k + 4));
        	final MutableRoaringBitmap rbram = BufferFastAggregation.and(
                    rambitmaps.get(k), rambitmaps.get(k + 1),
                    rambitmaps.get(k + 3), rambitmaps.get(k + 4));
            Assert.assertTrue(rb.equals(rbram));
        }
    }

    @Test
    public void unions() {
        System.out.println("testing Unions");
        for (int k = 0; k < mappedbitmaps.size() - 4; k += 4) {
            final MutableRoaringBitmap rb = BufferFastAggregation.or(
                    mappedbitmaps.get(k), mappedbitmaps.get(k + 1),
                    mappedbitmaps.get(k + 3),
                    mappedbitmaps.get(k + 4));
            final MutableRoaringBitmap rbram = BufferFastAggregation.or(
                    rambitmaps.get(k), rambitmaps.get(k + 1),
                    rambitmaps.get(k + 3), rambitmaps.get(k + 4));
            Assert.assertTrue(rb.equals(rbram));
        }
    }

    @Test
    public void XORs() {
        System.out.println("testing XORs");
        for (int k = 0; k < mappedbitmaps.size() - 4; k += 4) {
            final MutableRoaringBitmap rb = BufferFastAggregation.xor(
                    mappedbitmaps.get(k), mappedbitmaps.get(k + 1),
                    mappedbitmaps.get(k + 3),
                    mappedbitmaps.get(k + 4));
            final MutableRoaringBitmap rbram = BufferFastAggregation.xor(
                    rambitmaps.get(k), rambitmaps.get(k + 1),
                    rambitmaps.get(k + 3), rambitmaps.get(k + 4));
            Assert.assertTrue(rb.equals(rbram));
        }
    }
    
    @Test
    public void reserialize() throws IOException {
        System.out.println("testing reserialization");
        for (int k = 0; k < mappedbitmaps.size() ; ++k) {
            ImmutableRoaringBitmap rr = mappedbitmaps.get(k);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);
            rr.serialize(dos);
            dos.close();
            ByteBuffer bb = ByteBuffer.wrap(bos.toByteArray());
            ImmutableRoaringBitmap rrback = new ImmutableRoaringBitmap(bb);
            Assert.assertTrue(rr.equals(rrback));
            Assert.assertTrue(rr.equals(rrback.toMutableRoaringBitmap()));
            Assert.assertTrue(rr.toMutableRoaringBitmap().equals(rrback));
            Assert.assertTrue(rr.toMutableRoaringBitmap().equals(rambitmaps.get(k)));
        }
    }

    @AfterClass
    public static void clearFiles() {
        System.out.println("Cleaning memory-mapped file.");
        out = null;
        rambitmaps = null;
        mappedbitmaps = null;
        tmpfile.delete();
    }

    @BeforeClass
    public static void initFiles() throws IOException {
        System.out.println("Setting up memory-mapped file. (Can take some time.)");
        final ArrayList<Long> offsets = new ArrayList<Long>();
        tmpfile = File.createTempFile("roaring", "bin");
        tmpfile.deleteOnExit();
        final FileOutputStream fos = new FileOutputStream(tmpfile);
        final DataOutputStream dos = new DataOutputStream(fos);
        for (int N = 65536 * 16; N <= 65536 * 128; N *= 2) {
            for (int gap = 1; gap <= 65536; gap *= 4) {
                final MutableRoaringBitmap rb1 = new MutableRoaringBitmap();
                for (int x = 0; x < N; x += gap) {
                    rb1.add(x);
                }
                rambitmaps.add(rb1);
                offsets.add(fos.getChannel().position());
                rb1.serialize(dos);
                dos.flush();
                for (int offset = 1; offset <= gap; offset *= 8) {
                    final MutableRoaringBitmap rb2 = new MutableRoaringBitmap();
                    for (int x = 0; x < N; x += gap) {
                        rb2.add(x + offset);
                    }
                    offsets.add(fos.getChannel().position());
                    long pbef = fos.getChannel().position();
                    rb2.serialize(dos);
                    long paft = fos.getChannel().position();
                    if(paft - pbef != rb2.serializedSizeInBytes()) {
                        throw new RuntimeException("wrong serializedSizeInBytes:: paft-pbef = "+(paft - pbef)+", serializedSize = "+rb2.serializedSizeInBytes());
                    }
                    dos.flush();
                    rambitmaps.add(rb2);
                }
            }
        }
        final long totalcount = fos.getChannel().position();
        System.out.println("Wrote " + totalcount / 1024 + " KB");
        offsets.add(totalcount);
        dos.close();
        final RandomAccessFile memoryMappedFile = new RandomAccessFile(tmpfile, "r");
        out = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, totalcount);
        final long bef = System.currentTimeMillis();
        for (int k = 0; k < offsets.size() - 1; ++k) {
            ImmutableRoaringBitmap newbitmap = new ImmutableRoaringBitmap(out);
            if(newbitmap.serializedSizeInBytes()!= rambitmaps.get(k).serializedSizeInBytes()) {
                throw new RuntimeException("faulty reported serialization size "+newbitmap.serializedSizeInBytes()+" "+rambitmaps.get(k).serializedSizeInBytes());
            }
            if(!newbitmap.equals(rambitmaps.get(k))) {
                throw new RuntimeException("faulty serialization");
            }
            mappedbitmaps.add(newbitmap);
            out.position(out.position() + newbitmap.serializedSizeInBytes());
            if(out.position() != offsets.get(k+1).longValue())
                throw new RuntimeException("faulty serialization");
        }
        final long aft = System.currentTimeMillis();
        System.out.println("Mapped " + (offsets.size() - 1) + " bitmaps in " + (aft - bef) + "ms");
        memoryMappedFile.close();
    }

    static ArrayList<ImmutableRoaringBitmap> mappedbitmaps = new ArrayList<ImmutableRoaringBitmap>();

    static MappedByteBuffer out;

    static ArrayList<MutableRoaringBitmap> rambitmaps = new ArrayList<MutableRoaringBitmap>();

    static File tmpfile;
}
