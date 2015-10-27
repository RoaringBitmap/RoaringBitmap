package org.roaringbitmap;

import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.roaringbitmap.realdata.wrapper.Bitmap;

import java.util.ArrayList;
import java.util.List;

import static org.roaringbitmap.realdata.wrapper.BitmapFactory.*;

@State(Scope.Benchmark)
public abstract class AbstractBenchmarkState {

    public List<Bitmap> bitmaps;

    public AbstractBenchmarkState() {
    }

    public void setup(String dataset, String type, boolean immutable) throws Exception {
        bitmaps = new ArrayList<Bitmap>();

        ZipRealDataRetriever dataRetriever = new ZipRealDataRetriever(dataset);

        for (int[] data : dataRetriever.fetchBitPositions()) {

            Bitmap bitmap = null;

            if (CONCISE.equals(type)) {
                if (!immutable) {
                    bitmap = newConciseBitmap(data);
                } else {
                    bitmap = newImmutableConciseBitmap(data);
                }
            } else if (WAH.equals(type)) {
                if (!immutable) {
                    bitmap = newWahBitmap(data);
                } else {
                    bitmap = newImmutableWahBitmap(data);
                }
            } else if (EWAH.equals(type)) {
                if (!immutable) {
                    bitmap = newEwahBitmap(data);
                } else {
                    bitmap = newImmutableEwahBitmap(data);
                }
            } else if (EWAH32.equals(type)) {
                if (!immutable) {
                    bitmap = newEwah32Bitmap(data);
                } else {
                    bitmap = newImmutableEwah32Bitmap(data);
                }
            } else if (ROARING.equals(type)) {
                if (!immutable) {
                    bitmap = newRoaringBitmap(data);
                } else {
                    bitmap = newImmutableRoaringBitmap(data);
                }
            } else if (ROARING_WITH_RUN.equals(type)) {
                if (!immutable) {
                    bitmap = newRoaringWithRunBitmap(data);
                } else {
                    bitmap = newImmutableRoaringWithRunBitmap(data);
                }
            }

            if (bitmap == null) {
                throw new RuntimeException(String.format("Unsupported parameters: type=%s, immutable=%b", type, immutable));
            }

            bitmaps.add(bitmap);

        }
    }

    @TearDown
    public void tearDown() {
        cleanup();
    }

}
