/*
 * (c) Daniel Lemire, Owen Kaser, Samy Chambi, Jon Alvarado, Rory Graves, Björn Sperber
 * Licensed under the Apache License, Version 2.0.
 */
package org.roaringbitmap.buffer;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * Fast algorithms to aggregate many bitmaps.
 *
 * @author Daniel Lemire
 */
public final class BufferFastAggregation {

    /**
     * Private constructor to prevent instantiation of utility class
     */
    private BufferFastAggregation() {
    }

    /**
     * Sort the bitmap prior to using the and aggregate.
     *
     * @param bitmaps input bitmaps
     * @return aggregated bitmap
     */
    public static MappeableRoaringBitmap and(ImmutableRoaringBitmap... bitmaps) {
        if (bitmaps.length == 0)
          return new MappeableRoaringBitmap();
        else if(bitmaps.length == 1)
    	  return bitmaps[0].clone();
        final ImmutableRoaringBitmap[] array = Arrays.copyOf(bitmaps, bitmaps.length);
        Arrays.sort(array, new Comparator<ImmutableRoaringBitmap>() {
            @Override
            public int compare(ImmutableRoaringBitmap a,
                               ImmutableRoaringBitmap b) {
                return a.getSizeInBytes() - b.getSizeInBytes();
            }
        });
        MappeableRoaringBitmap answer = ImmutableRoaringBitmap.and(array[0], array[1]);
        for (int k = 2; k < array.length; ++k)
            answer.and(array[k]);
        return answer;
    }

    /**
     * Uses a priority queue to compute the or aggregate.
     *
     * @param bitmaps input bitmaps
     * @return aggregated bitmap
     */
    public static MappeableRoaringBitmap or(ImmutableRoaringBitmap... bitmaps) {
        if (bitmaps.length < 2)
            throw new IllegalArgumentException(
                    "Expecting at least 2 bitmaps");
        final PriorityQueue<ImmutableRoaringBitmap> pq = new PriorityQueue<ImmutableRoaringBitmap>(
                bitmaps.length,
                new Comparator<ImmutableRoaringBitmap>() {
                    @Override
                    public int compare(ImmutableRoaringBitmap a,
                                       ImmutableRoaringBitmap b) {
                        return a.getSizeInBytes() - b.getSizeInBytes();
                    }
                }
        );
        Collections.addAll(pq, bitmaps);
        while (pq.size() > 1) {
            final ImmutableRoaringBitmap x1 = pq.poll();
            final ImmutableRoaringBitmap x2 = pq.poll();
            pq.add(ImmutableRoaringBitmap.or(x1, x2));
        }
        return (MappeableRoaringBitmap) pq.poll();
    }

    /**
     * Uses a priority queue to compute the xor aggregate.
     *
     * @param bitmaps input bitmaps
     * @return aggregated bitmap
     */
    public static MappeableRoaringBitmap xor(ImmutableRoaringBitmap... bitmaps) {
        if (bitmaps.length < 2)
            throw new IllegalArgumentException(
                    "Expecting at least 2 bitmaps");
        final PriorityQueue<ImmutableRoaringBitmap> pq = new PriorityQueue<ImmutableRoaringBitmap>(
                bitmaps.length,
                new Comparator<ImmutableRoaringBitmap>() {
                    @Override
                    public int compare(ImmutableRoaringBitmap a,
                                       ImmutableRoaringBitmap b) {
                        return a.getSizeInBytes() - b.getSizeInBytes();
                    }
                }
        );
        Collections.addAll(pq, bitmaps);
        while (pq.size() > 1) {
            final ImmutableRoaringBitmap x1 = pq.poll();
            final ImmutableRoaringBitmap x2 = pq.poll();
            pq.add(ImmutableRoaringBitmap.xor(x1, x2));
        }
        return (MappeableRoaringBitmap) pq.poll();
    }

}
