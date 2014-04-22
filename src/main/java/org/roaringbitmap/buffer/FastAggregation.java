/*
 * Copyright 2013-2014 by Daniel Lemire, Owen Kaser and Samy Chambi
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
public final class FastAggregation {

    /**
     * Private constructor to prevent instantiation of utility class
     */
    private FastAggregation() {
    }

    /**
     * Sort the bitmap prior to using the and aggregate.
     *
     * @param bitmaps input bitmaps
     * @return aggregated bitmap
     */
    public static RoaringBitmap and(ImmutableRoaringBitmap... bitmaps) {
        if (bitmaps.length < 2)
            throw new IllegalArgumentException("Expecting at least 2 bitmaps");
        final ImmutableRoaringBitmap[] array = Arrays.copyOf(bitmaps, bitmaps.length);
        Arrays.sort(array, new Comparator<ImmutableRoaringBitmap>() {
            @Override
            public int compare(ImmutableRoaringBitmap a,
                               ImmutableRoaringBitmap b) {
                return a.getSizeInBytes() - b.getSizeInBytes();
            }
        });
        ImmutableRoaringBitmap answer = array[0];
        for (int k = 1; k < array.length; ++k)
            answer = ImmutableRoaringBitmap.and(answer, array[k]);
        return (RoaringBitmap) answer;
    }

    /**
     * Uses a priority queue to compute the or aggregate.
     *
     * @param bitmaps input bitmaps
     * @return aggregated bitmap
     */
    public static RoaringBitmap or(ImmutableRoaringBitmap... bitmaps) {
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
        return (RoaringBitmap) pq.poll();
    }

    /**
     * Uses a priority queue to compute the xor aggregate.
     *
     * @param bitmaps input bitmaps
     * @return aggregated bitmap
     */
    public static RoaringBitmap xor(ImmutableRoaringBitmap... bitmaps) {
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
        return (RoaringBitmap) pq.poll();
    }

}
