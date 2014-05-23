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
     * Sort the bitmap prior to using the and aggregate.
     * 
     * @param bitmaps
     *            input bitmaps
     * @return aggregated bitmap
     */
    public static MutableRoaringBitmap and(ImmutableRoaringBitmap... bitmaps) {
        if (bitmaps.length == 0)
            return new MutableRoaringBitmap();
        else if (bitmaps.length == 1)
            return bitmaps[0].toMutableRoaringBitmap();
        final ImmutableRoaringBitmap[] array = Arrays.copyOf(bitmaps,
                bitmaps.length);
        Arrays.sort(array, new Comparator<ImmutableRoaringBitmap>() {
            @Override
            public int compare(ImmutableRoaringBitmap a,
                    ImmutableRoaringBitmap b) {
                return a.getSizeInBytes() - b.getSizeInBytes();
            }
        });
        MutableRoaringBitmap answer = ImmutableRoaringBitmap.and(array[0],
                array[1]);
        for (int k = 2; k < array.length; ++k)
            answer.and(array[k]);
        return answer;
    }

    /**
     * Minimizes memory usage while computing the or aggregate.
     * 
     * @param bitmaps
     *            input bitmaps
     * @return aggregated bitmap
     * @see #or(ImmutableRoaringBitmap...)
     */
    public static MutableRoaringBitmap horizontal_or(
            ImmutableRoaringBitmap... bitmaps) {
        MutableRoaringBitmap answer = new MutableRoaringBitmap();
        if (bitmaps.length == 0)
            return answer;
        PriorityQueue<MappeableContainerPointer> pq = new PriorityQueue<MappeableContainerPointer>(
                bitmaps.length);
        for (int k = 0; k < bitmaps.length; ++k) {
            MappeableContainerPointer x = bitmaps[k].highLowContainer
                    .getContainerPointer();
            if (x.getContainer() != null)
                pq.add(x);
        }

        while (!pq.isEmpty()) {
            MappeableContainerPointer x1 = pq.poll();
            if (pq.isEmpty() || (pq.peek().key() != x1.key())) {
                answer.getMappeableRoaringArray().append(x1.key(),
                        x1.getContainer().clone());
                x1.advance();
                if (x1.getContainer() != null)
                    pq.add(x1);
                continue;
            }
            MappeableContainerPointer x2 = pq.poll();
            MappeableContainer newc = x1.getContainer().lazyOR(x2.getContainer());
            while (!pq.isEmpty() && (pq.peek().key() == x1.key())) {

                MappeableContainerPointer x = pq.poll();
                newc = newc.lazyIOR(x.getContainer());
                x.advance();
                if (x.getContainer() != null)
                    pq.add(x);
                else if (pq.isEmpty())
                    break;
            }
            if(newc.getCardinality()<0)
                ((MappeableBitmapContainer)newc).computeCardinality();
            answer.getMappeableRoaringArray().append(x1.key(), newc);
            x1.advance();
            if (x1.getContainer() != null)
                pq.add(x1);
            x2.advance();
            if (x2.getContainer() != null)
                pq.add(x2);
        }
        return answer;
    }

    /**
     * Minimizes memory usage while computing the xor aggregate.
     * 
     * @param bitmaps
     *            input bitmaps
     * @return aggregated bitmap
     * @see #xor(ImmutableRoaringBitmap...)
     */
    public static MutableRoaringBitmap horizontal_xor(
            ImmutableRoaringBitmap... bitmaps) {
        MutableRoaringBitmap answer = new MutableRoaringBitmap();
        if (bitmaps.length == 0)
            return answer;
        PriorityQueue<MappeableContainerPointer> pq = new PriorityQueue<MappeableContainerPointer>(
                bitmaps.length);
        for (int k = 0; k < bitmaps.length; ++k) {
            MappeableContainerPointer x = bitmaps[k].highLowContainer
                    .getContainerPointer();
            if (x.getContainer() != null)
                pq.add(x);
        }

        while (!pq.isEmpty()) {
            MappeableContainerPointer x1 = pq.poll();
            if (pq.isEmpty() || (pq.peek().key() != x1.key())) {
                answer.getMappeableRoaringArray().append(x1.key(),
                        x1.getContainer().clone());
                x1.advance();
                if (x1.getContainer() != null)
                    pq.add(x1);
                continue;
            }
            MappeableContainerPointer x2 = pq.poll();
            MappeableContainer newc = x1.getContainer().xor(x2.getContainer());
            while (!pq.isEmpty() && (pq.peek().key() == x1.key())) {

                MappeableContainerPointer x = pq.poll();
                newc = newc.ixor(x.getContainer());
                x.advance();
                if (x.getContainer() != null)
                    pq.add(x);
                else if (pq.isEmpty())
                    break;
            }
            answer.getMappeableRoaringArray().append(x1.key(), newc);
            x1.advance();
            if (x1.getContainer() != null)
                pq.add(x1);
            x2.advance();
            if (x2.getContainer() != null)
                pq.add(x2);
        }
        return answer;
    }

    /**
     * Uses a priority queue to compute the or aggregate.
     * 
     * @param bitmaps
     *            input bitmaps
     * @return aggregated bitmap
     * @see #horizontal_or(ImmutableRoaringBitmap...)
     */
    public static MutableRoaringBitmap or(ImmutableRoaringBitmap... bitmaps) {
        if (bitmaps.length == 0)
            return new MutableRoaringBitmap();
        else if (bitmaps.length == 1)
            return bitmaps[0].toMutableRoaringBitmap();
        final PriorityQueue<ImmutableRoaringBitmap> pq = new PriorityQueue<ImmutableRoaringBitmap>(
                bitmaps.length, new Comparator<ImmutableRoaringBitmap>() {
                    @Override
                    public int compare(ImmutableRoaringBitmap a,
                            ImmutableRoaringBitmap b) {
                        return a.getSizeInBytes() - b.getSizeInBytes();
                    }
                });
        Collections.addAll(pq, bitmaps);
        while (pq.size() > 1) {
            final ImmutableRoaringBitmap x1 = pq.poll();
            final ImmutableRoaringBitmap x2 = pq.poll();
            pq.add(ImmutableRoaringBitmap.or(x1, x2));
        }
        return (MutableRoaringBitmap) pq.poll();
    }

    /**
     * Uses a priority queue to compute the xor aggregate.
     * 
     * @param bitmaps
     *            input bitmaps
     * @return aggregated bitmap
     * @see #horizontal_xor(ImmutableRoaringBitmap...)
     */
    public static MutableRoaringBitmap xor(ImmutableRoaringBitmap... bitmaps) {
        if (bitmaps.length < 2)
            throw new IllegalArgumentException("Expecting at least 2 bitmaps");
        final PriorityQueue<ImmutableRoaringBitmap> pq = new PriorityQueue<ImmutableRoaringBitmap>(
                bitmaps.length, new Comparator<ImmutableRoaringBitmap>() {
                    @Override
                    public int compare(ImmutableRoaringBitmap a,
                            ImmutableRoaringBitmap b) {
                        return a.getSizeInBytes() - b.getSizeInBytes();
                    }
                });
        Collections.addAll(pq, bitmaps);
        while (pq.size() > 1) {
            final ImmutableRoaringBitmap x1 = pq.poll();
            final ImmutableRoaringBitmap x2 = pq.poll();
            pq.add(ImmutableRoaringBitmap.xor(x1, x2));
        }
        return (MutableRoaringBitmap) pq.poll();
    }

    /**
     * Private constructor to prevent instantiation of utility class
     */
    private BufferFastAggregation() {
    }

}
