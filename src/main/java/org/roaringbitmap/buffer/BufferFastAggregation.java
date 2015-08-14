/*
 * (c) the authors
 * Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap.buffer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

/**
 * Fast algorithms to aggregate many bitmaps.
 * 
 * @author Daniel Lemire
 */
public final class BufferFastAggregation {
    
    
    /**
     * Compute overall AND between bitmaps two-by-two.
     * 
     * This function runs in linear time with respect to the number of bitmaps.
     *
     * @param bitmaps input bitmaps
     * @return aggregated bitmap
     */
    public static MutableRoaringBitmap naive_and(MutableRoaringBitmap... bitmaps) {
       MutableRoaringBitmap answer = new MutableRoaringBitmap();
       for(int k = 0; k < bitmaps.length; ++k)
          answer.and(bitmaps[k]);
       return answer;
    }

    /**
     * Compute overall AND between bitmaps two-by-two.
     * 
     * This function runs in linear time with respect to the number of bitmaps.
     *
     * @param bitmaps input bitmaps (ImmutableRoaringBitmap or MutableRoaringBitmap)
     * @return aggregated bitmap
     */
    public static MutableRoaringBitmap naive_and(@SuppressWarnings("rawtypes") Iterator bitmaps) {
       MutableRoaringBitmap answer = new MutableRoaringBitmap();
       while(bitmaps.hasNext())
           answer.and((ImmutableRoaringBitmap) bitmaps.next());
       return answer;
    }

    
    /**
     * Compute overall OR between bitmaps two-by-two.
     *
     * This function runs in linear time with respect to the number of bitmaps.
     *
     * @param bitmaps input bitmaps
     * @return aggregated bitmap
     */
    public static MutableRoaringBitmap naive_or(MutableRoaringBitmap... bitmaps) {
       MutableRoaringBitmap answer = new MutableRoaringBitmap();
       for(int k = 0; k < bitmaps.length; ++k)
          answer.lazyor(bitmaps[k]);
       answer.repairAfterLazy();
       return answer;
    }



    /**
     * Compute overall OR between bitmaps two-by-two.
     * 
     * This function runs in linear time with respect to the number of bitmaps.
     *
     * @param bitmaps input bitmaps (ImmutableRoaringBitmap or MutableRoaringBitmap)
     * @return aggregated bitmap
     */
    public static MutableRoaringBitmap naive_or(@SuppressWarnings("rawtypes") Iterator bitmaps) {
       MutableRoaringBitmap answer = new MutableRoaringBitmap();
       while(bitmaps.hasNext())
           answer.lazyor((ImmutableRoaringBitmap) bitmaps.next());
       answer.repairAfterLazy();
       return answer;
    }
    
    /**
     * Compute overall XOR between bitmaps two-by-two.
     * 
     * This function runs in linear time with respect to the number of bitmaps.
     *
     * @param bitmaps input bitmaps
     * @return aggregated bitmap
     */
    public static MutableRoaringBitmap naive_xor(MutableRoaringBitmap... bitmaps) {
       MutableRoaringBitmap answer = new MutableRoaringBitmap();
       for(int k = 0; k < bitmaps.length; ++k)
          answer.xor(bitmaps[k]);
       return answer;
    }
    

    /**
     * Compute overall XOR between bitmaps two-by-two.
     * 
     * This function runs in linear time with respect to the number of bitmaps.
     *
     * @param bitmaps input bitmaps (ImmutableRoaringBitmap or MutableRoaringBitmap)
     * @return aggregated bitmap
     */
    public static MutableRoaringBitmap naive_xor(@SuppressWarnings("rawtypes") Iterator bitmaps) {
       MutableRoaringBitmap answer = new MutableRoaringBitmap();
       while(bitmaps.hasNext())
           answer.xor((ImmutableRoaringBitmap) bitmaps.next());
       return answer;
    }
    

    /**
     * Compute overall OR between bitmaps.
     *
     *
     * @param bitmaps input bitmaps
     * @return aggregated bitmap
     */
    public static MutableRoaringBitmap experimental_or(@SuppressWarnings("rawtypes") Iterator bitmaps) {
        ArrayList<ImmutableRoaringBitmap> list = new ArrayList<ImmutableRoaringBitmap>();
        boolean nonehaverun = true;
        while(bitmaps.hasNext()) {
            ImmutableRoaringBitmap tb = (ImmutableRoaringBitmap) bitmaps.next();
            if(tb.highLowContainer.hasRunContainer())
                nonehaverun = false;
            list.add(tb);
        }
        if(nonehaverun)
            // bitmap containers will probably do their work
            return naive_or(list.iterator());
        else 
            // when we have runs, it defeats the magic of bitmaps
            return priorityqueue_or(list.iterator());
    }


    
    /**
     * Compute overall OR between bitmaps.
     *
     * @param bitmaps input bitmaps
     * @return aggregated bitmap
     */
    public static MutableRoaringBitmap or(MutableRoaringBitmap... bitmaps) {
       return naive_or(bitmaps);
    }
    
    /**
     * Compute overall OR between bitmaps.
     *
     * @param bitmaps input bitmaps
     * @return aggregated bitmap
     */
    public static MutableRoaringBitmap or(ImmutableRoaringBitmap... bitmaps) {
       return naive_or(bitmaps);
    }
    

    /**
     * Compute overall OR between bitmaps.
     *
     * @param bitmaps input bitmaps (ImmutableRoaringBitmap or MutableRoaringBitmap)
     * @return aggregated bitmap
     */
    public static MutableRoaringBitmap or(@SuppressWarnings("rawtypes") Iterator bitmaps) {
        return naive_or(bitmaps);
    }
    
    /**
     * Compute overall XOR between bitmaps.
     * 
     *
     * @param bitmaps input bitmaps
     * @return aggregated bitmap
     */
    public static MutableRoaringBitmap xor(MutableRoaringBitmap... bitmaps) {
        return naive_xor(bitmaps);
    }
    

    /**
     * Compute overall XOR between bitmaps.
     * 
     *
     * @param bitmaps input bitmaps
     * @return aggregated bitmap
     */
    public static MutableRoaringBitmap xor(ImmutableRoaringBitmap... bitmaps) {
        return naive_xor(bitmaps);
    }

    /**
     * Compute overall XOR between bitmaps.
     *
     * @param bitmaps input bitmaps (ImmutableRoaringBitmap or MutableRoaringBitmap)
     * @return aggregated bitmap
     */
    public static MutableRoaringBitmap xor(@SuppressWarnings("rawtypes") Iterator bitmaps) {
       return naive_xor(bitmaps);
    }
    
    /**
     * Compute overall AND between bitmaps two-by-two.
     * 
     * This function runs in linear time with respect to the number of bitmaps.
     *
     * @param bitmaps input bitmaps
     * @return aggregated bitmap
     */
    public static MutableRoaringBitmap naive_and(ImmutableRoaringBitmap... bitmaps) {
       MutableRoaringBitmap answer = new MutableRoaringBitmap();
       for(int k = 0; k < bitmaps.length; ++k)
          answer.and(bitmaps[k]);
       return answer;
    }


    
    /**
     * Compute overall OR between bitmaps two-by-two.
     * 
     * This function runs in linear time with respect to the number of bitmaps.
     *
     * @param bitmaps input bitmaps
     * @return aggregated bitmap
     */
    public static MutableRoaringBitmap naive_or(ImmutableRoaringBitmap... bitmaps) {
       MutableRoaringBitmap answer = new MutableRoaringBitmap();
       for(int k = 0; k < bitmaps.length; ++k)
          answer.or(bitmaps[k]);
       return answer;
    }
    
    /**
     * Compute overall XOR between bitmaps two-by-two.
     * 
     * This function runs in linear time with respect to the number of bitmaps.
     *
     * @param bitmaps input bitmaps
     * @return aggregated bitmap
     */
    public static MutableRoaringBitmap naive_xor(ImmutableRoaringBitmap... bitmaps) {
       MutableRoaringBitmap answer = new MutableRoaringBitmap();
       for(int k = 0; k < bitmaps.length; ++k)
          answer.xor(bitmaps[k]);
       return answer;
    }
    


    
    /**
     * Convenience method converting one type of iterator into another,
     * to avoid unnecessary warnings.
     * 
     * @param i input bitmaps
     * @return an iterator over the provided iterator, with a different type
     */
    public static Iterator<ImmutableRoaringBitmap> convertToImmutable(final Iterator<MutableRoaringBitmap> i) {
        return new Iterator<ImmutableRoaringBitmap>() {

            @Override
            public boolean hasNext() {
                return i.hasNext();
            }

            @Override
            public ImmutableRoaringBitmap next() {
                return i.next();
            }

            @Override
                public void remove() {};
            
        };
        
    }

    private static ImmutableRoaringBitmap[] convertToImmutable(MutableRoaringBitmap[] array) {
        ImmutableRoaringBitmap[] answer = new ImmutableRoaringBitmap[array.length];
        for(int k = 0; k < answer.length; ++k)
            answer[k] = (ImmutableRoaringBitmap) array[k];
        return answer;
    }
    /**
     * Sort the bitmap prior to using the and aggregate.
     * 
     * This function runs in linearithmic (O(n log n))  time with respect to the number of bitmaps.
     * 
     * @param bitmaps
     *            input bitmaps
     * @return aggregated bitmap
     */
    public static MutableRoaringBitmap and(MutableRoaringBitmap... bitmaps) {
        return and(convertToImmutable(bitmaps));
    }

    
    /**
     * Sort the bitmap prior to using the and aggregate.
     * 
     * This function runs in linearithmic (O(n log n))  time with respect to the number of bitmaps.
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
     * Sort the bitmap prior to using the and aggregate.
     * 
     * This function runs in linearithmic (O(n log n))  time with respect to the number of bitmaps.
     *
     * @param bitmaps input bitmaps  (ImmutableRoaringBitmap or MutableRoaringBitmap)
     * @return aggregated bitmap
     */
    public static MutableRoaringBitmap and(@SuppressWarnings("rawtypes") Iterator bitmaps) {
        if (!bitmaps.hasNext())
            return new MutableRoaringBitmap();
        ArrayList<ImmutableRoaringBitmap> array = new ArrayList<ImmutableRoaringBitmap>();
        while(bitmaps.hasNext())
            array.add((ImmutableRoaringBitmap) bitmaps.next());
        if(array.size() == 1) return array.get(0).toMutableRoaringBitmap();
        Collections.sort(array, new Comparator<ImmutableRoaringBitmap>() {
            @Override
            public int compare(ImmutableRoaringBitmap a,
                    ImmutableRoaringBitmap b) {
                return a.getSizeInBytes() - b.getSizeInBytes();
            }
        });
        MutableRoaringBitmap answer = ImmutableRoaringBitmap.and(array.get(0), array.get(1));
        for (int k = 2; k < array.size(); ++k)
            answer.and(array.get(k));
        return answer;
    }
    
    
    /**
     * Calls naive_or.
     * 
     * @param bitmaps
     *            input bitmaps (ImmutableRoaringBitmap or MutableRoaringBitmap)
     * @return aggregated bitmap
     */
    @Deprecated
    public static MutableRoaringBitmap horizontal_or(
            @SuppressWarnings("rawtypes") Iterator bitmaps) {
        return naive_or(bitmaps);
    }
    
    /**
     * Minimizes memory usage while computing the or aggregate on a moderate number of bitmaps.
     * 
     * This function runs in linearithmic (O(n log n))  time with respect to the number of bitmaps.
     * 
     * @param bitmaps
     *            input bitmaps
     * @return aggregated bitmap
     * @see #or(ImmutableRoaringBitmap...)
     */
    public static MutableRoaringBitmap horizontal_or(
            MutableRoaringBitmap... bitmaps) {
        return horizontal_or(convertToImmutable(bitmaps));
    }
    
    /**
     * Minimizes memory usage while computing the or aggregate on a moderate number of bitmaps.
     * 
     * This function runs in linearithmic (O(n log n))  time with respect to the number of bitmaps.
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
            newc = newc.repairAfterLazy();
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
     * Minimizes memory usage while computing the xor aggregate  on a moderate number of bitmaps.
     * 
     * This function runs in linearithmic (O(n log n))  time with respect to the number of bitmaps.
     * 
     * @param bitmaps
     *            input bitmaps
     * @return aggregated bitmap
     * @see #xor(ImmutableRoaringBitmap...)
     */
    public static MutableRoaringBitmap horizontal_xor(
            MutableRoaringBitmap... bitmaps) {
        return horizontal_xor(convertToImmutable(bitmaps));
    }
    
    /**
     * Minimizes memory usage while computing the xor aggregate  on a moderate number of bitmaps.
     *      
     * This function runs in linearithmic (O(n log n))  time with respect to the number of bitmaps.
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
     * This function runs in linearithmic (O(n log n))  time with respect to the number of bitmaps.
     * 
     * @param bitmaps
     *            input bitmaps
     * @return aggregated bitmap
     * @see #horizontal_or(ImmutableRoaringBitmap...)
     */
    public static MutableRoaringBitmap priorityqueue_or(ImmutableRoaringBitmap... bitmaps) {
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
     * Uses a priority queue to compute the or aggregate.
     * 
     * This function runs in linearithmic (O(n log n))  time with respect to the number of bitmaps.
     * 
     * @param bitmaps
     *            input bitmaps
     * @return aggregated bitmap
     * @see #horizontal_or(ImmutableRoaringBitmap...)
     */
    public static MutableRoaringBitmap priorityqueue_or(@SuppressWarnings("rawtypes") Iterator bitmaps) {
        if (!bitmaps.hasNext())
            return new MutableRoaringBitmap();
        final PriorityQueue<ImmutableRoaringBitmap> pq = new PriorityQueue<ImmutableRoaringBitmap>(16,
                 new Comparator<ImmutableRoaringBitmap>() {
                    @Override
                    public int compare(ImmutableRoaringBitmap a,
                            ImmutableRoaringBitmap b) {
                        return a.getSizeInBytes() - b.getSizeInBytes();
                    }
                });
        while(bitmaps.hasNext())
            pq.add((ImmutableRoaringBitmap) bitmaps.next());
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
     * This function runs in linearithmic (O(n log n))  time with respect to the number of bitmaps.
     * 
     * @param bitmaps
     *            input bitmaps
     * @return aggregated bitmap
     * @see #horizontal_xor(ImmutableRoaringBitmap...)
     */
    public static MutableRoaringBitmap priorityqueue_xor(ImmutableRoaringBitmap... bitmaps) {
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