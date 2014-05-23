/*
 * (c) Daniel Lemire, Owen Kaser, Samy Chambi, Jon Alvarado, Rory Graves, Björn Sperber
 * Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap;

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
    private FastAggregation() {}

    /**
     * Sort the bitmap prior to using the and aggregate.
     *
     * @param bitmaps input bitmaps
     * @return aggregated bitmap
     */
    public static RoaringBitmap and(RoaringBitmap... bitmaps) {
        if (bitmaps.length == 0)
            return new RoaringBitmap();
        else if(bitmaps.length == 1)
        	return bitmaps[0].clone();
        RoaringBitmap[] array = Arrays.copyOf(bitmaps, bitmaps.length);
        Arrays.sort(array, new Comparator<RoaringBitmap>() {
            @Override
            public int compare(RoaringBitmap a, RoaringBitmap b) {
                return a.getSizeInBytes() - b.getSizeInBytes();
            }
        });
        RoaringBitmap answer = RoaringBitmap.and(array[0], array[1]);
        for (int k = 2; k < array.length; ++k)
            answer.and(array[k]);
        return answer;
    }

    /**
     * Uses a priority queue to compute the or aggregate.
     *
     * @param bitmaps input bitmaps
     * @return aggregated bitmap
     * @see #horizontal_or(RoaringBitmap...)
     */
    public static RoaringBitmap or(RoaringBitmap... bitmaps) {
        if (bitmaps.length == 0)
            return new RoaringBitmap();

        PriorityQueue<RoaringBitmap> pq = new PriorityQueue<RoaringBitmap>(bitmaps.length, new Comparator<RoaringBitmap>() {
            @Override
            public int compare(RoaringBitmap a,
                               RoaringBitmap b) {
                return a.getSizeInBytes() - b.getSizeInBytes();
            }
        });
        Collections.addAll(pq, bitmaps);
        while (pq.size() > 1) {
            RoaringBitmap x1 = pq.poll();
            RoaringBitmap x2 = pq.poll();
            pq.add(RoaringBitmap.or(x1, x2));
        }
        return pq.poll();
    }
    
    /**
     * Minimizes memory usage while computing the or aggregate.
     *
     * @param bitmaps input bitmaps
     * @return aggregated bitmap
     * @see #or(RoaringBitmap...)
     */
    public static RoaringBitmap horizontal_or(RoaringBitmap... bitmaps) {
    	RoaringBitmap answer = new RoaringBitmap();
    	if (bitmaps.length == 0)
            return answer;
        PriorityQueue<ContainerPointer> pq = new PriorityQueue<ContainerPointer>(bitmaps.length);
        for(int k = 0; k < bitmaps.length; ++k) {
        	ContainerPointer x = bitmaps[k].highLowContainer.getContainerPointer();
        	if(x.getContainer() != null)
        	  pq.add(x);
        }
        
        while (!pq.isEmpty()) {        	
        	ContainerPointer x1 = pq.poll();
        	if(pq.isEmpty() || (pq.peek().key() != x1.key())) {
        		answer.highLowContainer.append(x1.key(), x1.getContainer().clone());
        		x1.advance();
        		if(x1.getContainer() != null)
        			pq.add(x1);
        		continue;
        	}
        	ContainerPointer x2 = pq.poll();       	
        	Container newc = x1.getContainer().lazyOR(x2.getContainer());
        	while(!pq.isEmpty() && (pq.peek().key() == x1.key())) {

        		ContainerPointer x = pq.poll();       	
            	newc = newc.lazyIOR(x.getContainer());
        		x.advance();
        		if(x.getContainer() != null)
        			pq.add(x);
        		else if (pq.isEmpty()) break;
        	}
        	if(newc.getCardinality()<0)
        	    ((BitmapContainer)newc).computeCardinality();
        	answer.highLowContainer.append(x1.key(), newc);
        	x1.advance();
    		if(x1.getContainer() != null)
    			pq.add(x1);
    		x2.advance();
    		if(x2.getContainer() != null)
    			pq.add(x2);
        }
        return answer;
    }
    
    /**
     * Uses a priority queue to compute the xor aggregate.
     *
     * @param bitmaps input bitmaps
     * @return aggregated bitmap
     * @see #horizontal_xor(RoaringBitmap...)
     */
    public static RoaringBitmap xor(RoaringBitmap... bitmaps) {
        if (bitmaps.length == 0)
            return new RoaringBitmap();

        PriorityQueue<RoaringBitmap> pq = new PriorityQueue<RoaringBitmap>(bitmaps.length, new Comparator<RoaringBitmap>() {
            @Override
            public int compare(RoaringBitmap a,
                               RoaringBitmap b) {
                return a.getSizeInBytes() - b.getSizeInBytes();
            }
        });
        Collections.addAll(pq, bitmaps);
        while (pq.size() > 1) {
            RoaringBitmap x1 = pq.poll();
            RoaringBitmap x2 = pq.poll();
            pq.add(RoaringBitmap.xor(x1, x2));
        }
        return pq.poll();
    }

    /**
     * Minimizes memory usage while computing the xor aggregate.
     *
     * @param bitmaps input bitmaps
     * @return aggregated bitmap
     * @see #xor(RoaringBitmap...)
     */
    public static RoaringBitmap horizontal_xor(RoaringBitmap... bitmaps) {
    	RoaringBitmap answer = new RoaringBitmap();
    	if (bitmaps.length == 0)
            return answer;
        PriorityQueue<ContainerPointer> pq = new PriorityQueue<ContainerPointer>(bitmaps.length);
        for(int k = 0; k < bitmaps.length; ++k) {
        	ContainerPointer x = bitmaps[k].highLowContainer.getContainerPointer();
        	if(x.getContainer() != null)
        	  pq.add(x);
        }
        
        while (!pq.isEmpty()) {        	
        	ContainerPointer x1 = pq.poll();
        	if(pq.isEmpty() || (pq.peek().key() != x1.key())) {
        		answer.highLowContainer.append(x1.key(), x1.getContainer().clone());
        		x1.advance();
        		if(x1.getContainer() != null)
        			pq.add(x1);
        		continue;
        	}
        	ContainerPointer x2 = pq.poll();       	
        	Container newc = x1.getContainer().xor(x2.getContainer());
        	while(!pq.isEmpty() && (pq.peek().key() == x1.key())) {
        		ContainerPointer x = pq.poll();       	
        		newc = newc.ixor(x.getContainer());
        		x.advance();
        		if(x.getContainer() != null)
        			pq.add(x);
        		else if (pq.isEmpty()) break;
        	}
        	answer.highLowContainer.append(x1.key(), newc);
        	x1.advance();
    		if(x1.getContainer() != null)
    			pq.add(x1);
    		x2.advance();
    		if(x2.getContainer() != null)
    			pq.add(x2);
        }
        return answer;
    }

}
