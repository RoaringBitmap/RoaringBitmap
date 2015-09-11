/*
 * (c) the authors
 * Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
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
     * Compute overall AND between bitmaps two-by-two.
     * 
     * This function runs in linear time with respect to the number of bitmaps.
     *
     * @param bitmaps input bitmaps
     * @return aggregated bitmap
     */
    public static RoaringBitmap naive_and(RoaringBitmap... bitmaps) {
       if(bitmaps.length == 0) return new RoaringBitmap();
       RoaringBitmap answer = bitmaps[0].clone();
       for(int k = 1; k < bitmaps.length; ++k)
          answer.and(bitmaps[k]);
       return answer;
    }

    /**
     * Compute overall AND between bitmaps two-by-two.
     * 
     * This function runs in linear time with respect to the number of bitmaps.
     *
     * @param bitmaps input bitmaps
     * @return aggregated bitmap
     */
    public static RoaringBitmap naive_and(Iterator<RoaringBitmap> bitmaps) {
       if(!bitmaps.hasNext()) return new RoaringBitmap();
       RoaringBitmap answer = bitmaps.next().clone();
       while(bitmaps.hasNext()) {
           answer.and(bitmaps.next());
       }
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
    public static RoaringBitmap naive_or(RoaringBitmap... bitmaps) {
       RoaringBitmap answer = new RoaringBitmap();
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
     * @param bitmaps input bitmaps
     * @return aggregated bitmap
     */
    public static RoaringBitmap naive_or(Iterator<RoaringBitmap> bitmaps) {
       RoaringBitmap answer = new RoaringBitmap();
       while(bitmaps.hasNext()) {
           answer.lazyor(bitmaps.next());
       }
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
    public static RoaringBitmap naive_xor(RoaringBitmap... bitmaps) {
       RoaringBitmap answer = new RoaringBitmap();
       for(int k = 0; k < bitmaps.length; ++k)
          answer.xor(bitmaps[k]);
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
    public static RoaringBitmap naive_xor(Iterator<RoaringBitmap> bitmaps) {
       RoaringBitmap answer = new RoaringBitmap();
       while(bitmaps.hasNext())
           answer.xor(bitmaps.next());
       return answer;
    }


    /**
     * Compute overall OR between bitmaps.
     * 
     *
     * @param bitmaps input bitmaps
     * @return aggregated bitmap
     */
    public static RoaringBitmap or(RoaringBitmap... bitmaps) {
        return naive_or(bitmaps);
    }
    

    /**
     * Compute overall OR between bitmaps.
     *
     *
     * @param bitmaps input bitmaps
     * @return aggregated bitmap
     */
    public static RoaringBitmap or(Iterator<RoaringBitmap> bitmaps) {
        return naive_or(bitmaps);
    }
    
    /**
     * Compute overall XOR between bitmaps.
     *
     * 
     * @param bitmaps input bitmaps
     * @return aggregated bitmap
     */
    public static RoaringBitmap xor(RoaringBitmap... bitmaps) {
       return naive_xor(bitmaps);
    }
    

    /**
     * Compute overall XOR between bitmaps.
     * 
     *
     * @param bitmaps input bitmaps
     * @return aggregated bitmap
     */
    public static RoaringBitmap xor(Iterator<RoaringBitmap> bitmaps) {
       return naive_xor(bitmaps);
    }

    
    /**
     * Compute the AND aggregate.
     * 
     * In practice, calls {#link naive_and}
     * @param bitmaps input bitmaps
     * @return aggregated bitmap
     */
    public static RoaringBitmap and(RoaringBitmap... bitmaps) {
        return naive_and(bitmaps);
    }
    
    /**
     * Compute the AND aggregate.
     * 
     * In practice, calls {#link naive_and}
     * @param bitmaps input bitmaps
     * @return aggregated bitmap
     */
    public static RoaringBitmap and(Iterator<RoaringBitmap> bitmaps) {
        return naive_and(bitmaps);
    }
    
    /**
     * Calls naive_or.
     * 
     * @param bitmaps
     *            input bitmaps
     * @return aggregated bitmap
     */
    @Deprecated
    public static RoaringBitmap horizontal_or(Iterator<RoaringBitmap> bitmaps) {
        return naive_or(bitmaps);
    }

    /**
     * Uses a priority queue to compute the or aggregate.
     * 
     * This function runs in linearithmic (O(n log n))  time with respect to the number of bitmaps.
     *
     * @param bitmaps input bitmaps
     * @return aggregated bitmap
     * @see #horizontal_or(RoaringBitmap...)
     */
    public static RoaringBitmap priorityqueue_or(RoaringBitmap... bitmaps) {
        if (bitmaps.length == 0)
            return new RoaringBitmap();
        // we buffer the call to getSizeInBytes(), hence the code complexity
        final RoaringBitmap[] buffer = Arrays.copyOf(bitmaps,bitmaps.length);
        final int[] sizes = new int[buffer.length];
        final boolean[] istmp = new boolean[buffer.length];
        for(int k = 0 ; k < sizes.length; ++k)
            sizes[k] = buffer[k].getSizeInBytes();
        PriorityQueue<Integer> pq = new PriorityQueue<Integer>(128, new Comparator<Integer>() {
            @Override
            public int compare(Integer a,
                    Integer b) {
                return sizes[a] - sizes[b];
            }
        });
        for(int k = 0 ; k < sizes.length; ++k)
            pq.add(k);
        while (pq.size() > 1) {
            Integer x1 = pq.poll();
            Integer x2 = pq.poll();
            if(istmp[x2] && istmp[x1]) {
                buffer[x1] = RoaringBitmap.lazyorfromlazyinputs(buffer[x1], buffer[x2]);
                sizes[x1] = buffer[x1].getSizeInBytes();
                istmp[x1] = true;
                pq.add(x1);
            } else if(istmp[x2]) {
                buffer[x2].lazyor(buffer[x1]);
                sizes[x2] = buffer[x2].getSizeInBytes();
                pq.add(x2);
            } else if(istmp[x1]) {
                buffer[x1].lazyor(buffer[x2]);
                sizes[x1] = buffer[x1].getSizeInBytes();
                pq.add(x1);
            } else {
              buffer[x1] = RoaringBitmap.lazyor(buffer[x1], buffer[x2]);
              sizes[x1] = buffer[x1].getSizeInBytes();
              istmp[x1] = true;
              pq.add(x1);
            }
        }
        RoaringBitmap answer = buffer[pq.poll()];
        answer.repairAfterLazy();
        return answer;
    }
    
    /**
     * Uses a priority queue to compute the or aggregate.
     * 
     * This function runs in linearithmic (O(n log n))  time with respect to the number of bitmaps.
     *
     * @param bitmaps input bitmaps
     * @return aggregated bitmap
     * @see #horizontal_or(RoaringBitmap...)
     */
    public static RoaringBitmap priorityqueue_or(Iterator<RoaringBitmap> bitmaps) {
        if (!bitmaps.hasNext())
            return new RoaringBitmap();
        // we buffer the call to getSizeInBytes(), hence the code complexity
        ArrayList<RoaringBitmap> buffer = new ArrayList<RoaringBitmap>();
        while(bitmaps.hasNext())
            buffer.add(bitmaps.next());
        final int[] sizes = new int[buffer.size()];
        final boolean[] istmp = new boolean[buffer.size()];
        for(int k = 0 ; k < sizes.length; ++k)
            sizes[k] = buffer.get(k).getSizeInBytes();
        PriorityQueue<Integer> pq = new PriorityQueue<Integer>(128, new Comparator<Integer>() {
            @Override
            public int compare(Integer a,
                    Integer b) {
                return sizes[a] - sizes[b];
            }
        });
        for(int k = 0 ; k < sizes.length; ++k)
            pq.add(k);
        while (pq.size() > 1) {
            Integer x1 = pq.poll();
            Integer x2 = pq.poll();
            if(istmp[x2] && istmp[x1]) {
                buffer.set(x1, RoaringBitmap.lazyorfromlazyinputs(buffer.get(x1), buffer.get(x2)));
                sizes[x1] = buffer.get(x1).getSizeInBytes();
                istmp[x1] = true;
                pq.add(x1);
            } else if(istmp[x2]) {
                buffer.get(x2).lazyor(buffer.get(x1));
                RoaringBitmap c = buffer.get(x2).clone();
                c.repairAfterLazy();
                sizes[x2] = buffer.get(x2).getSizeInBytes();
                pq.add(x2);
            } else if(istmp[x1]) {
                buffer.get(x1).lazyor(buffer.get(x2));
                sizes[x1] = buffer.get(x1).getSizeInBytes();
                pq.add(x1);
            } else {
              buffer.set(x1, RoaringBitmap.lazyor(buffer.get(x1), buffer.get(x2)));
              sizes[x1] = buffer.get(x1).getSizeInBytes();
              istmp[x1] = true;
              pq.add(x1);
            }
        }
        RoaringBitmap answer = buffer.get(pq.poll());
        answer.repairAfterLazy();
        return answer;
    }
    
    /**
     * Minimizes memory usage while computing the or aggregate on a moderate number of bitmaps.
     *      
     * This function runs in linearithmic (O(n log n))  time with respect to the number of bitmaps.
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
        	newc = newc.repairAfterLazy();
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
     * Minimizes memory usage while computing the or aggregate on a moderate number of bitmaps.
     *      
     * This function runs in linearithmic (O(n log n))  time with respect to the number of bitmaps.
     *
     * @param bitmaps input bitmaps
     * @return aggregated bitmap
     * @see #or(RoaringBitmap...)
     */
    public static RoaringBitmap horizontal_or(List<RoaringBitmap> bitmaps) {
        RoaringBitmap answer = new RoaringBitmap();
        if (bitmaps.isEmpty())
            return answer;
        PriorityQueue<ContainerPointer> pq = new PriorityQueue<ContainerPointer>(bitmaps.size());
        for(int k = 0; k < bitmaps.size(); ++k) {
            ContainerPointer x = bitmaps.get(k).highLowContainer.getContainerPointer();
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
            newc = newc.repairAfterLazy();
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
     * This function runs in linearithmic (O(n log n))  time with respect to the number of bitmaps.
     *
     * @param bitmaps input bitmaps
     * @return aggregated bitmap
     * @see #horizontal_xor(RoaringBitmap...)
     */
    public static RoaringBitmap priorityqueue_xor(RoaringBitmap... bitmaps) {
        //TODO: This code could be faster, see priorityqueue_or 
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
     * Minimizes memory usage while computing the xor aggregate on a moderate number of bitmaps.
     * 
     * This function runs in linearithmic (O(n log n))  time with respect to the number of bitmaps.
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
