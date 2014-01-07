package me.lemire.roaringbitmap;


import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;



/**
 * Fast algorithms to aggregate many bitmaps.
 * 
 * @author Daniel Lemire
 * 
 */
public class FastAggregation {
	/**
	 * Uses a priority queue to compute the or aggregate.
	 */
	public static RoaringBitmap  or(RoaringBitmap... bitmaps) {
		PriorityQueue<RoaringBitmap> pq = new PriorityQueue<RoaringBitmap>(bitmaps.length,
				new Comparator<RoaringBitmap>() {
					@Override
					public int compare(RoaringBitmap a, RoaringBitmap b) {
						return a.getSizeInBytes() - b.getSizeInBytes();
					}
				});
		for (RoaringBitmap x : bitmaps) {
			pq.add(x);
		}
		while (pq.size() > 1) {
		        RoaringBitmap x1 = pq.poll();
		        RoaringBitmap x2 = pq.poll();
			pq.add(RoaringBitmap.or(x1, x2));
		}
		return pq.poll();
	}
	
        /**
         * Uses a priority queue to compute the or aggregate, use in-place computations.
         */
        public static RoaringBitmap  inplace_or(RoaringBitmap... bitmaps) {
                PriorityQueue<RoaringBitmapPointer> pq = new PriorityQueue<RoaringBitmapPointer>(
                        bitmaps.length, new Comparator<RoaringBitmapPointer>() {
                                @Override
                                public int compare(RoaringBitmapPointer a, RoaringBitmapPointer b) {
                                        return a.cs.getSizeInBytes() - b.cs.getSizeInBytes();
                                }
                        });
                for (RoaringBitmap x : bitmaps) {
                        pq.add(new RoaringBitmapPointer(x, true));
                }
                while (pq.size() > 1) {
                        RoaringBitmapPointer x1 = pq.poll();
                        RoaringBitmapPointer x2 = pq.poll();
                        if(x1.needsCloning)
                                x1.cs = x1.cs.clone();
                        x1.cs.inPlaceOR(x2.cs);
                        x1.needsCloning = false;
                        pq.add(x1);
                }
                return pq.poll().cs;    
        }
        	

        /**
         * Uses a priority queue to compute the xor aggregate, use in-place computations.
         */
        public static RoaringBitmap  inplace_xor(RoaringBitmap... bitmaps) {
                PriorityQueue<RoaringBitmapPointer> pq = new PriorityQueue<RoaringBitmapPointer>(
                        bitmaps.length, new Comparator<RoaringBitmapPointer>() {
                                @Override
                                public int compare(RoaringBitmapPointer a, RoaringBitmapPointer b) {
                                        return a.cs.getSizeInBytes() - b.cs.getSizeInBytes();
                                }
                        });
                for (RoaringBitmap x : bitmaps) {
                        pq.add(new RoaringBitmapPointer(x, true));
                }
                while (pq.size() > 1) {
                        RoaringBitmapPointer x1 = pq.poll();
                        RoaringBitmapPointer x2 = pq.poll();
                        if(x1.needsCloning)
                                x1.cs = x1.cs.clone();
                        x1.cs.inPlaceXOR(x2.cs);
                        x1.needsCloning = false;
                        pq.add(x1);
                }
                return pq.poll().cs;    
        }
                

	 /**
         * Uses a priority queue to compute the xor aggregate.
         */
        public static RoaringBitmap  xor(RoaringBitmap... bitmaps) {
                PriorityQueue<RoaringBitmap> pq = new PriorityQueue<RoaringBitmap>(bitmaps.length,
                                new Comparator<RoaringBitmap>() {
                                        @Override
                                        public int compare(RoaringBitmap a, RoaringBitmap b) {
                                                return a.getSizeInBytes() - b.getSizeInBytes();
                                        }
                                });
                for (RoaringBitmap x : bitmaps) {
                        pq.add(x);
                }
                while (pq.size() > 1) {
                        RoaringBitmap x1 = pq.poll();
                        RoaringBitmap x2 = pq.poll();
                        pq.add(RoaringBitmap.xor(x1, x2));
                }
                return pq.poll();
        }

        /**
        * Sort the bitmap prior to using the and aggregate.
        */
       public static RoaringBitmap  and(RoaringBitmap... bitmaps) {
               if(bitmaps.length == 0) return new RoaringBitmap();
               RoaringBitmap[] array = Arrays.copyOf(bitmaps, bitmaps.length);
               Arrays.sort(array, new Comparator<RoaringBitmap>() {
                                       @Override
                                       public int compare(RoaringBitmap a, RoaringBitmap b) {
                                               return a.getSizeInBytes() - b.getSizeInBytes();
                                       }
                               });
               RoaringBitmap answer = array[0];
               for(int k = 1 ; k < array.length; ++k)
                       answer = RoaringBitmap.and(answer, array[k]);
               return answer;
       }
       
       /**
        * Sort the bitmap prior to using the and aggregate, use in-place computations.
        */
       public static RoaringBitmap  inplace_and(RoaringBitmap... bitmaps) {
               if(bitmaps.length == 0) return new RoaringBitmap();
               RoaringBitmap[] array = Arrays.copyOf(bitmaps, bitmaps.length);
               Arrays.sort(array,new Comparator<RoaringBitmap>() {
                                       @Override
                                       public int compare(RoaringBitmap a, RoaringBitmap b) {
                                               return a.getSizeInBytes() - b.getSizeInBytes();
                                       }
                               });
               RoaringBitmap answer = array[0].clone();
               for(int k = 1 ; k < array.length; ++k)
                       answer.inPlaceAND(array[k]);
               return answer;
       }
}


class RoaringBitmapPointer {
        RoaringBitmap cs;
        boolean needsCloning;
        public RoaringBitmapPointer(RoaringBitmap c, boolean mustclone) {
                needsCloning = mustclone;
                cs = c;
                
        }
}