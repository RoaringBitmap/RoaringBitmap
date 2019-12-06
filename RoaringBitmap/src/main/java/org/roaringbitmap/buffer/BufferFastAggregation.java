/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap.buffer;

import java.util.*;


/**
 * Fast algorithms to aggregate many bitmaps.
 * 
 * @author Daniel Lemire
 */
public final class BufferFastAggregation {


  /**
   * Compute the AND aggregate.
   * 
   * In practice, calls {#link naive_and}
   * 
   * @param bitmaps input bitmaps
   * @return aggregated bitmap
   */
  public static MutableRoaringBitmap and(ImmutableRoaringBitmap... bitmaps) {
    return naive_and(bitmaps);
  }

  /**
   * Compute the AND aggregate.
   * 
   * In practice, calls {#link naive_and}
   *
   * @param bitmaps input bitmaps (ImmutableRoaringBitmap or MutableRoaringBitmap)
   * @return aggregated bitmap
   */
  public static MutableRoaringBitmap and(@SuppressWarnings("rawtypes") Iterator bitmaps) {
    return naive_and(bitmaps);
  }


  /**
   * Compute the AND aggregate.
   * 
   * In practice, calls {#link naive_and}
   * 
   * @param bitmaps input bitmaps
   * @return aggregated bitmap
   */
  public static MutableRoaringBitmap and(MutableRoaringBitmap... bitmaps) {
    return and(convertToImmutable(bitmaps));
  }



  /**
   * Convenience method converting one type of iterator into another, to avoid unnecessary warnings.
   * 
   * @param i input bitmaps
   * @return an iterator over the provided iterator, with a different type
   */
  public static Iterator<ImmutableRoaringBitmap> convertToImmutable(
      final Iterator<MutableRoaringBitmap> i) {
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
    System.arraycopy(array, 0, answer, 0, answer.length);
    return answer;
  }


  /**
   * Minimizes memory usage while computing the or aggregate on a moderate number of bitmaps.
   * 
   * This function runs in linearithmic (O(n log n)) time with respect to the number of bitmaps.
   * 
   * @param bitmaps input bitmaps
   * @return aggregated bitmap
   * @see #or(ImmutableRoaringBitmap...)
   */
  public static MutableRoaringBitmap horizontal_or(ImmutableRoaringBitmap... bitmaps) {
    MutableRoaringBitmap answer = new MutableRoaringBitmap();
    if (bitmaps.length == 0) {
      return answer;
    }
    PriorityQueue<MappeableContainerPointer> pq = new PriorityQueue<>(bitmaps.length);
    for (int k = 0; k < bitmaps.length; ++k) {
      MappeableContainerPointer x = bitmaps[k].highLowContainer.getContainerPointer();
      if (x.getContainer() != null) {
        pq.add(x);
      }
    }

    while (!pq.isEmpty()) {
      MappeableContainerPointer x1 = pq.poll();
      if (pq.isEmpty() || (pq.peek().key() != x1.key())) {
        answer.getMappeableRoaringArray().append(x1.key(), x1.getContainer().clone());
        x1.advance();
        if (x1.getContainer() != null) {
          pq.add(x1);
        }
        continue;
      }
      MappeableContainerPointer x2 = pq.poll();
      MappeableContainer newc = x1.getContainer().lazyOR(x2.getContainer());
      while (!pq.isEmpty() && (pq.peek().key() == x1.key())) {

        MappeableContainerPointer x = pq.poll();
        newc = newc.lazyIOR(x.getContainer());
        x.advance();
        if (x.getContainer() != null) {
          pq.add(x);
        } else if (pq.isEmpty()) {
          break;
        }
      }
      newc = newc.repairAfterLazy();
      answer.getMappeableRoaringArray().append(x1.key(), newc);
      x1.advance();
      if (x1.getContainer() != null) {
        pq.add(x1);
      }
      x2.advance();
      if (x2.getContainer() != null) {
        pq.add(x2);
      }
    }
    return answer;
  }


  /**
   * Calls naive_or.
   * 
   * @param bitmaps input bitmaps (ImmutableRoaringBitmap or MutableRoaringBitmap)
   * @return aggregated bitmap
   */
  @Deprecated
  public static MutableRoaringBitmap horizontal_or(@SuppressWarnings("rawtypes") Iterator bitmaps) {
    return naive_or(bitmaps);
  }

  /**
   * Minimizes memory usage while computing the or aggregate on a moderate number of bitmaps.
   * 
   * This function runs in linearithmic (O(n log n)) time with respect to the number of bitmaps.
   * 
   * @param bitmaps input bitmaps
   * @return aggregated bitmap
   * @see #or(ImmutableRoaringBitmap...)
   */
  public static MutableRoaringBitmap horizontal_or(MutableRoaringBitmap... bitmaps) {
    return horizontal_or(convertToImmutable(bitmaps));
  }


  /**
   * Minimizes memory usage while computing the xor aggregate on a moderate number of bitmaps.
   * 
   * This function runs in linearithmic (O(n log n)) time with respect to the number of bitmaps.
   * 
   * @param bitmaps input bitmaps
   * @return aggregated bitmap
   * @see #xor(ImmutableRoaringBitmap...)
   */
  public static MutableRoaringBitmap horizontal_xor(ImmutableRoaringBitmap... bitmaps) {
    MutableRoaringBitmap answer = new MutableRoaringBitmap();
    if (bitmaps.length == 0) {
      return answer;
    }
    PriorityQueue<MappeableContainerPointer> pq = new PriorityQueue<>(bitmaps.length);
    for (int k = 0; k < bitmaps.length; ++k) {
      MappeableContainerPointer x = bitmaps[k].highLowContainer.getContainerPointer();
      if (x.getContainer() != null) {
        pq.add(x);
      }
    }

    while (!pq.isEmpty()) {
      MappeableContainerPointer x1 = pq.poll();
      if (pq.isEmpty() || (pq.peek().key() != x1.key())) {
        answer.getMappeableRoaringArray().append(x1.key(), x1.getContainer().clone());
        x1.advance();
        if (x1.getContainer() != null) {
          pq.add(x1);
        }
        continue;
      }
      MappeableContainerPointer x2 = pq.poll();
      MappeableContainer newc = x1.getContainer().xor(x2.getContainer());
      while (!pq.isEmpty() && (pq.peek().key() == x1.key())) {

        MappeableContainerPointer x = pq.poll();
        newc = newc.ixor(x.getContainer());
        x.advance();
        if (x.getContainer() != null) {
          pq.add(x);
        } else if (pq.isEmpty()) {
          break;
        }
      }
      answer.getMappeableRoaringArray().append(x1.key(), newc);
      x1.advance();
      if (x1.getContainer() != null) {
        pq.add(x1);
      }
      x2.advance();
      if (x2.getContainer() != null) {
        pq.add(x2);
      }
    }
    return answer;
  }

  /**
   * Minimizes memory usage while computing the xor aggregate on a moderate number of bitmaps.
   * 
   * This function runs in linearithmic (O(n log n)) time with respect to the number of bitmaps.
   * 
   * @param bitmaps input bitmaps
   * @return aggregated bitmap
   * @see #xor(ImmutableRoaringBitmap...)
   */
  public static MutableRoaringBitmap horizontal_xor(MutableRoaringBitmap... bitmaps) {
    return horizontal_xor(convertToImmutable(bitmaps));
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
    MutableRoaringBitmap answer;

    if (bitmaps.length > 0) {
      answer = (bitmaps[0]).toMutableRoaringBitmap();
      for (int k = 1; k < bitmaps.length; ++k) {
        answer = ImmutableRoaringBitmap.and(answer, bitmaps[k]);
      }
    } else {
      answer = new MutableRoaringBitmap();
    }

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
    if (!bitmaps.hasNext()) {
      return new MutableRoaringBitmap();
    }
    MutableRoaringBitmap answer =
        ((ImmutableRoaringBitmap) bitmaps.next()).toMutableRoaringBitmap();
    while (bitmaps.hasNext()) {
      answer.and((ImmutableRoaringBitmap) bitmaps.next());
    }
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
  public static MutableRoaringBitmap naive_and(MutableRoaringBitmap... bitmaps) {
    if (bitmaps.length == 0) {
      return new MutableRoaringBitmap();
    }
    MutableRoaringBitmap answer = bitmaps[0].clone();
    for (int k = 1; k < bitmaps.length; ++k) {
      answer.and(bitmaps[k]);
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
  public static MutableRoaringBitmap naive_or(ImmutableRoaringBitmap... bitmaps) {
    MutableRoaringBitmap answer = new MutableRoaringBitmap();
    for (int k = 0; k < bitmaps.length; ++k) {
      answer.naivelazyor(bitmaps[k]);
    }
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
    while (bitmaps.hasNext()) {
      answer.naivelazyor((ImmutableRoaringBitmap) bitmaps.next());
    }
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
  public static MutableRoaringBitmap naive_or(MutableRoaringBitmap... bitmaps) {
    MutableRoaringBitmap answer = new MutableRoaringBitmap();
    for (int k = 0; k < bitmaps.length; ++k) {
      answer.lazyor(bitmaps[k]);
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
  public static MutableRoaringBitmap naive_xor(ImmutableRoaringBitmap... bitmaps) {
    MutableRoaringBitmap answer = new MutableRoaringBitmap();
    for (int k = 0; k < bitmaps.length; ++k) {
      answer.xor(bitmaps[k]);
    }
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
    while (bitmaps.hasNext()) {
      answer.xor((ImmutableRoaringBitmap) bitmaps.next());
    }
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
    for (int k = 0; k < bitmaps.length; ++k) {
      answer.xor(bitmaps[k]);
    }
    return answer;
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
   * Compute overall OR between bitmaps.
   *
   * @param bitmaps input bitmaps
   * @return aggregated bitmap
   */
  public static MutableRoaringBitmap or(MutableRoaringBitmap... bitmaps) {
    return naive_or(bitmaps);
  }

  /**
   * Uses a priority queue to compute the or aggregate.
   * 
   * This function runs in linearithmic (O(n log n)) time with respect to the number of bitmaps.
   * 
   * @param bitmaps input bitmaps
   * @return aggregated bitmap
   * @see #horizontal_or(ImmutableRoaringBitmap...)
   */
  public static MutableRoaringBitmap priorityqueue_or(ImmutableRoaringBitmap... bitmaps) {
    if (bitmaps.length == 0) {
      return new MutableRoaringBitmap();
    } else if (bitmaps.length == 1) {
      return bitmaps[0].toMutableRoaringBitmap();
    }
    // we buffer the call to getLongSizeInBytes(), hence the code complexity
    final ImmutableRoaringBitmap[] buffer = Arrays.copyOf(bitmaps, bitmaps.length);
    final int[] sizes = new int[buffer.length];
    final boolean[] istmp = new boolean[buffer.length];
    for (int k = 0; k < sizes.length; ++k) {
      sizes[k] = buffer[k].serializedSizeInBytes();
    }
    PriorityQueue<Integer> pq = new PriorityQueue<>(128, new Comparator<Integer>() {
      @Override
      public int compare(Integer a, Integer b) {
        return sizes[a] - sizes[b];
      }
    });
    for (int k = 0; k < sizes.length; ++k) {
      pq.add(k);
    }
    while (pq.size() > 1) {
      Integer x1 = pq.poll();
      Integer x2 = pq.poll();
      if (istmp[x1] && istmp[x2]) {
        buffer[x1] = MutableRoaringBitmap.lazyorfromlazyinputs((MutableRoaringBitmap) buffer[x1],
            (MutableRoaringBitmap) buffer[x2]);
        sizes[x1] = buffer[x1].serializedSizeInBytes();
        pq.add(x1);
      } else if (istmp[x2]) {
        ((MutableRoaringBitmap) buffer[x2]).lazyor(buffer[x1]);
        sizes[x2] = buffer[x2].serializedSizeInBytes();
        pq.add(x2);
      } else if (istmp[x1]) {
        ((MutableRoaringBitmap) buffer[x1]).lazyor(buffer[x2]);
        sizes[x1] = buffer[x1].serializedSizeInBytes();
        pq.add(x1);
      } else {
        buffer[x1] = ImmutableRoaringBitmap.lazyor(buffer[x1], buffer[x2]);
        sizes[x1] = buffer[x1].serializedSizeInBytes();
        istmp[x1] = true;
        pq.add(x1);
      }
    }
    MutableRoaringBitmap answer = (MutableRoaringBitmap) buffer[pq.poll()];
    answer.repairAfterLazy();
    return answer;
  }

  /**
   * Uses a priority queue to compute the or aggregate.
   * 
   * This function runs in linearithmic (O(n log n)) time with respect to the number of bitmaps.
   * 
   * @param bitmaps input bitmaps
   * @return aggregated bitmap
   * @see #horizontal_or(ImmutableRoaringBitmap...)
   */
  public static MutableRoaringBitmap priorityqueue_or(
      @SuppressWarnings("rawtypes") Iterator bitmaps) {
    if (!bitmaps.hasNext()) {
      return new MutableRoaringBitmap();
    }
    // we buffer the call to getLongSizeInBytes(), hence the code complexity
    ArrayList<ImmutableRoaringBitmap> buffer = new ArrayList<>();
    while (bitmaps.hasNext()) {
      buffer.add((ImmutableRoaringBitmap) bitmaps.next());
    }
    final long[] sizes = new long[buffer.size()];
    final boolean[] istmp = new boolean[buffer.size()];
    for (int k = 0; k < sizes.length; ++k) {
      sizes[k] = buffer.get(k).getLongSizeInBytes();
    }
    PriorityQueue<Integer> pq = new PriorityQueue<>(128, new Comparator<Integer>() {
      @Override
      public int compare(Integer a, Integer b) {
        return (int)(sizes[a] - sizes[b]);
      }
    });
    for (int k = 0; k < sizes.length; ++k) {
      pq.add(k);
    }
    if (pq.size() == 1) {
      return buffer.get(pq.poll()).toMutableRoaringBitmap();
    }
    while (pq.size() > 1) {
      Integer x1 = pq.poll();
      Integer x2 = pq.poll();
      if (istmp[x1] && istmp[x2]) {
        buffer.set(x1, MutableRoaringBitmap.lazyorfromlazyinputs(
            (MutableRoaringBitmap) buffer.get(x1), (MutableRoaringBitmap) buffer.get(x2)));
        sizes[x1] = buffer.get(x1).getLongSizeInBytes();
        pq.add(x1);
      } else if (istmp[x2]) {
        ((MutableRoaringBitmap) buffer.get(x2)).lazyor(buffer.get(x1));
        sizes[x2] = buffer.get(x2).getLongSizeInBytes();
        pq.add(x2);
      } else if (istmp[x1]) {
        ((MutableRoaringBitmap) buffer.get(x1)).lazyor(buffer.get(x2));
        sizes[x1] = buffer.get(x1).getLongSizeInBytes();
        pq.add(x1);
      } else {
        buffer.set(x1, ImmutableRoaringBitmap.lazyor(buffer.get(x1), buffer.get(x2)));
        sizes[x1] = buffer.get(x1).getLongSizeInBytes();
        istmp[x1] = true;
        pq.add(x1);
      }
    }
    MutableRoaringBitmap answer = (MutableRoaringBitmap) buffer.get(pq.poll());
    answer.repairAfterLazy();
    return answer;
  }

  /**
   * Uses a priority queue to compute the xor aggregate.
   * 
   * This function runs in linearithmic (O(n log n)) time with respect to the number of bitmaps.
   * 
   * @param bitmaps input bitmaps
   * @return aggregated bitmap
   * @see #horizontal_xor(ImmutableRoaringBitmap...)
   */
  public static MutableRoaringBitmap priorityqueue_xor(ImmutableRoaringBitmap... bitmaps) {
    // code could be faster, see priorityqueue_or
    if (bitmaps.length < 2) {
      throw new IllegalArgumentException("Expecting at least 2 bitmaps");
    }
    final PriorityQueue<ImmutableRoaringBitmap> pq =
        new PriorityQueue<>(bitmaps.length, new Comparator<ImmutableRoaringBitmap>() {
          @Override
          public int compare(ImmutableRoaringBitmap a, ImmutableRoaringBitmap b) {
            return (int)(a.getLongSizeInBytes() - b.getLongSizeInBytes());
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
   * Private constructor to prevent instantiation of utility class
   */
  private BufferFastAggregation() {}

}
