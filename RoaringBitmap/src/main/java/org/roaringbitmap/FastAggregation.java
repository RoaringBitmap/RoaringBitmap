/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap;

import java.util.*;


/**
 * Fast algorithms to aggregate many bitmaps.
 *
 * @author Daniel Lemire
 */
public final class FastAggregation {


  /**
   * Compute the AND aggregate.
   *
   * In practice, calls {#link naive_and}
   *
   * @param bitmaps input bitmaps
   * @return aggregated bitmap
   */
  public static RoaringBitmap and(Iterator<? extends RoaringBitmap> bitmaps) {
    return naive_and(bitmaps);
  }

  /**
   * Compute the AND aggregate.
   *
   *
   * @param bitmaps input bitmaps
   * @return aggregated bitmap
   */
  public static RoaringBitmap and(RoaringBitmap... bitmaps) {
    if (bitmaps.length > 2) {
      return workShyAnd(new long[1024], bitmaps);
    }
    return naive_and(bitmaps);
  }

  /**
   * Compute the AND aggregate.
   *
   * @param aggregationBuffer a buffer for aggregation
   * @param bitmaps input bitmaps
   * @return aggregated bitmap
   */
  public static RoaringBitmap and(long[] aggregationBuffer, RoaringBitmap... bitmaps) {
    if (bitmaps.length > 2) {
      if(aggregationBuffer.length < 1024) {
        throw new IllegalArgumentException("buffer should have at least 1024 elements.");
      }
      try {
        return workShyAnd(aggregationBuffer, bitmaps);
      } finally {
        Arrays.fill(aggregationBuffer, 0L);
      }
    }
    return naive_and(bitmaps);
  }

  /**
   * Calls naive_or.
   *
   * @param bitmaps input bitmaps
   * @return aggregated bitmap
   */
  @Deprecated
  public static RoaringBitmap horizontal_or(Iterator<? extends RoaringBitmap> bitmaps) {
    return naive_or(bitmaps);
  }


  /**
   * Minimizes memory usage while computing the or aggregate on a moderate number of bitmaps.
   *
   * This function runs in linearithmic (O(n log n)) time with respect to the number of bitmaps.
   *
   * @param bitmaps input bitmaps
   * @return aggregated bitmap
   * @see #or(RoaringBitmap...)
   */
  public static RoaringBitmap horizontal_or(List<? extends RoaringBitmap> bitmaps) {
    RoaringBitmap answer = new RoaringBitmap();
    if (bitmaps.isEmpty()) {
      return answer;
    }
    PriorityQueue<ContainerPointer> pq = new PriorityQueue<>(bitmaps.size());
    for (int k = 0; k < bitmaps.size(); ++k) {
      ContainerPointer x = bitmaps.get(k).highLowContainer.getContainerPointer();
      if (x.getContainer() != null) {
        pq.add(x);
      }
    }

    while (!pq.isEmpty()) {
      ContainerPointer x1 = pq.poll();
      if (pq.isEmpty() || (pq.peek().key() != x1.key())) {
        answer.highLowContainer.append(x1.key(), x1.getContainer().clone());
        x1.advance();
        if (x1.getContainer() != null) {
          pq.add(x1);
        }
        continue;
      }
      ContainerPointer x2 = pq.poll();
      Container newc = x1.getContainer().lazyOR(x2.getContainer());
      while (!pq.isEmpty() && (pq.peek().key() == x1.key())) {

        ContainerPointer x = pq.poll();
        newc = newc.lazyIOR(x.getContainer());
        x.advance();
        if (x.getContainer() != null) {
          pq.add(x);
        } else if (pq.isEmpty()) {
          break;
        }
      }
      newc = newc.repairAfterLazy();
      answer.highLowContainer.append(x1.key(), newc);
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
   * Minimizes memory usage while computing the or aggregate on a moderate number of bitmaps.
   *
   * This function runs in linearithmic (O(n log n)) time with respect to the number of bitmaps.
   *
   * @param bitmaps input bitmaps
   * @return aggregated bitmap
   * @see #or(RoaringBitmap...)
   */
  public static RoaringBitmap horizontal_or(RoaringBitmap... bitmaps) {
    RoaringBitmap answer = new RoaringBitmap();
    if (bitmaps.length == 0) {
      return answer;
    }
    PriorityQueue<ContainerPointer> pq = new PriorityQueue<>(bitmaps.length);
    for (int k = 0; k < bitmaps.length; ++k) {
      ContainerPointer x = bitmaps[k].highLowContainer.getContainerPointer();
      if (x.getContainer() != null) {
        pq.add(x);
      }
    }

    while (!pq.isEmpty()) {
      ContainerPointer x1 = pq.poll();
      if (pq.isEmpty() || (pq.peek().key() != x1.key())) {
        answer.highLowContainer.append(x1.key(), x1.getContainer().clone());
        x1.advance();
        if (x1.getContainer() != null) {
          pq.add(x1);
        }
        continue;
      }
      ContainerPointer x2 = pq.poll();
      Container newc = x1.getContainer().lazyOR(x2.getContainer());
      while (!pq.isEmpty() && (pq.peek().key() == x1.key())) {

        ContainerPointer x = pq.poll();
        newc = newc.lazyIOR(x.getContainer());
        x.advance();
        if (x.getContainer() != null) {
          pq.add(x);
        } else if (pq.isEmpty()) {
          break;
        }
      }
      newc = newc.repairAfterLazy();
      answer.highLowContainer.append(x1.key(), newc);
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
   * @see #xor(RoaringBitmap...)
   */
  public static RoaringBitmap horizontal_xor(RoaringBitmap... bitmaps) {
    RoaringBitmap answer = new RoaringBitmap();
    if (bitmaps.length == 0) {
      return answer;
    }
    PriorityQueue<ContainerPointer> pq = new PriorityQueue<>(bitmaps.length);
    for (int k = 0; k < bitmaps.length; ++k) {
      ContainerPointer x = bitmaps[k].highLowContainer.getContainerPointer();
      if (x.getContainer() != null) {
        pq.add(x);
      }
    }

    while (!pq.isEmpty()) {
      ContainerPointer x1 = pq.poll();
      if (pq.isEmpty() || (pq.peek().key() != x1.key())) {
        answer.highLowContainer.append(x1.key(), x1.getContainer().clone());
        x1.advance();
        if (x1.getContainer() != null) {
          pq.add(x1);
        }
        continue;
      }
      ContainerPointer x2 = pq.poll();
      Container newc = x1.getContainer().xor(x2.getContainer());
      while (!pq.isEmpty() && (pq.peek().key() == x1.key())) {
        ContainerPointer x = pq.poll();
        newc = newc.ixor(x.getContainer());
        x.advance();
        if (x.getContainer() != null) {
          pq.add(x);
        } else if (pq.isEmpty()) {
          break;
        }
      }
      answer.highLowContainer.append(x1.key(), newc);
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
   * Compute overall AND between bitmaps two-by-two.
   *
   * This function runs in linear time with respect to the number of bitmaps.
   *
   * @param bitmaps input bitmaps
   * @return aggregated bitmap
   */
  public static RoaringBitmap naive_and(Iterator<? extends RoaringBitmap> bitmaps) {
    if (!bitmaps.hasNext()) {
      return new RoaringBitmap();
    }
    RoaringBitmap answer = bitmaps.next().clone();
    while (bitmaps.hasNext() && !answer.isEmpty()) {
      answer.and(bitmaps.next());
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
  public static RoaringBitmap naive_and(RoaringBitmap... bitmaps) {
    if (bitmaps.length == 0) {
      return new RoaringBitmap();
    }
    RoaringBitmap answer = bitmaps[0].clone();
    for (int k = 1; k < bitmaps.length && !answer.isEmpty(); ++k) {
      answer.and(bitmaps[k]);
    }
    return answer;
  }

  /**
   * Computes the intersection by first intersecting the keys, avoids
   * materialising containers.
   *
   * @param buffer an 8KB buffer
   * @param bitmaps the inputs
   * @return the intersection of the bitmaps
   */
  public static RoaringBitmap workShyAnd(long[] buffer, RoaringBitmap... bitmaps) {
    long[] words = buffer;
    RoaringBitmap first = bitmaps[0];
    for (int i = 0; i < first.highLowContainer.size; ++i) {
      char key = first.highLowContainer.keys[i];
      words[key >>> 6] |= 1L << key;
    }
    int numContainers = first.highLowContainer.size;
    for (int i = 1; i < bitmaps.length && numContainers > 0; ++i) {
      numContainers = Util.intersectArrayIntoBitmap(words,
              bitmaps[i].highLowContainer.keys, bitmaps[i].highLowContainer.size);
    }
    if (numContainers == 0) {
      return new RoaringBitmap();
    }
    char[] keys = new char[numContainers];
    int base = 0;
    int pos = 0;
    for (long word : words) {
      while (word != 0L) {
        keys[pos++] = (char)(base + Long.numberOfTrailingZeros(word));
        word &= (word - 1);
      }
      base += 64;
    }
    Container[][] containers = new Container[numContainers][bitmaps.length];
    for (int i = 0; i < bitmaps.length; ++i) {
      RoaringBitmap bitmap = bitmaps[i];
      int position = 0;
      for (int j = 0; j < bitmap.highLowContainer.size; ++j) {
        char key = bitmap.highLowContainer.keys[j];
        if ((words[key >>> 6] & (1L << key)) != 0) {
          containers[position++][i] = bitmap.highLowContainer.values[j];
        }
      }
    }

    RoaringArray array =
            new RoaringArray(keys, new Container[numContainers], 0);
    for (int i = 0; i < numContainers; ++i) {
      Container[] slice = containers[i];
      Arrays.fill(words, -1L);
      Container tmp = new BitmapContainer(words, -1);
      for (Container container : slice) {
        Container and = tmp.iand(container);
        if (and != tmp) {
          tmp = and;
        }
      }
      tmp = tmp.repairAfterLazy();
      if (!tmp.isEmpty()) {
        array.append(keys[i], tmp instanceof BitmapContainer ? tmp.clone() : tmp);
      }
    }
    return new RoaringBitmap(array);
  }



  /**
   * Computes the intersection by first intersecting the keys, avoids
   * materialising containers, limits memory usage.  You must provide a long[] array
   * of length at least 1024, initialized with zeroes. We do not check whether the array
   * is initialized with zeros: it is the caller's responsability.
   * You should expect this function to be slower than workShyAnd and the reduction
   * in memory usage might be small.
   *
   * @param buffer should be a 1024-long array
   * @param bitmaps the inputs
   * @return the intersection of the bitmaps
   */
  public static RoaringBitmap workAndMemoryShyAnd(long[] buffer, RoaringBitmap... bitmaps) {
    if(buffer.length < 1024) {
      throw new IllegalArgumentException("buffer should have at least 1024 elements.");
    } 
    long[] words = buffer;
    RoaringBitmap first = bitmaps[0];
    for (int i = 0; i < first.highLowContainer.size; ++i) {
      char key = first.highLowContainer.keys[i];
      words[key >>> 6] |= 1L << key;
    }
    int numContainers = first.highLowContainer.size;
    for (int i = 1; i < bitmaps.length && numContainers > 0; ++i) {
      numContainers = Util.intersectArrayIntoBitmap(words,
              bitmaps[i].highLowContainer.keys, bitmaps[i].highLowContainer.size);
    }
    if (numContainers == 0) {
      return new RoaringBitmap();
    }
    char[] keys = new char[numContainers];
    int base = 0;
    int pos = 0;
    for (long word : words) {
      while (word != 0L) {
        keys[pos++] = (char)(base + Long.numberOfTrailingZeros(word));
        word &= (word - 1);
      }
      base += 64;
    }
    RoaringArray array =
            new RoaringArray(keys, new Container[numContainers], 0);
    for (int i = 0; i < numContainers; ++i) {
      char MatchingKey = keys[i];
      Arrays.fill(words, -1L);
      Container tmp = new BitmapContainer(words, -1);
      for(RoaringBitmap bitmap: bitmaps) {
        int idx = bitmap.highLowContainer.getIndex(MatchingKey);
        if(idx < 0) {
          continue;
        }
        Container container = bitmap.highLowContainer.getContainerAtIndex(idx);
        Container and = tmp.iand(container);
        if (and != tmp) {
          tmp = and;
        }
      }
      tmp = tmp.repairAfterLazy();
      if (!tmp.isEmpty()) {
        array.append(keys[i], tmp instanceof BitmapContainer ? tmp.clone() : tmp);
      }
    }
    return new RoaringBitmap(array);
  }

  /**
   * Compute overall OR between bitmaps two-by-two.
   *
   * This function runs in linear time with respect to the number of bitmaps.
   *
   * @param bitmaps input bitmaps
   * @return aggregated bitmap
   */
  public static RoaringBitmap naive_or(Iterator<? extends RoaringBitmap> bitmaps) {
    RoaringBitmap answer = new RoaringBitmap();
    while (bitmaps.hasNext()) {
      answer.naivelazyor(bitmaps.next());
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
  public static RoaringBitmap naive_or(RoaringBitmap... bitmaps) {
    RoaringBitmap answer = new RoaringBitmap();
    for (int k = 0; k < bitmaps.length; ++k) {
      answer.naivelazyor(bitmaps[k]);
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
  public static RoaringBitmap naive_xor(Iterator<? extends RoaringBitmap> bitmaps) {
    RoaringBitmap answer = new RoaringBitmap();
    while (bitmaps.hasNext()) {
      answer.xor(bitmaps.next());
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
  public static RoaringBitmap naive_xor(RoaringBitmap... bitmaps) {
    RoaringBitmap answer = new RoaringBitmap();
    for (int k = 0; k < bitmaps.length; ++k) {
      answer.xor(bitmaps[k]);
    }
    return answer;
  }

  /**
   * Compute overall OR between bitmaps.
   *
   *
   * @param bitmaps input bitmaps
   * @return aggregated bitmap
   */
  public static RoaringBitmap or(Iterator<? extends RoaringBitmap> bitmaps) {
    return naive_or(bitmaps);
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
   * Uses a priority queue to compute the or aggregate.
   *
   * This function runs in linearithmic (O(n log n)) time with respect to the number of bitmaps.
   *
   * @param bitmaps input bitmaps
   * @return aggregated bitmap
   * @see #horizontal_or(RoaringBitmap...)
   */
  public static RoaringBitmap priorityqueue_or(Iterator<? extends RoaringBitmap> bitmaps) {
    if (!bitmaps.hasNext()) {
      return new RoaringBitmap();
    }
    // we buffer the call to getSizeInBytes(), hence the code complexity
    ArrayList<RoaringBitmap> buffer = new ArrayList<>();
    while (bitmaps.hasNext()) {
      buffer.add(bitmaps.next());
    }
    final long[] sizes = new long[buffer.size()];
    final boolean[] istmp = new boolean[buffer.size()];
    for (int k = 0; k < sizes.length; ++k) {
      sizes[k] = buffer.get(k).getLongSizeInBytes();
    }
    PriorityQueue<Integer> pq = new PriorityQueue<>(128, new Comparator<Integer>() {
      @Override
      public int compare(Integer a, Integer b) {
        return (int) (sizes[a] - sizes[b]);
      }
    });
    for (int k = 0; k < sizes.length; ++k) {
      pq.add(k);
    }
    while (pq.size() > 1) {
      Integer x1 = pq.poll();
      Integer x2 = pq.poll();
      if (istmp[x2] && istmp[x1]) {
        buffer.set(x1, RoaringBitmap.lazyorfromlazyinputs(buffer.get(x1), buffer.get(x2)));
        sizes[x1] = buffer.get(x1).getLongSizeInBytes();
        istmp[x1] = true;
        pq.add(x1);
      } else if (istmp[x2]) {
        buffer.get(x2).lazyor(buffer.get(x1));
        sizes[x2] = buffer.get(x2).getLongSizeInBytes();
        pq.add(x2);
      } else if (istmp[x1]) {
        buffer.get(x1).lazyor(buffer.get(x2));
        sizes[x1] = buffer.get(x1).getLongSizeInBytes();
        pq.add(x1);
      } else {
        buffer.set(x1, RoaringBitmap.lazyor(buffer.get(x1), buffer.get(x2)));
        sizes[x1] = buffer.get(x1).getLongSizeInBytes();
        istmp[x1] = true;
        pq.add(x1);
      }
    }
    RoaringBitmap answer = buffer.get(pq.poll());
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
   * @see #horizontal_or(RoaringBitmap...)
   */
  public static RoaringBitmap priorityqueue_or(RoaringBitmap... bitmaps) {
    if (bitmaps.length == 0) {
      return new RoaringBitmap();
    }
    // we buffer the call to getSizeInBytes(), hence the code complexity
    final RoaringBitmap[] buffer = Arrays.copyOf(bitmaps, bitmaps.length);
    final long[] sizes = new long[buffer.length];
    final boolean[] istmp = new boolean[buffer.length];
    for (int k = 0; k < sizes.length; ++k) {
      sizes[k] = buffer[k].getLongSizeInBytes();
    }
    PriorityQueue<Integer> pq = new PriorityQueue<>(128, new Comparator<Integer>() {
      @Override
      public int compare(Integer a, Integer b) {
        return (int) (sizes[a] - sizes[b]);
      }
    });
    for (int k = 0; k < sizes.length; ++k) {
      pq.add(k);
    }
    while (pq.size() > 1) {
      Integer x1 = pq.poll();
      Integer x2 = pq.poll();
      if (istmp[x2] && istmp[x1]) {
        buffer[x1] = RoaringBitmap.lazyorfromlazyinputs(buffer[x1], buffer[x2]);
        sizes[x1] = buffer[x1].getLongSizeInBytes();
        istmp[x1] = true;
        pq.add(x1);
      } else if (istmp[x2]) {
        buffer[x2].lazyor(buffer[x1]);
        sizes[x2] = buffer[x2].getLongSizeInBytes();
        pq.add(x2);
      } else if (istmp[x1]) {
        buffer[x1].lazyor(buffer[x2]);
        sizes[x1] = buffer[x1].getLongSizeInBytes();
        pq.add(x1);
      } else {
        buffer[x1] = RoaringBitmap.lazyor(buffer[x1], buffer[x2]);
        sizes[x1] = buffer[x1].getLongSizeInBytes();
        istmp[x1] = true;
        pq.add(x1);
      }
    }
    RoaringBitmap answer = buffer[pq.poll()];
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
   * @see #horizontal_xor(RoaringBitmap...)
   */
  public static RoaringBitmap priorityqueue_xor(RoaringBitmap... bitmaps) {
    // TODO: This code could be faster, see priorityqueue_or
    if (bitmaps.length == 0) {
      return new RoaringBitmap();
    }

    PriorityQueue<RoaringBitmap> pq =
        new PriorityQueue<>(bitmaps.length, new Comparator<RoaringBitmap>() {
          @Override
          public int compare(RoaringBitmap a, RoaringBitmap b) {
            return (int)(a.getLongSizeInBytes() - b.getLongSizeInBytes());
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
   * Compute overall XOR between bitmaps.
   *
   *
   * @param bitmaps input bitmaps
   * @return aggregated bitmap
   */
  public static RoaringBitmap xor(Iterator<? extends RoaringBitmap> bitmaps) {
    return naive_xor(bitmaps);
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
   * Private constructor to prevent instantiation of utility class
   */
  private FastAggregation() {

  }

}
