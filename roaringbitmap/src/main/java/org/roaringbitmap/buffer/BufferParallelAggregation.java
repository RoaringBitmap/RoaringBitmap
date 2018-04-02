package org.roaringbitmap.buffer;

import java.nio.LongBuffer;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.IntStream;

import static java.lang.Long.numberOfTrailingZeros;
import static org.roaringbitmap.Util.compareUnsigned;
import static org.roaringbitmap.buffer.BufferUtil.toIntUnsigned;

/**
 *
 * These utility methods provide parallel implementations of
 * logical aggregation operators. AND is not implemented
 * because it is unlikely to be profitable.
 *
 * There is a temporary memory overhead in using these methods,
 * since a materialisation of the rotated containers grouped by key
 * is created in each case.
 *
 * Each method executes on the default fork join pool by default.
 * If this is undesirable (it usually is) wrap the call inside
 * a submission of a runnable to your own thread pool.
 *
 * <pre>
 * {@code
 *
 *       //...
 *
 *       ExecutorService executor = ...
 *       ImmutableRoaringBitmap[] bitmaps = ...
 *       // executes on executors threads
 *       MutableRoaringBitmap result = executor.submit(
 *            () -> BufferParallelAggregation.or(bitmaps)).get();
 * }
 * </pre>
 */
public class BufferParallelAggregation {

  private static final Collector<
          Map.Entry<Short,List<MappeableContainer>>, MutableRoaringArray, MutableRoaringBitmap> XOR
          = new ContainerCollector(BufferParallelAggregation::xor);

  private static final OrCollector OR = new OrCollector();

  /**
   * Creates a buffer which should be used from a single thread
   */
  public static class AggregationBuffer {

    private final long[] keys;
    private final List<MappeableContainer>[] buffer;

    private AggregationBuffer() {
      this.keys = new long[1 << 10];
      this.buffer = new List[1 << 16];
      for (int i = 0; i < buffer.length; ++i) {
        buffer[i] = new ArrayList<>();
      }
    }

    private List<MappeableContainer>[] getBuffer() {
      return buffer;
    }

    private long[] getKeys() {
      return keys;
    }
  }

  /**
   * Creates an aggregation buffer for buffered aggregations
   * @return an aggregation buffer
   */
  public static AggregationBuffer newAggregationBuffer() {
    return new AggregationBuffer();
  }

  /**
   * Collects containers grouped by their key into a RoaringBitmap, applying the
   * supplied aggregation function to each group.
   */
  public static class ContainerCollector implements Collector<
          Map.Entry<Short,List<MappeableContainer>>, MutableRoaringArray, MutableRoaringBitmap> {

    private final Function<List<MappeableContainer>, MappeableContainer> reducer;

    /**
     * Creates a collector with the reducer function.
     * @param reducer a function to apply to containers with the same key.
     */
    public ContainerCollector(Function<List<MappeableContainer>, MappeableContainer> reducer) {
      this.reducer = reducer;
    }

    @Override
    public Supplier<MutableRoaringArray> supplier() {
      return MutableRoaringArray::new;
    }

    @Override
    public BiConsumer<
            MutableRoaringArray, Map.Entry<Short, List<MappeableContainer>>> accumulator() {
      return (l, r) -> {
        assert l.size == 0 || compareUnsigned(l.keys[l.size - 1], r.getKey()) < 0;
        MappeableContainer container = reducer.apply(r.getValue());
        if (!container.isEmpty()) {
          l.append(r.getKey(), container);
        }
      };
    }

    @Override
    public BinaryOperator<MutableRoaringArray> combiner() {
      return (l, r) -> {
        assert l.size == 0 || r.size == 0 || compareUnsigned(l.keys[l.size - 1], r.keys[0]) < 0;
        l.append(r);
        return l;
      };
    }

    @Override
    public Function<MutableRoaringArray, MutableRoaringBitmap> finisher() {
      return MutableRoaringBitmap::new;
    }

    @Override
    public Set<Characteristics> characteristics() {
      return EnumSet.noneOf(Characteristics.class);
    }
  }

  /**
   * Collects a list of containers into a single container.
   */
  public static class OrCollector
          implements Collector<List<MappeableContainer>, MappeableContainer, MappeableContainer> {

    @Override
    public Supplier<MappeableContainer> supplier() {
      return () -> new MappeableBitmapContainer(LongBuffer.allocate(1 << 10), -1);
    }

    @Override
    public BiConsumer<MappeableContainer, List<MappeableContainer>> accumulator() {
      return (l, r) -> l.lazyIOR(or(r));
    }

    @Override
    public BinaryOperator<MappeableContainer> combiner() {
      return MappeableContainer::lazyIOR;
    }

    @Override
    public Function<MappeableContainer, MappeableContainer> finisher() {
      return MappeableContainer::repairAfterLazy;
    }

    @Override
    public Set<Characteristics> characteristics() {
      return EnumSet.of(Characteristics.UNORDERED);
    }
  }

  /**
   * Groups the containers by their keys
   * @param bitmaps input bitmaps
   * @return The containers from the bitmaps grouped by key
   */
  public static SortedMap<Short, List<MappeableContainer>> groupByKey(
          ImmutableRoaringBitmap... bitmaps) {
    Map<Short, List<MappeableContainer>> grouped = new HashMap<>();
    for (ImmutableRoaringBitmap bitmap : bitmaps) {
      MappeableContainerPointer it = bitmap.highLowContainer.getContainerPointer();
      while (null != it.getContainer()) {
        MappeableContainer container = it.getContainer();
        Short key = it.key();
        List<MappeableContainer> slice = grouped.get(key);
        if (null == slice) {
          slice = new ArrayList<>();
          grouped.put(key, slice);
        }
        slice.add(container);
        it.advance();
      }
    }
    SortedMap<Short, List<MappeableContainer>> sorted = new TreeMap<>(BufferUtil::compareUnsigned);
    sorted.putAll(grouped);
    return sorted;
  }

  /**
   * Computes the bitwise union of the input bitmaps
   * @param bitmaps the input bitmaps
   * @return the union of the bitmaps
   */
  public static MutableRoaringBitmap or(ImmutableRoaringBitmap... bitmaps) {
    SortedMap<Short, List<MappeableContainer>> grouped = groupByKey(bitmaps);
    short[] keys = new short[grouped.size()];
    MappeableContainer[] values = new MappeableContainer[grouped.size()];
    List<List<MappeableContainer>> slices = new ArrayList<>(grouped.size());
    int i = 0;
    for (Map.Entry<Short, List<MappeableContainer>> slice : grouped.entrySet()) {
      keys[i++] = slice.getKey();
      slices.add(slice.getValue());
    }
    IntStream.range(0, i)
            .parallel()
            .forEach(position -> values[position] = or(slices.get(position)));
    return new MutableRoaringBitmap(new MutableRoaringArray(keys, values, i));
  }

  /**
   * Computes the bitwise union of the input bitmaps.
   * Avoids allocating memory by using the supplied buffer.
   * * It is assumed that the instance of the aggregation buffer is not in use
   * on any other thread. Ensuring this is the caller's responsibility.
   * @see BufferParallelAggregation#newAggregationBuffer()
   *
   * @param aggregationBuffer an aggregation buffer for the calling thread
   * @param bitmaps the input bitmaps
   * @return the union of the bitmaps
   */
  public static MutableRoaringBitmap bufferedOr(AggregationBuffer aggregationBuffer,
                                                ImmutableRoaringBitmap... bitmaps) {
    List<MappeableContainer>[] buffer = aggregationBuffer.getBuffer();
    short[] keys = prepareAggregation(aggregationBuffer, bitmaps);
    MappeableContainer[] values = new MappeableContainer[keys.length];
    IntStream.range(0, keys.length)
            .parallel()
            .forEach(position -> {
              List<MappeableContainer> slice = buffer[toIntUnsigned(keys[position])];
              values[position] = or(slice);
              slice.clear();
            });
    return new MutableRoaringBitmap(new MutableRoaringArray(keys, values, keys.length));
  }

  /**
   * Computes the bitwise symmetric difference of the input bitmaps
   * @param bitmaps the input bitmaps
   * @return the symmetric difference of the bitmaps
   */
  public static MutableRoaringBitmap xor(ImmutableRoaringBitmap... bitmaps) {
    return groupByKey(bitmaps)
            .entrySet()
            .parallelStream()
            .collect(XOR);
  }



  private static MappeableContainer xor(List<MappeableContainer> containers) {
    MappeableContainer result = containers.get(0).clone();
    for (int i = 1; i < containers.size(); ++i) {
      result = result.ixor(containers.get(i));
    }
    return result;
  }

  private static MappeableContainer or(List<MappeableContainer> containers) {
    int parallelism;
    // if there are few enough containers it's possible no bitmaps will be materialised
    if (containers.size() < 16) {
      MappeableContainer result = containers.get(0).clone();
      for (int i = 1; i < containers.size(); ++i) {
        result = result.lazyIOR(containers.get(i));
      }
      return result.repairAfterLazy();
    }
    // heuristic to save memory if the union is large and likely to end up as a bitmap
    if (containers.size() < 512 || (parallelism = availableParallelism()) == 1) {
      MappeableContainer result = new MappeableBitmapContainer(LongBuffer.allocate(1 << 10), -1);
      for (MappeableContainer container : containers) {
        result = result.lazyIOR(container);
      }
      return result.repairAfterLazy();
    }
    // we have an enormous slice (probably skewed), parallelise it
    int partitionSize = (containers.size() + parallelism - 1) / parallelism;
    return IntStream.range(0, parallelism)
            .parallel()
            .mapToObj(i -> containers.subList(i * partitionSize,
                    Math.min((i + 1) * partitionSize, containers.size())))
            .collect(OR);
  }

  private static int availableParallelism() {
    return ForkJoinTask.inForkJoinPool()
            ? ForkJoinTask.getPool().getParallelism()
            : ForkJoinPool.getCommonPoolParallelism();
  }

  private static short[] prepareAggregation(AggregationBuffer aggregationBuffer,
                                            ImmutableRoaringBitmap... bitmaps) {
    long[] bitset = aggregationBuffer.getKeys();
    List<MappeableContainer>[] buffer = aggregationBuffer.getBuffer();
    int size = 0;
    for (ImmutableRoaringBitmap bitmap : bitmaps) {
      MappeableContainerPointer pointer = bitmap.getContainerPointer();
      while (null != pointer.getContainer()) {
        short key = pointer.key();
        MappeableContainer container = pointer.getContainer();
        int bit = key & 0xFFFF;
        int wordIndex = bit >>> 6;
        long word = bitset[wordIndex];
        bitset[wordIndex] |= (1L << bit);
        size += Long.bitCount(word ^ bitset[wordIndex]);
        buffer[bit].add(container);
        pointer.advance();
      }
    }
    short[] keys = new short[size];
    int prefix = 0;
    int k = 0;
    for (int i = 0; i < bitset.length; ++i) {
      while (bitset[i] != 0) {
        keys[k++] = (short) (prefix + numberOfTrailingZeros(bitset[i]));
        bitset[i] &= (bitset[i] - 1);
      }
      prefix += 64;
    }
    return keys;
  }

}

