package org.roaringbitmap;

import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.IntStream;

import static java.lang.ThreadLocal.withInitial;
import static org.roaringbitmap.Util.compareUnsigned;
import static org.roaringbitmap.Util.toIntUnsigned;

/**
 * These utility methods provide parallel implementations of
 * logical aggregation operators. AND is not implemented
 * because it is unlikely to be profitable.
 * <p>
 * There is a temporary memory overhead in using these methods,
 * since a materialisation of the rotated containers grouped by key
 * is created in each case.
 * <p>
 * Each method executes on the default fork join pool by default.
 * If this is undesirable (it usually is) wrap the call inside
 * a submission of a runnable to your own thread pool.
 * <p>
 * <pre>
 * {@code
 *
 *       //...
 *
 *       ExecutorService executor = ...
 *       RoaringBitmap[] bitmaps = ...
 *       // executes on executors threads
 *       RoaringBitmap result = executor.submit(() -> ParallelAggregation.or(bitmaps)).get();
 * }
 * </pre>
 */
public class ParallelAggregation {

  private static final Collector<Map.Entry<Short, List<Container>>, RoaringArray, RoaringBitmap> XOR
          = new ContainerCollector(ParallelAggregation::xor);

  private static final OrCollector OR = new OrCollector();

  /**
   * Creates a buffer which can safely be reused but is cheaper if limited
   * to a single calling thread
   */
  public static class AggregationBuffer {

    private final long[] keys;
    private final List<Container>[] buffer;

    private AggregationBuffer() {
      this.keys = new long[1 << 10];
      this.buffer = new List[1 << 16];
      for (int i = 0; i < buffer.length; ++i) {
        buffer[i] = new ArrayList<>();
      }
    }
    
    private List<Container>[] getBuffer() {
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
  public static class ContainerCollector implements
          Collector<Map.Entry<Short, List<Container>>, RoaringArray, RoaringBitmap> {

    private final Function<List<Container>, Container> reducer;

    /**
     * Creates a collector with the reducer function.
     *
     * @param reducer a function to apply to containers with the same key.
     */
    public ContainerCollector(Function<List<Container>, Container> reducer) {
      this.reducer = reducer;
    }

    @Override
    public Supplier<RoaringArray> supplier() {
      return RoaringArray::new;
    }

    @Override
    public BiConsumer<RoaringArray, Map.Entry<Short, List<Container>>> accumulator() {
      return (l, r) -> {
        assert l.size == 0 || compareUnsigned(l.keys[l.size - 1], r.getKey()) < 0;
        Container container = reducer.apply(r.getValue());
        if (!container.isEmpty()) {
          l.append(r.getKey(), container);
        }
      };
    }

    @Override
    public BinaryOperator<RoaringArray> combiner() {
      return (l, r) -> {
        assert l.size == 0 || r.size == 0 || compareUnsigned(l.keys[l.size - 1], r.keys[0]) < 0;
        l.append(r);
        return l;
      };
    }

    @Override
    public Function<RoaringArray, RoaringBitmap> finisher() {
      return RoaringBitmap::new;
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
          implements Collector<List<Container>, Container, Container> {

    @Override
    public Supplier<Container> supplier() {
      return () -> new BitmapContainer(new long[1 << 10], -1);
    }

    @Override
    public BiConsumer<Container, List<Container>> accumulator() {
      return (l, r) -> l.lazyIOR(or(r));
    }

    @Override
    public BinaryOperator<Container> combiner() {
      return Container::lazyIOR;
    }

    @Override
    public Function<Container, Container> finisher() {
      return Container::repairAfterLazy;
    }

    @Override
    public Set<Characteristics> characteristics() {
      return EnumSet.of(Characteristics.UNORDERED);
    }
  }

  /**
   * Groups the containers by their keys
   *
   * @param bitmaps input bitmaps
   * @return The containers from the bitmaps grouped by key
   */
  public static SortedMap<Short, List<Container>> groupByKey(RoaringBitmap... bitmaps) {
    Map<Short, List<Container>> grouped = new HashMap<>();
    for (RoaringBitmap bitmap : bitmaps) {
      RoaringArray ra = bitmap.highLowContainer;
      for (int i = 0; i < ra.size; ++i) {
        Container container = ra.values[i];
        Short key = ra.keys[i];
        List<Container> slice = grouped.get(key);
        if (null == slice) {
          slice = new ArrayList<>();
          grouped.put(key, slice);
        }
        slice.add(container);
      }
    }
    SortedMap<Short, List<Container>> sorted = new TreeMap<>(Util::compareUnsigned);
    sorted.putAll(grouped);
    return sorted;
  }

  /**
   * Computes the bitwise union of the input bitmaps.
   * Avoids allocating memory by using an aggregation buffer.
   * It is assumed that the instance of the aggregation buffer is not in use
   * on any other thread. Ensuring this is the caller's responsibility.
   * @see ParallelAggregation#newAggregationBuffer()
   *
   * @param aggregationBuffer an aggregation buffer for the calling thread
   * @param bitmaps the input bitmaps
   * @return the union of the bitmaps
   */
  public static RoaringBitmap bufferedOr(AggregationBuffer aggregationBuffer,
                                         RoaringBitmap... bitmaps) {
    List<Container>[] buffer = aggregationBuffer.getBuffer();
    short[] keys = prepareAggregation(aggregationBuffer, bitmaps);
    Container[] values = new Container[keys.length];
    IntStream.range(0, keys.length)
            .parallel()
            .forEach(position -> {
              List<Container> slice = buffer[toIntUnsigned(keys[position])];
              values[position] = or(slice);
              slice.clear();
            });
    return new RoaringBitmap(new RoaringArray(keys, values, keys.length));
  }

  /**
   * Computes the bitwise union of the input bitmaps
   *
   * @param bitmaps the input bitmaps
   * @return the union of the bitmaps
   */
  public static RoaringBitmap or(RoaringBitmap... bitmaps) {
    SortedMap<Short, List<Container>> grouped = groupByKey(bitmaps);
    short[] keys = new short[grouped.size()];
    Container[] values = new Container[grouped.size()];
    List<List<Container>> slices = new ArrayList<>(grouped.size());
    int i = 0;
    for (Map.Entry<Short, List<Container>> slice : grouped.entrySet()) {
      keys[i++] = slice.getKey();
      slices.add(slice.getValue());
    }
    IntStream.range(0, i)
            .parallel()
            .forEach(position -> values[position] = or(slices.get(position)));
    return new RoaringBitmap(new RoaringArray(keys, values, i));
  }

  /**
   * Computes the bitwise symmetric difference of the input bitmaps
   *
   * @param bitmaps the input bitmaps
   * @return the symmetric difference of the bitmaps
   */
  public static RoaringBitmap xor(RoaringBitmap... bitmaps) {
    return groupByKey(bitmaps)
            .entrySet()
            .parallelStream()
            .collect(XOR);
  }

  private static Container xor(List<Container> containers) {
    Container result = containers.get(0).clone();
    for (int i = 1; i < containers.size(); ++i) {
      result = result.ixor(containers.get(i));
    }
    return result;
  }

  private static Container or(List<Container> containers) {
    int parallelism;
    // if there are few enough containers it's possible no bitmaps will be materialised
    if (containers.size() < 16) {
      Container result = containers.get(0).clone();
      for (int i = 1; i < containers.size(); ++i) {
        result = result.lazyIOR(containers.get(i));
      }
      return result.repairAfterLazy();
    }
    // heuristic to save memory if the union is large and likely to end up as a bitmap
    if (containers.size() < 512 || (parallelism = availableParallelism()) == 1) {
      Container result = new BitmapContainer(new long[1 << 10], -1);
      for (Container container : containers) {
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
                                            RoaringBitmap... bitmaps) {
    long[] bitset = aggregationBuffer.getKeys();
    List<Container>[] buffer = aggregationBuffer.getBuffer();
    int size = 0;
    for (RoaringBitmap bitmap : bitmaps) {
      RoaringArray hlc = bitmap.highLowContainer;
      for (int position = 0; position < hlc.size; ++position) {
        short key = hlc.keys[position];
        Container container = hlc.values[position];
        int bit = key & 0xFFFF;
        int wordIndex = bit >>> 6;
        long word = bitset[wordIndex];
        bitset[wordIndex] |= (1L << bit);
        size += Long.bitCount(word ^ bitset[wordIndex]);
        buffer[bit].add(container);
      }
    }
    short[] keys = new short[size];
    int prefix = 0;
    int k = 0;
    for (int i = 0; i < bitset.length; ++i) {
      while (bitset[i] != 0) {
        keys[k++] = (short) (prefix + Long.numberOfTrailingZeros(bitset[i]));
        bitset[i] &= (bitset[i] - 1);
      }
      prefix += 64;
    }
    return keys;
  }
}
