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
 *       RoaringBitmap[] bitmaps = ...
 *       // executes on executors threads
 *       RoaringBitmap result = executor.submit(() -> ParallelAggregation.or(bitmaps)).get();
 * }
 * </pre>
 */
public class ParallelAggregation {

  private static final Collector<Map.Entry<Character, List<Container>>,
          RoaringArray, RoaringBitmap>
          XOR = new ContainerCollector(ParallelAggregation::xor);

  private static final OrCollector OR = new OrCollector();

  /**
   * Collects containers grouped by their key into a RoaringBitmap, applying the
   * supplied aggregation function to each group.
   */
  public static class ContainerCollector implements
          Collector<Map.Entry<Character, List<Container>>, RoaringArray, RoaringBitmap> {

    private final Function<List<Container>, Container> reducer;

    /**
     * Creates a collector with the reducer function.
     * @param reducer a function to apply to containers with the same key.
     */
    ContainerCollector(Function<List<Container>, Container> reducer) {
      this.reducer = reducer;
    }

    @Override
    public Supplier<RoaringArray> supplier() {
      return RoaringArray::new;
    }

    @Override
    public BiConsumer<RoaringArray, Map.Entry<Character, List<Container>>> accumulator() {
      return (l, r) -> {
        assert l.size == 0 || l.keys[l.size - 1] < r.getKey();
        Container container = reducer.apply(r.getValue());
        if (!container.isEmpty()) {
          l.append(r.getKey(), container);
        }
      };
    }

    @Override
    public BinaryOperator<RoaringArray> combiner() {
      return (l, r) -> {
        assert l.size == 0 || r.size == 0 || l.keys[l.size - 1] - r.keys[0] < 0;
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
   * @param bitmaps input bitmaps
   * @return The containers from the bitmaps grouped by key
   */
  public static SortedMap<Character, List<Container>> groupByKey(RoaringBitmap... bitmaps) {
    Map<Character, List<Container>> grouped = new HashMap<>();
    for (RoaringBitmap bitmap : bitmaps) {
      RoaringArray ra = bitmap.highLowContainer;
      for (int i = 0; i < ra.size; ++i) {
        Container container = ra.values[i];
        Character key = ra.keys[i];
        List<Container> slice = grouped.get(key);
        if (null == slice) {
          slice = new ArrayList<>();
          grouped.put(key, slice);
        }
        slice.add(container);
      }
    }
    return new TreeMap<>(grouped);
  }


  /**
   * Computes the bitwise union of the input bitmaps
   * @param bitmaps the input bitmaps
   * @return the union of the bitmaps
   */
  public static RoaringBitmap or(RoaringBitmap... bitmaps) {
    SortedMap<Character, List<Container>> grouped = groupByKey(bitmaps);
    char[] keys = new char[grouped.size()];
    Container[] values = new Container[grouped.size()];
    List<List<Container>> slices = new ArrayList<>(grouped.size());
    int i = 0;
    for (Map.Entry<Character, List<Container>> slice : grouped.entrySet()) {
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
    int step = Math.floorDiv(containers.size(), parallelism);
    int mod = Math.floorMod(containers.size(), parallelism);
    // we have an enormous slice (probably skewed), parallelise it
    return IntStream.range(0, parallelism)
            .parallel()
            .mapToObj(i -> containers.subList(i * step + Math.min(i, mod),
                    (i + 1) * step + Math.min(i + 1, mod)))
            .collect(OR);
  }

  private static int availableParallelism() {
    return ForkJoinTask.inForkJoinPool()
            ? ForkJoinTask.getPool().getParallelism()
            : ForkJoinPool.getCommonPoolParallelism();
  }

}
