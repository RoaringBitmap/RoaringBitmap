package org.roaringbitmap.buffer;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.roaringbitmap.Util.compareUnsigned;

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
          Map.Entry<Short,List<MappeableContainer>>, MutableRoaringArray, MutableRoaringBitmap> OR
          = new ContainerCollector(BufferParallelAggregation::or);

  private static final Collector<
          Map.Entry<Short,List<MappeableContainer>>, MutableRoaringArray, MutableRoaringBitmap> XOR
          = new ContainerCollector(BufferParallelAggregation::xor);

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
    return groupByKey(bitmaps)
            .entrySet()
            .parallelStream()
            .collect(OR);
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

  private static MappeableContainer or(List<MappeableContainer> containers) {
    MappeableContainer result = containers.get(0).clone();
    if (containers.size() == 1) {
      return result;
    }
    for (int i = 1; i < containers.size(); ++i) {
      result = result.lazyIOR(containers.get(i));
    }
    return result.repairAfterLazy();
  }

  private static MappeableContainer xor(List<MappeableContainer> containers) {
    MappeableContainer result = containers.get(0).clone();
    for (int i = 1; i < containers.size(); ++i) {
      result = result.ixor(containers.get(i));
    }
    return result;
  }

}

