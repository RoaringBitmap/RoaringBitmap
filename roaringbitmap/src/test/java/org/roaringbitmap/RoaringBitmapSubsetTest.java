package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class RoaringBitmapSubsetTest {

  private static final Predicate<Integer> DIVISIBLE_BY_4 = i -> i % 4 == 0;

  private static final Predicate<Integer> DIVISIBLE_BY_3 = i -> i % 3 == 0;

  public static Stream<Arguments> params() {
    return Stream.of(
        Arguments.of( // array vs array
            ImmutableSet.of(1, 2, 3, 4), ImmutableSet.of(2, 3)),
        Arguments.of( // array vs empty
            ImmutableSet.of(1, 2, 3, 4), ImmutableSet.of()),
        Arguments.of( // identical arrays
            ImmutableSet.of(1, 2, 3, 4), ImmutableSet.of(1, 2, 3, 4)),
        Arguments.of( // disjoint arrays
            ImmutableSet.of(10, 12, 14, 15), ImmutableSet.of(1, 2, 3, 4)),
        Arguments.of( // disjoint arrays, cardinality mismatch
            ImmutableSet.of(10, 12, 14), ImmutableSet.of(1, 2, 3, 4)),
        Arguments.of( // run vs array, subset
            ContiguousSet.create(Range.closed(1, 1 << 8), DiscreteDomain.integers()),
            ImmutableSet.of(1, 2, 3, 4)),
        Arguments.of( // run vs array, subset
            ContiguousSet.create(Range.closed(1, 1 << 16), DiscreteDomain.integers()),
            ImmutableSet.of(1, 2, 3, 4)),
        Arguments.of( // run vs empty
            ContiguousSet.create(Range.closed(1, 1 << 16), DiscreteDomain.integers()),
            ImmutableSet.of()),
        Arguments.of( // identical runs, 1 container
            ContiguousSet.create(Range.closed(1, 1 << 16), DiscreteDomain.integers()),
            ContiguousSet.create(Range.closed(1, 1 << 16), DiscreteDomain.integers())),
        Arguments.of( // identical runs, 2 containers
            ContiguousSet.create(Range.closed(1, 1 << 20), DiscreteDomain.integers()),
            ContiguousSet.create(Range.closed(1, 1 << 20), DiscreteDomain.integers())),
        Arguments.of( // disjoint array vs run, either side of container boundary
            ContiguousSet.create(Range.closed(1, 1 << 16), DiscreteDomain.integers()),
            ImmutableSet.of((1 << 16) + 1, (1 << 16) + 2, (1 << 16) + 3, (1 << 16) + 4)),
        Arguments.of( // disjoint array vs run
            ContiguousSet.create(Range.closed(3, 1 << 16), DiscreteDomain.integers()),
            ImmutableSet.of(1, 2)),
        Arguments.of( // run vs run, overlap with shift
            ContiguousSet.create(Range.closed(1, 1 << 8), DiscreteDomain.integers()),
            ContiguousSet.create(Range.closed(1 << 4, 1 << 12), DiscreteDomain.integers())),
        Arguments.of( // run vs run, subset
            ContiguousSet.create(Range.closed(1, 1 << 20), DiscreteDomain.integers()),
            ImmutableSet.of(1, 1 << 8)),
        Arguments.of( // run vs run, overlap with shift, 2 containers
            ContiguousSet.create(Range.closed(1, 1 << 20), DiscreteDomain.integers()),
            ImmutableSet.of(1 << 6, 1 << 26)),
        Arguments.of( // run vs 2 container run, overlap
            ImmutableSet.of(1, 1 << 16),
            ContiguousSet.create(Range.closed(0, 1 << 20), DiscreteDomain.integers())),
        Arguments.of( // bitmap vs intersecting array
            ImmutableSet.copyOf(
                Iterables.filter(
                    ContiguousSet.create(Range.closed(1, 1 << 15), DiscreteDomain.integers()),
                    DIVISIBLE_BY_4::test)),
            ImmutableSet.of(4, 8)),
        Arguments.of( // bitmap vs bitmap, cardinality mismatch
            ImmutableSet.copyOf(
                Iterables.filter(
                    ContiguousSet.create(Range.closed(1, 1 << 16), DiscreteDomain.integers()),
                    DIVISIBLE_BY_4::test)),
            ImmutableSet.copyOf(
                Iterables.filter(
                    ContiguousSet.create(Range.closed(1, 1 << 15), DiscreteDomain.integers()),
                    DIVISIBLE_BY_4::test))),
        Arguments.of( // bitmap vs empty
            ImmutableSet.copyOf(
                Iterables.filter(
                    ContiguousSet.create(Range.closed(1, 1 << 15), DiscreteDomain.integers()),
                    DIVISIBLE_BY_4::test)),
            ImmutableSet.of()),
        Arguments.of( // identical bitmaps
            ImmutableSet.copyOf(
                Iterables.filter(
                    ContiguousSet.create(Range.closed(1, 1 << 15), DiscreteDomain.integers()),
                    DIVISIBLE_BY_4::test)),
            ImmutableSet.copyOf(
                Iterables.filter(
                    ContiguousSet.create(Range.closed(1, 1 << 15), DiscreteDomain.integers()),
                    DIVISIBLE_BY_4::test))),
        Arguments.of( // bitmap vs overlapping but disjoint array
            ImmutableSet.of(3, 7),
            ImmutableSet.copyOf(
                Iterables.filter(
                    ContiguousSet.create(Range.closed(1, 1 << 15), DiscreteDomain.integers()),
                    DIVISIBLE_BY_4::test))),
        Arguments.of( // bitmap vs overlapping but disjoint bitmap
            ImmutableSet.copyOf(
                Iterables.filter(
                    ContiguousSet.create(Range.closed(1, 1 << 15), DiscreteDomain.integers()),
                    DIVISIBLE_BY_3::test)),
            ImmutableSet.copyOf(
                Iterables.filter(
                    ContiguousSet.create(Range.closed(1, 1 << 15), DiscreteDomain.integers()),
                    DIVISIBLE_BY_4::test))),
        Arguments.of( // disjoint, large (signed-negative) keys
            ImmutableSet.of(0xbf09001d, 0xbf090169), ImmutableSet.of(0x8088000e, 0x80880029)));
  }

  @ParameterizedTest(name = "assert that {1} is subset of {0}")
  @MethodSource("params")
  public void testProperSubset(Set<Integer> superSet, Set<Integer> subSet) {
    RoaringBitmap superSetRB = create(superSet);
    RoaringBitmap subSetRB = create(subSet);
    assertEquals(superSet.containsAll(subSet), superSetRB.contains(subSetRB));
    // reverse the test
    assertEquals(subSet.containsAll(superSet), subSetRB.contains(superSetRB));
  }

  private RoaringBitmap create(Set<Integer> set) {
    RoaringBitmap rb = new RoaringBitmap();
    if (set instanceof ContiguousSet) {
      ContiguousSet<Integer> contiguousSet = (ContiguousSet<Integer>) set;
      rb.add(contiguousSet.first().longValue(), contiguousSet.last().longValue());
    } else {
      for (Integer i : set) {
        rb.add(i);
      }
    }
    return rb;
  }
}
