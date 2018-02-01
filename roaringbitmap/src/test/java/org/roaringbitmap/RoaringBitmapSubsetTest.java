package org.roaringbitmap;

import com.google.common.base.Predicate;
import com.google.common.collect.*;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Set;

@RunWith(Parameterized.class)
public class RoaringBitmapSubsetTest {


  private static final Predicate<Integer> DIVISIBLE_BY_4 = i -> i % 4 == 0;

  private static final Predicate<Integer> DIVISIBLE_BY_3 = i -> i % 3 == 0;

  @Parameterized.Parameters(name = "assert that {1} is subset of {0}")
  public static Object[][] params() {
    return new Object[][]
            {
                    { // array vs array
                            ImmutableSet.of(1, 2, 3, 4),
                            ImmutableSet.of(2, 3)
                    },
                    { // array vs empty
                            ImmutableSet.of(1, 2, 3, 4),
                            ImmutableSet.of()
                    },
                    { // identical arrays
                            ImmutableSet.of(1, 2, 3, 4),
                            ImmutableSet.of(1, 2, 3, 4)
                    },
                    { // disjoint arrays
                            ImmutableSet.of(10, 12, 14, 15),
                            ImmutableSet.of(1, 2, 3, 4)
                    },
                    { // disjoint arrays, cardinality mismatch
                            ImmutableSet.of(10, 12, 14),
                            ImmutableSet.of(1, 2, 3, 4)
                    },
                    { // run vs array, subset
                            ContiguousSet.create(Range.closed(1, 1 << 8), DiscreteDomain.integers()),
                            ImmutableSet.of(1, 2, 3, 4)
                    },
                    { // run vs array, subset
                            ContiguousSet.create(Range.closed(1, 1 << 16), DiscreteDomain.integers()),
                            ImmutableSet.of(1, 2, 3, 4)
                    },
                    { // run vs empty
                            ContiguousSet.create(Range.closed(1, 1 << 16), DiscreteDomain.integers()),
                            ImmutableSet.of()
                    },
                    { // identical runs, 1 container
                            ContiguousSet.create(Range.closed(1, 1 << 16), DiscreteDomain.integers()),
                            ContiguousSet.create(Range.closed(1, 1 << 16), DiscreteDomain.integers())
                    },
                    { // identical runs, 2 containers
                            ContiguousSet.create(Range.closed(1, 1 << 20), DiscreteDomain.integers()),
                            ContiguousSet.create(Range.closed(1, 1 << 20), DiscreteDomain.integers())
                    },
                    { // disjoint array vs run, either side of container boundary
                            ContiguousSet.create(Range.closed(1, 1 << 16), DiscreteDomain.integers()),
                            ImmutableSet.of((1 << 16) + 1, (1 << 16) + 2, (1 << 16) + 3, (1 << 16) + 4)
                    },
                    { // disjoint array vs run
                            ContiguousSet.create(Range.closed(3, 1 << 16), DiscreteDomain.integers()),
                            ImmutableSet.of(1, 2)
                    },
                    { // run vs run, overlap with shift
                            ContiguousSet.create(Range.closed(1, 1 << 8), DiscreteDomain.integers()),
                            ContiguousSet.create(Range.closed(1 << 4, 1 << 12), DiscreteDomain.integers())
                    },
                    { // run vs run, subset
                            ContiguousSet.create(Range.closed(1, 1 << 20), DiscreteDomain.integers()),
                            ImmutableSet.of(1, 1 << 8)
                    },
                    { // run vs run, overlap with shift, 2 containers
                            ContiguousSet.create(Range.closed(1, 1 << 20), DiscreteDomain.integers()),
                            ImmutableSet.of(1 << 6, 1 << 26)
                    },
                    { // run vs 2 container run, overlap
                            ImmutableSet.of(1, 1 << 16),
                            ContiguousSet.create(Range.closed(0, 1 << 20), DiscreteDomain.integers())
                    },
                    { // bitmap vs intersecting array
                            ImmutableSet.copyOf(Iterables.filter(ContiguousSet.create(Range.closed(1, 1 << 15),
                                                                                      DiscreteDomain.integers()),
                                                                 DIVISIBLE_BY_4)),
                            ImmutableSet.of(4, 8)
                    },
                    { // bitmap vs bitmap, cardinality mismatch
                            ImmutableSet.copyOf(Iterables.filter(ContiguousSet.create(Range.closed(1, 1 << 16),
                                                                                      DiscreteDomain.integers()),
                                                                 DIVISIBLE_BY_4)),
                            ImmutableSet.copyOf(Iterables.filter(ContiguousSet.create(Range.closed(1, 1 << 15),
                                                                                      DiscreteDomain.integers()),
                                                                 DIVISIBLE_BY_4))
                    },
                    { // bitmap vs empty
                            ImmutableSet.copyOf(Iterables.filter(ContiguousSet.create(Range.closed(1, 1 << 15),
                                                                                      DiscreteDomain.integers()),
                                                                 DIVISIBLE_BY_4)),
                            ImmutableSet.of()
                    },
                    { // identical bitmaps
                            ImmutableSet.copyOf(Iterables.filter(ContiguousSet.create(Range.closed(1, 1 << 15),
                                                                                      DiscreteDomain.integers()),
                                                                 DIVISIBLE_BY_4)),
                            ImmutableSet.copyOf(Iterables.filter(ContiguousSet.create(Range.closed(1, 1 << 15),
                                                                                      DiscreteDomain.integers()),
                                                                 DIVISIBLE_BY_4))
                    },
                    { // bitmap vs overlapping but disjoint array
                            ImmutableSet.of(3, 7),
                            ImmutableSet.copyOf(Iterables.filter(ContiguousSet.create(Range.closed(1, 1 << 15),
                                                                                      DiscreteDomain.integers()),
                                                                 DIVISIBLE_BY_4))
                    },
                    { // bitmap vs overlapping but disjoint bitmap
                      ImmutableSet.copyOf(Iterables.filter(ContiguousSet.create(Range.closed(1, 1 << 15),
                                                                                DiscreteDomain.integers()),
                                                           DIVISIBLE_BY_3)),
                      ImmutableSet.copyOf(Iterables.filter(ContiguousSet.create(Range.closed(1, 1 << 15),
                                                                                DiscreteDomain.integers()),
                                                           DIVISIBLE_BY_4))
                    },
                    { // disjoint, large (signed-negative) keys
                            ImmutableSet.of(0xbf09001d,0xbf090169),
                            ImmutableSet.of(0x8088000e,0x80880029)
                    }
            };
  }

  private final Set<Integer> superSet;
  private final Set<Integer> subSet;

  public RoaringBitmapSubsetTest(Set<Integer> superSet, Set<Integer> subSet) {
    this.superSet = superSet;
    this.subSet = subSet;
  }


  @Test
  public void testProperSubset() {
    RoaringBitmap superSetRB = create(superSet);
    RoaringBitmap subSetRB = create(subSet);
    Assert.assertEquals(superSet.containsAll(subSet), superSetRB.contains(subSetRB));
    // reverse the test
    Assert.assertEquals(subSet.containsAll(superSet), subSetRB.contains(superSetRB));
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
