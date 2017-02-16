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


  private static final Predicate<Integer> DIVISIBLE_BY_4 = new Predicate<Integer>() {
    @Override
    public boolean apply(Integer i) {
      return i % 4 == 0;
    }
  };

  @Parameterized.Parameters(name = "assert that {1} is subset of {0}")
  public static Object[][] params() {
    return new Object[][]
            {
                    {
                            ImmutableSet.of(1, 2, 3, 4),
                            ImmutableSet.of(2, 3)
                    },
                    {
                            ImmutableSet.of(1, 2, 3, 4),
                            ImmutableSet.of(1, 2, 3, 4)
                    },
                    {
                            ImmutableSet.of(2, 3),
                            ImmutableSet.of(1, 2, 3, 4)
                    },
                    {
                            ImmutableSet.of(10, 12, 14, 15),
                            ImmutableSet.of(1, 2, 3, 4)
                    },
                    {
                            ImmutableSet.of(10, 12, 14),
                            ImmutableSet.of(1, 2, 3, 4)
                    },
                    {
                            ContiguousSet.create(Range.closed(1, 1 << 8), DiscreteDomain.integers()),
                            ImmutableSet.of(1, 2, 3, 4)
                    },
                    {
                            ContiguousSet.create(Range.closed(1, 1 << 16), DiscreteDomain.integers()),
                            ImmutableSet.of(1, 2, 3, 4)
                    },
                    {
                            ContiguousSet.create(Range.closed(1, 1 << 16), DiscreteDomain.integers()),
                            ImmutableSet.of((1 << 16) + 1, (1 << 16) + 2, (1 << 16) + 3, (1 << 16) + 4)
                    },
                    {
                            ContiguousSet.create(Range.closed(1, 1 << 8), DiscreteDomain.integers()),
                            ContiguousSet.create(Range.closed(1 << 4, 1 << 12), DiscreteDomain.integers())
                    },
                    {
                            ContiguousSet.create(Range.closed(1, 1 << 20), DiscreteDomain.integers()),
                            ImmutableSet.of(1, 1 << 8)
                    },
                    {
                            ContiguousSet.create(Range.closed(1, 1 << 20), DiscreteDomain.integers()),
                            ImmutableSet.of(1, 1 << 16)
                    },
                    {
                            ImmutableSet.of(1, 1 << 16),
                            ContiguousSet.create(Range.closed(0, 1 << 20), DiscreteDomain.integers())
                    },
                    {
                            ImmutableSet.copyOf(Iterables.filter(ContiguousSet.create(Range.closed(1, 1 << 18),
                                                                                      DiscreteDomain.integers()),
                                                                 DIVISIBLE_BY_4)),
                            ImmutableSet.of(4, 8)
                    },
                    {
                            ImmutableSet.of(4, 8),
                            ImmutableSet.copyOf(Iterables.filter(ContiguousSet.create(Range.closed(1, 1 << 18),
                                                                                      DiscreteDomain.integers()),
                                                                 DIVISIBLE_BY_4))
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
    boolean expectation = superSet.containsAll(subSet);
    RoaringBitmap superSetRB = create(superSet);
    RoaringBitmap subSetRB = create(subSet);
    Assert.assertEquals(expectation, superSetRB.contains(subSetRB));
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
