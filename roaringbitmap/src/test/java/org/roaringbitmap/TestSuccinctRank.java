package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import java.util.Random;

/**
 * Tests for {@link SuccinctRank}.
 *
 * @author gerald.green
 * @since Dec-2024
 */
class TestSuccinctRank {

  @Test
  void emptyBitmap() {
    final RoaringBitmap rb = new RoaringBitmap();
    final SuccinctRank rank = SuccinctRank.build(rb);

    assertEquals(0L, rank.cardinality());
    assertEquals(0L, rank.rank(0));
    assertEquals(0L, rank.rank(100));
    assertEquals(0L, rank.rank(Integer.MAX_VALUE));
  }

  @Test
  void singleElement() {
    final RoaringBitmap rb = new RoaringBitmap();
    rb.add(42);
    final SuccinctRank rank = SuccinctRank.build(rb);

    assertEquals(1L, rank.cardinality());
    assertEquals(1L, rank.rank(42)); 
    assertEquals(0L, rank.rank(41)); 
    assertEquals(1L, rank.rank(43)); 
  }

  @Test
  void consecutiveElements() {
    final RoaringBitmap rb = new RoaringBitmap();
    rb.add(0L, 100L); 
    final SuccinctRank rank = SuccinctRank.build(rb);

    assertEquals(100L, rank.cardinality());
    assertEquals(1L, rank.rank(0)); 
    assertEquals(50L, rank.rank(49)); 
    assertEquals(100L, rank.rank(99)); 
    assertEquals(100L, rank.rank(100)); 
  }

  @Test
  void sparseElements() {
    final RoaringBitmap rb = new RoaringBitmap();
    rb.add(10);
    rb.add(100);
    rb.add(1000);
    rb.add(10000);
    final SuccinctRank rank = SuccinctRank.build(rb);

    assertEquals(4L, rank.cardinality());
    assertEquals(1L, rank.rank(10)); 
    assertEquals(2L, rank.rank(100)); 
    assertEquals(3L, rank.rank(1000)); 
    assertEquals(4L, rank.rank(10000)); 
    assertEquals(0L, rank.rank(9)); 
    assertEquals(1L, rank.rank(11)); 
  }

  @Test
  void multipleContainers() {
    final RoaringBitmap rb = new RoaringBitmap();
    // Container 0 (high key 0): values 0-99
    rb.add(0L, 100L);
    // Container 1 (high key 1): values 65536-65635
    rb.add(65536L, 65636L);
    // Container 2 (high key 2): values 131072-131171
    rb.add(131072L, 131172L);

    final SuccinctRank rank = SuccinctRank.build(rb);

    assertEquals(300L, rank.cardinality());

    // First container
    assertEquals(1L, rank.rank(0));
    assertEquals(100L, rank.rank(99));
    assertEquals(100L, rank.rank(100)); 

    // Second container
    assertEquals(101L, rank.rank(65536));
    assertEquals(200L, rank.rank(65635));
    assertEquals(200L, rank.rank(65636)); 

    // Third container
    assertEquals(201L, rank.rank(131072));
    assertEquals(300L, rank.rank(131171));
    assertEquals(300L, rank.rank(131172)); 
  }

  @Test
  void comprehensiveRankValidation() {
    final RoaringBitmap rb = new RoaringBitmap();

    // Add various patterns to create different container types
    // Dense region (will become BitmapContainer)
    for (int i = 0; i < 5000; i++) {
      rb.add(i);
    }
    // Sparse region (will be ArrayContainer)
    for (int i = 100000; i < 100100; i += 2) {
      rb.add(i);
    }
    // Run region
    rb.add(200000L, 201000L);

    final SuccinctRank fastRank = SuccinctRank.build(rb);

    // Verify all elements match RoaringBitmap.rankLong()
    for (final int x : rb) {
      final long actualRank = fastRank.rank(x);
      final long expectedRank = rb.rankLong(x);
      assertEquals(expectedRank, actualRank, "Expected rank " + expectedRank + " for value " + x);
    }
    assertEquals(rb.getLongCardinality(), fastRank.cardinality());
  }

  @Test
  void randomBitmapRankValidation() {
    final Random random = new Random(12345L);
    final RoaringBitmap rb = new RoaringBitmap();

    // Add 10000 random values
    for (int i = 0; i < 10000; i++) {
      rb.add(random.nextInt() & 0x7FFFFFFF); 
    }

    final SuccinctRank fastRank = SuccinctRank.build(rb);
    assertEquals(rb.getLongCardinality(), fastRank.cardinality());

    // Test all elements match RoaringBitmap.rankLong()
    for (final int x : rb) {
      final long actualRank = fastRank.rank(x);
      final long expectedRank = rb.rankLong(x);
      assertEquals(expectedRank, actualRank, "Expected rank " + expectedRank + " for value " + x);
    }

    // Test 1000 random queries (may or may not be in bitmap)
    for (int i = 0; i < 1000; i++) {
      final int x = random.nextInt() & 0x7FFFFFFF;
      final long result = fastRank.rank(x);
      final long expected = rb.rankLong(x);
      assertEquals(expected, result, "Rank mismatch for value " + x);
    }
  }

  @Test
  void highKeyBoundaries() {
    final RoaringBitmap rb = new RoaringBitmap();
    // Add elements at high key boundaries
    rb.add(0); 
    rb.add(65535); 
    rb.add(65536); 
    rb.add(131071); 
    rb.add(131072); 

    final SuccinctRank rank = SuccinctRank.build(rb);

    assertEquals(5L, rank.cardinality());
    assertEquals(1L, rank.rank(0));
    assertEquals(2L, rank.rank(65535));
    assertEquals(3L, rank.rank(65536));
    assertEquals(4L, rank.rank(131071));
    assertEquals(5L, rank.rank(131072));
  }

  @Test
  void largeHighKeys() {
    final RoaringBitmap rb = new RoaringBitmap();
    // Add values with large high keys (near max unsigned int)
    final int base = 0x7FFF0000; 
    rb.add(base);
    rb.add(base + 100);
    rb.add(base + 65536); 

    final SuccinctRank rank = SuccinctRank.build(rb);

    assertEquals(3L, rank.cardinality());
    assertEquals(1L, rank.rank(base));
    assertEquals(2L, rank.rank(base + 100));
    assertEquals(3L, rank.rank(base + 65536));
    assertEquals(0L, rank.rank(base - 1));
    assertEquals(1L, rank.rank(base + 1));
  }

  @Test
  void nullSourceThrowsException() {
    assertThrows(NullPointerException.class, () -> SuccinctRank.build(null));
  }

  @Test
  void snapshotReturnsCorrectBitmap() {
    final RoaringBitmap rb = new RoaringBitmap();
    rb.add(1);
    rb.add(100);
    rb.add(1000);

    final SuccinctRank rank = SuccinctRank.build(rb);
    final RoaringBitmap snapshot = rank.snapshot();

    assertNotNull(snapshot);
    assertEquals(3, snapshot.getCardinality());
    assertTrue(snapshot.contains(1));
    assertTrue(snapshot.contains(100));
    assertTrue(snapshot.contains(1000));
  }

  @Test
  void smallBitmapUsesLinearScan() {
    final RoaringBitmap rb = new RoaringBitmap();
    // Add elements in 5 containers (below threshold of 16)
    for (int i = 0; i < 5; i++) {
      rb.add(i * 65536);
    }

    final SuccinctRank rank = SuccinctRank.build(rb);
    assertTrue(rank.usesLinearScan());
    assertEquals(5, rank.containerCount());
  }

  @Test
  void largeBitmapUsesSuccinctStructure() {
    final RoaringBitmap rb = new RoaringBitmap();
    // Add elements in 20 containers (above threshold of 16)
    for (int i = 0; i < 20; i++) {
      rb.add(i * 65536);
    }

    final SuccinctRank rank = SuccinctRank.build(rb);
    assertFalse(rank.usesLinearScan());
    assertEquals(20, rank.containerCount());
  }

  @Test
  void manyContainersRankValidation() {
    final RoaringBitmap rb = new RoaringBitmap();

    // Create 100 containers with 10 elements each
    for (int container = 0; container < 100; container++) {
      final int base = container * 65536;
      for (int i = 0; i < 10; i++) {
        rb.add(base + i * 100);
      }
    }

    final SuccinctRank fastRank = SuccinctRank.build(rb);
    assertEquals(1000L, fastRank.cardinality());

    // Test various points across containers match RoaringBitmap.rankLong()
    for (int container = 0; container < 100; container++) {
      final int base = container * 65536;
      for (int i = 0; i < 10; i++) {
        final int x = base + i * 100;
        final long actualRank = fastRank.rank(x);
        final long expectedRank = rb.rankLong(x);
        assertEquals(
            expectedRank,
            actualRank,
            "Expected rank " + expectedRank + " at container " + container + " value " + x);
      }
    }
  }

  @Test
  void denseContainerWithBitmapContainer() {
    final RoaringBitmap rb = new RoaringBitmap();
    // Create a dense container (will be stored as BitmapContainer)
    rb.add(0L, 50000L);

    final SuccinctRank rank = SuccinctRank.build(rb);
    assertEquals(50000L, rank.cardinality());

    // Verify correctness at various points in the dense container
    assertEquals(1L, rank.rank(0));
    assertEquals(25000L, rank.rank(24999));
    assertEquals(50000L, rank.rank(49999));
    assertEquals(50000L, rank.rank(50000));
  }

  @Test
  void bitmapContainerWordBoundaries() {
    final RoaringBitmap rb = new RoaringBitmap();
    // Test word boundaries (every 64 bits) and block boundaries (every 256 bits)
    // Word 0: bits 0-63
    rb.add(0);
    rb.add(63);
    // Word 1: bits 64-127
    rb.add(64);
    rb.add(127);
    // Word 3: bits 192-255 (end of first 4-word block)
    rb.add(192);
    rb.add(255);
    // Word 4: bits 256-319 (start of second 4-word block)
    rb.add(256);
    rb.add(319);

    final SuccinctRank rank = SuccinctRank.build(rb);
    assertEquals(8L, rank.cardinality());

    assertEquals(1L, rank.rank(0));
    assertEquals(2L, rank.rank(63));
    assertEquals(3L, rank.rank(64));
    assertEquals(4L, rank.rank(127));
    assertEquals(5L, rank.rank(192));
    assertEquals(6L, rank.rank(255));
    assertEquals(7L, rank.rank(256));
    assertEquals(8L, rank.rank(319));
  }

  @Test
  void bitmapContainerAllWordPositions() {
    final RoaringBitmap rb = new RoaringBitmap();
    // Add one element in each of the first 8 words to test switch cases
    // This tests mod4 = 0, 1, 2, 3 for the switch statement
    for (int word = 0; word < 8; word++) {
      rb.add(word * 64 + 32); 
    }

    final SuccinctRank rank = SuccinctRank.build(rb);
    assertEquals(8L, rank.cardinality());

    for (int word = 0; word < 8; word++) {
      final int value = word * 64 + 32;
      assertEquals(
          (long) word + 1, rank.rank(value), "rank(" + value + ") should be " + (word + 1));
    }
  }

  @Test
  void mixedContainerTypes() {
    final RoaringBitmap rb = new RoaringBitmap();

    // ArrayContainer (sparse)
    for (int i = 0; i < 100; i++) {
      rb.add(i * 100);
    }

    // BitmapContainer (dense) in next container
    rb.add(65536L, 65536L + 50000);

    // ArrayContainer (sparse) in third container
    for (int i = 0; i < 50; i++) {
      rb.add(131072 + i * 200);
    }

    final SuccinctRank fastRank = SuccinctRank.build(rb);

    // Verify all elements match RoaringBitmap.rankLong()
    for (final int x : rb) {
      final long actualRank = fastRank.rank(x);
      final long expectedRank = rb.rankLong(x);
      assertEquals(expectedRank, actualRank, "Expected rank " + expectedRank + " for value " + x);
    }
  }

  @Test
  void containerCountReported() {
    final RoaringBitmap rb = new RoaringBitmap();
    rb.add(0);
    rb.add(65536);
    rb.add(131072);

    final SuccinctRank rank = SuccinctRank.build(rb);
    assertEquals(3, rank.containerCount());
  }

  @Test
  void runContainerSupport() {
    final RoaringBitmap rb = new RoaringBitmap();
    // Add a long run that will be stored as RunContainer
    rb.add(1000L, 10000L); 

    final SuccinctRank rank = SuccinctRank.build(rb);
    assertEquals(9000L, rank.cardinality());

    // Test values at the start, middle, and end of the run
    assertEquals(1L, rank.rank(1000));
    assertEquals(4501L, rank.rank(5500));
    assertEquals(9000L, rank.rank(9999));

    // Test values outside the run
    assertEquals(0L, rank.rank(999));
    assertEquals(9000L, rank.rank(10000));
  }

  @Test
  void multipleRunContainers() {
    final RoaringBitmap rb = new RoaringBitmap();
    // Add multiple runs in different containers
    rb.add(100L, 200L); 
    rb.add(65536L, 65736L); 
    rb.add(131072L, 131372L); 

    final SuccinctRank rank = SuccinctRank.build(rb);
    assertEquals(600L, rank.cardinality());

    // Test first run
    assertEquals(1L, rank.rank(100));
    assertEquals(100L, rank.rank(199));

    // Test second run
    assertEquals(101L, rank.rank(65536));
    assertEquals(300L, rank.rank(65735));

    // Test third run
    assertEquals(301L, rank.rank(131072));
    assertEquals(600L, rank.rank(131371));
  }

  @Test
  void runContainerWithGaps() {
    final RoaringBitmap rb = new RoaringBitmap();
    // Add runs with gaps to test proper handling
    rb.add(100L, 200L); 
    rb.add(300L, 400L); 
    rb.add(500L, 600L); 

    final SuccinctRank rank = SuccinctRank.build(rb);
    assertEquals(300L, rank.cardinality());

    // Test first run
    assertEquals(1L, rank.rank(100));
    assertEquals(100L, rank.rank(199));

    // Test gap
    assertEquals(100L, rank.rank(250));

    // Test second run
    assertEquals(101L, rank.rank(300));
    assertEquals(200L, rank.rank(399));

    // Test third run
    assertEquals(201L, rank.rank(500));
    assertEquals(300L, rank.rank(599));
  }

  @Test
  void mixedContainerTypesWithRuns() {
    final RoaringBitmap rb = new RoaringBitmap();

    // ArrayContainer (sparse) in container 0
    for (int i = 0; i < 100; i++) {
      rb.add(i * 100);
    }

    // BitmapContainer (dense) in container 1
    rb.add(65536L, 65536L + 50000);

    // RunContainer in container 2
    rb.add(131072L, 131072L + 5000);

    final SuccinctRank fastRank = SuccinctRank.build(rb);

    // Verify all elements match RoaringBitmap.rankLong()
    for (final int x : rb) {
      final long actualRank = fastRank.rank(x);
      final long expectedRank = rb.rankLong(x);
      assertEquals(expectedRank, actualRank, "Expected rank " + expectedRank + " for value " + x);
    }
  }

  @Test
  void runContainerEdgeCases() {
    final RoaringBitmap rb = new RoaringBitmap();
    // Single-element run (edge case)
    rb.add(1000);
    // Two-element run
    rb.add(2000L, 2002L);
    // Large run
    rb.add(65536L, 65536L + 10000);

    final SuccinctRank rank = SuccinctRank.build(rb);
    assertEquals(10003L, rank.cardinality());

    // Test single-element run
    assertEquals(1L, rank.rank(1000));
    assertEquals(1L, rank.rank(1001));

    // Test two-element run
    assertEquals(2L, rank.rank(2000));
    assertEquals(3L, rank.rank(2001));

    // Test large run boundaries
    assertEquals(4L, rank.rank(65536));
    assertEquals(10003L, rank.rank(65536 + 9999));
  }

  @Test
  void verifyImmutabilityWarning() {
    final RoaringBitmap rb = new RoaringBitmap();
    rb.add(1);
    rb.add(100);
    rb.add(1000);

    final SuccinctRank rank = SuccinctRank.build(rb);
    assertEquals(1L, rank.rank(1));
    assertEquals(2L, rank.rank(100));
    assertEquals(3L, rank.rank(1000));

    // The rank structure references the bitmap directly
    // If we modify the bitmap, rank queries will be incorrect
    final RoaringBitmap snapshot = rank.snapshot();
    assertTrue(snapshot.contains(1));
    assertTrue(snapshot.contains(100));
    assertTrue(snapshot.contains(1000));
  }

  @Test
  void smallToLargeTransition() {
    // Test the threshold between small and large implementations
    final RoaringBitmap small = new RoaringBitmap();
    // Add exactly 16 containers (at threshold)
    for (int i = 0; i < 16; i++) {
      small.add(i * 65536);
    }

    final SuccinctRank smallRank = SuccinctRank.build(small);
    assertTrue(smallRank.usesLinearScan());
    assertEquals(16, smallRank.containerCount());

    // Add one more container (above threshold)
    final RoaringBitmap large = small.clone();
    large.add(16 * 65536);

    final SuccinctRank largeRank = SuccinctRank.build(large);
    assertFalse(largeRank.usesLinearScan());
    assertEquals(17, largeRank.containerCount());

    // Verify correctness across the threshold
    for (int i = 0; i < 16; i++) {
      final int value = i * 65536;
      assertEquals((long) i + 1, smallRank.rank(value));
      assertEquals((long) i + 1, largeRank.rank(value));
    }
    assertEquals(17L, largeRank.rank(16 * 65536));
  }

  @Test
  void negativeValues() {
    final RoaringBitmap rb = new RoaringBitmap();
    // Add negative values - they're treated as large unsigned ints
    rb.add(-1); 
    rb.add(-100); 
    rb.add(-65536); 
    rb.add(0);
    rb.add(100);

    final SuccinctRank rank = SuccinctRank.build(rb);
    assertEquals(5L, rank.cardinality());

    // Verify rank queries for negative values work correctly
    assertEquals(1L, rank.rank(0));
    assertEquals(2L, rank.rank(100));
    assertEquals(3L, rank.rank(-65536));
    assertEquals(4L, rank.rank(-100));
    assertEquals(5L, rank.rank(-1));
  }

  @Test
  void integerBoundaries() {
    final RoaringBitmap rb = new RoaringBitmap();
    rb.add(Integer.MIN_VALUE);
    rb.add(Integer.MIN_VALUE + 1);
    rb.add(-1);
    rb.add(0);
    rb.add(1);
    rb.add(Integer.MAX_VALUE - 1);
    rb.add(Integer.MAX_VALUE);

    final SuccinctRank rank = SuccinctRank.build(rb);
    assertEquals(7L, rank.cardinality());

    assertEquals(1L, rank.rank(0));
    assertEquals(2L, rank.rank(1));
    assertEquals(3L, rank.rank(Integer.MAX_VALUE - 1));
    assertEquals(4L, rank.rank(Integer.MAX_VALUE));
    assertEquals(5L, rank.rank(Integer.MIN_VALUE));
    assertEquals(6L, rank.rank(Integer.MIN_VALUE + 1));
    assertEquals(7L, rank.rank(-1));

    // Query for values not in the bitmap
    assertEquals(2L, rank.rank(2));
    assertEquals(6L, rank.rank(Integer.MIN_VALUE + 2)); 
    assertEquals(2L, rank.rank(Integer.MAX_VALUE - 2)); 
  }

  @Test
  void fullContainer() {
    final RoaringBitmap rb = new RoaringBitmap();
    // Add all 65536 values in a single container (high key 0)
    rb.add(0L, 65536L);

    final SuccinctRank rank = SuccinctRank.build(rb);
    assertEquals(65536L, rank.cardinality());

    // Test various positions in the full container
    assertEquals(1L, rank.rank(0));
    assertEquals(2L, rank.rank(1));
    assertEquals(32768L, rank.rank(32767));
    assertEquals(32769L, rank.rank(32768));
    assertEquals(65536L, rank.rank(65535));

    // Test value in next container (not present)
    assertEquals(65536L, rank.rank(65536));
  }

  @Test
  void maxHighKey() {
    final RoaringBitmap rb = new RoaringBitmap();
    // High key 65535 is the maximum (represents values 0xFFFF0000 to 0xFFFFFFFF)
    final int base = -65536; 
    rb.add(base);
    rb.add(base + 1);
    rb.add(base + 100);
    rb.add(-1); 

    final SuccinctRank rank = SuccinctRank.build(rb);
    assertEquals(4L, rank.cardinality());

    assertEquals(1L, rank.rank(base));
    assertEquals(2L, rank.rank(base + 1));
    assertEquals(3L, rank.rank(base + 100));
    assertEquals(4L, rank.rank(-1));

    assertEquals(0L, rank.rank(base - 1));
    assertEquals(2L, rank.rank(base + 2));
  }

  @Test
  void valuesInGapsBetweenContainers() {
    final RoaringBitmap rb = new RoaringBitmap();
    // Add values in containers 0, 5, and 10
    rb.add(100); 
    rb.add(5 * 65536 + 200); 
    rb.add(10 * 65536 + 300); 

    final SuccinctRank rank = SuccinctRank.build(rb);
    assertEquals(3L, rank.cardinality());

    // Test values in gaps between containers
    assertEquals(1L, rank.rank(1 * 65536)); 
    assertEquals(1L, rank.rank(2 * 65536)); 
    assertEquals(1L, rank.rank(3 * 65536)); 
    assertEquals(1L, rank.rank(4 * 65536)); 
    assertEquals(2L, rank.rank(6 * 65536)); 
    assertEquals(2L, rank.rank(7 * 65536)); 
    assertEquals(2L, rank.rank(8 * 65536)); 
    assertEquals(2L, rank.rank(9 * 65536)); 

    // Verify present values
    assertEquals(1L, rank.rank(100));
    assertEquals(2L, rank.rank(5 * 65536 + 200));
    assertEquals(3L, rank.rank(10 * 65536 + 300));
  }

  @Test
  void exactlySeventeenContainers() {
    final RoaringBitmap rb = new RoaringBitmap();
    // Add exactly 17 containers (just above threshold)
    for (int i = 0; i < 17; i++) {
      rb.add(i * 65536 + i); 
    }

    final SuccinctRank rank = SuccinctRank.build(rb);
    assertFalse(rank.usesLinearScan());
    assertEquals(17, rank.containerCount());
    assertEquals(17L, rank.cardinality());

    // Verify all values have correct ranks
    for (int i = 0; i < 17; i++) {
      assertEquals((long) i + 1, rank.rank(i * 65536 + i));
    }
  }

  @Test
  void manyContainersStressTest() {
    final RoaringBitmap rb = new RoaringBitmap();
    // Add 1000 containers to test scalability
    for (int i = 0; i < 1000; i++) {
      rb.add(i * 65536);
    }

    final SuccinctRank rank = SuccinctRank.build(rb);
    assertFalse(rank.usesLinearScan());
    assertEquals(1000, rank.containerCount());
    assertEquals(1000L, rank.cardinality());

    // Spot check various positions
    assertEquals(1L, rank.rank(0));
    assertEquals(501L, rank.rank(500 * 65536));
    assertEquals(1000L, rank.rank(999 * 65536));

    // Verify gaps
    assertEquals(1L, rank.rank(1));
    assertEquals(501L, rank.rank(500 * 65536 + 1));
  }

  @Test
  void multipleConsecutiveQueries() {
    final RoaringBitmap rb = new RoaringBitmap();
    rb.add(1);
    rb.add(100);
    rb.add(1000);

    final SuccinctRank rank = SuccinctRank.build(rb);

    // Query the same values multiple times to ensure statelessness
    for (int i = 0; i < 10; i++) {
      assertEquals(1L, rank.rank(1));
      assertEquals(2L, rank.rank(100));
      assertEquals(3L, rank.rank(1000));
      assertEquals(1L, rank.rank(2));
    }
  }

  @Test
  void sparseBitmapContainer() {
    final RoaringBitmap rb = new RoaringBitmap();
    // Create a BitmapContainer with only first and last bit set
    rb.add(0);
    rb.add(65535);
    // Force it to be a BitmapContainer by adding and removing many elements
    for (int i = 1; i < 5000; i++) {
      rb.add(i);
    }
    for (int i = 1; i < 5000; i++) {
      rb.remove(i);
    }

    final SuccinctRank rank = SuccinctRank.build(rb);
    assertEquals(2L, rank.cardinality());

    assertEquals(1L, rank.rank(0));
    assertEquals(2L, rank.rank(65535));
    assertEquals(1L, rank.rank(1));
    assertEquals(1L, rank.rank(65534));
  }

  @Test
  void alternatingBitsPattern() {
    final RoaringBitmap rb = new RoaringBitmap();
    // Add alternating bits to test word boundary handling
    for (int i = 0; i < 65536; i += 2) {
      rb.add(i);
    }

    final SuccinctRank rank = SuccinctRank.build(rb);
    assertEquals(32768L, rank.cardinality());

    // Test various positions
    assertEquals(1L, rank.rank(0));
    assertEquals(2L, rank.rank(2));
    assertEquals(3L, rank.rank(4));
    assertEquals(16385L, rank.rank(32768));
    assertEquals(32768L, rank.rank(65534));

    // Test odd values (not present)
    assertEquals(1L, rank.rank(1));
    assertEquals(2L, rank.rank(3));
    assertEquals(32768L, rank.rank(65535));
  }

  @Test
  void containerAtCardinalityThreshold() {
    final RoaringBitmap rb = new RoaringBitmap();
    // Add exactly 4096 elements (typical ArrayContainer to BitmapContainer threshold)
    for (int i = 0; i < 4096; i++) {
      rb.add(i);
    }

    final SuccinctRank rank = SuccinctRank.build(rb);
    assertEquals(4096L, rank.cardinality());

    assertEquals(1L, rank.rank(0));
    assertEquals(2048L, rank.rank(2047));
    assertEquals(4096L, rank.rank(4095));
    assertEquals(4096L, rank.rank(4096));
  }

  @Test
  void runContainerSpanningEntireContainer() {
    final RoaringBitmap rb = new RoaringBitmap();
    // Add all 65536 values as a run
    rb.add(0L, 65536L);

    final SuccinctRank rank = SuccinctRank.build(rb);
    assertEquals(65536L, rank.cardinality());

    // Test boundaries
    assertEquals(1L, rank.rank(0));
    assertEquals(32769L, rank.rank(32768));
    assertEquals(65536L, rank.rank(65535));

    // Value in next container
    assertEquals(65536L, rank.rank(65536));
  }

  @Test
  void queryValueGreaterThanAllValues() {
    final RoaringBitmap rb = new RoaringBitmap();
    rb.add(0);
    rb.add(100);
    rb.add(1000);

    final SuccinctRank rank = SuccinctRank.build(rb);

    // Query values greater than all values in bitmap
    assertEquals(3L, rank.rank(1001));
    assertEquals(3L, rank.rank(10000));
    assertEquals(3L, rank.rank(1000000));
    assertEquals(3L, rank.rank(Integer.MAX_VALUE));
  }

  @Test
  void queryValueLessThanAllValues() {
    final RoaringBitmap rb = new RoaringBitmap();
    rb.add(1000);
    rb.add(2000);
    rb.add(3000);

    final SuccinctRank rank = SuccinctRank.build(rb);

    // Query values less than all values in bitmap
    assertEquals(0L, rank.rank(0));
    assertEquals(0L, rank.rank(100));
    assertEquals(0L, rank.rank(999));
  }

  @Test
  void highKeyZeroExplicit() {
    final RoaringBitmap rb = new RoaringBitmap();
    // Explicitly test container at high key 0
    rb.add(0); 
    rb.add(1); 
    rb.add(65535); 

    final SuccinctRank rank = SuccinctRank.build(rb);
    assertEquals(3L, rank.cardinality());

    assertEquals(1L, rank.rank(0));
    assertEquals(2L, rank.rank(1));
    assertEquals(3L, rank.rank(65535));

    // Next container
    assertEquals(3L, rank.rank(65536));
  }

  @Test
  void mixedQueriesOnLargeBitmap() {
    final RoaringBitmap rb = new RoaringBitmap();
    // Create large bitmap with 50 containers, various patterns
    for (int container = 0; container < 50; container++) {
      final int base = container * 65536;
      if (container % 3 == 0) {
        // Dense container
        rb.add((long) base, (long) base + 10000);
      } else if (container % 3 == 1) {
        // Sparse container
        for (int i = 0; i < 100; i++) {
          rb.add(base + i * 100);
        }
      } else {
        // Run container
        rb.add((long) base, (long) base + 5000);
      }
    }

    final SuccinctRank rank = SuccinctRank.build(rb);
    assertFalse(rank.usesLinearScan());

    // Verify all elements match RoaringBitmap.rankLong()
    for (final int x : rb) {
      final long actualRank = rank.rank(x);
      final long expectedRank = rb.rankLong(x);
      assertEquals(expectedRank, actualRank, "Expected rank " + expectedRank + " for value " + x);
    }

    assertEquals(rb.getLongCardinality(), rank.cardinality());
  }
}
