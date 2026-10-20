package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.roaringbitmap.PersistentRoaringBitmapChecks.UNIVERSE;
import static org.roaringbitmap.PersistentRoaringBitmapChecks.assertOwnership;
import static org.roaringbitmap.PersistentRoaringBitmapChecks.assertValues;

import org.roaringbitmap.PersistentRoaringBitmapChecks.Model;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;

/**
 * A seeded random walk over a pool of bitmaps that share containers, each paired with an
 * independent model of its values. After every step, every bitmap must hold its model's values in
 * normal form, and no container may be held twice without being shared at each holder.
 *
 * <p>Run longer walks with {@code -Dpersistent.walk.steps=100000 -Dpersistent.walk.seeds=50}.
 */
public class PersistentRoaringBitmapModelTest {

  private static final int STEPS = Integer.getInteger("persistent.walk.steps", 1_000);
  private static final int SEEDS = Integer.getInteger("persistent.walk.seeds", 16);

  /** Few keys, so that writes regularly meet shared containers, on both sides of 2^31. */
  private static final int[] KEYS = {0, 1, 2, 3, 4, 0x7FFF, 0x8000, 0x8001, 0xFFFF};

  @Test
  public void randomWalk() {
    for (int seed = 0; seed < SEEDS; seed++) {
      new Walk(seed).run(STEPS);
    }
  }

  private static final class Walk {
    private final SplittableRandom random;
    private final long seed;
    private final List<PersistentRoaringBitmap> pool = new ArrayList<>();
    private final List<Model> models = new ArrayList<>();
    private int writes;
    private int sharedWrites;

    Walk(long seed) {
      this.seed = seed;
      this.random = new SplittableRandom(seed);
      int size = 8 + random.nextInt(9);
      for (int i = 0; i < size; i++) {
        pool.add(new PersistentRoaringBitmap());
        models.add(Model.EMPTY);
      }
    }

    void run(int steps) {
      for (int step = 0; step < steps; step++) {
        String operation = step();
        String context = "seed " + seed + ", step " + step + ": " + operation;
        for (int i = 0; i < pool.size(); i++) {
          assertValues(models.get(i), pool.get(i), context + ", bitmap " + i);
        }
        assertOwnership(pool);
        if (step % 50 == 0) {
          for (PersistentRoaringBitmap bitmap : pool) {
            PersistentRoaringBitmap copy =
                PersistentRoaringBitmap.fromBitmap(bitmap.toRoaringBitmap());
            copy.runOptimize();
            assertTrue(copy.validate(), context);
            // equality and hash codes depend neither on the encodings nor on sharing
            assertEquals(bitmap, copy, context);
            assertEquals(bitmap.hashCode(), copy.hashCode(), context);
          }
        }
      }
      // the walk must keep exercising copy-on-write
      assertTrue(
          sharedWrites > writes / 20, "seed " + seed + ": " + sharedWrites + " of " + writes);
    }

    /** Counts writes to a key range that meet a shared container. */
    private void count(PersistentRoaringBitmap x, long start, long end) {
      writes++;
      RoaringArray array = PersistentRoaringBitmapChecks.containers(x);
      for (int slot = 0; slot < array.size; slot++) {
        long key = array.keys[slot];
        boolean inRange = key >= start >>> 16 && key <= (end - 1) >>> 16;
        if (inRange && PersistentRoaringBitmapChecks.isShared(x, slot)) {
          sharedWrites++;
          return;
        }
      }
    }

    private int pick() {
      return random.nextInt(pool.size());
    }

    private long base() {
      return (long) KEYS[random.nextInt(KEYS.length)] << 16;
    }

    /** A value, biased towards the bounds of its block and of the existing runs. */
    private long value() {
      switch (random.nextInt(4)) {
        case 0:
          return base() + random.nextInt(65_536);
        case 1:
          return base() + (random.nextBoolean() ? random.nextInt(4) : 65_535 - random.nextInt(4));
        case 2:
          return base() + random.nextInt(300);
        default:
          PersistentRoaringBitmap source = pool.get(pick());
          if (source.isEmpty()) {
            return base();
          }
          int rank = (int) random.nextLong(source.getLongCardinality());
          long present = Integer.toUnsignedLong(source.select(rank));
          return Math.max(0, Math.min(UNIVERSE - 1, present + random.nextInt(3) - 1));
      }
    }

    /**
     * A range: within a block, across a few blocks, covering whole blocks, or reaching 2^32. Ranges
     * span at most a few blocks, so that the checks after each step stay fast.
     */
    private long[] range() {
      long start;
      long end;
      switch (random.nextInt(5)) {
        case 0:
          start = value();
          end = Math.min(UNIVERSE, start + 1 + random.nextInt(2_000));
          break;
        case 1:
          start = base();
          end = Math.min(UNIVERSE, start + 65_536 * (1 + random.nextInt(2)));
          break;
        case 2:
          start = value();
          end = Math.min(UNIVERSE, start + 1 + random.nextInt(3 * 65_536));
          break;
        case 3:
          start = (0xFFFFL << 16) + random.nextInt(65_536);
          end = UNIVERSE;
          break;
        default:
          start = base() + random.nextInt(65_536);
          end = Math.min(UNIVERSE, start + 65_536 + random.nextInt(140_000));
      }
      return new long[] {Math.min(start, end), Math.max(start, end)};
    }

    private void set(int i, PersistentRoaringBitmap bitmap, Model model) {
      pool.set(i, bitmap);
      models.set(i, model);
    }

    private String step() {
      int i = pick();
      int j = pick();
      int k = pick();
      PersistentRoaringBitmap x = pool.get(i);
      Model model = models.get(i);
      int choice = random.nextInt(100);
      if (choice < 20) {
        long value = value();
        count(x, value, value + 1);
        x.add((int) value);
        models.set(i, model.add(value));
        return "add(" + value + ") to " + i;
      } else if (choice < 35) {
        long value = value();
        count(x, value, value + 1);
        x.remove((int) value);
        models.set(i, model.remove(value));
        return "remove(" + value + ") from " + i;
      } else if (choice < 45) {
        long[] r = range();
        count(x, r[0], r[1]);
        x.add(r[0], r[1]);
        models.set(i, model.or(Model.range(r[0], r[1])));
        return "add(" + r[0] + ", " + r[1] + ") to " + i;
      } else if (choice < 55) {
        long[] r = range();
        count(x, r[0], r[1]);
        x.remove(r[0], r[1]);
        models.set(i, model.andNot(Model.range(r[0], r[1])));
        return "remove(" + r[0] + ", " + r[1] + ") from " + i;
      } else if (choice < 63) {
        set(j, x.clone(), model);
        return "clone " + i + " into " + j;
      } else if (choice < 83) {
        PersistentRoaringBitmap y = pool.get(j);
        Model other = models.get(j);
        switch (random.nextInt(4)) {
          case 0:
            set(k, PersistentRoaringBitmap.or(x, y), model.or(other));
            return "or(" + i + ", " + j + ") into " + k;
          case 1:
            set(k, PersistentRoaringBitmap.and(x, y), model.and(other));
            return "and(" + i + ", " + j + ") into " + k;
          case 2:
            set(k, PersistentRoaringBitmap.xor(x, y), model.xor(other));
            return "xor(" + i + ", " + j + ") into " + k;
          default:
            set(k, PersistentRoaringBitmap.andNot(x, y), model.andNot(other));
            return "andNot(" + i + ", " + j + ") into " + k;
        }
      } else if (choice < 88) {
        long cardinality = model.cardinality();
        int n = (int) Math.min(Integer.MAX_VALUE, random.nextLong(cardinality + 2));
        set(j, x.limit(n), model.limit(n));
        return "limit(" + n + ") of " + i + " into " + j;
      } else if (choice < 92) {
        x.runOptimize();
        return "runOptimize " + i;
      } else if (choice < 95) {
        x.trim();
        return "trim " + i;
      } else if (choice < 97) {
        PersistentRoaringBitmap source = pool.get(j);
        ByteBuffer bytes = ByteBuffer.allocate(source.serializedSizeInBytes());
        source.serialize(bytes);
        bytes.flip();
        try {
          x.deserialize(bytes);
        } catch (IOException e) {
          throw new AssertionError(e);
        }
        models.set(i, models.get(j));
        return "deserialize " + j + " into " + i;
      } else if (choice < 99) {
        set(j, PersistentRoaringBitmap.fromBitmap(x.toRoaringBitmap()), model);
        return "copy " + i + " into " + j + " through RoaringBitmap";
      }
      x.clear();
      models.set(i, Model.EMPTY);
      return "clear " + i;
    }
  }
}
