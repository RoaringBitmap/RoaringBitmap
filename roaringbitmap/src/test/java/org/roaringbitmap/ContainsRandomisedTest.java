package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MappeableContainer;
import org.roaringbitmap.buffer.MappeableContainerPointer;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.BitSet;
import java.util.Random;
import java.util.TreeMap;

/**
 * Checks point membership queries against a reference model, for every bitmap flavour and for
 * array, bitmap and run containers of many sizes. Runs are probed at and around their edges.
 */
public class ContainsRandomisedTest {

  /** Builds a reference model: high 16 bits to the set of low 16 bits. */
  private static TreeMap<Integer, BitSet> randomModel(Random r) {
    TreeMap<Integer, BitSet> model = new TreeMap<>();
    int nkeys = 1 + r.nextInt(r.nextBoolean() ? 8 : 300);
    while (model.size() < nkeys) {
      int key;
      switch (r.nextInt(4)) {
        case 0:
          key = 0;
          break;
        case 1:
          key = 0xFFFF;
          break;
        default:
          key = r.nextInt(1 << 16);
      }
      model.put(key, randomContainer(r));
    }
    return model;
  }

  private static BitSet randomContainer(Random r) {
    BitSet bs = new BitSet(1 << 16);
    switch (r.nextInt(3)) {
      case 0: // array container
        {
          int card = 1 + r.nextInt(r.nextBoolean() ? 40 : 4000);
          if (r.nextBoolean()) {
            bs.set(0);
          }
          if (r.nextBoolean()) {
            bs.set(0xFFFF);
          }
          while (bs.cardinality() < card) {
            bs.set(r.nextInt(1 << 16));
          }
          break;
        }
      case 1: // bitmap container
        for (int i = 0; i < (1 << 16); i++) {
          if (r.nextBoolean()) {
            bs.set(i);
          }
        }
        break;
      default: // run container
        {
          int nruns = 1 + r.nextInt(r.nextBoolean() ? 40 : 2000);
          int maxStride = (1 << 16) / nruns;
          int pos = r.nextBoolean() ? 0 : r.nextInt(Math.max(1, maxStride / 2));
          for (int i = 0; i < nruns && pos < (1 << 16); i++) {
            int len = 1 + r.nextInt(Math.max(1, maxStride / 2));
            int end = Math.min(1 << 16, pos + len);
            bs.set(pos, end);
            pos = end + 1 + r.nextInt(Math.max(1, maxStride / 2));
          }
          if (r.nextInt(4) == 0) {
            bs.set(0xFFF0, 1 << 16); // run reaching the end of the container
          }
        }
    }
    return bs;
  }

  /** Values to probe: every run edge and its neighbours, plus random values. */
  private static int[] probes(Random r, TreeMap<Integer, BitSet> model) {
    java.util.ArrayList<Integer> list = new java.util.ArrayList<>();
    for (java.util.Map.Entry<Integer, BitSet> e : model.entrySet()) {
      int high = e.getKey() << 16;
      BitSet bs = e.getValue();
      for (int i = bs.nextSetBit(0); i >= 0; ) {
        int end = bs.nextClearBit(i); // run is [i, end)
        for (int d = -1; d <= 1; d++) {
          if (i + d >= 0 && i + d < (1 << 16)) {
            list.add(high | (i + d));
          }
          if (end - 1 + d >= 0 && end - 1 + d < (1 << 16)) {
            list.add(high | (end - 1 + d));
          }
        }
        i = end < (1 << 16) ? bs.nextSetBit(end) : -1;
      }
      list.add(high);
      list.add(high | 0xFFFF);
      // neighbouring keys, which may be absent
      list.add(high - (1 << 16));
      list.add(high + (1 << 16));
    }
    for (int i = 0; i < 20000; i++) {
      list.add(r.nextInt());
    }
    int[] answer = new int[list.size()];
    for (int i = 0; i < answer.length; i++) {
      answer[i] = list.get(i);
    }
    return answer;
  }

  private static boolean expected(TreeMap<Integer, BitSet> model, int x) {
    BitSet bs = model.get(x >>> 16);
    return bs != null && bs.get(x & 0xFFFF);
  }

  private static ByteBuffer serialize(RoaringBitmap rb, boolean direct) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    rb.serialize(new DataOutputStream(bos));
    byte[] bytes = bos.toByteArray();
    // put the data at a non-zero offset to catch position bugs
    ByteBuffer bb =
        direct
            ? ByteBuffer.allocateDirect(bytes.length + 3)
            : ByteBuffer.allocate(bytes.length + 3);
    bb.position(3);
    bb.put(bytes);
    bb.position(3);
    return bb.slice().order(ByteOrder.LITTLE_ENDIAN);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
  public void containsMatchesModel(int seed) throws IOException {
    Random r = new Random(seed);
    TreeMap<Integer, BitSet> model = randomModel(r);
    RoaringBitmap rb = new RoaringBitmap();
    for (java.util.Map.Entry<Integer, BitSet> e : model.entrySet()) {
      int high = e.getKey() << 16;
      BitSet bs = e.getValue();
      for (int i = bs.nextSetBit(0); i >= 0; ) {
        int end = bs.nextClearBit(i);
        rb.add((long) (high | i) & 0xFFFFFFFFL, ((long) (high | i) & 0xFFFFFFFFL) + (end - i));
        i = end < (1 << 16) ? bs.nextSetBit(end) : -1;
      }
    }
    rb.runOptimize();

    MutableRoaringBitmap mutable = rb.toMutableRoaringBitmap();
    ImmutableRoaringBitmap heap = new ImmutableRoaringBitmap(serialize(rb, false));
    ImmutableRoaringBitmap direct = new ImmutableRoaringBitmap(serialize(rb, true));
    Roaring64Bitmap r64 = new Roaring64Bitmap();
    Roaring64NavigableMap nav = new Roaring64NavigableMap();
    long offset = 7L << 32;
    rb.forEach(
        (IntConsumer)
            x -> {
              r64.addLong(offset + Integer.toUnsignedLong(x));
              nav.addLong(offset + Integer.toUnsignedLong(x));
            });
    r64.runOptimize();
    nav.runOptimize();

    for (int x : probes(r, model)) {
      boolean exp = expected(model, x);
      assertEquals(exp, rb.contains(x), "RoaringBitmap " + x);
      assertEquals(exp, mutable.contains(x), "MutableRoaringBitmap " + x);
      assertEquals(exp, heap.contains(x), "ImmutableRoaringBitmap (heap) " + x);
      assertEquals(exp, direct.contains(x), "ImmutableRoaringBitmap (direct) " + x);
      long lx = offset + Integer.toUnsignedLong(x);
      assertEquals(exp, r64.contains(lx), "Roaring64Bitmap " + x);
      assertEquals(exp, nav.contains(lx), "Roaring64NavigableMap " + x);
      assertEquals(false, r64.contains(Integer.toUnsignedLong(x)), "Roaring64Bitmap low " + x);
    }

    // Containers that are views over the mapped ByteBuffer (not array-backed).
    for (ImmutableRoaringBitmap ib : new ImmutableRoaringBitmap[] {heap, direct}) {
      MappeableContainerPointer cp = ib.getContainerPointer();
      while (cp.getContainer() != null) {
        MappeableContainer c = cp.getContainer();
        BitSet bs = model.get((int) cp.key());
        for (int low = 0; low < (1 << 16); low++) {
          assertEquals(bs.get(low), c.contains((char) low), "mapped container " + low);
        }
        cp.advance();
      }
    }
  }
}
