/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;

/**
 * A 32-bit Roaring bitmap whose versions share containers: a <a
 * href="https://en.wikipedia.org/wiki/Persistent_data_structure">persistent data structure</a>.
 * Every version of a bitmap that a caller still holds stays valid and unchanged, whatever later
 * happens to other versions that share its internals. "Persistent" does not mean "stored on disk":
 * there is no new format, and {@link #serialize(DataOutput)} writes exactly the bytes that {@link
 * RoaringBitmap#serialize(DataOutput)} writes for the same containers.
 *
 * <pre>{@code
 * PersistentRoaringBitmap original = PersistentRoaringBitmap.bitmapOfRange(0, 1_000_000);
 * PersistentRoaringBitmap copy = original.clone(); // copies no container
 * copy.remove(5); // copies the container of 5 first: original still holds 5
 * }</pre>
 *
 * <p><b>Sharing.</b> {@link #clone()} puts every container into the copy instead of copying it.
 * A container that more than one bitmap may hold is copied just before the first write to it
 * (copy-on-write), so a write to one bitmap is never observable through another. Sharing is never
 * visible in the values, in {@link #equals(Object)} or in {@link #hashCode()}. {@link #limit(int)}
 * and the static operations still copy, as {@link RoaringBitmap} does.
 *
 * <p><b>Cost model.</b> {@link #clone()} copies only the arrays holding the containers, O(number
 * of containers), and marks both sides as lending all their containers. The first write to either
 * side afterwards costs O(number of containers) once, and every container it then writes is
 * copied once. If both sides write to every container, about twice as many containers are copied
 * as by a deep copy. {@link #fromBitmap(RoaringBitmap)} and {@link #toRoaringBitmap()} deep-copy.
 *
 * <p><b>Thread safety.</b> As with {@link RoaringBitmap}, a bitmap is not thread-safe for writes
 * and is safe for concurrent reads, with one precise addition:
 *
 * <ol>
 *   <li>Any number of threads may perform non-mutating operations on the same bitmap concurrently,
 *       provided no thread mutates it meanwhile. Non-mutating operations are the reads and {@link
 *       #clone()}.
 *   <li>{@link #clone()} writes nothing but one flag of the bitmap, which records that it lent its
 *       containers. It only ever sets the flag, and it writes no container.
 *   <li>Every mutation of a bitmap must happen-after every earlier operation that read or cloned
 *       it, and every later read by another thread must happen-after the mutation: the ordinary
 *       Java rules for conflicting accesses. Then the owner sees every flag a reader set. Breaking
 *       this rule costs more than with {@link RoaringBitmap}: bitmaps that share containers with
 *       the mutated one may then observe the write too.
 *   <li>A bitmap handed to another thread must be safely published.
 * </ol>
 *
 * <p><b>Interoperability.</b> This class is not a {@link RoaringBitmap} and cannot be passed to
 * APIs that expect one: {@link #fromBitmap(RoaringBitmap)} and {@link #toRoaringBitmap()} convert,
 * deep-copying every container. No method accepts or returns a container, so the containers of a
 * persistent bitmap are never reachable from code that does not take part in copy-on-write.
 *
 * <p><b>Untrusted input.</b> As with {@link RoaringBitmap}, a bitmap deserialized from an
 * untrusted source must not be used until {@link #validate()} returns {@code true}. {@link
 * #validate()} has exactly {@link RoaringBitmap#validate()}'s semantics, which also reject a run
 * container that is not the smallest encoding of its values. Ordinary writes can leave such
 * containers, so call {@link #runOptimize()} before serializing data that a consumer will validate.
 *
 * <p><b>Differences from {@link RoaringBitmap}.</b> {@link #runOptimize()} returns whether anything
 * changed. {@link #hashCode()} depends only on the values. {@link #limit(int)} does not overflow
 * near {@link Integer#MAX_VALUE}, and {@link #contains(long, long)} is exact for every range. The
 * same history can leave different container encodings in the two classes, with the same values.
 */
public final class PersistentRoaringBitmap
    implements ImmutableBitmapDataProvider, BitmapDataProvider, Iterable<Integer>, Cloneable {

  /*
   * Ownership. A slot is shared if lent || shared[slot], and owned otherwise.
   *
   * - An owned slot is the only reference to its container anywhere, so it may be written in place.
   *   A container held by several slots, of this or other bitmaps, is shared at every one of them.
   *   Two distinct attached containers never share a backing array.
   * - Only the owner writes shared[] and clears lent. clone() only ever sets its source's lent to
   *   true.
   * - shared.length >= size, and shared[i] is false for i >= size.
   * - Containers enter only freshly created or deep-copied, and leave only deep-copied. Neither the
   *   delegate nor its arrays are handed out, except to the read-only iterators.
   * - Attached containers are non-empty and not lazy, and an array container holds at most 4,096
   *   values while a bitmap container holds more.
   *
   * Writes go through RoaringBitmap's own mutators: a pre-pass first makes owned exactly the slots
   * that the mutator will write, and the flags are then realigned with the slots. This relies on
   * add(int), remove(int), add(long, long) and remove(long, long) writing only the containers of the
   * keys of their value or range, and remove(long, long) writing only the first and last of them.
   */

  /** Holds the keys and the containers; private to this bitmap and never handed out. */
  private final RoaringBitmap view;

  /** shared[i]: the container in slot i may also be held by another bitmap. */
  private boolean[] shared;

  /** Every container of this bitmap may also be held by another bitmap. */
  private boolean lent;

  /** Creates an empty bitmap. */
  public PersistentRoaringBitmap() {
    this(new RoaringArray());
  }

  /** Wraps containers that nothing else references. */
  private PersistentRoaringBitmap(RoaringArray containers) {
    this(containers, new boolean[containers.keys.length], false);
  }

  private PersistentRoaringBitmap(RoaringArray containers, boolean[] shared, boolean lent) {
    this.view = new RoaringBitmap(containers);
    this.shared = shared;
    this.lent = lent;
  }

  /**
   * Generates a new bitmap that has the specified values set to true.
   *
   * @param values the values to set
   * @return a new bitmap
   * @see RoaringBitmap#bitmapOf(int...)
   */
  public static PersistentRoaringBitmap bitmapOf(int... values) {
    return new PersistentRoaringBitmap(RoaringBitmap.bitmapOf(values).highLowContainer);
  }

  /**
   * Generates a new bitmap that holds the values in [min, max).
   *
   * @param min the inclusive lower bound, in [0, 0xffffffff]
   * @param max the exclusive upper bound, in [0, 0xffffffff + 1]
   * @return a new bitmap
   * @see RoaringBitmap#bitmapOfRange(long, long)
   */
  public static PersistentRoaringBitmap bitmapOfRange(long min, long max) {
    return new PersistentRoaringBitmap(RoaringBitmap.bitmapOfRange(min, max).highLowContainer);
  }

  /**
   * Creates a bitmap with the values of a {@link RoaringBitmap}, deep-copying every container. The
   * source is not modified and shares nothing with the result.
   *
   * @param source a bitmap produced by the public API (not one in the middle of a lazy aggregation)
   * @return a new bitmap with the same values
   */
  public static PersistentRoaringBitmap fromBitmap(RoaringBitmap source) {
    RoaringArray from = source.highLowContainer;
    RoaringArray containers = new RoaringArray(from.size);
    for (int i = 0; i < from.size; i++) {
      Container container = from.values[i].clone();
      if (container.getCardinality() < 0) {
        // a lazy bitmap container: repair the private copy and keep what the repair returns
        container = container.repairAfterLazy();
      }
      if (!container.isEmpty()) {
        containers.append(from.keys[i], container);
      }
    }
    return new PersistentRoaringBitmap(containers);
  }

  /**
   * Returns a {@link RoaringBitmap} with the same values, deep-copying every container. The result
   * shares nothing with this bitmap.
   *
   * @return a new bitmap with the same values
   */
  public RoaringBitmap toRoaringBitmap() {
    return view.clone();
  }

  /**
   * Returns a bitmap with the same values that shares every container with this one. Only the
   * arrays holding the containers are copied. Afterwards, the first write to a container, on either
   * side, copies that container.
   *
   * <p>This is a non-mutating operation: it marks this bitmap as having lent its containers, and
   * changes nothing else (see the thread-safety section of the class documentation).
   *
   * @return a new bitmap with the same values
   */
  @Override
  public PersistentRoaringBitmap clone() {
    RoaringArray array = view.highLowContainer;
    lent = true;
    RoaringArray copy =
        new RoaringArray(
            Arrays.copyOf(array.keys, array.size),
            Arrays.copyOf(array.values, array.size),
            array.size);
    return new PersistentRoaringBitmap(copy, new boolean[array.size], true);
  }

  /**
   * Bitwise OR (union). Neither bitmap is written.
   *
   * @param x1 first bitmap
   * @param x2 other bitmap
   * @return a new bitmap with the union of the values
   */
  public static PersistentRoaringBitmap or(PersistentRoaringBitmap x1, PersistentRoaringBitmap x2) {
    return new PersistentRoaringBitmap(RoaringBitmap.or(x1.view, x2.view).highLowContainer);
  }

  /**
   * Bitwise AND (intersection). Neither bitmap is written.
   *
   * @param x1 first bitmap
   * @param x2 other bitmap
   * @return a new bitmap with the intersection of the values
   */
  public static PersistentRoaringBitmap and(
      PersistentRoaringBitmap x1, PersistentRoaringBitmap x2) {
    return new PersistentRoaringBitmap(RoaringBitmap.and(x1.view, x2.view).highLowContainer);
  }

  /**
   * Bitwise XOR (symmetric difference). Neither bitmap is written.
   *
   * @param x1 first bitmap
   * @param x2 other bitmap
   * @return a new bitmap with the symmetric difference of the values
   */
  public static PersistentRoaringBitmap xor(
      PersistentRoaringBitmap x1, PersistentRoaringBitmap x2) {
    return new PersistentRoaringBitmap(RoaringBitmap.xor(x1.view, x2.view).highLowContainer);
  }

  /**
   * Bitwise ANDNOT (difference). Neither bitmap is written.
   *
   * @param x1 first bitmap
   * @param x2 other bitmap
   * @return a new bitmap with the values of {@code x1} that {@code x2} does not hold
   */
  public static PersistentRoaringBitmap andNot(
      PersistentRoaringBitmap x1, PersistentRoaringBitmap x2) {
    return new PersistentRoaringBitmap(RoaringBitmap.andNot(x1.view, x2.view).highLowContainer);
  }

  /**
   * Cardinality of the bitwise AND (intersection). Neither bitmap is written.
   *
   * @param x1 first bitmap
   * @param x2 other bitmap
   * @return as if you did and(x1, x2).getCardinality()
   */
  public static int andCardinality(PersistentRoaringBitmap x1, PersistentRoaringBitmap x2) {
    return RoaringBitmap.andCardinality(x1.view, x2.view);
  }

  /**
   * Cardinality of the bitwise OR (union). Neither bitmap is written.
   *
   * @param x1 first bitmap
   * @param x2 other bitmap
   * @return as if you did or(x1, x2).getCardinality()
   */
  public static int orCardinality(PersistentRoaringBitmap x1, PersistentRoaringBitmap x2) {
    return RoaringBitmap.orCardinality(x1.view, x2.view);
  }

  /**
   * Cardinality of the bitwise XOR (symmetric difference). Neither bitmap is written.
   *
   * @param x1 first bitmap
   * @param x2 other bitmap
   * @return as if you did xor(x1, x2).getCardinality()
   */
  public static int xorCardinality(PersistentRoaringBitmap x1, PersistentRoaringBitmap x2) {
    return RoaringBitmap.xorCardinality(x1.view, x2.view);
  }

  /**
   * Cardinality of the bitwise ANDNOT (difference). Neither bitmap is written.
   *
   * @param x1 first bitmap
   * @param x2 other bitmap
   * @return as if you did andNot(x1, x2).getCardinality()
   */
  public static int andNotCardinality(PersistentRoaringBitmap x1, PersistentRoaringBitmap x2) {
    return RoaringBitmap.andNotCardinality(x1.view, x2.view);
  }

  /**
   * Checks whether the two bitmaps intersect. Neither bitmap is written.
   *
   * @param x1 first bitmap
   * @param x2 other bitmap
   * @return true if they hold a common value
   */
  public static boolean intersects(PersistentRoaringBitmap x1, PersistentRoaringBitmap x2) {
    return RoaringBitmap.intersects(x1.view, x2.view);
  }

  /**
   * Returns a bitmap with the smallest {@code maxcardinality} values of this one, or all of them
   * if there are fewer, copying the containers. A non-positive {@code maxcardinality} gives an
   * empty bitmap.
   *
   * @param maxcardinality the maximal cardinality of the result
   * @return a new bitmap with at most {@code maxcardinality} values
   */
  @Override
  public PersistentRoaringBitmap limit(int maxcardinality) {
    RoaringArray array = view.highLowContainer;
    int remaining = maxcardinality;
    int whole = 0;
    while (whole < array.size && remaining > 0) {
      int cardinality = array.values[whole].getCardinality();
      if (cardinality > remaining) {
        break;
      }
      remaining -= cardinality;
      whole++;
    }
    boolean partial = whole < array.size && remaining > 0;
    int size = partial ? whole + 1 : whole;
    char[] keys = Arrays.copyOf(array.keys, size);
    Container[] values = Arrays.copyOf(array.values, size);
    for (int i = 0; i < whole; i++) {
      values[i] = values[i].clone();
    }
    if (partial) {
      values[whole] = values[whole].limit(remaining); // 0 < remaining < its cardinality
    }
    return new PersistentRoaringBitmap(new RoaringArray(keys, values, size));
  }

  /** Set the value to "true", whether it already appears or not. */
  @Override
  public void add(int x) {
    if (view.contains(x)) {
      return;
    }
    RoaringArray array = view.highLowContainer;
    int i = array.getIndex(Util.highbits(x));
    unlend();
    int size = array.size;
    if (i >= 0) {
      copyIfShared(i);
      view.add(x);
    } else {
      view.add(x);
      realignFlags(-i - 1, -i - 1, 1, size);
    }
  }

  /**
   * Adds all values in [rangeStart, rangeEnd). The containers of keys that the range covers
   * entirely are replaced by a single run, without copying them first.
   *
   * @param rangeStart inclusive beginning of range, in [0, 0xffffffff]
   * @param rangeEnd exclusive ending of range, in [0, 0xffffffff + 1]
   */
  @Override
  public void add(long rangeStart, long rangeEnd) {
    rangeSanityCheck(rangeStart, rangeEnd);
    if (rangeStart >= rangeEnd
        || view.rangeCardinality(rangeStart, rangeEnd) == rangeEnd - rangeStart) {
      return;
    }
    unlend();
    RoaringArray array = view.highLowContainer;
    int firstKey = Util.highbits(rangeStart);
    int lastKey = Util.highbits(rangeEnd - 1);
    int from = lowerBound(array, firstKey);
    int to = lowerBound(array, lastKey + 1);
    for (int i = from; i < to; i++) {
      long keyStart = (long) array.keys[i] << 16;
      if (rangeStart <= keyStart && keyStart + 0x10000 <= rangeEnd) {
        array.values[i] = RunContainer.full();
        shared[i] = false;
      } else {
        copyIfShared(i);
      }
    }
    int size = array.size;
    view.add(rangeStart, rangeEnd);
    realignFlags(from, to, lastKey - firstKey + 1, size);
  }

  /** If present, remove the value (effectively, set its bit to false). */
  @Override
  public void remove(int x) {
    if (!view.contains(x)) {
      return;
    }
    RoaringArray array = view.highLowContainer;
    int i = array.getIndex(Util.highbits(x));
    unlend();
    copyIfShared(i);
    int size = array.size;
    view.remove(x);
    if (array.size < size) {
      realignFlags(i, i + 1, 0, size);
    }
  }

  /**
   * Removes all values in [rangeStart, rangeEnd). A shared container that loses all its values is
   * dropped without being copied.
   *
   * @param rangeStart inclusive beginning of range, in [0, 0xffffffff]
   * @param rangeEnd exclusive ending of range, in [0, 0xffffffff + 1]
   */
  public void remove(long rangeStart, long rangeEnd) {
    rangeSanityCheck(rangeStart, rangeEnd);
    if (rangeStart >= rangeEnd || !view.intersects(rangeStart, rangeEnd)) {
      return;
    }
    unlend();
    int firstKey = Util.highbits(rangeStart);
    int lastKey = Util.highbits(rangeEnd - 1);
    // RoaringBitmap.remove writes only the containers of the first and the last key
    prepareRemoval(firstKey, rangeStart, rangeEnd);
    if (lastKey != firstKey) {
      prepareRemoval(lastKey, rangeStart, rangeEnd);
    }
    RoaringArray array = view.highLowContainer;
    int from = lowerBound(array, firstKey);
    int to = lowerBound(array, lastKey + 1);
    int size = array.size;
    view.remove(rangeStart, rangeEnd);
    realignFlags(from, to, (to - from) - (size - array.size), size);
  }

  /**
   * Makes the slot of {@code key} owned before {@code remove(rangeStart, rangeEnd)} writes it: a
   * shared container that the range empties is dropped, and any other shared one is copied.
   */
  private void prepareRemoval(int key, long rangeStart, long rangeEnd) {
    RoaringArray array = view.highLowContainer;
    int i = array.getIndex((char) key);
    if (i < 0 || !shared[i]) {
      return;
    }
    Container container = array.values[i];
    long keyStart = (long) key << 16;
    if (rangeStart <= keyStart + container.first() && keyStart + container.last() < rangeEnd) {
      int size = array.size;
      array.removeAtIndex(i);
      realignFlags(i, i + 1, 0, size);
    } else {
      copyIfShared(i);
    }
  }

  /** Removes all values, dropping every container without writing it. */
  public void clear() {
    view.highLowContainer = new RoaringArray();
    shared = new boolean[view.highLowContainer.keys.length];
    lent = false;
  }

  /**
   * Uses a run-length encoding where it is more space efficient. Shared containers are replaced by
   * converted copies; they are never converted in place.
   *
   * @return whether any container changed its encoding
   */
  public boolean runOptimize() {
    RoaringArray array = view.highLowContainer;
    boolean changed = false;
    for (int i = 0; i < array.size; i++) {
      Container container = array.values[i];
      Container optimized = container.runOptimize(); // a new container, or the same unchanged
      if (optimized != container) {
        unlend();
        array.values[i] = optimized;
        shared[i] = false;
        changed = true;
      }
    }
    return changed;
  }

  /** Recovers allocated but unused memory, in this bitmap's arrays and in its owned containers. */
  @Override
  public void trim() {
    RoaringArray array = view.highLowContainer;
    array.keys = Arrays.copyOf(array.keys, array.size);
    array.values = Arrays.copyOf(array.values, array.size);
    shared = Arrays.copyOf(shared, array.size);
    if (!lent) {
      for (int i = 0; i < array.size; i++) {
        if (!shared[i]) {
          array.values[i].trim();
        }
      }
    }
  }

  /**
   * Deserializes a bitmap in the portable format, replacing the values of this one. If decoding
   * throws, this bitmap keeps its previous values. Containers this bitmap shared are dropped
   * without being written.
   *
   * <p>If the source is untrusted, use the bitmap only after {@link #validate()} returns {@code
   * true}.
   *
   * @param in the DataInput stream
   * @throws IOException Signals that an I/O exception has occurred.
   * @see RoaringBitmap#deserialize(DataInput)
   */
  public void deserialize(DataInput in) throws IOException {
    RoaringBitmap decoded = new RoaringBitmap();
    decoded.deserialize(in);
    install(decoded.highLowContainer);
  }

  /**
   * Deserializes a bitmap in the portable format, replacing the values of this one. If decoding
   * throws, this bitmap keeps its previous values. Containers this bitmap shared are dropped
   * without being written. The position of the buffer is not changed.
   *
   * <p>If the source is untrusted, use the bitmap only after {@link #validate()} returns {@code
   * true}.
   *
   * @param bbf the byte buffer (can be mapped, direct, array backed etc.)
   * @throws IOException Signals that an I/O exception has occurred.
   * @see RoaringBitmap#deserialize(ByteBuffer)
   */
  public void deserialize(ByteBuffer bbf) throws IOException {
    RoaringBitmap decoded = new RoaringBitmap();
    decoded.deserialize(bbf);
    install(decoded.highLowContainer);
  }

  private void install(RoaringArray containers) {
    view.highLowContainer = containers;
    shared = new boolean[containers.keys.length];
    lent = false;
  }

  /** Before a write that can make a slot owned: records lent in the per-slot flags. */
  private void unlend() {
    if (lent) {
      Arrays.fill(shared, 0, view.highLowContainer.size, true);
      lent = false;
    }
  }

  /** Makes slot {@code i} owned by copying its container if it is shared. Requires !lent. */
  private void copyIfShared(int i) {
    if (shared[i]) {
      RoaringArray array = view.highLowContainer;
      array.values[i] = array.values[i].clone();
      shared[i] = false;
    }
  }

  /**
   * Realigns the flags after the delegate replaced the slots [from, to) of an array of {@code
   * oldSize} slots by {@code count} slots that it wrote or inserted, all owned.
   */
  private void realignFlags(int from, int to, int count, int oldSize) {
    int newSize = view.highLowContainer.size;
    if (shared.length < newSize) {
      shared = Arrays.copyOf(shared, view.highLowContainer.keys.length);
    }
    System.arraycopy(shared, to, shared, from + count, oldSize - to);
    Arrays.fill(shared, from, from + count, false);
    if (newSize < oldSize) {
      Arrays.fill(shared, newSize, oldSize, false);
    }
  }

  /** The index of the first slot whose key is at least {@code key}. */
  private static int lowerBound(RoaringArray array, int key) {
    if (key > Util.maxLowBitAsInteger()) {
      return array.size;
    }
    int i = array.getIndex((char) key);
    return i >= 0 ? i : -i - 1;
  }

  /** The argument check of RoaringBitmap's range methods, which is private there. */
  private static void rangeSanityCheck(long rangeStart, long rangeEnd) {
    if (rangeStart < 0 || rangeStart > (1L << 32) - 1) {
      throw new IllegalArgumentException(
          "rangeStart=" + rangeStart + " should be in [0, 0xffffffff]");
    }
    if (rangeEnd > (1L << 32) || rangeEnd < 0) {
      throw new IllegalArgumentException(
          "rangeEnd=" + rangeEnd + " should be in [0, 0xffffffff + 1]");
    }
  }

  @Override
  public boolean contains(int x) {
    return view.contains(x);
  }

  /**
   * Checks whether the bitmap holds every value of the range.
   *
   * @param minimum the inclusive lower bound of the range, in [0, 0xffffffff]
   * @param supremum the exclusive upper bound of the range, in [0, 0xffffffff + 1]
   * @return whether the range is non-empty and every value in it is present
   */
  public boolean contains(long minimum, long supremum) {
    rangeSanityCheck(minimum, supremum);
    return supremum > minimum && view.rangeCardinality(minimum, supremum) == supremum - minimum;
  }

  /**
   * Checks whether the bitmap holds a value of the range.
   *
   * @param minimum the inclusive lower bound of the range, in [0, 0xffffffff]
   * @param supremum the exclusive upper bound of the range, in [0, 0xffffffff + 1]
   * @return whether some value in the range is present
   */
  public boolean intersects(long minimum, long supremum) {
    return view.intersects(minimum, supremum);
  }

  @Override
  public int getCardinality() {
    return view.getCardinality();
  }

  @Override
  public long getLongCardinality() {
    return view.getLongCardinality();
  }

  @Override
  public void forEach(IntConsumer ic) {
    view.forEach(ic);
  }

  @Override
  public PeekableIntIterator getIntIterator() {
    return view.getIntIterator();
  }

  @Override
  public PeekableIntIterator getSignedIntIterator() {
    return view.getSignedIntIterator();
  }

  @Override
  public PeekableIntIterator getReverseIntIterator() {
    return view.getReverseIntIterator();
  }

  @Override
  public BatchIterator getBatchIterator() {
    return view.getBatchIterator();
  }

  /** Iterates over the values in unsigned ascending order. */
  @Override
  public Iterator<Integer> iterator() {
    return view.iterator();
  }

  /**
   * Estimate of the memory usage, counting every container as if this bitmap held it alone. It does
   * not measure the heap retained by this bitmap: shared containers are counted in every holder.
   */
  @Override
  public int getSizeInBytes() {
    return view.getSizeInBytes();
  }

  /**
   * Estimate of the memory usage, counting every container as if this bitmap held it alone. It does
   * not measure the heap retained by this bitmap: shared containers are counted in every holder.
   */
  @Override
  public long getLongSizeInBytes() {
    return view.getLongSizeInBytes();
  }

  @Override
  public boolean isEmpty() {
    return view.isEmpty();
  }

  @Override
  public int rank(int x) {
    return view.rank(x);
  }

  @Override
  public long rankLong(int x) {
    return view.rankLong(x);
  }

  @Override
  public long rangeCardinality(long start, long end) {
    return view.rangeCardinality(start, end);
  }

  @Override
  public int select(int j) {
    return view.select(j);
  }

  @Override
  public int first() {
    return view.first();
  }

  @Override
  public int last() {
    return view.last();
  }

  @Override
  public int firstSigned() {
    return view.firstSigned();
  }

  @Override
  public int lastSigned() {
    return view.lastSigned();
  }

  @Override
  public long nextValue(int fromValue) {
    return view.nextValue(fromValue);
  }

  @Override
  public long previousValue(int fromValue) {
    return view.previousValue(fromValue);
  }

  @Override
  public long nextAbsentValue(int fromValue) {
    return view.nextAbsentValue(fromValue);
  }

  @Override
  public long previousAbsentValue(int fromValue) {
    return view.previousAbsentValue(fromValue);
  }

  /**
   * Serializes this bitmap in the portable format, exactly as {@link
   * RoaringBitmap#serialize(DataOutput)} does for the same containers.
   */
  @Override
  public void serialize(DataOutput out) throws IOException {
    view.serialize(out);
  }

  /**
   * Serializes this bitmap in the portable format, exactly as {@link
   * RoaringBitmap#serialize(ByteBuffer)} does for the same containers.
   */
  @Override
  public void serialize(ByteBuffer buffer) {
    view.serialize(buffer);
  }

  @Override
  public int serializedSizeInBytes() {
    return view.serializedSizeInBytes();
  }

  @Override
  public int[] toArray() {
    return view.toArray();
  }

  @Override
  public int getContainerCount() {
    return view.getContainerCount();
  }

  /**
   * Checks whether any container uses run-length encoding.
   *
   * @return whether this bitmap has run compression
   */
  public boolean hasRunCompression() {
    return view.hasRunCompression();
  }

  /**
   * Validates the content of the bitmap, with {@link RoaringBitmap#validate()}'s rules. Useful
   * after deserialization from an untrusted source.
   *
   * @return true if the content is valid
   */
  public boolean validate() {
    return view.validate();
  }

  /**
   * Two persistent bitmaps are equal if they hold the same values, whatever their encodings and
   * whatever they share. A persistent bitmap is never equal to a {@link RoaringBitmap}.
   */
  @Override
  public boolean equals(Object o) {
    return o == this
        || o instanceof PersistentRoaringBitmap && view.equals(((PersistentRoaringBitmap) o).view);
  }

  /**
   * A hash code that depends only on the values, not on how they are encoded: it combines the
   * maximal runs of consecutive values within each 65,536-value block.
   */
  @Override
  public int hashCode() {
    RoaringArray array = view.highLowContainer;
    int hash = 0;
    for (int i = 0; i < array.size; i++) {
      Container container = array.values[i];
      int high = array.keys[i] << 16;
      int start = container.first();
      int end = container.nextAbsentValue((char) start);
      while (end <= Util.maxLowBitAsInteger()) {
        int next = container.nextValue((char) end);
        if (next < 0) {
          break;
        }
        if (next != end) { // a run container can stop a run between two adjacent runs
          hash = 31 * (31 * hash + (high | start)) + (end - start);
          start = next;
        }
        end = container.nextAbsentValue((char) next);
      }
      hash = 31 * (31 * hash + (high | start)) + (end - start);
    }
    return hash;
  }

  @Override
  public String toString() {
    return view.toString();
  }
}
