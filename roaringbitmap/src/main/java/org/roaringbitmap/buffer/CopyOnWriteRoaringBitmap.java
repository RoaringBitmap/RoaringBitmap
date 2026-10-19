package org.roaringbitmap.buffer;

/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/**
 * A MutableRoaringBitmap that implements copy-on-write semantics for containers.
 * This allows efficient sharing of memory-mapped containers until they need to be modified.
 */
public class CopyOnWriteRoaringBitmap extends MutableRoaringBitmap {
  // The containers still shared with the source bitmap. They are compared by identity, which,
  // unlike their index, does not change when other containers are inserted or removed.
  private final Set<MappeableContainer> shared = Collections.newSetFromMap(new IdentityHashMap<>());

  /**
   * Create a new copy-on-write bitmap.
   */
  public CopyOnWriteRoaringBitmap() {
    this(4); // Initial capacity
  }

  /**
   * Creates an empty copy-on-write bitmap with a specified initial capacity.
   * Use this to avoid internal array resizing when the number of containers is known in advance.
   *
   * @param initialCapacity the initial size of the underlying container array
   */
  private CopyOnWriteRoaringBitmap(int initialCapacity) {
    super(initialCapacity);
  }

  /**
   * Create a copy-on-write bitmap from an immutable bitmap.
   */
  public static CopyOnWriteRoaringBitmap fromImmutable(ImmutableRoaringBitmap immutable) {
    CopyOnWriteRoaringBitmap result = new CopyOnWriteRoaringBitmap(immutable.getContainerCount());
    MappeableContainerPointer mcp = immutable.highLowContainer.getContainerPointer();
    while (mcp.hasContainer()) {
      MappeableContainer container = mcp.getContainer();
      result.getMappeableRoaringArray().appendCopyOnWrite(mcp.key(), container);
      result.shared.add(container);
      mcp.advance();
    }
    return result;
  }

  /**
   * Ensure the container at the given index is copied if needed.
   */
  private void ensureContainerCopied(int index) {
    MutableRoaringArray array = getMappeableRoaringArray();
    MappeableContainer container = array.getContainerAtIndex(index);
    if (shared.remove(container)) {
      array.setContainerAtIndex(index, container.clone());
    }
  }

  /**
   * Ensure the container that holds the given value is copied if needed.
   */
  private void ensureContainerCopiedFor(int x) {
    if (!shared.isEmpty()) {
      int index = getMappeableRoaringArray().getIndex(BufferUtil.highbits(x));
      if (index >= 0) {
        ensureContainerCopied(index);
      }
    }
  }

  /**
   * Ensure the containers that hold values in [rangeStart, rangeEnd) are copied if needed.
   */
  private void ensureContainersCopied(long rangeStart, long rangeEnd) {
    long start = Math.max(rangeStart, 0);
    long end = Math.min(rangeEnd, 1L << 32);
    if (shared.isEmpty() || start >= end) {
      return; // nothing shared, or an empty or invalid range that writes nothing
    }
    int lastKey = (int) ((end - 1) >>> 16);
    MutableRoaringArray array = getMappeableRoaringArray();
    int index = array.getIndex((char) (start >>> 16));
    if (index < 0) {
      index = -index - 1;
    }
    for (; index < array.size && array.getKeyAtIndex(index) <= lastKey; index++) {
      ensureContainerCopied(index);
    }
  }

  /**
   * Ensure all containers are copied if needed.
   */
  private void ensureAllContainersCopied() {
    if (!shared.isEmpty()) {
      MutableRoaringArray array = getMappeableRoaringArray();
      for (int i = 0; i < array.size; i++) {
        ensureContainerCopied(i);
      }
      // forget the shared containers that were removed from this bitmap
      shared.clear();
    }
  }

  @Override
  public void add(int x) {
    ensureContainerCopiedFor(x);
    super.add(x);
  }

  @Override
  public void remove(int x) {
    ensureContainerCopiedFor(x);
    super.remove(x);
  }

  @Override
  public void flip(int x) {
    ensureContainerCopiedFor(x);
    super.flip(x);
  }

  @Override
  public void or(ImmutableRoaringBitmap x2) {
    ensureAllContainersCopied();
    super.or(x2);
  }

  @Override
  public void and(ImmutableRoaringBitmap x2) {
    ensureAllContainersCopied();
    super.and(x2);
  }

  @Override
  public void xor(ImmutableRoaringBitmap x2) {
    ensureAllContainersCopied();
    super.xor(x2);
  }

  @Override
  public void andNot(ImmutableRoaringBitmap x2) {
    ensureAllContainersCopied();
    super.andNot(x2);
  }

  @Override
  public void addN(final int[] dat, final int offset, final int n) {
    if (offset >= 0 && n >= 0 && offset <= dat.length - n) { // otherwise super.addN throws
      for (int i = offset; i < offset + n; i++) {
        ensureContainerCopiedFor(dat[i]);
      }
    }
    super.addN(dat, offset, n);
  }

  @Override
  public void add(final long rangeStart, final long rangeEnd) {
    ensureContainersCopied(rangeStart, rangeEnd);
    super.add(rangeStart, rangeEnd);
  }

  @Override
  public void remove(final long rangeStart, final long rangeEnd) {
    ensureContainersCopied(rangeStart, rangeEnd);
    super.remove(rangeStart, rangeEnd);
  }

  @Override
  public void flip(final long rangeStart, final long rangeEnd) {
    ensureContainersCopied(rangeStart, rangeEnd);
    super.flip(rangeStart, rangeEnd);
  }

  @Override
  public boolean checkedAdd(final int x) {
    ensureContainerCopiedFor(x);
    return super.checkedAdd(x);
  }

  @Override
  public boolean checkedRemove(final int x) {
    ensureContainerCopiedFor(x);
    return super.checkedRemove(x);
  }

  @Override
  public void orNot(ImmutableRoaringBitmap other, long rangeEnd) {
    ensureContainersCopied(0, rangeEnd);
    super.orNot(other, rangeEnd);
  }

  @Override
  protected void lazyor(final ImmutableRoaringBitmap x2) {
    ensureAllContainersCopied();
    super.lazyor(x2);
  }

  @Override
  protected void naivelazyor(final ImmutableRoaringBitmap x2) {
    ensureAllContainersCopied();
    super.naivelazyor(x2);
  }

  @Override
  public CopyOnWriteRoaringBitmap clone() {
    CopyOnWriteRoaringBitmap result = new CopyOnWriteRoaringBitmap();
    // MutableRoaringArray.clone() copies every container, so the clone shares none
    result.highLowContainer = this.highLowContainer.clone();
    return result;
  }
}
