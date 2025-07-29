package org.roaringbitmap.buffer;

/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */



import java.util.Arrays;

/**
 * A MutableRoaringBitmap that implements copy-on-write semantics for containers.
 * This allows efficient sharing of memory-mapped containers until they need to be modified.
 */
public class CopyOnWriteRoaringBitmap extends MutableRoaringBitmap {
  private boolean[] needsCopy;
  private boolean copyOnWrite = true;

  /**
   * Create a new copy-on-write bitmap.
   */
  public CopyOnWriteRoaringBitmap() {
    super();
    needsCopy = new boolean[4]; // Initial capacity
  }

  /**
   * Create a copy-on-write bitmap from an immutable bitmap.
   */
  public static CopyOnWriteRoaringBitmap fromImmutable(ImmutableRoaringBitmap immutable) {
    CopyOnWriteRoaringBitmap result = new CopyOnWriteRoaringBitmap();
    MappeableContainerPointer mcp = immutable.highLowContainer.getContainerPointer();
    while (mcp.hasContainer()) {
      result.getMappeableRoaringArray().appendCopyOnWrite(mcp.key(), mcp.getContainer());
      mcp.advance();
    }
    result.needsCopy = new boolean[result.getMappeableRoaringArray().size];
    Arrays.fill(result.needsCopy, true);
    return result;
  }

  /**
   * Ensure the container at the given index is copied if needed.
   */
  private void ensureContainerCopied(int index) {
    if (copyOnWrite && index < needsCopy.length && needsCopy[index]) {
      MutableRoaringArray array = getMappeableRoaringArray();
      array.setContainerAtIndex(index, array.getContainerAtIndex(index).clone());
      needsCopy[index] = false;
    }
  }

  /**
   * Ensure all containers are copied if needed.
   */
  private void ensureAllContainersCopied() {
    if (copyOnWrite) {
      MutableRoaringArray array = getMappeableRoaringArray();
      for (int i = 0; i < array.size && i < needsCopy.length; i++) {
        if (needsCopy[i]) {
          array.setContainerAtIndex(i, array.getContainerAtIndex(i).clone());
          needsCopy[i] = false;
        }
      }
    }
  }

  /**
   * Extend the needsCopy array if necessary.
   */
  private void extendNeedsCopyArray(int minSize) {
    if (needsCopy.length < minSize) {
      boolean[] newArray = new boolean[Math.max(minSize, needsCopy.length * 2)];
      System.arraycopy(needsCopy, 0, newArray, 0, needsCopy.length);
      needsCopy = newArray;
    }
  }

  @Override
  public void add(int x) {
    if (copyOnWrite) {
      char hb = (char) (x >>> 16);
      MutableRoaringArray array = getMappeableRoaringArray();
      int index = array.getIndex(hb);
      if (index >= 0) {
        ensureContainerCopied(index);
      }
    }
    super.add(x);
  }

  @Override
  public void remove(int x) {
    if (copyOnWrite) {
      char hb = (char) (x >>> 16);
      MutableRoaringArray array = getMappeableRoaringArray();
      int index = array.getIndex(hb);
      if (index >= 0) {
        ensureContainerCopied(index);
      }
    }
    super.remove(x);
  }

  @Override
  public void flip(int x) {
    if (copyOnWrite) {
      char hb = (char) (x >>> 16);
      MutableRoaringArray array = getMappeableRoaringArray();
      int index = array.getIndex(hb);
      if (index >= 0) {
        ensureContainerCopied(index);
      }
    }
    super.flip(x);
  }

  @Override
  public void or(ImmutableRoaringBitmap x2) {
    if (copyOnWrite) {
      ensureAllContainersCopied();
      copyOnWrite = false;
    }
    super.or(x2);
  }

  @Override
  public void and(ImmutableRoaringBitmap x2) {
    if (copyOnWrite) {
      ensureAllContainersCopied();
      copyOnWrite = false;
    }
    super.and(x2);
  }

  @Override
  public void xor(ImmutableRoaringBitmap x2) {
    if (copyOnWrite) {
      ensureAllContainersCopied();
      copyOnWrite = false;
    }
    super.xor(x2);
  }

  @Override
  public void andNot(ImmutableRoaringBitmap x2) {
    if (copyOnWrite) {
      ensureAllContainersCopied();
      copyOnWrite = false;
    }
    super.andNot(x2);
  }

  @Override
  public CopyOnWriteRoaringBitmap clone() {
    CopyOnWriteRoaringBitmap result = new CopyOnWriteRoaringBitmap();
    result.copyOnWrite = this.copyOnWrite;
    if (copyOnWrite) {
      // Share containers
      result.highLowContainer = this.highLowContainer.clone();
      result.needsCopy = this.needsCopy.clone();
    } else {
      // Deep copy
      result.highLowContainer = this.highLowContainer.clone();
      result.copyOnWrite = false;
    }
    return result;
  }
}
