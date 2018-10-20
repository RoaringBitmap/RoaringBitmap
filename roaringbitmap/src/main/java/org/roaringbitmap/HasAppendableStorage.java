package org.roaringbitmap;

public interface HasAppendableStorage<T> {
  AppendableStorage<T> getStorage();
}
