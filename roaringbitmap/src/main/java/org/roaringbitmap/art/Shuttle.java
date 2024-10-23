package org.roaringbitmap.art;

public interface Shuttle {

  /**
   * should be called firstly before calling other methods
   */
  public void initShuttle();

  /**
   * Call this instead of initShuttle, if the iterator should start from a bound
   * (upper/lower depending on direction)
   *
   * @param key the upper/lower bound to start from
   */
  public void initShuttleFrom(long key);

  /**
   *
   * @return true: has a LeafNode, false: has no LeafNode
   */
  public boolean moveToNextLeaf();

  /**
   * get the current LeafNode after calling the method moveToNextLeaf
   * @return the current visiting LeafNode
   */
  public LeafNode getCurrentLeafNode();

  /**
   * remove the current visiting LeafNode and its corresponding value container
   */
  public void remove();
}
