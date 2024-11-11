package org.roaringbitmap.bsi;

import java.io.Serializable;

/**
 * A generic class for pairs.
 * Copied from org.apache.hadoop.hbase.util.Pair
 *
 * @param <T1>
 * @param <T2>
 */
public class Pair<T1, T2> implements Serializable {
  private static final long serialVersionUID = -3986244606585552569L;

  protected T1 left = null;
  protected T2 right = null;

  /**
   * Default constructor.
   */
  public Pair() {}

  /**
   * Constructor
   *
   * @param a operand
   * @param b operand
   */
  public Pair(T1 a, T2 b) {
    this.left = a;
    this.right = b;
  }

  /**
   * Constructs a new pair, inferring the type via the passed arguments
   *
   * @param <T1> type for left
   * @param <T2> type for right
   * @param a  left element
   * @param b  right element
   * @return a new pair containing the passed arguments
   */
  public static <T1, T2> Pair<T1, T2> newPair(T1 a, T2 b) {
    return new Pair<T1, T2>(a, b);
  }

  private static boolean equals(Object x, Object y) {
    return (x == null && y == null) || (x != null && x.equals(y));
  }

  /**
   * Return the left element stored in the pair.
   *
   * @return T1
   */
  public T1 getLeft() {
    return left;
  }

  public T1 getKey() {
    return left;
  }

  /**
   * Replace the left element of the pair.
   *
   * @param a operand
   */
  public void setFirst(T1 a) {
    this.left = a;
  }

  /**
   * Return the right element stored in the pair.
   *
   * @return T2
   */
  public T2 getRight() {
    return right;
  }

  public T2 getValue() {
    return right;
  }

  /**
   * Replace the right element of the pair.
   *
   * @param b operand
   */
  public void setSecond(T2 b) {
    this.right = b;
  }

  @Override
  @SuppressWarnings("rawtypes")
  public boolean equals(Object other) {
    return other instanceof Pair
        && equals(left, ((Pair) other).left)
        && equals(right, ((Pair) other).right);
  }

  @Override
  public int hashCode() {
    if (left == null) return (right == null) ? 0 : right.hashCode() + 1;
    else if (right == null) return left.hashCode() + 2;
    else return left.hashCode() * 17 + right.hashCode();
  }

  @Override
  public String toString() {
    return "{" + getLeft() + "," + getRight() + "}";
  }
}
