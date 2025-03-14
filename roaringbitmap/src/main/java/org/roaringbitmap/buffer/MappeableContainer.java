/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap.buffer;

import org.roaringbitmap.CharIterator;
import org.roaringbitmap.Container;
import org.roaringbitmap.ContainerBatchIterator;
import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.PeekableCharIterator;
import org.roaringbitmap.WordStorage;

import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

/**
 * Base container class. This class is similar to org.roaringbitmap.Container but meant to be used
 * with memory mapping.
 */
public abstract class MappeableContainer
    implements Iterable<Character>, Cloneable, Externalizable, WordStorage<MappeableContainer> {
  /**
   * Create a container initialized with a range of consecutive values
   *
   * @param start first index
   * @param last last index (range is exclusive)
   * @return a new container initialized with the specified values
   */
  public static MappeableContainer rangeOfOnes(final int start, final int last) {
    final int arrayContainerOverRunThreshold = 2;
    final int cardinality = last - start;

    if (cardinality <= arrayContainerOverRunThreshold) {
      return new MappeableArrayContainer(start, last);
    }
    return new MappeableRunContainer(start, last);
  }

  /**
   * Return a new container with all chars in [begin,end) added using an unsigned interpretation.
   *
   * @param begin start of range (inclusive)
   * @param end end of range (exclusive)
   * @return the new container
   */
  public abstract MappeableContainer add(int begin, int end);

  /**
   * Add a char to the container. May generate a new container.
   *
   * @param x char to be added
   * @return the new container
   */
  public abstract MappeableContainer add(char x);

  /**
   * Checks whether the container is empty or not.
   * @return true if the container is empty.
   */
  public abstract boolean isEmpty();

  /**
   * Checks whether the container is full or not.
   * @return true if the container is full.
   */
  public abstract boolean isFull();

  /**
   * Computes the union of this container with the bits present in the array,
   * modifying the array.
   * @param bits a 1024 element array to be interpreted as a bit set
   */
  public abstract void orInto(long[] bits);

  /**
   * Computes the intersection of this container with the bits present in the array,
   * modifying the array.
   * @param bits a 1024 element array to be interpreted as a bit set
   */
  public abstract void andInto(long[] bits);

  /**
   * Computes the intersection of the negation of this container with the bits present in the array,
   * modifying the array.
   * @param bits a 1024 element array to be interpreted as a bit set
   */
  public abstract void removeFrom(long[] bits);

  /**
   * Computes the bitwise AND of this container with another (intersection). This container as well
   * as the provided container are left unaffected.
   *
   * @param x other container
   * @return aggregated container
   */
  public abstract MappeableContainer and(MappeableArrayContainer x);

  /**
   * Computes the bitwise AND of this container with another (intersection). This container as well
   * as the provided container are left unaffected.
   *
   * @param x other container
   * @return aggregated container
   */
  public abstract MappeableContainer and(MappeableBitmapContainer x);

  protected MappeableContainer and(MappeableContainer x) {
    if (x instanceof MappeableArrayContainer) {
      return and((MappeableArrayContainer) x);
    } else if (x instanceof MappeableRunContainer) {
      return and((MappeableRunContainer) x);
    }
    return and((MappeableBitmapContainer) x);
  }

  protected abstract int andCardinality(MappeableArrayContainer x);

  protected abstract int andCardinality(MappeableBitmapContainer x);

  protected abstract int andCardinality(MappeableRunContainer x);

  /**
   * Returns the cardinality of the XOR between the passed container and this
   * container without materialising a temporary container.
   * @param other other container
   * @return the cardinality of the symmetric difference of the two containers
   */
  public int xorCardinality(MappeableContainer other) {
    return getCardinality() + other.getCardinality() - 2 * andCardinality(other);
  }

  /**
   * Computes the bitwise AND of this container with another (intersection). This container as well
   * as the provided container are left unaffected.
   *
   * @param x other container
   * @return aggregated container
   */
  public int andCardinality(MappeableContainer x) {
    if (this.isEmpty()) {
      return 0;
    } else if (x.isEmpty()) {
      return 0;
    } else {
      if (x instanceof MappeableArrayContainer) {
        return andCardinality((MappeableArrayContainer) x);
      } else if (x instanceof MappeableBitmapContainer) {
        return andCardinality((MappeableBitmapContainer) x);
      }
      return andCardinality((MappeableRunContainer) x);
    }
  }

  /**
   * Computes the bitwise AND of this container with another (intersection). This container as well
   * as the provided container are left unaffected.
   *
   * @param x other container
   * @return aggregated container
   */
  public abstract MappeableContainer and(MappeableRunContainer x);

  /**
   * Computes the bitwise ANDNOT of this container with another (difference). This container as well
   * as the provided container are left unaffected.
   *
   * @param x other container
   * @return aggregated container
   */
  public abstract MappeableContainer andNot(MappeableArrayContainer x);

  /**
   * Computes the bitwise ANDNOT of this container with another (difference). This container as well
   * as the provided container are left unaffected.
   *
   * @param x other container
   * @return aggregated container
   */
  public abstract MappeableContainer andNot(MappeableBitmapContainer x);

  protected MappeableContainer andNot(MappeableContainer x) {
    if (x instanceof MappeableArrayContainer) {
      return andNot((MappeableArrayContainer) x);
    } else if (x instanceof MappeableRunContainer) {
      return andNot((MappeableRunContainer) x);
    }

    return andNot((MappeableBitmapContainer) x);
  }

  /**
   * Computes the bitwise ANDNOT of this container with another (difference). This container as well
   * as the provided container are left unaffected.
   *
   * @param x other container
   * @return aggregated container
   */
  public abstract MappeableContainer andNot(MappeableRunContainer x);

  /**
   * Computes the bitwise ORNOT of this container with another. This container as well
   * as the provided container are left unaffected.
   *
   * @param x other container
   * @param endOfRange end of range (size of the universe)
   * @return aggregated container
   */
  public MappeableContainer orNot(MappeableContainer x, int endOfRange) {
    if (endOfRange < 0x10000) {
      return or(x.not(0, endOfRange).iremove(endOfRange, 0x10000));
    }
    return or(x.not(0, 0x10000));
  }

  /**
   * Computes the in-place bitwise ORNOT of this container with another. The current
   * container is generally modified, whereas the provided container (x) is unaffected. May generate
   * a new container.
   *
   * @param x other container
   * @param endOfRange the exclusive end
   * @return aggregated container
   */
  public MappeableContainer iorNot(MappeableContainer x, int endOfRange) {
    if (endOfRange < 0x10000) {
      return ior(x.not(0, endOfRange).iremove(endOfRange, 0x10000));
    }
    return ior(x.not(0, 0x10000));
  }

  /**
   * Empties the container
   */
  public abstract void clear();

  @Override
  public abstract MappeableContainer clone();

  /**
   * Checks whether the contain contains the provided value
   *
   * @param x value to check
   * @return whether the value is in the container
   */
  public abstract boolean contains(char x);

  /**
   * Checks whether the container is a subset of this container or not
   * @param subset the container to be tested
   * @return true if the parameter is a subset of this container
   */
  public boolean contains(MappeableContainer subset) {
    if (subset instanceof MappeableRunContainer) {
      return contains((MappeableRunContainer) subset);
    } else if (subset instanceof MappeableArrayContainer) {
      return contains((MappeableArrayContainer) subset);
    } else if (subset instanceof MappeableBitmapContainer) {
      return contains((MappeableBitmapContainer) subset);
    }
    return false;
  }

  protected abstract boolean contains(MappeableRunContainer runContainer);

  protected abstract boolean contains(MappeableArrayContainer arrayContainer);

  protected abstract boolean contains(MappeableBitmapContainer bitmapContainer);

  /**
   * Checks if the container intersects with a range
   * @param minimum the inclusive unsigned lower bound of the range
   * @param supremum the exclusive unsigned upper bound of the range
   * @return true if the container intersects the range
   */
  public abstract boolean intersects(int minimum, int supremum);

  /**
   * Checks whether the container contains the entire range
   * @param minimum the inclusive lower bound of the range
   * @param supremum the exclusive upper bound of the range
   * @return true if the container contains the range
   */
  public abstract boolean contains(int minimum, int supremum);

  /**
   * Fill the least significant 16 bits of the integer array, starting at index index, with the
   * char values from this container. The caller is responsible to allocate enough room. The most
   * significant 16 bits of each integer are given by the most significant bits of the provided
   * mask.
   *
   * @param x provided array
   * @param i starting index
   * @param mask indicates most significant bits
   */
  public abstract void fillLeastSignificant16bits(int[] x, int i, int mask);

  /**
   * Add a char to the container if it is not present, otherwise remove it. May generate a new
   * container.
   *
   * @param x char to be added
   * @return the new container
   */
  public abstract MappeableContainer flip(char x);

  /**
   * Size of the underlying array
   *
   * @return size in bytes
   */
  protected abstract int getArraySizeInBytes();

  /**
   * Computes the distinct number of char values in the container. Can be expected to run in
   * constant time.
   *
   * @return the cardinality
   */
  public abstract int getCardinality();

  /**
   * Get the name of this container.
   *
   * @return name of the container
   */
  public String getContainerName() {
    if (this instanceof MappeableBitmapContainer) {
      return ContainerNames[0];
    } else if (this instanceof MappeableArrayContainer) {
      return ContainerNames[1];
    } else {
      return ContainerNames[2];
    }
  }

  /**
   * Name of the various possible containers
   */
  public static String[] ContainerNames = {"mappeablebitmap", "mappeablearray", "mappeablerun"};

  /**
   * Iterator to visit the char values in the container in descending order.
   *
   * @return iterator
   */
  public abstract CharIterator getReverseCharIterator();

  /**
   * Iterator to visit the char values in the container in ascending order.
   *
   * @return iterator
   */
  public abstract PeekableCharIterator getCharIterator();

  /**
   * Gets an iterator to visit the contents of the container in batches
   * @return iterator
   */
  public abstract ContainerBatchIterator getBatchIterator();

  /**
   * Iterate through the values of this container and pass them
   * along to the IntConsumer, using msb as the 16 most significant bits.
   * @param msb 16 most significant bits
   * @param ic consumer
   */
  public abstract void forEach(char msb, IntConsumer ic);

  /**
   * Computes an estimate of the memory usage of this container. The estimate is not meant to be
   * exact.
   *
   * @return estimated memory usage in bytes
   */
  public abstract int getSizeInBytes();

  /**
   * Add all chars in [begin,end) using an unsigned interpretation. May generate a new container.
   *
   * @param begin start of range (inclusive)
   * @param end end of range (exclusive)
   * @return the new container
   */
  public abstract MappeableContainer iadd(int begin, int end);

  /**
   * Computes the in-place bitwise AND of this container with another (intersection). The current
   * container is generally modified, whereas the provided container (x) is unaffected. May generate
   * a new container.
   *
   * @param x other container
   * @return aggregated container
   */
  public abstract MappeableContainer iand(MappeableArrayContainer x);

  /**
   * Computes the in-place bitwise AND of this container with another (intersection). The current
   * container is generally modified, whereas the provided container (x) is unaffected. May generate
   * a new container.
   *
   * @param x other container
   * @return aggregated container
   */
  public abstract MappeableContainer iand(MappeableBitmapContainer x);

  protected MappeableContainer iand(MappeableContainer x) {
    if (x instanceof MappeableArrayContainer) {
      return iand((MappeableArrayContainer) x);
    } else if (x instanceof MappeableRunContainer) {
      return iand((MappeableRunContainer) x);
    }

    return iand((MappeableBitmapContainer) x);
  }

  /**
   * Computes the in-place bitwise AND of this container with another (intersection). The current
   * container is generally modified, whereas the provided container (x) is unaffected. May generate
   * a new container.
   *
   * @param x other container
   * @return aggregated container
   */
  public abstract MappeableContainer iand(MappeableRunContainer x);

  /**
   * Computes the in-place bitwise ANDNOT of this container with another (difference). The current
   * container is generally modified, whereas the provided container (x) is unaffected. May generate
   * a new container.
   *
   * @param x other container
   * @return aggregated container
   */
  public abstract MappeableContainer iandNot(MappeableArrayContainer x);

  /**
   * Computes the in-place bitwise ANDNOT of this container with another (difference). The current
   * container is generally modified, whereas the provided container (x) is unaffected. May generate
   * a new container.
   *
   * @param x other container
   * @return aggregated container
   */
  public abstract MappeableContainer iandNot(MappeableBitmapContainer x);

  protected MappeableContainer iandNot(MappeableContainer x) {
    if (x instanceof MappeableArrayContainer) {
      return iandNot((MappeableArrayContainer) x);
    } else if (x instanceof MappeableRunContainer) {
      return iandNot((MappeableRunContainer) x);
    }

    return iandNot((MappeableBitmapContainer) x);
  }

  /**
   * Computes the in-place bitwise ANDNOT of this container with another (difference). The current
   * container is generally modified, whereas the provided container (x) is unaffected. May generate
   * a new container.
   *
   * @param x other container
   * @return aggregated container
   */
  public abstract MappeableContainer iandNot(MappeableRunContainer x);

  /**
   * Computes the in-place bitwise NOT of this container (complement). Only those bits within the
   * range are affected. The current container is generally modified. May generate a new container.
   *
   * @param rangeStart beginning of range (inclusive); 0 is beginning of this container.
   * @param rangeEnd ending of range (exclusive)
   * @return (partially) completmented container
   */
  public abstract MappeableContainer inot(int rangeStart, int rangeEnd);

  /**
   * Returns true if the current container intersects the other container.
   *
   * @param x other container
   * @return whether they intersect
   */
  public abstract boolean intersects(MappeableArrayContainer x);

  /**
   * Returns true if the current container intersects the other container.
   *
   * @param x other container
   * @return whether they intersect
   */
  public abstract boolean intersects(MappeableBitmapContainer x);

  /**
   * Returns true if the current container intersects the other container.
   *
   * @param x other container
   * @return whether they intersect
   */
  public boolean intersects(MappeableContainer x) {
    if (x instanceof MappeableArrayContainer) {
      return intersects((MappeableArrayContainer) x);
    } else if (x instanceof MappeableBitmapContainer) {
      return intersects((MappeableBitmapContainer) x);
    }
    return intersects((MappeableRunContainer) x);
  }

  /**
   * Returns true if the current container intersects the other container.
   *
   * @param x other container
   * @return whether they intersect
   */
  public abstract boolean intersects(MappeableRunContainer x);

  /**
   * Computes the in-place bitwise OR of this container with another (union). The current container
   * is generally modified, whereas the provided container (x) is unaffected. May generate a new
   * container.
   *
   * @param x other container
   * @return aggregated container
   */
  public abstract MappeableContainer ior(MappeableArrayContainer x);

  /**
   * Computes the in-place bitwise OR of this container with another (union). The current container
   * is generally modified, whereas the provided container (x) is unaffected. May generate a new
   * container.
   *
   * @param x other container
   * @return aggregated container
   */
  public abstract MappeableContainer ior(MappeableBitmapContainer x);

  protected MappeableContainer ior(MappeableContainer x) {
    if (x instanceof MappeableArrayContainer) {
      return ior((MappeableArrayContainer) x);
    } else if (x instanceof MappeableRunContainer) {
      return ior((MappeableRunContainer) x);
    }

    return ior((MappeableBitmapContainer) x);
  }

  /**
   * Computes the in-place bitwise OR of this container with another (union). The current container
   * is generally modified, whereas the provided container (x) is unaffected. May generate a new
   * container.
   *
   * @param x other container
   * @return aggregated container
   */
  public abstract MappeableContainer ior(MappeableRunContainer x);

  /**
   * Remove chars in [begin,end) using an unsigned interpretation. May generate a new container.
   *
   * @param begin start of range (inclusive)
   * @param end end of range (exclusive)
   * @return the new container
   */
  public abstract MappeableContainer iremove(int begin, int end);

  protected abstract boolean isArrayBacked();

  /**
   * Computes the in-place bitwise XOR of this container with another (symmetric difference). The
   * current container is generally modified, whereas the provided container (x) is unaffected. May
   * generate a new container.
   *
   * @param x other container
   * @return aggregated container
   */
  public abstract MappeableContainer ixor(MappeableArrayContainer x);

  /**
   * Computes the in-place bitwise XOR of this container with another (symmetric difference). The
   * current container is generally modified, whereas the provided container (x) is unaffected. May
   * generate a new container.
   *
   * @param x other container
   * @return aggregated container
   */
  public abstract MappeableContainer ixor(MappeableBitmapContainer x);

  protected MappeableContainer ixor(MappeableContainer x) {
    if (x instanceof MappeableArrayContainer) {
      return ixor((MappeableArrayContainer) x);
    } else if (x instanceof MappeableRunContainer) {
      return ixor((MappeableRunContainer) x);
    }

    return ixor((MappeableBitmapContainer) x);
  }

  /**
   * Computes the in-place bitwise XOR of this container with another (symmetric difference). The
   * current container is generally modified, whereas the provided container (x) is unaffected. May
   * generate a new container.
   *
   * @param x other container
   * @return aggregated container
   */
  public abstract MappeableContainer ixor(MappeableRunContainer x);

  /**
   * Computes the in-place bitwise OR of this container with another (union). The current container
   * is generally modified, whereas the provided container (x) is unaffected. May generate a new
   * container. The resulting container may not track its cardinality correctly. The resulting
   * container may not track its cardinality correctly. This can be fixed as follows:
   * if(c.getCardinality()&lt;0) ((MappeableBitmapContainer)c).computeCardinality();
   *
   * @param x other container
   * @return aggregated container
   */
  public MappeableContainer lazyIOR(MappeableContainer x) {
    if (this instanceof MappeableArrayContainer) {
      if (x instanceof MappeableArrayContainer) {
        return ((MappeableArrayContainer) this).lazyor((MappeableArrayContainer) x);
      } else if (x instanceof MappeableBitmapContainer) {
        return ((MappeableBitmapContainer) x).lazyor((MappeableArrayContainer) this);
      }
      return ((MappeableRunContainer) x).lazyor((MappeableArrayContainer) this);
    } else if (this instanceof MappeableRunContainer) {
      if (x instanceof MappeableArrayContainer) {
        return ((MappeableRunContainer) this).ilazyor((MappeableArrayContainer) x);
      } else if (x instanceof MappeableBitmapContainer) {
        return ((MappeableBitmapContainer) x).lazyor((MappeableRunContainer) this);
      }
      return ior((MappeableRunContainer) x);
    } else {
      if (x instanceof MappeableArrayContainer) {
        return ((MappeableBitmapContainer) this).ilazyor((MappeableArrayContainer) x);
      } else if (x instanceof MappeableBitmapContainer) {
        return ((MappeableBitmapContainer) this).ilazyor((MappeableBitmapContainer) x);
      }
      return ((MappeableBitmapContainer) this).ilazyor((MappeableRunContainer) x);
    }
  }

  /**
   * Computes the bitwise OR of this container with another (union). This container as well as the
   * provided container are left unaffected. The resulting container may not track its cardinality
   * correctly. This can be fixed as follows: if(c.getCardinality()&lt;0)
   * ((MappeableBitmapContainer)c).computeCardinality();
   *
   * @param x other container
   * @return aggregated container
   */
  public MappeableContainer lazyOR(MappeableContainer x) {
    if (this instanceof MappeableArrayContainer) {
      if (x instanceof MappeableArrayContainer) {
        return ((MappeableArrayContainer) this).lazyor((MappeableArrayContainer) x);
      } else if (x instanceof MappeableBitmapContainer) {
        return ((MappeableBitmapContainer) x).lazyor((MappeableArrayContainer) this);
      }
      return ((MappeableRunContainer) x).lazyor((MappeableArrayContainer) this);
    } else if (this instanceof MappeableRunContainer) {
      if (x instanceof MappeableArrayContainer) {
        return ((MappeableRunContainer) this).lazyor((MappeableArrayContainer) x);
      } else if (x instanceof MappeableBitmapContainer) {
        return ((MappeableBitmapContainer) x).lazyor((MappeableRunContainer) this);
      }
      return or((MappeableRunContainer) x);
    } else {
      if (x instanceof MappeableArrayContainer) {
        return ((MappeableBitmapContainer) this).lazyor((MappeableArrayContainer) x);
      } else if (x instanceof MappeableBitmapContainer) {
        return ((MappeableBitmapContainer) this).lazyor((MappeableBitmapContainer) x);
      }
      return ((MappeableBitmapContainer) this).lazyor((MappeableRunContainer) x);
    }
  }

  /**
   * Create a new MappeableContainer containing at most maxcardinality integers.
   *
   * @param maxcardinality maximal cardinality
   * @return a new bitmap with cardinality no more than maxcardinality
   */
  public abstract MappeableContainer limit(int maxcardinality);

  /**
   * Computes the bitwise NOT of this container (complement). Only those bits within the range are
   * affected. The current container is left unaffected.
   *
   * @param rangeStart beginning of range (inclusive); 0 is beginning of this container.
   * @param rangeEnd ending of range (exclusive)
   * @return (partially) completmented container
   */
  public abstract MappeableContainer not(int rangeStart, int rangeEnd);

  abstract int numberOfRuns();

  /**
   * Computes the bitwise OR of this container with another (union). This container as well as the
   * provided container are left unaffected.
   *
   * @param x other container
   * @return aggregated container
   */
  public abstract MappeableContainer or(MappeableArrayContainer x);

  /**
   * Computes the bitwise OR of this container with another (union). This container as well as the
   * provided container are left unaffected.
   *
   * @param x other container
   * @return aggregated container
   */
  public abstract MappeableContainer or(MappeableBitmapContainer x);

  protected MappeableContainer or(MappeableContainer x) {
    if (x instanceof MappeableArrayContainer) {
      return or((MappeableArrayContainer) x);
    } else if (x instanceof MappeableRunContainer) {
      return or((MappeableRunContainer) x);
    }

    return or((MappeableBitmapContainer) x);
  }

  /**
   * Computes the bitwise OR of this container with another (union). This container as well as the
   * provided container are left unaffected.
   *
   * @param x other container
   * @return aggregated container
   */
  public abstract MappeableContainer or(MappeableRunContainer x);

  /**
   * Rank returns the number of integers that are smaller or equal to x (Rank(infinity) would be
   * GetCardinality()).
   *
   * @param lowbits upper limit
   *
   * @return the rank
   */
  public abstract int rank(char lowbits);

  /**
   * Return a new container with all chars in [begin,end) remove using an unsigned interpretation.
   *
   * @param begin start of range (inclusive)
   * @param end end of range (exclusive)
   * @return the new container
   */
  public abstract MappeableContainer remove(int begin, int end);

  /**
   * Remove the char from this container. May create a new container.
   *
   * @param x to be removed
   * @return New container
   */
  public abstract MappeableContainer remove(char x);

  /**
   * The output of a lazyOR or lazyIOR might be an invalid container, this should be called on it.
   *
   * @return a new valid container
   */
  public abstract MappeableContainer repairAfterLazy();

  /**
   * Convert to MappeableRunContainers, when the result is smaller. Overridden by
   * MappeableRunContainer to possibly switch from MappeableRunContainer to a smaller alternative.
   *
   * @return the new container
   */
  public abstract MappeableContainer runOptimize();

  /**
   * Return the jth value
   *
   * @param j index of the value
   *
   * @return the value
   */
  public abstract char select(int j);

  /**
   * Report the number of bytes required to serialize this container.
   *
   * @return the size in bytes
   */
  public abstract int serializedSizeInBytes();

  /**
   * Convert to a non-mappeable container.
   *
   * @return the non-mappeable container
   */
  public abstract Container toContainer();

  /**
   * If possible, recover wasted memory.
   */
  public abstract void trim();

  /**
   * Write just the underlying array.
   *
   * @param out output stream
   * @throws IOException in case of failure
   */
  protected abstract void writeArray(DataOutput out) throws IOException;

  /**
   * Write just the underlying array.
   *
   * @param buffer the buffer to write to
   */
  protected abstract void writeArray(ByteBuffer buffer);

  /**
   * Computes the bitwise XOR of this container with another (symmetric difference). This container
   * as well as the provided container are left unaffected.
   *
   * @param x other container
   * @return aggregated container
   */
  public abstract MappeableContainer xor(MappeableArrayContainer x);

  /**
   * Computes the bitwise XOR of this container with another (symmetric difference). This container
   * as well as the provided container are left unaffected.
   *
   * @param x other container
   * @return aggregated container
   */
  public abstract MappeableContainer xor(MappeableBitmapContainer x);

  protected MappeableContainer xor(MappeableContainer x) {
    if (x instanceof MappeableArrayContainer) {
      return xor((MappeableArrayContainer) x);
    } else if (x instanceof MappeableRunContainer) {
      return xor((MappeableRunContainer) x);
    }

    return xor((MappeableBitmapContainer) x);
  }

  /**
   * Computes the bitwise XOR of this container with another (symmetric difference). This container
   * as well as the provided container are left unaffected.
   *
   * @param x other parameter
   * @return aggregated container
   */
  public abstract MappeableContainer xor(MappeableRunContainer x);

  /**
   * Convert the current container to a BitmapContainer, if a conversion is needed.
   * If the container is already a bitmap, the container is returned unchanged.
   * @return a bitmap container
   */
  public abstract MappeableBitmapContainer toBitmapContainer();

  /**
   * Get the first integer held in the container
   * @return the first integer in the container
   * @throws NoSuchElementException if empty
   */
  public abstract int first();

  /**
   * Get the last integer held in the container
   * @return the last integer in the container
   * @throws NoSuchElementException if empty
   */
  public abstract int last();

  /**
   * Gets the first value greater than or equal to the lower bound, or -1 if no such value exists.
   * @param fromValue the lower bound (inclusive)
   * @return the next value
   */
  public abstract int nextValue(char fromValue);

  /**
   * Gets the last value less than or equal to the upper bound, or -1 if no such value exists.
   * @param fromValue the upper bound (inclusive)
   * @return the previous value
   */
  public abstract int previousValue(char fromValue);

  /**
   * Gets the first absent value greater than or equal to the lower bound.
   * @param fromValue the lower bound (inclusive)
   * @return the next absent value
   */
  public abstract int nextAbsentValue(char fromValue);

  /**
   * Gets the last value less than or equal to the upper bound.
   * @param fromValue the upper bound (inclusive)
   * @return the previous absent value
   */
  public abstract int previousAbsentValue(char fromValue);

  /**
   * Throw if the container is empty
   * @param condition a boolean expression
   * @throws NoSuchElementException if empty
   */
  protected void assertNonEmpty(boolean condition) {
    if (condition) {
      throw new NoSuchElementException("Empty " + getContainerName());
    }
  }

  /**
   * Validate the content of the container. Useful after deserialization.
   * @return true if the container is valid.
   */
  public abstract Boolean validate();
}
