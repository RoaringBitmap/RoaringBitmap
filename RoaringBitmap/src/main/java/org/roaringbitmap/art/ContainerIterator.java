package org.roaringbitmap.art;

import java.util.Iterator;
import org.roaringbitmap.Container;

public class ContainerIterator implements Iterator<Container> {

  private Containers containers;
  private Iterator<Container[]> containerArrIte;
  private Container[] currentSecondLevelArr;
  private int currentSecondLevelArrSize;
  private int currentSecondLevelArrIdx;
  private int currentFistLevelArrIdx;
  private boolean currentSecondLevelArrIteOver;
  private Container currentContainer;
  private boolean consumedCurrent;

  /**
   * construct a containers iterator
   * @param containers the containers
   */
  public ContainerIterator(Containers containers) {
    this.containers = containers;
    containerArrIte = containers.getContainerArrays().iterator();
    this.currentSecondLevelArrIteOver = true;
    this.consumedCurrent = true;
    this.currentFistLevelArrIdx = -1;
    this.currentSecondLevelArrIdx = 0;
    this.currentSecondLevelArrSize = 0;
  }

  @Override
  public boolean hasNext() {
    boolean hasContainer = containers.getContainerSize() > 0;
    if (!hasContainer) {
      return false;
    }
    if (!consumedCurrent) {
      return true;
    }
    boolean foundOneContainer = false;
    while (currentSecondLevelArrIteOver && containerArrIte.hasNext()) {
      currentSecondLevelArr = containerArrIte.next();
      currentFistLevelArrIdx++;
      this.currentSecondLevelArrIdx = 0;
      this.currentSecondLevelArrSize = currentSecondLevelArr.length;
      while (currentSecondLevelArrIdx < currentSecondLevelArrSize) {
        Container container = currentSecondLevelArr[currentSecondLevelArrIdx];
        if (container != null) {
          currentContainer = container;
          consumedCurrent = false;
          this.currentSecondLevelArrIteOver = false;
          foundOneContainer = true;
          currentSecondLevelArrIdx++;
          break;
        } else {
          currentSecondLevelArrIdx++;
        }
      }
    }
    if (!currentSecondLevelArrIteOver && !foundOneContainer) {
      while (currentSecondLevelArrIdx < currentSecondLevelArrSize) {
        Container container = currentSecondLevelArr[currentSecondLevelArrIdx];
        if (container != null) {
          currentContainer = container;
          consumedCurrent = false;
          this.currentSecondLevelArrIteOver = false;
          currentSecondLevelArrIdx++;
          foundOneContainer = true;
          break;
        } else {
          currentSecondLevelArrIdx++;
        }
      }
      if (currentSecondLevelArrIdx == currentSecondLevelArrSize) {
        this.currentSecondLevelArrIteOver = true;
      }
    }
    return foundOneContainer;
  }

  @Override
  public Container next() {
    consumedCurrent = true;
    return currentContainer;
  }

  public long getCurrentContainerIdx() {
    int secondLevelArrIdx = currentSecondLevelArrIdx - 1;
    return Containers.toContainerIdx(currentFistLevelArrIdx, secondLevelArrIdx);
  }

  /**
   * replace current container
   * @param container the fresh container which is to replace the current old one
   */
  public void replace(Container container) {
    int secondLevelArrIdx = currentSecondLevelArrIdx - 1;
    containers.replace(currentFistLevelArrIdx, secondLevelArrIdx, container);
  }
}
