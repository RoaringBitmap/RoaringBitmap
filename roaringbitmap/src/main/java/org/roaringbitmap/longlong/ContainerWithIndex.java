package org.roaringbitmap.longlong;

import org.roaringbitmap.Container;
import org.roaringbitmap.art.ContainerHolder;

public class ContainerWithIndex implements ContainerHolder {

  private final HighLowContainer containers;
  private Container container;
  private long containerIdx;

  public ContainerWithIndex(HighLowContainer containers, Container container, long containerIdx) {
    this.containers = containers;
    this.container = container;
    this.containerIdx = containerIdx;
  }

  public Container getContainer() {
    return container;
  }

  @Override
  public void setContainer(Container container) {
    containers.replaceContainer(containerIdx, container);
    this.container = container;
  }
}
