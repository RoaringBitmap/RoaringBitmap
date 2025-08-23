package org.roaringbitmap.art;

import org.roaringbitmap.Container;

public interface ContainerHolder {
    Container getContainer();
    void setContainer(Container container);
}
