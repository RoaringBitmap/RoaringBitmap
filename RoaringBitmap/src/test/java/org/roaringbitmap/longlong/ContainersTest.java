package org.roaringbitmap.longlong;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.ArrayContainer;
import org.roaringbitmap.BitmapContainer;
import org.roaringbitmap.Container;
import org.roaringbitmap.RunContainer;
import org.roaringbitmap.art.ContainerIterator;
import org.roaringbitmap.art.Containers;

public class ContainersTest {

  @Test
  public void test1() {
    Containers containers = new Containers();
    ArrayContainer arrayContainer = new ArrayContainer();
    long cidx = containers.addContainer(arrayContainer);
    Container container = containers.getContainer(cidx);
    Assertions.assertTrue(container == arrayContainer);
    BitmapContainer bitmapContainer = new BitmapContainer();
    cidx = containers.addContainer(bitmapContainer);
    container = containers.getContainer(cidx);
    Assertions.assertTrue(container == bitmapContainer);
    RunContainer runContainer = new RunContainer();
    cidx = containers.addContainer(runContainer);
    container = containers.getContainer(cidx);
    Assertions.assertTrue(container == runContainer);
    long containerSize = containers.getContainerSize();
    Assertions.assertTrue(containerSize == 3);
    ArrayContainer anotherArrayContainer = new ArrayContainer();
    containers.replace(cidx, anotherArrayContainer);
    container = containers.getContainer(cidx);
    Assertions.assertTrue(container != arrayContainer);
    Assertions.assertTrue(container == anotherArrayContainer);
    ContainerIterator containerIterator = containers.iterator();
    int i = 0;
    while (containerIterator.hasNext()) {
      containerIterator.next();
      i++;
    }
    Assertions.assertTrue(i == 3);
  }

  @Test
  public void test2() throws IOException {
    Containers containers = new Containers();
    ArrayContainer arrayContainer = new ArrayContainer();
    for (int i = 0; i < 20; i++) {
      arrayContainer.add((char) i);
    }
    long containerIdx = containers.addContainer(arrayContainer);
    long sizeInBytesL = containers.serializedSizeInBytes();
    int sizeInBytes = (int) sizeInBytesL;
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(sizeInBytes);
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    containers.serialize(dataOutputStream);
    Containers deseredOne = new Containers();
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(
        byteArrayOutputStream.toByteArray());
    DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);
    deseredOne.deserialize(dataInputStream);
    long containerSize = deseredOne.getContainerSize();
    Assertions.assertTrue(containerSize == 1);
    Container container = containers.getContainer(containerIdx);
    Assertions.assertTrue(container instanceof ArrayContainer);
    ArrayContainer deseredArrayContainer = (ArrayContainer) container;
    Assertions.assertTrue(deseredArrayContainer.getCardinality() == 20);
  }
}
