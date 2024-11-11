package org.roaringbitmap.longlong;

import org.roaringbitmap.ArrayContainer;
import org.roaringbitmap.BitmapContainer;
import org.roaringbitmap.Container;
import org.roaringbitmap.RunContainer;
import org.roaringbitmap.art.ContainerIterator;
import org.roaringbitmap.art.Containers;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ContainersTest {

  @Test
  public void test1() {
    Containers containers = new Containers();
    ArrayContainer arrayContainer = new ArrayContainer();
    long cidx = containers.addContainer(arrayContainer);
    long cidx0 = cidx;
    Container container = containers.getContainer(cidx);
    Assertions.assertTrue(container == arrayContainer);
    BitmapContainer bitmapContainer = new BitmapContainer();
    cidx = containers.addContainer(bitmapContainer);
    long cidx1 = cidx;
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
      if (i == 0) {
        long currentContainerIdx = containerIterator.getCurrentContainerIdx();
        Assertions.assertEquals(cidx0, currentContainerIdx);
        RunContainer rc = new RunContainer(new char[] {23, 24}, 1);
        containerIterator.replace(rc);
      }
      i++;
    }
    Assertions.assertTrue(i == 3);
    Container replacedContainer = containers.getContainer(cidx0);
    Assertions.assertEquals(23, replacedContainer.select(0));
    ArrayContainer arrayContainer1 = new ArrayContainer(new char[] {10, 20, 30});
    containers.replace(cidx1, arrayContainer1);
    replacedContainer = containers.getContainer(cidx1);
    Assertions.assertTrue(replacedContainer == arrayContainer1);
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
    ByteArrayInputStream byteArrayInputStream =
        new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
    DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);
    deseredOne.deserialize(dataInputStream);
    long containerSize = deseredOne.getContainerSize();
    Assertions.assertEquals(1, containerSize);
    Container container = containers.getContainer(containerIdx);
    Assertions.assertTrue(container instanceof ArrayContainer);
    ArrayContainer deseredArrayContainer = (ArrayContainer) container;
    Assertions.assertEquals(20, deseredArrayContainer.getCardinality());

    ByteBuffer byteBuffer = ByteBuffer.allocate(sizeInBytes).order(ByteOrder.LITTLE_ENDIAN);
    containers.serialize(byteBuffer);
    byteBuffer.flip();
    Containers deserBBOne = new Containers();
    deserBBOne.deserialize(byteBuffer);
    containerSize = deserBBOne.getContainerSize();
    Assertions.assertEquals(1, containerSize);
    container = deserBBOne.getContainer(containerIdx);
    Assertions.assertTrue(container instanceof ArrayContainer);
    Assertions.assertEquals(20, container.getCardinality());
  }
}
