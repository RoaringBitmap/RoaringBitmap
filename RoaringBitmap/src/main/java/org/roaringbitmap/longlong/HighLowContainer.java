package org.roaringbitmap.longlong;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.roaringbitmap.Container;
import org.roaringbitmap.art.Art;
import org.roaringbitmap.art.ContainerIterator;
import org.roaringbitmap.art.Containers;
import org.roaringbitmap.art.KeyIterator;
import org.roaringbitmap.art.LeafNodeIterator;
import org.roaringbitmap.art.Node;

public class HighLowContainer {

  private Art art;
  private Containers containers;
  private static final byte EMPTY_TAG = 0;
  private static final byte NOT_EMPTY_TAG = 1;

  public HighLowContainer() {
    art = new Art();
    containers = new Containers();
  }

  public Container getContainer(long containerIdx) {
    return containers.getContainer(containerIdx);
  }

  /**
   * search the container by the given 48 bit high part key
   * @param highPart the 48 bit key array
   * @return the container with the container index
   */
  public ContainerWithIndex searchContainer(byte[] highPart) {
    long containerIdx = art.findByKey(highPart);
    if (containerIdx < 0) {
      return null;
    } else {
      Container container = containers.getContainer(containerIdx);
      return new ContainerWithIndex(container, containerIdx);
    }
  }

  /**
   * put the 48 bit key and the corresponding container
   * @param highPart the 48 bit key
   * @param container the container
   */
  public void put(byte[] highPart, Container container) {
    long containerIdx = containers.addContainer(container);
    art.insert(highPart, containerIdx);
  }

  /**
   * Attempt to remove the container that corresponds to the 48 bit key.
   * @param highPart the 48 bit key
   */
  public void remove(byte[] highPart) {
    long containerIdx = art.remove(highPart);
    if (containerIdx != Node.ILLEGAL_IDX) {
      containers.remove(containerIdx);
    }
  }

  /**
   * get a container iterator
   * @return a container iterator
   */
  public ContainerIterator containerIterator() {
    return containers.iterator();
  }

  /**
   * get a key iterator
   * @return a key iterator
   */
  public KeyIterator highKeyIterator() {
    return art.iterator(containers);
  }

  /**
   * @param reverse true ：ascending order, false: descending order
   * @return the leaf node iterator
   */
  public LeafNodeIterator highKeyLeafNodeIterator(boolean reverse) {
    return art.leafNodeIterator(reverse, containers);
  }

  /**
   * replace the specified position one with a fresh container
   * @param containerIdx the position of the container
   * @param container the fresh container
   */
  public void replaceContainer(long containerIdx, Container container) {
    containers.replace(containerIdx, container);
  }

  /**
   * whether it's empty
   * @return true: empty,false: not empty
   */
  public boolean isEmpty() {
    return art.isEmpty();
  }

  /**
   * serialize into the ByteBuffer in little endian
   * @param buffer the ByteBuffer should be large enough to hold the data
   * @throws IOException indicate exception happened
   */
  public void serialize(ByteBuffer buffer) throws IOException {
    ByteBuffer byteBuffer = buffer.order() == LITTLE_ENDIAN ? buffer
        : buffer.slice().order(LITTLE_ENDIAN);
    if (art.isEmpty()) {
      byteBuffer.put(EMPTY_TAG);
      return;
    } else {
      byteBuffer.put(NOT_EMPTY_TAG);
    }
    art.serializeArt(byteBuffer);
    containers.serialize(byteBuffer);
    if (byteBuffer != buffer) {
      buffer.position(buffer.position() + byteBuffer.position());
    }
  }

  /**
   * deserialize from the input ByteBuffer in little endian
   * @param buffer the ByteBuffer
   * @throws IOException indicate exception happened
   */
  public void deserialize(ByteBuffer buffer) throws IOException {
    ByteBuffer byteBuffer = buffer.order() == LITTLE_ENDIAN ? buffer
        : buffer.slice().order(LITTLE_ENDIAN);
    clear();
    byte emptyTag = byteBuffer.get();
    if (emptyTag == EMPTY_TAG) {
      return;
    }
    art.deserializeArt(byteBuffer);
    containers.deserialize(byteBuffer);
  }

  /**
   * serialized size in bytes
   * @return the size in bytes
   */
  public long serializedSizeInBytes() {
    long totalSize = 1L;
    if (art.isEmpty()) {
      return totalSize;
    }
    totalSize += art.serializeSizeInBytes();
    totalSize += containers.serializedSizeInBytes();
    return totalSize;
  }

  /**
   * serialize into the byte stream
   * @param dataOutput the output stream
   * @throws IOException indicate the io exception happened
   */
  public void serialize(DataOutput dataOutput) throws IOException {
    if (art.isEmpty()) {
      dataOutput.writeByte(EMPTY_TAG);
      return;
    } else {
      dataOutput.writeByte(NOT_EMPTY_TAG);
    }
    art.serializeArt(dataOutput);
    containers.serialize(dataOutput);
  }

  /**
   * deserialize from the input byte stream
   * @param dataInput the input byte stream
   * @throws IOException indicate the io exception happened
   */
  public void deserialize(DataInput dataInput) throws IOException {
    clear();
    byte emptyTag = dataInput.readByte();
    if (emptyTag == EMPTY_TAG) {
      return;
    }
    art.deserializeArt(dataInput);
    containers.deserialize(dataInput);
  }

  /**
   * clear to be a empty fresh one
   */
  public void clear() {
    art = new Art();
    containers = new Containers();
  }

  @Override
  public int hashCode() {
    int hashCode = 0;
    KeyIterator keyIterator = highKeyIterator();
    while (keyIterator.hasNext()) {
      byte[] key = keyIterator.next();
      int result = 1;
      for (byte element : key) {
        result = 31 * result + element;
      }
      long containerIdx = keyIterator.currentContainerIdx();
      Container container = containers.getContainer(containerIdx);
      hashCode = 31 * hashCode + result + container.hashCode();
    }
    return hashCode;
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof HighLowContainer) {
      HighLowContainer otherHighLowContainer = (HighLowContainer) object;
      if (this.art.getKeySize() != otherHighLowContainer.art.getKeySize()) {
        return false;
      }
      KeyIterator thisKeyIte = this.highKeyIterator();
      while (thisKeyIte.hasNext()) {
        byte[] thisHigh = thisKeyIte.next();
        long containerIdx = thisKeyIte.currentContainerIdx();
        Container thisContainer = this.getContainer(containerIdx);
        ContainerWithIndex containerWithIndex = otherHighLowContainer.searchContainer(thisHigh);
        if (containerWithIndex == null) {
          return false;
        }
        Container otherContainer = containerWithIndex.getContainer();
        if (!thisContainer.equals(otherContainer)) {
          return false;
        }
      }
      return true;
    }
    return false;
  }
}
