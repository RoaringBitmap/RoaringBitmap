package org.roaringbitmap;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import com.google.common.primitives.Chars;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestReverseIteratorsOfContainers {

  @ParameterizedTest
  @ArgumentsSource(TestReverseIteratorsOfContainers.ContainerProvider.class)
  public void testSkips(Converter converter) {
    final Random source = new Random(0xcb000a2b9b5bdfb6l);
    final char[] data = takeSortedAndDistinct(source, 45000);
    Container container = new ArrayContainer(data);

    container = converter.apply(container);

    PeekableCharIterator pii = container.getCharIterator();
    for (int i = 0; i < data.length; ++i) {
      pii.advanceIfNeeded(data[i]);
      assertEquals((int) data[i], (int) pii.peekNext());
    }
    pii = container.getCharIterator();
    for (int i = 0; i < data.length; ++i) {
      pii.advanceIfNeeded(data[i]);
      assertEquals(data[i], pii.next());
    }
    pii = container.getCharIterator();
    for (int i = 1; i < data.length; ++i) {
      pii.advanceIfNeeded(data[i - 1]);
      pii.next();
      assertEquals(data[i], pii.peekNext());
    }
    container.getCharIterator().advanceIfNeeded((char) -1);// should not crash
  }

  @ParameterizedTest
  @ArgumentsSource(TestReverseIteratorsOfContainers.ContainerProvider.class)
  public void testSkipsDense(Converter converter) {
    Container container = new ArrayContainer();
    int n = 30000;
    for (char i = 0; i < n; ++i) {
      char c = (char) (2 * i);
      container = container.add(c);
    }

    container = converter.apply(container);

    for (char i = 0; i < n; ++i) {
      PeekableCharIterator pii = container.getCharIterator();
      char c = (char) (2 * i);
      pii.advanceIfNeeded(c);
      assertEquals(pii.peekNext(), c);
      assertEquals(pii.next(), c);
    }
  }

  @ParameterizedTest
  @ArgumentsSource(TestReverseIteratorsOfContainers.ContainerProvider.class)
  public void testSkipsRun(Converter converter) {
    Container container = new ArrayContainer();
    container = container.add(4, 60000);

    container = converter.apply(container);
    for (int i = 4; i < 60000; ++i) {
      PeekableCharIterator pii = container.getCharIterator();
      pii.advanceIfNeeded((char) i);
      assertEquals(pii.peekNext(), i);
      assertEquals(pii.next(), i);
    }
  }

  @ParameterizedTest
  @ArgumentsSource(TestReverseIteratorsOfContainers.ContainerProvider.class)
  public void testEmptySkips(Converter converter) {
    Container container = new ArrayContainer();
    container = converter.apply(container);
    PeekableCharIterator it = container.getCharIterator();
    it.advanceIfNeeded((char) 0);
  }

  @ParameterizedTest
  @ArgumentsSource(TestReverseIteratorsOfContainers.ContainerProvider.class)
  public void testSkipsReverse(Converter converter) {
    final Random source = new Random(0xcb000a2b9b5bdfb6l);
    final char[] data = takeSortedAndDistinct(source, 45000);
    Container container = new ArrayContainer(data);
    container = converter.apply(container);

    PeekableCharIterator pii = container.getReverseCharIterator();
    for (int i = data.length - 1; i >= 0; --i) {
      pii.advanceIfNeeded(data[i]);
      assertEquals(data[i], pii.peekNext());
    }
    pii = container.getReverseCharIterator();
    for (int i = data.length - 1; i >= 0; --i) {
      pii.advanceIfNeeded(data[i]);
      assertEquals(data[i], pii.next());
    }
    pii = container.getReverseCharIterator();
    for (int i = data.length - 2; i >= 0; --i) {
      pii.advanceIfNeeded(data[i + 1]);
      pii.next();
      assertEquals(data[i], pii.peekNext());
    }
    container.getReverseCharIterator().advanceIfNeeded((char) -1);// should not crash
  }

  @ParameterizedTest
  @ArgumentsSource(TestReverseIteratorsOfContainers.ContainerProvider.class)
  public void testSkipsDenseReverse(Converter converter) {
    Container container = new ArrayContainer();
    char n = 30000;
    for (char i = 0; i < n; ++i) {
      char c = (char) (2 * i);
      container = container.add(c);
    }

    container = converter.apply(container);

    for (int i = n - 1; i >= 0; --i) {
      PeekableCharIterator pii = container.getReverseCharIterator();
      char c = (char) (2 * i);
      pii.advanceIfNeeded(c);
      assertEquals(2 * i, pii.peekNext());
      assertEquals(2 * i, pii.next());
    }
  }

  @ParameterizedTest
  @ArgumentsSource(TestReverseIteratorsOfContainers.ContainerProvider.class)
  public void testSkipsRunReverse(Converter converter) {

    Container container = new ArrayContainer();
    container = container.add(4, 60000);

    container = converter.apply(container);
    for (int i = 59999; i >= 4; --i) {
      PeekableCharIterator pii = container.getReverseCharIterator();
      pii.advanceIfNeeded((char) i);
      assertEquals(pii.peekNext(), i);
      assertEquals(pii.next(), i);
    }

  }

  @ParameterizedTest
  @ArgumentsSource(TestReverseIteratorsOfContainers.ContainerProvider.class)
  public void testEmptySkipsReverse(Converter converter) {
    Container container = new ArrayContainer();
    container = converter.apply(container);
    PeekableCharIterator it = container.getReverseCharIterator();
    it.advanceIfNeeded((char) 0);
  }

  private static char[] takeSortedAndDistinct(Random source, int count) {
    LinkedHashSet<Character> chars = new LinkedHashSet<>(count);
    for (int size = 0; size < count; size++) {
      char next;
      do {
        next = (char) Math.abs(source.nextInt(Character.MAX_VALUE));
      } while (!chars.add(next));
    }
    char[] unboxed = Chars.toArray(chars);
    Arrays.sort(unboxed);
    return unboxed;
  }

  static interface Converter extends Function<Container, Container> {

  }

  static class ArrayContainerConverter implements Converter {

    @Override
    public Container apply(Container container) {
      char[] toReturn = new char[container.getCardinality()];
      int offset = 0;
      PeekableCharIterator it = container.getCharIterator();

      while (it.hasNext()) {
        toReturn[offset++] = it.next();
      }

      return new ArrayContainer(toReturn);
    }

    @Override
    public String toString() {
      return "ArrayContainer";
    }

  }

  static class BitmapContainerConverter implements Converter {

    @Override
    public Container apply(Container container) {
      container = container.toBitmapContainer();
      if (!(container instanceof BitmapContainer)) {
        throw new RuntimeException("Not a bitmap");
      }
      return container;
    }

    @Override
    public String toString() {
      return "BitmapContainer";
    }

  }

  static class RunContainerConverter implements Converter {

    @Override
    public Container apply(Container container) {
      PeekableCharIterator it = container.getCharIterator();
      container = new RunContainer();
      while (it.hasNext()) {
        container = container.add(it.next());
      }
      if (!(container instanceof RunContainer)) {
        throw new RuntimeException("Not a runContainer");
      }
      return container;
    }

    @Override
    public String toString() {
      return "RunContainer";
    }

  }

  static class ContainerProvider implements ArgumentsProvider {

    Stream<Converter> streams = Stream.of(new ArrayContainerConverter(), new BitmapContainerConverter(), new RunContainerConverter());

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
      return streams.map(f -> Arguments.of(f));
    }
  }

}
