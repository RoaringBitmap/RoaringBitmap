package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

class CharRangeFillerTest {

  @Test
  void deallocate() {
    CharRangeFiller.allocate((char) 12345);
    CharRangeFiller.deallocate();
    assertEquals(0, CharRangeFiller.length());
  }

  @Test
  void completeRangeAllocated() {
    CharRangeFiller.deallocate();
    CharRangeFiller.allocate(Character.MAX_VALUE);
    assertEquals(Character.MAX_VALUE + 1, CharRangeFiller.length());
  }

  @Test
  void allocateCompletely() {
    CharRangeFiller.deallocate();
    CharRangeFiller.allocateCompletely();
    assertEquals(Character.MAX_VALUE + 1, CharRangeFiller.length());
  }

  @Test
  void partOfRangeAllocated() {
    CharRangeFiller.deallocate();
    CharRangeFiller.allocate((char) 12345);
    assertEquals(12345 + 1, CharRangeFiller.length());
  }

  @Test
  void allocatedNullCharOnly() {
    CharRangeFiller.deallocate();
    CharRangeFiller.allocate((char) 0);
    assertEquals(1, CharRangeFiller.length());
  }

  @Test
  void fillFromMoreThanTo() {
    char[] filled = new char[200];
    CharRangeFiller.fill(filled, 0, 200, 100);
    assertArrayEquals(new char[200], filled);
  }

  @Test
  void fillMoreThanSizeArray() {
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> {
          CharRangeFiller.fill(new char[200], 0, 0, 201);
        });
  }

  @Test
  void fillFromNegativeIndex() {
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> {
          CharRangeFiller.fill(new char[200], -10, 0, 100);
        });
  }

  @Test
  void fillIndexesAroundCharacterRange() {
    // test included even CharRangeFiller is not used with char array of such length
    char[] expected = new char[Character.MAX_VALUE + 200];
    expected[Character.MAX_VALUE] = 1;
    expected[Character.MAX_VALUE + 1] = 2;
    expected[Character.MAX_VALUE + 2] = 3;
    char[] filled = new char[Character.MAX_VALUE + 200];
    CharRangeFiller.fill(filled, Character.MAX_VALUE, 1, 4);
    assertArrayEquals(expected, filled);
  }

  @Test
  void fillFromIndexOutsideCharacterRange() {
    // test included even CharRangeFiller is not used with char array of such length
    char[] expected = new char[Character.MAX_VALUE + 200];
    expected[Character.MAX_VALUE + 1] = 1;
    expected[Character.MAX_VALUE + 2] = 2;
    char[] filled = new char[Character.MAX_VALUE + 200];
    CharRangeFiller.fill(filled, Character.MAX_VALUE + 1, 1, 3);
    assertArrayEquals(expected, filled);
  }

  @Test
  void fillFromNegativeValue() {
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> {
          CharRangeFiller.fill(new char[200], 0, -1, 100);
        });
  }

  @Test
  void fillFromValueOutOfCharRange() {
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> {
          CharRangeFiller.fill(new char[200], 0, Character.MAX_VALUE + 1, 100);
        });
  }

  @Test
  void fillToNegativeValue() {
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> {
          CharRangeFiller.fill(new char[200], 0, 0, -1);
        });
  }

  @Test
  void fillToValueOutOfCharRange() {
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> {
          CharRangeFiller.fill(new char[200], 0, Character.MAX_VALUE, Character.MAX_VALUE + 2);
        });
  }

  @Test
  void fillToTooLargeIndex() {
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> {
          CharRangeFiller.fill(new char[200], 300, 0, 100);
        });
  }

  @Test
  void fillEmptyRange() {
    CharRangeFiller.allocate((char) (300));
    char[] filled = new char[300];
    CharRangeFiller.fill(filled, 0, 200, 200);
    assertArrayEquals(new char[300], filled);
  }

  @Test
  void fillEmptyRangeFromIsGreaterThanTo() {
    CharRangeFiller.allocate((char) (300));
    char[] filled = new char[300];
    CharRangeFiller.fill(filled, 0, 201, 200);
    assertArrayEquals(new char[300], filled);
  }

  public static Stream<Arguments> outsideAllocatedRange() {
    List<Arguments> cases = new ArrayList<>();
    int[] allocated = new int[] {1000, Character.MAX_VALUE - 1, Character.MAX_VALUE};
    for (int i = 0; i < allocated.length; i++) {
      int ALLOCATED = allocated[i];
      int IN_RANGE = ALLOCATED - 100;
      int OUT_OF_RANGE = ALLOCATED + 100;
      int[] pos = new int[] {IN_RANGE, ALLOCATED, OUT_OF_RANGE};
      for (int j = 0; j < pos.length; j++) {
        int POS = pos[j];
        cases.add(Arguments.of(ALLOCATED, POS, IN_RANGE, ALLOCATED - 1));
        cases.add(Arguments.of(ALLOCATED, POS, IN_RANGE, ALLOCATED));
        if (ALLOCATED + 1 <= Character.MAX_VALUE) {
          cases.add(Arguments.of(ALLOCATED, POS, IN_RANGE, ALLOCATED + 1));
        }
        if (OUT_OF_RANGE <= Character.MAX_VALUE) {
          cases.add(Arguments.of(ALLOCATED, POS, ALLOCATED - 1, OUT_OF_RANGE));
          cases.add(Arguments.of(ALLOCATED, POS, ALLOCATED + 0, OUT_OF_RANGE));
          cases.add(Arguments.of(ALLOCATED, POS, ALLOCATED + 1, OUT_OF_RANGE));
          cases.add(Arguments.of((char) ALLOCATED, POS, IN_RANGE, OUT_OF_RANGE));
        }
        if (OUT_OF_RANGE + 10 <= Character.MAX_VALUE) {
          cases.add(Arguments.of(ALLOCATED, POS, OUT_OF_RANGE, OUT_OF_RANGE + 10));
        }
      }
    }
    for (int pos = 0; pos < 3; pos++) {
      for (int allocated2 = 0; allocated2 < 3; allocated2++) {
        for (int from = 0; from < 2; from++) {
          for (int to = from + 1; to < 3; to++) {
            cases.add(Arguments.of(allocated2, pos, from, to));
          }
        }
      }
    }
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("outsideAllocatedRange")
  void fillOutsideAllocatedRange(int allocated, int pos, int from, int to) {
    CharRangeFiller.allocate((char) allocated);
    char[] expected = new char[allocated + pos + 10];
    char[] tested = new char[allocated + pos + 10];
    CharRangeFiller.fillIteratively(expected, pos, from, to);
    CharRangeFiller.fill(tested, pos, from, to);
    assertArrayEquals(expected, tested);
  }

  @Test
  void fillIterativelyAndUsingArraycopyComparison() {
    CharRangeFiller.allocate(Character.MAX_VALUE);
    for (int offset = 0; offset < 100; offset += 10) {
      for (int from = 0; from < 10; from += 3) {
        for (int to = from + CharRangeFiller.USE_ARRAYCOPY_MIN_SIZE; to < from + 100; to += 15) {
          char[] a = new char[210];
          char[] b = new char[210];
          CharRangeFiller.fillIteratively(a, offset, from, to);
          CharRangeFiller.fill(b, offset, from, to);
          assertArrayEquals(
              a,
              b,
              "dest offset: "
                  + offset
                  + ", range: "
                  + from
                  + "-"
                  + to
                  + "\r\n"
                  + Arrays.toString(a)
                  + "\r\n"
                  + Arrays.toString(b));
        }
      }
    }
  }
}
