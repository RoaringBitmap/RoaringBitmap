package org.roaringbitmap.longlong;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class LongUtilsTest {

    public static Stream<Arguments> getByteData() {
        return Stream.of(
                Arguments.of(0x0000000000000000L, 0, (byte) 0x00),
                Arguments.of(0x0000000000000000L, 1, (byte) 0x00),
                Arguments.of(0x0000000000000000L, 2, (byte) 0x00),
                Arguments.of(0x0000000000000000L, 3, (byte) 0x00),
                Arguments.of(0x0000000000000000L, 4, (byte) 0x00),
                Arguments.of(0x0000000000000000L, 5, (byte) 0x00),
                Arguments.of(0x0000000000000000L, 6, (byte) 0x00),
                Arguments.of(0x0000000000000000L, 7, (byte) 0x00),
                Arguments.of(0x0123456789abcdefL, 0, (byte) 0x01),
                Arguments.of(0x0123456789abcdefL, 1, (byte) 0x23),
                Arguments.of(0x0123456789abcdefL, 2, (byte) 0x45),
                Arguments.of(0x0123456789abcdefL, 3, (byte) 0x67),
                Arguments.of(0x0123456789abcdefL, 4, (byte) 0x89),
                Arguments.of(0x0123456789abcdefL, 5, (byte) 0xab),
                Arguments.of(0x0123456789abcdefL, 6, (byte) 0xcd),
                Arguments.of(0x0123456789abcdefL, 7, (byte) 0xef),
                Arguments.of(0xFFFFFFFFFFFFFFFFL, 0, (byte) 0xFF),
                Arguments.of(0xFFFFFFFFFFFFFFFFL, 1, (byte) 0xFF),
                Arguments.of(0xFFFFFFFFFFFFFFFFL, 2, (byte) 0xFF),
                Arguments.of(0xFFFFFFFFFFFFFFFFL, 3, (byte) 0xFF),
                Arguments.of(0xFFFFFFFFFFFFFFFFL, 4, (byte) 0xFF),
                Arguments.of(0xFFFFFFFFFFFFFFFFL, 5, (byte) 0xFF),
                Arguments.of(0xFFFFFFFFFFFFFFFFL, 6, (byte) 0xFF),
                Arguments.of(0xFFFFFFFFFFFFFFFFL, 7, (byte) 0xFF)
        );

    }

    @ParameterizedTest
    @MethodSource("getByteData")
    void getByte(long value, int index, byte expected) {
        byte actual = LongUtils.getByte(value, index);
        assertEquals(expected, actual,  String.format("%016x", value) +  " at index " + index);
    }
}