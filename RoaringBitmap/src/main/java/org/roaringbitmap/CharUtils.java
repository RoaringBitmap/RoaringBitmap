package org.roaringbitmap;

// TODO: Delete if unused
public class CharUtils {

    /**
     * Returns the char nearest in value to value.
     *
     * @param value any int value
     * @return the same value cast to char if it is in the range of the char type,
     *         Character.MAX_VALUE if it is too large,
     *         or Character.MIN_VALUE if it is too small.
     */
    public static char saturatedCast(int value) {
        if (Character.MAX_VALUE < value) {
            return Character.MAX_VALUE;
        } else if (value < 0) {
            return Character.MIN_VALUE;
        } else {
            return (char) value;
        }
    }

    /**
     * Returns the next higher char value, if it exists, otherwise Character.MAX_VALUE.
     *
     * @param value any char value
     * @return the next higher char value, if any.
     */
    public static char saturatedInc(char value) {
        if (value == Character.MAX_VALUE) {
            return Character.MAX_VALUE;
        } else {
            return (char) (value + 1);
        }
    }
}
