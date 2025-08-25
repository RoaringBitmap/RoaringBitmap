package org.roaringbitmap.longlong;

public class Keys {
    /**
     * Compares two unsigned byte arrays for order.
     * Result is a negative integer, zero, or a positive integer
     * as the first array is less than, equal to, or greater than the second.
     *
     * @return result of comparing
     */
    public static int compareUnsigned(byte[] a, byte[] b) {
        if (a == null) {
            return b == null ? 0 : 1;
        }
        if (b == null) {
            return -1;
        }
        for (int i = 0; i < Math.min(a.length, b.length); i++) {
            int aVal = a[i] & 0xff;
            int bVal = b[i] & 0xff;
            if (aVal != bVal) {
                return Integer.compare(aVal, bVal);
            }
        }
        return Integer.compare(a.length, b.length);
    }
}
