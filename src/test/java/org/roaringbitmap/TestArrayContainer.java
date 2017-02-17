package org.roaringbitmap;

import com.google.common.primitives.Ints;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.*;
import static org.junit.Assert.assertFalse;

public class TestArrayContainer {

    @Test
    public void testConst() {
        ArrayContainer ac1 = new ArrayContainer(5, 15);
        short[] data = {5, 6, 7, 8, 9, 10, 11, 12, 13, 14};
        ArrayContainer ac2 = new ArrayContainer(data);
        Assert.assertEquals(ac1, ac2);
    }

    @Test
    public void testRemove() {
        ArrayContainer ac1 = new ArrayContainer(5, 15);
        ac1.remove((short)14);
        ArrayContainer ac2 = new ArrayContainer(5, 14);
        Assert.assertEquals(ac1, ac2);
    }

    @Test
    public void testToString() {
        ArrayContainer ac1 = new ArrayContainer(5, 15);
        Assert.assertEquals("{5,6,7,8,9,10,11,12,13,14}", ac1.toString());
    }

    @Test
    public void testIandNot() {
        ArrayContainer ac1 = new ArrayContainer(5, 15);
        ArrayContainer ac2 = new ArrayContainer(10, 15);
        BitmapContainer bc = new BitmapContainer(5, 10);
        ArrayContainer ac3 = ac1.iandNot(bc);
        Assert.assertEquals(ac2, ac3);
    }

    @Test
    public void testReverseArrayContainerShortIterator() {
        //Test Clone
        ArrayContainer ac1 = new ArrayContainer(5, 15);
        ReverseArrayContainerShortIterator rac1 = new ReverseArrayContainerShortIterator(ac1);
        ShortIterator rac2 = rac1.clone();
        Assert.assertEquals(asList(rac1), asList(rac2));
    }

    private static List<Integer> asList(ShortIterator ints) {
        int[] values = new int[10];
        int size = 0;
        while (ints.hasNext()) {
            if (!(size < values.length)) {
                values = Arrays.copyOf(values, values.length * 2);
            }
            values[size++] = ints.next();
        }
        return Ints.asList(Arrays.copyOf(values, size));
    }

    @Test
    public void roundtrip() throws Exception {
        Container ac = new ArrayContainer();
        ac = ac.add(1, 5);
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ObjectOutputStream oo = new ObjectOutputStream(bos)) {
            ac.writeExternal(oo);
        }
        Container ac2 = new ArrayContainer();
        final ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        ac2.readExternal(new ObjectInputStream(bis));

        assertEquals(4, ac2.getCardinality());
        for (int i = 1; i < 5; i++) {
            assertTrue(ac2.contains((short) i));
        }
    }

    @Test
    public void intersectsArray() throws Exception {
        Container ac = new ArrayContainer();
        ac = ac.add(1, 10);
        Container ac2 = new ArrayContainer();
        ac2 = ac2.add(5, 25);
        assertTrue(ac.intersects(ac2));
    }

    @Test
    public void orFullToRunContainer() {
        ArrayContainer ac = new ArrayContainer(0, 1 << 12);
        BitmapContainer half = new BitmapContainer(1 << 12, 1 << 16);
        Container result = ac.or(half);
        assertEquals(1 << 16, result.getCardinality());
        assertThat(result, instanceOf(RunContainer.class));
    }

    @Test
    public void orFullToRunContainer2() {
        ArrayContainer ac = new ArrayContainer(0, 1 << 15);
        ArrayContainer half = new ArrayContainer(1 << 15, 1 << 16);
        Container result = ac.or(half);
        assertEquals(1 << 16, result.getCardinality());
        assertThat(result, instanceOf(RunContainer.class));
    }

    @Test
    public void iandBitmap() throws Exception {
        Container ac = new ArrayContainer();
        ac = ac.add(1, 10);
        Container bc = new BitmapContainer();
        bc = bc.add(5, 25);
        ac.iand(bc);
        assertEquals(5, ac.getCardinality());
        for (int i = 5; i < 10; i++) {
            assertTrue(ac.contains((short) i));
        }
    }

    @Test
    public void iandRun() throws Exception {
        Container ac = new ArrayContainer();
        ac = ac.add(1, 10);
        Container rc = new RunContainer();
        rc = rc.add(5, 25);
        ac = ac.iand(rc);
        assertEquals(5, ac.getCardinality());
        for (int i = 5; i < 10; i++) {
            assertTrue(ac.contains((short) i));
        }
    }

    @Test
    public void addEmptyRange() {
        Container ac = new ArrayContainer();
        ac = ac.add(1,1);
        assertEquals(0, ac.getCardinality());
    }

    @Test(expected = IllegalArgumentException.class)
    public void addInvalidRange() {
        Container ac = new ArrayContainer();
        ac.add(13,1);
    }

    @Test
    public void iaddEmptyRange() {
        Container ac = new ArrayContainer();
        ac = ac.iadd(1,1);
        assertEquals(0, ac.getCardinality());
    }

    @Test(expected = IllegalArgumentException.class)
    public void iaddInvalidRange() {
        Container ac = new ArrayContainer();
        ac.iadd(13,1);
    }

    @Test
    public void clear() throws Exception {
        Container ac = new ArrayContainer();
        ac = ac.add(1, 10);
        ac.clear();
        assertEquals(0, ac.getCardinality());
    }

    @Test
    public void testLazyORFull() {
        ArrayContainer ac = new ArrayContainer(0, 1 << 15);
        ArrayContainer ac2 = new ArrayContainer(1 << 15, 1 << 16);
        Container rbc = ac.lazyor(ac2);
        assertEquals(-1, rbc.getCardinality());
        Container repaired = rbc.repairAfterLazy();
        assertEquals(1 << 16, repaired.getCardinality());
        assertThat(repaired, instanceOf(RunContainer.class));
    }

    @Test
    public void testFirstLast() {
        Container ac = new ArrayContainer();
        assertEquals(0, ac.first());
        assertEquals(0, ac.last());
        final int firstInclusive = 1;
        int lastExclusive = firstInclusive;
        for (int i = 0; i < ArrayContainer.DEFAULT_MAX_SIZE / 10; ++i) {
            int newLastExclusive = lastExclusive + 10;
            ac = ac.add(lastExclusive, newLastExclusive);
            assertEquals(firstInclusive, ac.first());
            assertEquals(newLastExclusive - 1, ac.last());
            lastExclusive = newLastExclusive;
        }
    }

    @Test
    public void testContainsBitmapContainer_EmptyContainsEmpty() {
        Container ac = new ArrayContainer();
        Container subset = new BitmapContainer();
        assertTrue(ac.contains(subset));
    }

    @Test
    public void testContainsBitmapContainer_IncludeProperSubset() {
        Container ac = new ArrayContainer().add(0,10);
        Container subset = new BitmapContainer().add(0,9);
        assertTrue(ac.contains(subset));
    }


    @Test
    public void testContainsBitmapContainer_IncludeProperSubsetDifferentStart() {
        Container ac = new ArrayContainer().add(0,10);
        Container subset = new BitmapContainer().add(1,9);
        assertTrue(ac.contains(subset));
    }

    @Test
    public void testContainsBitmapContainer_ExcludeShiftedSet() {
        Container ac = new ArrayContainer().add(0,10);
        Container subset = new BitmapContainer().add(2,12);
        assertFalse(ac.contains(subset));
    }

    @Test
    public void testContainsBitmapContainer_IncludeSelf() {
        Container ac = new ArrayContainer().add(0,10);
        Container subset = new BitmapContainer().add(0,10);
        assertTrue(ac.contains(subset));
    }

    @Test
    public void testContainsBitmapContainer_ExcludeSuperSet() {
        Container ac = new ArrayContainer().add(0,10);
        Container superset = new BitmapContainer().add(0,20);
        assertFalse(ac.contains(superset));
    }

    @Test
    public void testContainsBitmapContainer_ExcludeDisJointSet() {
        Container ac = new ArrayContainer().add(0,10);
        Container disjoint = new BitmapContainer().add(20, 40);
        assertFalse(ac.contains(disjoint));
        assertFalse(disjoint.contains(ac));
    }

    @Test
    public void testContainsRunContainer_EmptyContainsEmpty() {
        Container ac = new ArrayContainer();
        Container subset = new RunContainer();
        assertTrue(ac.contains(subset));
    }

    @Test
    public void testContainsRunContainer_IncludeProperSubset() {
        Container ac = new ArrayContainer().add(0,10);
        Container subset = new RunContainer().add(0,9);
        assertTrue(ac.contains(subset));
    }

    @Test
    public void testContainsRunContainer_IncludeSelf() {
        Container ac = new ArrayContainer().add(0,10);
        Container subset = new RunContainer().add(0,10);
        assertTrue(ac.contains(subset));
    }

    @Test
    public void testContainsRunContainer_ExcludeSuperSet() {
        Container ac = new ArrayContainer().add(0,10);
        Container superset = new RunContainer().add(0,20);
        assertFalse(ac.contains(superset));
    }

    @Test
    public void testContainsRunContainer_IncludeProperSubsetDifferentStart() {
        Container ac = new ArrayContainer().add(0,10);
        Container subset = new RunContainer().add(1,9);
        assertTrue(ac.contains(subset));
    }

    @Test
    public void testContainsRunContainer_ExcludeShiftedSet() {
        Container ac = new ArrayContainer().add(0,10);
        Container subset = new RunContainer().add(2,12);
        assertFalse(ac.contains(subset));
    }

    @Test
    public void testContainsRunContainer_ExcludeDisJointSet() {
        Container ac = new ArrayContainer().add(0,10);
        Container disjoint = new RunContainer().add(20, 40);
        assertFalse(ac.contains(disjoint));
        assertFalse(disjoint.contains(ac));
    }

    @Test
    public void testContainsArrayContainer_EmptyContainsEmpty() {
        Container ac = new ArrayContainer();
        Container subset = new ArrayContainer();
        assertTrue(ac.contains(subset));
    }

    @Test
    public void testContainsArrayContainer_IncludeProperSubset() {
        Container ac = new ArrayContainer().add(0,10);
        Container subset = new ArrayContainer().add(0,9);
        assertTrue(ac.contains(subset));
    }

    @Test
    public void testContainsArrayContainer_IncludeProperSubsetDifferentStart() {
        Container ac = new ArrayContainer().add(0,10);
        Container subset = new ArrayContainer().add(2,9);
        assertTrue(ac.contains(subset));
    }

    @Test
    public void testContainsArrayContainer_ExcludeShiftedSet() {
        Container ac = new ArrayContainer().add(0,10);
        Container shifted = new ArrayContainer().add(2,12);
        assertFalse(ac.contains(shifted));
    }

    @Test
    public void testContainsArrayContainer_IncludeSelf() {
        Container ac = new ArrayContainer().add(0,10);
        Container subset = new ArrayContainer().add(0,10);
        assertTrue(ac.contains(subset));
    }

    @Test
    public void testContainsArrayContainer_ExcludeSuperSet() {
        Container ac = new ArrayContainer().add(0,10);
        Container superset = new ArrayContainer().add(0,20);
        assertFalse(ac.contains(superset));
    }

    @Test
    public void testContainsArrayContainer_ExcludeDisJointSet() {
        Container ac = new ArrayContainer().add(0, 10);
        Container disjoint = new ArrayContainer().add(20, 40);
        assertFalse(ac.contains(disjoint));
        assertFalse(disjoint.contains(ac));
    }
}
