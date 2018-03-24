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
import java.util.NoSuchElementException;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.*;

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
        ac1.add((short) -3);
        ac1.add((short) -17);
        Assert.assertEquals("{5,6,7,8,9,10,11,12,13,14,65519,65533}", ac1.toString());
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
    public void iaddSanityTest() {
        Container ac = new ArrayContainer();
        ac = ac.iadd(10,20);
        //insert disjoint at end
        ac = ac.iadd(30,70);
        //insert disjoint between
        ac = ac.iadd(25,26);
        //insert disjoint at start
        ac = ac.iadd(1,2);
        //insert overlap at end
        ac = ac.iadd(60,80);
        //insert overlap between
        ac = ac.iadd(10,30);
        //insert overlap at start
        ac = ac.iadd(1,20);
        assertEquals(79, ac.getCardinality());
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

    @Test(expected = NoSuchElementException.class)
    public void testFirst_Empty() {
        new ArrayContainer().first();
    }

    @Test(expected = NoSuchElementException.class)
    public void testLast_Empty() {
        new ArrayContainer().last();
    }

    @Test
    public void testFirstLast() {
        Container rc = new ArrayContainer();
        final int firstInclusive = 1;
        int lastExclusive = firstInclusive;
        for (int i = 0; i < 1 << 16 - 10; ++i) {
            int newLastExclusive = lastExclusive + 10;
            rc = rc.add(lastExclusive, newLastExclusive);
            assertEquals(firstInclusive, rc.first());
            assertEquals(newLastExclusive - 1, rc.last());
            lastExclusive = newLastExclusive;
        }
    }

    @Test
    public void testContainsBitmapContainer_ExcludeShiftedSet() {
        Container ac = new ArrayContainer().add(0,10);
        Container subset = new BitmapContainer().add(2,12);
        assertFalse(ac.contains(subset));
    }

    @Test
    public void testContainsBitmapContainer_AlwaysFalse() {
        Container ac = new ArrayContainer().add(0,10);
        Container subset = new BitmapContainer().add(0,10);
        assertFalse(ac.contains(subset));
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
  
    @Test
    public void iorNotIncreaseCapacity() {
      Container ac1 = new ArrayContainer();
      Container ac2 = new ArrayContainer();
      ac1.add((short) 128);
      ac1.add((short) 256);
      ac2.add((short) 1024);
      
      ac1.ior(ac2);
      assertTrue(ac1.contains((short) 128));
      assertTrue(ac1.contains((short) 256));
      assertTrue(ac1.contains((short) 1024));
    }
    
    @Test
    public void iorIncreaseCapacity() {
      Container ac1 = new ArrayContainer();
      Container ac2 = new ArrayContainer();
      ac1.add((short) 128);
      ac1.add((short) 256);
      ac1.add((short) 512);
      ac1.add((short) 513);
      ac2.add((short) 1024);
      
      ac1.ior(ac2);
      assertTrue(ac1.contains((short) 128));
      assertTrue(ac1.contains((short) 256));
      assertTrue(ac1.contains((short) 512));
      assertTrue(ac1.contains((short) 513));
      assertTrue(ac1.contains((short) 1024));
    }
    
    @Test
    public void iorSanityCheck() {
      Container ac = new ArrayContainer().add(0, 10);
      Container disjoint = new ArrayContainer().add(20, 40);
      ac.ior(disjoint);
      assertTrue(ac.contains(disjoint));
    }

    @Test
    public void testIntersectsWithRange() {
        Container container = new ArrayContainer().add(0, 10);
        assertTrue(container.intersects(0, 1));
        assertTrue(container.intersects(0, 101));
        assertTrue(container.intersects(0, lower16Bits(-1)));
        assertFalse(container.intersects(11, lower16Bits(-1)));
    }


    @Test
    public void testIntersectsWithRange2() {
        Container container = new ArrayContainer().add(lower16Bits(-50), lower16Bits(-10));
        assertFalse(container.intersects(0, 1));
        assertTrue(container.intersects(0, lower16Bits(-40)));
        assertFalse(container.intersects(lower16Bits(-100), lower16Bits(-55)));
        assertFalse(container.intersects(lower16Bits(-9), lower16Bits(-1)));
        assertTrue(container.intersects(11, 1 << 16));
    }


    @Test
    public void testIntersectsWithRange3() {
        Container container = new ArrayContainer()
                .add((short) 1)
                .add((short) 300)
                .add((short) 1024);
        assertTrue(container.intersects(0, 300));
        assertTrue(container.intersects(1, 300));
        assertFalse(container.intersects(2, 300));
        assertFalse(container.intersects(2, 299));
        assertTrue(container.intersects(0, lower16Bits(-1)));
        assertFalse(container.intersects(1025, 1 << 16));
    }


    @Test
    public void testContainsRange() {
        Container ac = new ArrayContainer().add(20, 100);
        assertFalse(ac.contains(1, 21));
        assertFalse(ac.contains(1, 19));
        assertTrue(ac.contains(20, 100));
        assertTrue(ac.contains(20, 99));
        assertTrue(ac.contains(21, 100));
        assertFalse(ac.contains(21, 101));
        assertFalse(ac.contains(19, 99));
        assertFalse(ac.contains(190, 9999));
    }

    @Test
    public void testContainsRange2() {
        Container ac = new ArrayContainer()
                .add((short)1).add((short)10)
                .add(20, 100);
        assertFalse(ac.contains(1, 21));
        assertFalse(ac.contains(1, 20));
        assertTrue(ac.contains(1, 2));
    }

    @Test
    public void testContainsRangeUnsigned() {
        Container ac = new ArrayContainer().add(1 << 15, 1 << 8 | 1 << 15);
        assertTrue(ac.contains(1 << 15, 1 << 8 | 1 << 15));
        assertTrue(ac.contains(1 + (1 << 15), (1 << 8 | 1 << 15) - 1));
        assertFalse(ac.contains(1 + (1 << 15), (1 << 8 | 1 << 15) + 1));
        assertFalse(ac.contains((1 << 15) - 1, (1 << 8 | 1 << 15) - 1));
        assertFalse(ac.contains(0, 1 << 15));
        assertFalse(ac.contains(1 << 8 | 1 << 15 | 1, 1 << 16));
    }

    private static int lower16Bits(int x) {
        return ((short)x) & 0xFFFF;
    }
}
