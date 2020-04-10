package org.roaringbitmap.buffer;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Execution(ExecutionMode.CONCURRENT)
public class TestFastAggregation {

  private static ImmutableRoaringBitmap toMapped(MutableRoaringBitmap r) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    try {
      r.serialize(dos);
      dos.close();
    } catch (IOException e) {
      throw new RuntimeException(e.toString());
    }
    ByteBuffer bb = ByteBuffer.wrap(bos.toByteArray());
    return new ImmutableRoaringBitmap(bb);
  }

  @Test
  public void testNaiveAnd() {
    int[] array1 = {39173, 39174};
    int[] array2 = {39173, 39174, 39175, 39176, 39177, 39178, 39179};
    int[] array3 = {39173, 39174};
    int[] array4 = {};
    MutableRoaringBitmap data1 = MutableRoaringBitmap.bitmapOf(array1);
    MutableRoaringBitmap data2 = MutableRoaringBitmap.bitmapOf(array2);
    MutableRoaringBitmap data3 = MutableRoaringBitmap.bitmapOf(array3);
    MutableRoaringBitmap data4 = MutableRoaringBitmap.bitmapOf(array4);
    assertEquals(data3, BufferFastAggregation.naive_and(data1, data2));
    assertEquals(new MutableRoaringBitmap(), BufferFastAggregation.naive_and(data4));
  }

  @Test
  public void testPriorityQueueOr() {
    int[] array1 = {1232, 3324, 123, 43243, 1322, 7897, 8767};
    int[] array2 = {39173, 39174, 39175, 39176, 39177, 39178, 39179};
    int[] array3 =
        {1232, 3324, 123, 43243, 1322, 7897, 8767, 39173, 39174, 39175, 39176, 39177, 39178, 39179};
    int[] array4 = {};
    ArrayList<MutableRoaringBitmap> data5 = new ArrayList<>();
    ArrayList<MutableRoaringBitmap> data6 = new ArrayList<>();
    MutableRoaringBitmap data1 = MutableRoaringBitmap.bitmapOf(array1);
    MutableRoaringBitmap data2 = MutableRoaringBitmap.bitmapOf(array2);
    MutableRoaringBitmap data3 = MutableRoaringBitmap.bitmapOf(array3);
    MutableRoaringBitmap data4 = MutableRoaringBitmap.bitmapOf(array4);
    data5.add(data1);
    data5.add(data2);
    assertEquals(data3, BufferFastAggregation.priorityqueue_or(data1, data2));
    assertEquals(data1, BufferFastAggregation.priorityqueue_or(data1));
    assertEquals(data1, BufferFastAggregation.priorityqueue_or(data1, data4));
    assertEquals(data3, BufferFastAggregation.priorityqueue_or(data5.iterator()));
    assertEquals(new MutableRoaringBitmap(),
        BufferFastAggregation.priorityqueue_or(data6.iterator()));
    data6.add(data1);
    assertEquals(data1, BufferFastAggregation.priorityqueue_or(data6.iterator()));
  }

  @Test
  public void testPriorityQueueXor() {
    assertThrows(IllegalArgumentException.class, () -> {
      int[] array1 = {1232, 3324, 123, 43243, 1322, 7897, 8767};
      int[] array2 = {39173, 39174, 39175, 39176, 39177, 39178, 39179};
      int[] array3 =
              {1232, 3324, 123, 43243, 1322, 7897, 8767, 39173, 39174, 39175, 39176, 39177, 39178, 39179};
      ImmutableRoaringBitmap data1 = MutableRoaringBitmap.bitmapOf(array1);
      ImmutableRoaringBitmap data2 = MutableRoaringBitmap.bitmapOf(array2);
      ImmutableRoaringBitmap data3 = MutableRoaringBitmap.bitmapOf(array3);
      assertEquals(data3, BufferFastAggregation.priorityqueue_xor(data1, data2));
      BufferFastAggregation.priorityqueue_xor(data1);
    });
  }
  

  @Test
  public void testNaiveAndMapped() {
    int[] array1 = {39173, 39174};
    int[] array2 = {39173, 39174, 39175, 39176, 39177, 39178, 39179};
    int[] array3 = {39173, 39174};
    int[] array4 = {};
    ImmutableRoaringBitmap data1 = toMapped(MutableRoaringBitmap.bitmapOf(array1));
    ImmutableRoaringBitmap data2 = toMapped(MutableRoaringBitmap.bitmapOf(array2));
    ImmutableRoaringBitmap data3 = toMapped(MutableRoaringBitmap.bitmapOf(array3));
    ImmutableRoaringBitmap data4 = toMapped(MutableRoaringBitmap.bitmapOf(array4));
    assertEquals(data3, BufferFastAggregation.naive_and(data1, data2));
    assertEquals(new MutableRoaringBitmap(), BufferFastAggregation.naive_and(data4));
  }

  @Test
  public void testPriorityQueueOrMapped() {
    int[] array1 = {1232, 3324, 123, 43243, 1322, 7897, 8767};
    int[] array2 = {39173, 39174, 39175, 39176, 39177, 39178, 39179};
    int[] array3 =
        {1232, 3324, 123, 43243, 1322, 7897, 8767, 39173, 39174, 39175, 39176, 39177, 39178, 39179};
    int[] array4 = {};
    ArrayList<ImmutableRoaringBitmap> data5 = new ArrayList<>();
    ArrayList<ImmutableRoaringBitmap> data6 = new ArrayList<>();
    ImmutableRoaringBitmap data1 = toMapped(MutableRoaringBitmap.bitmapOf(array1));
    ImmutableRoaringBitmap data2 = toMapped(MutableRoaringBitmap.bitmapOf(array2));
    ImmutableRoaringBitmap data3 = toMapped(MutableRoaringBitmap.bitmapOf(array3));
    ImmutableRoaringBitmap data4 = toMapped(MutableRoaringBitmap.bitmapOf(array4));
    data5.add(data1);
    data5.add(data2);
    assertEquals(data3, BufferFastAggregation.priorityqueue_or(data1, data2));
    assertEquals(data1, BufferFastAggregation.priorityqueue_or(data1));
    assertEquals(data1, BufferFastAggregation.priorityqueue_or(data1, data4));
    assertEquals(data3, BufferFastAggregation.priorityqueue_or(data5.iterator()));
    assertEquals(new MutableRoaringBitmap(),
        BufferFastAggregation.priorityqueue_or(data6.iterator()));
    data6.add(data1);
    assertEquals(data1, BufferFastAggregation.priorityqueue_or(data6.iterator()));
  }
  
  public void testBigOrMapped() {
    MutableRoaringBitmap rb1 = new MutableRoaringBitmap();
    MutableRoaringBitmap rb2 = new MutableRoaringBitmap();
    MutableRoaringBitmap rb3 = new MutableRoaringBitmap();
    for(int k = 100; k < 10000; ++k) {
      if((k % 3) == 0) {
        rb1.add(k);
      }
      if((k % 3) == 1) {
        rb2.add(k);
      }
      if((k % 3) == 2) {
        rb3.add(k);
      }
    }
    ImmutableRoaringBitmap data1 = toMapped(rb1);
    ImmutableRoaringBitmap data2 = toMapped(rb2);
    ImmutableRoaringBitmap data3 = toMapped(rb3);
    MutableRoaringBitmap mrb = data1.clone().toMutableRoaringBitmap();
    mrb.add(100L,10000L);
    assertEquals(mrb, BufferFastAggregation.or(data1,data2,data3));
  }

  @Test
  public void testPriorityQueueXorMapped() {
    assertThrows(IllegalArgumentException.class, () -> {
      int[] array1 = {1232, 3324, 123, 43243, 1322, 7897, 8767};
      int[] array2 = {39173, 39174, 39175, 39176, 39177, 39178, 39179};
      int[] array3 =
              {1232, 3324, 123, 43243, 1322, 7897, 8767, 39173, 39174, 39175, 39176, 39177, 39178, 39179};
      ImmutableRoaringBitmap data1 = toMapped(MutableRoaringBitmap.bitmapOf(array1));
      ImmutableRoaringBitmap data2 = toMapped(MutableRoaringBitmap.bitmapOf(array2));
      ImmutableRoaringBitmap data3 = toMapped(MutableRoaringBitmap.bitmapOf(array3));
      assertEquals(data3, BufferFastAggregation.priorityqueue_xor(data1, data2));
      BufferFastAggregation.priorityqueue_xor(data1);
    });
  }
}
