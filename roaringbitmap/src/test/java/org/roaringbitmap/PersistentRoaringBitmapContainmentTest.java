package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.roaringbitmap.PersistentRoaringBitmapChecks.assertSameInstances;
import static org.roaringbitmap.PersistentRoaringBitmapChecks.assertUnchanged;
import static org.roaringbitmap.PersistentRoaringBitmapChecks.assertValues;
import static org.roaringbitmap.PersistentRoaringBitmapChecks.backingArray;
import static org.roaringbitmap.PersistentRoaringBitmapChecks.containers;
import static org.roaringbitmap.PersistentRoaringBitmapChecks.instances;
import static org.roaringbitmap.PersistentRoaringBitmapChecks.snapshot;

import org.roaringbitmap.PersistentRoaringBitmapChecks.Model;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/** No container of a persistent bitmap is reachable from code outside copy-on-write. */
public class PersistentRoaringBitmapContainmentTest {

  /**
   * The members of RoaringBitmap, RoaringArray and the containers that PersistentRoaringBitmap
   * references, as owner.name:descriptor. Reads must write nothing, and the four delegated
   * mutators must write only the containers of the keys of their value or range. A change to this
   * list, or to the code behind any of these members, needs that reviewed again.
   */
  private static final String[] REVIEWED = {
    "Container.clone:()Lorg/roaringbitmap/Container;",
    "Container.first:()I",
    "Container.getCardinality:()I",
    "Container.isEmpty:()Z",
    "Container.limit:(I)Lorg/roaringbitmap/Container;",
    "Container.nextAbsentValue:(C)I",
    "Container.nextValue:(C)I",
    "Container.repairAfterLazy:()Lorg/roaringbitmap/Container;",
    "Container.runOptimize:()Lorg/roaringbitmap/Container;",
    "RoaringArray.<init>:()V",
    "RoaringArray.<init>:(I)V",
    "RoaringArray.<init>:([C[Lorg/roaringbitmap/Container;I)V",
    "RoaringArray.append:(CLorg/roaringbitmap/Container;)V",
    "RoaringArray.keys:[C",
    "RoaringArray.size:I",
    "RoaringArray.values:[Lorg/roaringbitmap/Container;",
    "RoaringBitmap.<init>:()V",
    "RoaringBitmap.<init>:(Lorg/roaringbitmap/RoaringArray;)V",
    "RoaringBitmap.add:(I)V",
    "RoaringBitmap.add:(JJ)V",
    "RoaringBitmap.and:(Lorg/roaringbitmap/RoaringBitmap;Lorg/roaringbitmap/RoaringBitmap;)Lorg/roaringbitmap/RoaringBitmap;",
    "RoaringBitmap.andCardinality:(Lorg/roaringbitmap/RoaringBitmap;Lorg/roaringbitmap/RoaringBitmap;)I",
    "RoaringBitmap.andNot:(Lorg/roaringbitmap/RoaringBitmap;Lorg/roaringbitmap/RoaringBitmap;)Lorg/roaringbitmap/RoaringBitmap;",
    "RoaringBitmap.andNotCardinality:(Lorg/roaringbitmap/RoaringBitmap;Lorg/roaringbitmap/RoaringBitmap;)I",
    "RoaringBitmap.bitmapOf:([I)Lorg/roaringbitmap/RoaringBitmap;",
    "RoaringBitmap.bitmapOfRange:(JJ)Lorg/roaringbitmap/RoaringBitmap;",
    "RoaringBitmap.clear:()V",
    "RoaringBitmap.clone:()Lorg/roaringbitmap/RoaringBitmap;",
    "RoaringBitmap.contains:(I)Z",
    "RoaringBitmap.deserialize:(Ljava/io/DataInput;)V",
    "RoaringBitmap.deserialize:(Ljava/nio/ByteBuffer;)V",
    "RoaringBitmap.equals:(Ljava/lang/Object;)Z",
    "RoaringBitmap.first:()I",
    "RoaringBitmap.firstSigned:()I",
    "RoaringBitmap.forEach:(Lorg/roaringbitmap/IntConsumer;)V",
    "RoaringBitmap.getBatchIterator:()Lorg/roaringbitmap/RoaringBatchIterator;",
    "RoaringBitmap.getCardinality:()I",
    "RoaringBitmap.getContainerCount:()I",
    "RoaringBitmap.getIntIterator:()Lorg/roaringbitmap/PeekableIntIterator;",
    "RoaringBitmap.getLongCardinality:()J",
    "RoaringBitmap.getLongSizeInBytes:()J",
    "RoaringBitmap.getReverseIntIterator:()Lorg/roaringbitmap/PeekableIntIterator;",
    "RoaringBitmap.getSignedIntIterator:()Lorg/roaringbitmap/PeekableIntIterator;",
    "RoaringBitmap.getSizeInBytes:()I",
    "RoaringBitmap.hasRunCompression:()Z",
    "RoaringBitmap.highLowContainer:Lorg/roaringbitmap/RoaringArray;",
    "RoaringBitmap.intersects:(JJ)Z",
    "RoaringBitmap.intersects:(Lorg/roaringbitmap/RoaringBitmap;Lorg/roaringbitmap/RoaringBitmap;)Z",
    "RoaringBitmap.isEmpty:()Z",
    "RoaringBitmap.iterator:()Ljava/util/Iterator;",
    "RoaringBitmap.last:()I",
    "RoaringBitmap.lastSigned:()I",
    "RoaringBitmap.nextAbsentValue:(I)J",
    "RoaringBitmap.nextValue:(I)J",
    "RoaringBitmap.or:(Lorg/roaringbitmap/RoaringBitmap;Lorg/roaringbitmap/RoaringBitmap;)Lorg/roaringbitmap/RoaringBitmap;",
    "RoaringBitmap.orCardinality:(Lorg/roaringbitmap/RoaringBitmap;Lorg/roaringbitmap/RoaringBitmap;)I",
    "RoaringBitmap.previousAbsentValue:(I)J",
    "RoaringBitmap.previousValue:(I)J",
    "RoaringBitmap.rangeCardinality:(JJ)J",
    "RoaringBitmap.rank:(I)I",
    "RoaringBitmap.rankLong:(I)J",
    "RoaringBitmap.remove:(I)V",
    "RoaringBitmap.remove:(JJ)V",
    "RoaringBitmap.select:(I)I",
    "RoaringBitmap.serialize:(Ljava/io/DataOutput;)V",
    "RoaringBitmap.serialize:(Ljava/nio/ByteBuffer;)V",
    "RoaringBitmap.serializedSizeInBytes:()I",
    "RoaringBitmap.toArray:()[I",
    "RoaringBitmap.toString:()Ljava/lang/String;",
    "RoaringBitmap.trim:()V",
    "RoaringBitmap.validate:()Ljava/lang/Boolean;",
    "RoaringBitmap.xor:(Lorg/roaringbitmap/RoaringBitmap;Lorg/roaringbitmap/RoaringBitmap;)Lorg/roaringbitmap/RoaringBitmap;",
    "RoaringBitmap.xorCardinality:(Lorg/roaringbitmap/RoaringBitmap;Lorg/roaringbitmap/RoaringBitmap;)I"
  };

  private static final Set<String> OWNERS =
      new HashSet<>(
          Arrays.asList(
              "org/roaringbitmap/RoaringBitmap",
              "org/roaringbitmap/RoaringArray",
              "org/roaringbitmap/Container",
              "org/roaringbitmap/ArrayContainer",
              "org/roaringbitmap/BitmapContainer",
              "org/roaringbitmap/RunContainer"));

  @Test
  public void classShape() {
    Class<PersistentRoaringBitmap> type = PersistentRoaringBitmap.class;
    assertTrue(Modifier.isFinal(type.getModifiers()));
    assertEquals(Object.class, type.getSuperclass());
    assertEquals(
        new HashSet<>(
            Arrays.asList(
                ImmutableBitmapDataProvider.class,
                BitmapDataProvider.class,
                Iterable.class,
                Cloneable.class)),
        new HashSet<>(Arrays.asList(type.getInterfaces())));
    for (Field field : type.getDeclaredFields()) {
      assertTrue(Modifier.isPrivate(field.getModifiers()), field.getName());
    }
    assertEquals(0, type.getDeclaredClasses().length);
  }

  @Test
  public void noMethodAcceptsOrReturnsInternals() {
    List<Class<?>> types = new ArrayList<>();
    for (Method method : PersistentRoaringBitmap.class.getDeclaredMethods()) {
      if (Modifier.isPrivate(method.getModifiers())) {
        continue;
      }
      List<Class<?>> signature = new ArrayList<>(Arrays.asList(method.getParameterTypes()));
      signature.add(method.getReturnType());
      for (Class<?> type : signature) {
        assertInternalFree(type, method.getName());
        boolean conversion =
            method.getName().equals("fromBitmap") || method.getName().equals("toRoaringBitmap");
        assertTrue(type != RoaringBitmap.class || conversion, method.toString());
        types.add(type);
      }
    }
    for (Constructor<?> constructor : PersistentRoaringBitmap.class.getDeclaredConstructors()) {
      if (!Modifier.isPrivate(constructor.getModifiers())) {
        assertEquals(0, constructor.getParameterCount(), constructor.toString());
      }
    }
    assertTrue(types.contains(RoaringBitmap.class));
  }

  private static void assertInternalFree(Class<?> type, String method) {
    assertFalse(Container.class.isAssignableFrom(type), method);
    assertFalse(type == RoaringArray.class, method);
    assertFalse(ContainerPointer.class.isAssignableFrom(type), method);
    assertFalse(
        type.isArray() && Container.class.isAssignableFrom(type.getComponentType()), method);
  }

  @Test
  public void referencedMembersAreTheReviewedOnes() throws IOException {
    assertEquals(
        new TreeSet<>(Arrays.asList(REVIEWED)),
        referencedMembers(PersistentRoaringBitmap.class),
        "PersistentRoaringBitmap references other members: review them, then update REVIEWED");
  }

  /** The members of the OWNERS types that a class file references, from its constant pool. */
  static TreeSet<String> referencedMembers(Class<?> type) throws IOException {
    try (InputStream stream = type.getResourceAsStream(type.getSimpleName() + ".class")) {
      DataInputStream in = new DataInputStream(stream);
      in.readInt(); // magic
      in.readUnsignedShort(); // minor version
      in.readUnsignedShort(); // major version
      int count = in.readUnsignedShort();
      int[] tags = new int[count];
      String[] utf = new String[count];
      int[][] refs = new int[count][];
      for (int i = 1; i < count; i++) {
        tags[i] = in.readUnsignedByte();
        switch (tags[i]) {
          case 1: // Utf8
            utf[i] = in.readUTF();
            break;
          case 3: // Integer
          case 4: // Float
            in.readInt();
            break;
          case 5: // Long
          case 6: // Double
            in.readLong();
            i++; // takes two entries
            break;
          case 7: // Class
          case 8: // String
          case 16: // MethodType
          case 19: // Module
          case 20: // Package
            refs[i] = new int[] {in.readUnsignedShort()};
            break;
          case 15: // MethodHandle
            in.readUnsignedByte();
            in.readUnsignedShort();
            break;
          default: // Fieldref, Methodref, InterfaceMethodref, NameAndType, Dynamic, InvokeDynamic
            refs[i] = new int[] {in.readUnsignedShort(), in.readUnsignedShort()};
        }
      }
      TreeSet<String> members = new TreeSet<>();
      for (int i = 1; i < count; i++) {
        if (tags[i] == 9 || tags[i] == 10 || tags[i] == 11) {
          String owner = utf[refs[refs[i][0]][0]];
          int[] nameAndType = refs[refs[i][1]];
          if (OWNERS.contains(owner)) {
            members.add(
                owner.substring(owner.lastIndexOf('/') + 1)
                    + "."
                    + utf[nameAndType[0]]
                    + ":"
                    + utf[nameAndType[1]]);
          }
        }
      }
      return members;
    }
  }

  @Test
  public void readsWriteNothing() throws IOException {
    PersistentRoaringBitmap x = PersistentRoaringBitmapTest.varied();
    PersistentRoaringBitmap y = PersistentRoaringBitmapTest.varied();
    for (PersistentRoaringBitmap bitmap : new PersistentRoaringBitmap[] {x, y}) {
      Map<Container, PersistentRoaringBitmapChecks.Storage> storage = snapshot(x, y);
      Container[] values = instances(bitmap);
      read(bitmap, y);
      assertUnchanged(storage, "reads");
      assertSameInstances(values, bitmap, "reads");
    }
  }

  private static void read(PersistentRoaringBitmap bitmap, PersistentRoaringBitmap other)
      throws IOException {
    long sum = 0;
    for (long probe : PersistentRoaringBitmapTest.probes(bitmap)) {
      int x = (int) probe;
      sum += bitmap.contains(x) ? 1 : 0;
      sum += bitmap.rank(x) + bitmap.rankLong(x);
      sum += bitmap.nextValue(x) + bitmap.previousValue(x) + bitmap.nextAbsentValue(x);
      sum += bitmap.rangeCardinality(probe, probe + 1);
      sum += bitmap.intersects(probe, probe + 1) ? 1 : 0;
      sum += bitmap.contains(probe, probe + 1) ? 1 : 0;
    }
    sum += bitmap.select(3) + bitmap.first() + bitmap.last();
    sum += bitmap.firstSigned() + bitmap.lastSigned();
    sum += bitmap.getCardinality() + bitmap.getLongCardinality() + bitmap.getContainerCount();
    sum += bitmap.getSizeInBytes() + bitmap.getLongSizeInBytes() + bitmap.serializedSizeInBytes();
    sum += bitmap.toArray().length + bitmap.stream().count() + bitmap.reverseStream().count();
    sum += bitmap.hashCode() + bitmap.toString().length();
    sum += bitmap.validate() ? 1 : 0;
    sum += bitmap.hasRunCompression() ? 1 : 0;
    sum += bitmap.equals(other) ? 1 : 0;
    sum += bitmap.isEmpty() ? 1 : 0;
    PeekableIntIterator it = bitmap.getIntIterator();
    it.advanceIfNeeded(70_000);
    sum += it.peekNext();
    for (IntIterator i :
        new IntIterator[] {bitmap.getSignedIntIterator(), bitmap.getReverseIntIterator()}) {
      while (i.hasNext()) {
        sum += i.next();
      }
    }
    BatchIterator batches = bitmap.getBatchIterator();
    int[] buffer = new int[100];
    while (batches.hasNext()) {
      sum += batches.nextBatch(buffer);
    }
    for (int value : bitmap) {
      sum += value;
    }
    final long[] each = {0};
    bitmap.forEach((IntConsumer) v -> each[0] += v);
    sum += PersistentRoaringBitmap.andCardinality(bitmap, other);
    sum += PersistentRoaringBitmap.orCardinality(bitmap, other);
    sum += PersistentRoaringBitmap.xorCardinality(bitmap, other);
    sum += PersistentRoaringBitmap.andNotCardinality(bitmap, other);
    sum += PersistentRoaringBitmap.intersects(bitmap, other) ? 1 : 0;
    bitmap.serialize(ByteBuffer.allocate(bitmap.serializedSizeInBytes()));
    bitmap.serialize(new DataOutputStream(new ByteArrayOutputStream()));
    sum += bitmap.toRoaringBitmap().getCardinality();
    assertTrue(sum + each[0] != 0);
  }

  @Test
  public void conversionsShareNothing() {
    PersistentRoaringBitmap x = PersistentRoaringBitmapTest.varied();
    Model model = PersistentRoaringBitmapTest.variedModel();
    Map<Container, PersistentRoaringBitmapChecks.Storage> storage = snapshot(x);

    RoaringBitmap exported = x.toRoaringBitmap();
    assertDisjoint(containers(x), exported.highLowContainer);
    exported.add(0L, 3L << 16);
    exported.remove(3L << 16, 1L << 32);
    exported.runOptimize();
    assertUnchanged(storage, "after writes to the exported bitmap");
    assertValues(model, x, "after writes to the exported bitmap");

    RoaringBitmap source = PersistentRoaringBitmapTest.varied().toRoaringBitmap();
    RoaringBitmap sourceCopy = source.clone();
    PersistentRoaringBitmap imported = PersistentRoaringBitmap.fromBitmap(source);
    assertDisjoint(containers(imported), source.highLowContainer);
    Map<Container, PersistentRoaringBitmapChecks.Storage> importedStorage = snapshot(imported);
    source.add(0L, 3L << 16);
    source.remove(3L << 16, 1L << 32);
    assertUnchanged(importedStorage, "after writes to the source");
    assertValues(model, imported, "after writes to the source");
    source = sourceCopy.clone();
    imported = PersistentRoaringBitmap.fromBitmap(source);
    imported.add(0L, 3L << 16);
    imported.remove(3L << 16, 1L << 32);
    assertEquals(sourceCopy, source);
  }

  private static void assertDisjoint(RoaringArray a, RoaringArray b) {
    Map<Object, Boolean> arrays = new IdentityHashMap<>();
    for (int i = 0; i < a.size; i++) {
      arrays.put(a.values[i], true);
      arrays.put(backingArray(a.values[i]), true);
    }
    for (int i = 0; i < b.size; i++) {
      assertFalse(arrays.containsKey(b.values[i]));
      assertFalse(arrays.containsKey(backingArray(b.values[i])));
    }
    assertNotSame(a.keys, b.keys);
    assertNotSame(a.values, b.values);
  }

  @Test
  public void importsLazySources() throws IOException {
    RoaringBitmap lazy = RoaringBitmap.bitmapOf(1, 70_000);
    lazy.naivelazyor(RoaringBitmap.bitmapOf(1, 70_000)); // leaves lazy one-value bitmap containers
    Container first = lazy.highLowContainer.getContainerAtIndex(0);
    assertTrue(first instanceof BitmapContainer && first.getCardinality() < 0);
    PersistentRoaringBitmap imported = PersistentRoaringBitmap.fromBitmap(lazy);
    assertValues(Model.of(1, 70_000), imported, "imported");
    assertTrue(imported.validate());
    PersistentRoaringBitmap decoded = new PersistentRoaringBitmap();
    decoded.deserialize(ByteBuffer.wrap(PersistentRoaringBitmapTest.serialize(imported)));
    assertValues(Model.of(1, 70_000), decoded, "decoded");
    // the source is neither repaired nor otherwise written
    assertTrue(lazy.highLowContainer.getContainerAtIndex(0) == first);
    assertTrue(first.getCardinality() < 0);
  }

  @Test
  public void importDropsEmptyContainers() {
    RoaringBitmap withEmpty = RoaringBitmap.bitmapOf(1, 70_000);
    withEmpty.highLowContainer.getContainerAtIndex(0).remove((char) 1); // array, now empty
    PersistentRoaringBitmap imported = PersistentRoaringBitmap.fromBitmap(withEmpty);
    assertValues(Model.of(70_000), imported, "imported");
  }
}
