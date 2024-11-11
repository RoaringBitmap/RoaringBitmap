package org.roaringbitmap;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.KryoDataInput;
import com.esotericsoftware.kryo.io.KryoDataOutput;
import com.esotericsoftware.kryo.io.Output;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class KryoTest {
  public static Kryo createKryo() {
    Kryo kryo = new Kryo();
    kryo.setRegistrationRequired(false);
    kryo.register(RoaringBitmap.class, new RoaringSerializer());
    return kryo;
  }

  public static void writeRoaringToFile(
      File f, RoaringBitmap roaring, Serializer<RoaringBitmap> serializer)
      throws FileNotFoundException {
    Kryo kryo = createKryo();
    Output kryoOutputMap = new Output(new FileOutputStream(f));
    kryo.writeObjectOrNull(kryoOutputMap, roaring, serializer);
    kryoOutputMap.flush();
    kryoOutputMap.close();
  }

  public static RoaringBitmap readRoaringFromFile(File f, Serializer<RoaringBitmap> serializer)
      throws FileNotFoundException {
    Kryo kryo = createKryo();
    Input inputMap = new Input(new FileInputStream(f));
    RoaringBitmap roaring = kryo.readObjectOrNull(inputMap, RoaringBitmap.class, serializer);
    inputMap.close();
    return roaring;
  }

  @Test
  public void roaringTest() throws IOException {
    RoaringSerializer serializer = new RoaringSerializer();
    RoaringBitmap roaringDense = new RoaringBitmap();
    for (int i = 0; i < 100_000; i++) {
      roaringDense.add(i);
    }
    File tmpfiledense = File.createTempFile("roaring_dense", "bin");
    tmpfiledense.deleteOnExit();
    writeRoaringToFile(tmpfiledense, roaringDense, serializer);
    RoaringBitmap denseRoaringFromFile = readRoaringFromFile(tmpfiledense, serializer);
    assertEquals(denseRoaringFromFile, roaringDense);

    RoaringBitmap roaringSparse = new RoaringBitmap();
    for (int i = 0; i < 100_000; i++) {
      if (i % 11 == 0) {
        roaringSparse.add(i);
      }
    }
    File tmpfilesparse = File.createTempFile("roaring_sparse", "bin");
    writeRoaringToFile(tmpfilesparse, roaringSparse, serializer);
    RoaringBitmap sparseRoaringFromFile = readRoaringFromFile(tmpfilesparse, serializer);
    assertEquals(sparseRoaringFromFile, roaringSparse);
  }
}

class RoaringSerializer extends Serializer<RoaringBitmap> {
  @Override
  public void write(Kryo kryo, Output output, RoaringBitmap bitmap) {
    try {
      bitmap.serialize(new KryoDataOutput(output));
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException();
    }
  }

  @Override
  public RoaringBitmap read(Kryo kryo, Input input, Class<? extends RoaringBitmap> type) {
    RoaringBitmap bitmap = new RoaringBitmap();
    try {
      bitmap.deserialize(new KryoDataInput(input));
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException();
    }
    return bitmap;
  }
}
