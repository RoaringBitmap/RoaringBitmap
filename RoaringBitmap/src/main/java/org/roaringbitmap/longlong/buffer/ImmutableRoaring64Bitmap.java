/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */
package org.roaringbitmap.longlong.buffer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.roaringbitmap.BitmapDataProvider;
import org.roaringbitmap.ImmutableBitmapDataProvider;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.roaringbitmap.longlong.Roaring64Bitmap;

public class ImmutableRoaring64Bitmap {

	public ImmutableRoaring64Bitmap(ByteBuffer byteBuffer) {
		this(List.of(byteBuffer));
	}
	
	public ImmutableRoaring64Bitmap(List<ByteBuffer> byteBuffers) {
		ByteBuffer firstBuffer = byteBuffers.get(0);
		boolean signedLongs = firstBuffer.get() > 0;

	    int nbHighs = firstBuffer.getInt();
	    Map<Integer, ImmutableBitmapDataProvider> highToBitmap;
	    // Other NavigableMap may accept a target capacity
	    if (signedLongs) {
	      highToBitmap = new TreeMap<>();
	    } else {
	      highToBitmap = new TreeMap<>(RoaringIntPacking.unsignedComparator());
	    }

	    for (int i = 0; i < nbHighs; i++) {
	      int high = firstBuffer.get();
	      
	      highToBitmap.put(high, new ImmutableRoaringBitmap(firstBuffer));
	    }

	    resetPerfHelpers();
	}
}
