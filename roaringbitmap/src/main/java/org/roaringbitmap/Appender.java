package org.roaringbitmap;


interface Appender<C, T extends BitmapDataProvider & AppendableStorage<C>>
        extends RoaringBitmapWriter<T> {
}
