package org.roaringbitmap;


interface Appender<C, T extends BitmapDataProvider & HasAppendableStorage<C>>
        extends RoaringBitmapWriter<T> {
}
