package org.roaringbitmap;

/**
 *
 */
public final class ContainerFactory {



        /**
         * Get an array container, possibly by reusing an old container.
         * @return an array container
         */
        public static ArrayContainer getArrayContainer() {
                return new ArrayContainer();
        }

        /**
         * Get a new bitmap container, possible reusing an older container.
         * The returned BitmapContainer will not be initialized as empty by
         * default. Call "clear" if needed.
         * 
         * @return a BitmapContainer
         */
        public static BitmapContainer getUnintializedBitmapContainer() {
                return new BitmapContainer();
        }

}
