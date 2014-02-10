package org.roaringbitmap;

/**
 * A simple iterator over integer values
 *
 */
public interface IntIterator {
        /**
         * @return whether there is another value
         */
        public boolean hasNext();

        /**
         * @return next integer value
         */
        public int next();

        /**
         * remove current value
         */
        public void remove();
}
