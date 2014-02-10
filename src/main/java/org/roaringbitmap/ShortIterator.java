package org.roaringbitmap;

/**
 * @author lemire
 *
 */
public interface ShortIterator {
        /**
         * @return whether there is another value
         */
        public boolean hasNext();

        /**
         * @return next short value
         */
        public short next();
        /**
         * remove current value
         */
        public void remove();

}
