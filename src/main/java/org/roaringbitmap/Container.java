package org.roaringbitmap;

/**
 * @author lemire
 *
 */
public abstract class Container implements Iterable<Short>, Cloneable {

        /**
         * @param x
         * @return
         */
        public abstract Container add(short x);

        /**
         * @param x
         * @return
         */
        public abstract Container and(ArrayContainer x);

        /**
         * @param x
         * @return
         */
        public abstract Container and(BitmapContainer x);

        /**
         * @param x
         * @return
         */
        public Container and(Container x) {
                if (x instanceof ArrayContainer)
                        return and((ArrayContainer) x);
                else
                        return and((BitmapContainer) x);

        }

        /**
         * @param x
         * @return
         */
        public abstract Container andNot(ArrayContainer x);

        /**
         * @param x
         * @return
         */
        public abstract Container andNot(BitmapContainer x);

        /**
         * @param x
         * @return
         */
        public Container andNot(Container x) {
                if (x instanceof ArrayContainer)
                        return andNot((ArrayContainer) x);
                else
                        return andNot((BitmapContainer) x);
        }

        /**
         * 
         */
        public abstract void clear();

        @Override
        public Container clone() {
                try {
                        return (Container) super.clone();
                } catch (CloneNotSupportedException e) {
                        throw new java.lang.RuntimeException();
                }
        }

        /**
         * @param x
         * @return
         */
        public abstract boolean contains(short x);

        /**
         * @return
         */
        public abstract int getCardinality();

        /**
         * @return
         */
        public abstract ShortIterator getShortIterator();

        /**
         * @return
         */
        public abstract int getSizeInBytes();

        /**
         * @param x
         * @return
         */
        public abstract Container iand(ArrayContainer x);

        public abstract Container iand(BitmapContainer x);

        /**
         * @param x
         * @return
         */
        public Container iand(Container x) {
                if (x instanceof ArrayContainer)
                        return iand((ArrayContainer) x);
                else
                        return iand((BitmapContainer) x);

        }

        /**
         * @param x
         * @return
         */
        public abstract Container iandNot(ArrayContainer x);

        /**
         * @param x
         * @return
         */
        public abstract Container iandNot(BitmapContainer x);

        /**
         * @param x
         * @return
         */
        public Container iandNot(Container x) {
                if (x instanceof ArrayContainer)
                        return iandNot((ArrayContainer) x);
                else
                        return iandNot((BitmapContainer) x);
        }

        /**
         * @param x
         * @return
         */
        public abstract Container ior(ArrayContainer x);

        /**
         * @param x
         * @return
         */
        public abstract Container ior(BitmapContainer x);

        /**
         * @param x
         * @return
         */
        public Container ior(Container x) {
                if (x instanceof ArrayContainer)
                        return ior((ArrayContainer) x);
                else
                        return ior((BitmapContainer) x);
        }

        /**
         * @param x
         * @return
         */
        public abstract Container ixor(ArrayContainer x);

        /**
         * @param x
         * @return
         */
        public abstract Container ixor(BitmapContainer x);

        /**
         * @param x
         * @return
         */
        public Container ixor(Container x) {
                if (x instanceof ArrayContainer)
                        return ixor((ArrayContainer) x);
                else
                        return ixor((BitmapContainer) x);

        }

        /**
         * @param x
         * @return
         */
        public abstract Container or(ArrayContainer x);

        public abstract Container or(BitmapContainer x);

        public Container or(Container x) {
                if (x instanceof ArrayContainer)
                        return or((ArrayContainer) x);
                else
                        return or((BitmapContainer) x);
        }

        public abstract Container remove(short x);

        public abstract void trim();

        public abstract Container xor(ArrayContainer x);

        public abstract Container xor(BitmapContainer x);

        public Container xor(Container x) {
                if (x instanceof ArrayContainer)
                        return xor((ArrayContainer) x);
                else
                        return xor((BitmapContainer) x);

        }
}
