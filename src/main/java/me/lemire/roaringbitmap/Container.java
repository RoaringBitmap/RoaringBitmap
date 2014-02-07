package me.lemire.roaringbitmap;

public abstract class Container implements Iterable<Short>, Cloneable {

        public abstract Container add(short x);

        public abstract Container and(ArrayContainer x);

        public abstract Container and(BitmapContainer x);

        public Container and(Container x) {
                if (x instanceof ArrayContainer)
                        return and((ArrayContainer) x);
                else
                        return and((BitmapContainer) x);

        }

        public abstract Container andNot(ArrayContainer x);

        public abstract Container andNot(BitmapContainer x);

        public Container andNot(Container x) {
                if (x instanceof ArrayContainer)
                        return andNot((ArrayContainer) x);
                else
                        return andNot((BitmapContainer) x);
        }

        public abstract void clear();

        @Override
        public Container clone() {
                try {
                        return (Container) super.clone();
                } catch (CloneNotSupportedException e) {
                        throw new java.lang.RuntimeException();
                }
        }

        public abstract boolean contains(short x);

        public abstract int getCardinality();

        public abstract ShortIterator getShortIterator();

        public abstract int getSizeInBits();

        public abstract int getSizeInBytes();

        public abstract Container iand(ArrayContainer x);

        public abstract Container iand(BitmapContainer x);

        public Container iand(Container x) {
                if (x instanceof ArrayContainer)
                        return iand((ArrayContainer) x);
                else
                        return iand((BitmapContainer) x);

        }

        public abstract Container iandNot(ArrayContainer x);

        public abstract Container iandNot(BitmapContainer x);

        public Container iandNot(Container x) {
                if (x instanceof ArrayContainer)
                        return iandNot((ArrayContainer) x);
                else
                        return iandNot((BitmapContainer) x);
        }

        public abstract Container ior(ArrayContainer x);

        public abstract Container ior(BitmapContainer x);

        public Container ior(Container x) {
                if (x instanceof ArrayContainer)
                        return ior((ArrayContainer) x);
                else
                        return ior((BitmapContainer) x);
        }

        public abstract Container ixor(ArrayContainer x);

        public abstract Container ixor(BitmapContainer x);

        public Container ixor(Container x) {
                if (x instanceof ArrayContainer)
                        return ixor((ArrayContainer) x);
                else
                        return ixor((BitmapContainer) x);

        }

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
