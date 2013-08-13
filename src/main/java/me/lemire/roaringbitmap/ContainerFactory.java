package me.lemire.roaringbitmap;

import java.util.ArrayList;

/**
 * The goal of the factory is to reused discarded container.
 *
 *
 */
public class ContainerFactory {
        
        static ArrayList<ArrayContainer> buffer = new  ArrayList<ArrayContainer>();
        static ArrayList<BitmapContainer> bbuffer = new  ArrayList<BitmapContainer>();

        static int capacity = 10; 
        public static ArrayContainer getArrayContainer() {
                if(buffer.isEmpty())
                        return new ArrayContainer();
                else 
                    return buffer.remove(buffer.size()-1);
        }
        public static ArrayContainer getArrayContainer(BitmapContainer bc) {
                ArrayContainer ac = buffer.isEmpty() ?  new ArrayContainer(): buffer.remove(buffer.size()-1);
                ac.loadData(bc);
                putBackInStore(bc); //Conserving the bitmapContainer
                return ac;
        }
        
        public static BitmapContainer getBitmapContainer() {
                if(bbuffer.isEmpty())
                        return new BitmapContainer();
                else 
                    return bbuffer.remove(buffer.size()-1);
        }

        public static BitmapContainer getBitmapContainer(ArrayContainer ac) {
                BitmapContainer bc = bbuffer.isEmpty() ?  new BitmapContainer(): bbuffer.remove(buffer.size()-1);
                bc.loadData(ac);
                putBackInStore(ac); //After transformation, the ArrayContainer is conserved
                return bc;
        }
        
        public static void putBackInStore(ArrayContainer x) {
                if(capacity < buffer.size()) {
                        x.clear();
                        buffer.add(x);
                }
        }
        public static void putBackInStore(BitmapContainer x) {
                if(capacity < buffer.size()) {
                        x.clear();
                        bbuffer.add(x);
                }
        }

}
