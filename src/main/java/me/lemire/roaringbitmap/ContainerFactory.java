package me.lemire.roaringbitmap;

import java.util.ArrayList;

/**
 * The goal of the factory is to reused discarded container.
 *
 *
 */
public class ContainerFactory {
        
        static ArrayList<ArrayContainer> buffer = new  ArrayList<ArrayContainer>();
        static ArrayList<BitmapContainer> Bbuffer = new  ArrayList<BitmapContainer>();

        static int capacity = 10; 
        public static ArrayContainer getArrayContainer() {
                if(buffer.isEmpty())
                        return new ArrayContainer();
                else 
                    return buffer.remove(buffer.size()-1);
        }
        public static ArrayContainer copyToArrayContainer(BitmapContainer bc) {
                ArrayContainer ac = buffer.isEmpty() ?  new ArrayContainer(): buffer.remove(buffer.size()-1);
                ac.loadData(bc);
                return ac;
        }
        
        public static ArrayContainer transformToArrayContainer(BitmapContainer bc) {
                ArrayContainer ac = buffer.isEmpty() ?  new ArrayContainer(): buffer.remove(buffer.size()-1);
                ac.loadData(bc);
                putBackInStore(bc);
                return ac;
        }

        public static BitmapContainer getBitmapContainer() {
                if(Bbuffer.isEmpty())
                        return new BitmapContainer();
                else 
                    return Bbuffer.remove(Bbuffer.size()-1);
        }

        public static BitmapContainer copyToArrayContainer(ArrayContainer ac) {
                BitmapContainer bc = Bbuffer.isEmpty() ?  new BitmapContainer(): Bbuffer.remove(Bbuffer.size()-1);
                bc.loadData(ac);
                return bc;
        }
        public static BitmapContainer transformToBitmapContainer(ArrayContainer ac) {
                BitmapContainer bc = Bbuffer.isEmpty() ?  new BitmapContainer(): Bbuffer.remove(Bbuffer.size()-1);
                bc.loadData(ac);
                putBackInStore(ac);
                return bc;
        }
        
        public static void putBackInStore(ArrayContainer x) {
                if(capacity > buffer.size()) {
                        x.clear();
                        buffer.add(x);
                }
        }
        public static void putBackInStore(BitmapContainer x) {
                if(capacity > Bbuffer.size()) {
                        x.clear();
                        Bbuffer.add(x);
                }
        }

}
