package me.lemire.roaringbitmap;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * The goal of the factory is to reused discarded container.
 *
 *
 */
public  final class ContainerFactory {
        
        static ArrayList<ArrayContainer> buffer = new  ArrayList<ArrayContainer>();
        static ArrayList<BitmapContainer> Bbuffer = new  ArrayList<BitmapContainer>();

        static int capacity = 10;
        
        public static ArrayContainer getArrayContainer() {
                if(buffer.isEmpty()) {
                        return new ArrayContainer();
                }
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
        
        public static BitmapContainer getCopyOfBitmapContainer(BitmapContainer bc) {
                if(Bbuffer.isEmpty())
                        return bc.clone();
                BitmapContainer ans =  Bbuffer.remove(Bbuffer.size()-1);
                ans.cardinality = bc.cardinality;
                System.arraycopy(bc.bitmap, 0, ans.bitmap, 0, bc.bitmap.length);
                return ans;
        }
        
        public static ArrayContainer getCopyOfArrayContainer(ArrayContainer ac) {
                if(buffer.isEmpty())
                        return ac.clone();
                ArrayContainer ans =  buffer.remove(buffer.size()-1);
                ans.cardinality = ac.cardinality;
                if(ans.content.length>=ac.content.length)
                        System.arraycopy(ac.content, 0, ans.content, 0, ac.cardinality);
                else ans.content = Arrays.copyOf(ac.content,ac.content.length);
                return ans;
        }

        /**
         * Warning: the BitmapContainer won't be initialized as empty.
         * @return a BitmapContainer
         */
        public static BitmapContainer getBitmapContainer() {
                if(Bbuffer.isEmpty())
                        return new BitmapContainer();
                return Bbuffer.remove(Bbuffer.size()-1);
        }

        public static BitmapContainer copyToArrayContainer(ArrayContainer ac) {
                BitmapContainer bc = Bbuffer.isEmpty() ?  new BitmapContainer(): Bbuffer.remove(Bbuffer.size()-1);
                bc.loadData(ac);
                return bc;
        }
        public static BitmapContainer transformToBitmapContainer(ArrayContainer ac) {
                BitmapContainer bc = Bbuffer.isEmpty()? new BitmapContainer(): Bbuffer.remove(Bbuffer.size()-1);
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
        
        public static boolean isInStore(BitmapContainer x) {
                return Bbuffer.indexOf(x) >=0;
        }
        public static boolean isInStore(ArrayContainer x) {
                return buffer.indexOf(x) >=0;
        }
}
