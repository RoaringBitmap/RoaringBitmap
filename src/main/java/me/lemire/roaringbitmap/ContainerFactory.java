package me.lemire.roaringbitmap;

import java.util.ArrayList;

/**
 * The goal of the factory is to reused discarded container.
 *
 *
 */
public class ContainerFactory {
        
        static ArrayList<ArrayContainer> buffer = new  ArrayList<ArrayContainer>();
        static int capacity = 10; 
        public static ArrayContainer getArrayContainer() {
                if(buffer.isEmpty())
                        return new ArrayContainer();
                else 
                    return buffer.remove(buffer.size()-1);
        }
        
        public static void putBackInStore(ArrayContainer x) {
                if(capacity < buffer.size()) {
                        x.clear();
                        buffer.add(x);
                }
                
        }

}
