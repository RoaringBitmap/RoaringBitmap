package me.lemire.roaringbitmap;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

public class RoaringBitmap implements Iterable<Integer> {

	SortedMap<Short, Container> c = new TreeMap<Short, Container>(); // does not have to be a tree

	public void add(int x) {
		short hb = Util.highbits(x);
		if (c.containsKey(hb)) {
			c.put(hb, c.get(hb).add(Util.lowbits(x)));
		} else {
			ArrayContainer newac = new ArrayContainer();
			c.put(hb, newac.add(Util.lowbits(x)));
		}
	}
	
	public static RoaringBitmap and(RoaringBitmap x1, RoaringBitmap x2) {
		RoaringBitmap answer = new RoaringBitmap();
		final Iterator<Entry<Short, Container>> p1 = x1.c.entrySet().iterator();
		final Iterator<Entry<Short, Container>> p2 = x2.c.entrySet().iterator();
		main:
		if(p1.hasNext() && p2.hasNext()) {
			Entry<Short, Container> s1 = p1.next();
			Entry<Short, Container> s2 = p2.next();
			do {
				if(s1.getKey().shortValue()<s2.getKey().shortValue()) {
					if(!p1.hasNext()) break main;
					s1 = p1.next();
				} else
				if(s1.getKey().shortValue()>s2.getKey().shortValue()) {
					if(!p2.hasNext()) break main;
					s2 = p2.next();
				} else {
					answer.c.put(s1.getKey(), Util.and(s1.getValue(),s2.getValue()));
					if(!p1.hasNext()) break main;
					s1 = p1.next();
					if(!p2.hasNext()) break main;
					s2 = p2.next();

				}
				
			} while(p1.hasNext() && p2.hasNext());
		}
		return answer;
		
	}

	public void remove(int x) {
		short hb = Util.highbits(x);
		if (c.containsValue(hb)) {
			Container cc = c.get(hb).remove(Util.lowbits(x));
			if(cc.getCardinality() == 0)
				c.remove(hb);
			else
				c.put(hb, cc);
		}
	}

	public boolean contains(int x) {
		short hb = Util.highbits(x);
		if (c.containsValue(hb))
			return c.get(hb).contains(Util.lowbits(x));
		return false;
	}

	public Iterator<Integer> iterator() {
		final Iterator<Entry<Short, Container>> parent = c.entrySet().iterator();
		
		
		return new Iterator<Integer>() {
			int parentw;
			int actualval;
		
			Iterator<Short> child;
			public boolean hasNext() {
				return child.hasNext();
			}

			public Iterator<Integer> init() {
				if(parent.hasNext()) {
					Entry<Short, Container> esc = parent.next();
					parentw = esc.getKey().shortValue() << 16;
					child = esc.getValue().iterator();
				} else {
					child = new Iterator<Short> (){
						public boolean hasNext() {
							return false;
						}
						public Short next() {
							return null;
						}
						public void remove() {
						}
						
					};
				}
				return this;
			}

			public Integer next() {
				int  lowerbits = child.next().shortValue() & 0xFFFF ;				
				actualval = lowerbits | parentw;
				if(!child.hasNext()) {
					if(parent.hasNext()) {
						Entry<Short, Container> esc = parent.next();
						parentw = esc.getKey().shortValue() << 16;
						child = esc.getValue().iterator();
					}
				}
				return actualval;
			}

			public void remove() {
				RoaringBitmap.this.remove(actualval);
			}
			
		}.init();
	}
}
