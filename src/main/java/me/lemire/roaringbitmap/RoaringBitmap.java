package me.lemire.roaringbitmap;

import it.unimi.dsi.fastutil.shorts.Short2ObjectAVLTreeMap;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map.Entry;

public class RoaringBitmap implements Iterable<Integer>, Cloneable, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3L;
	public Short2ObjectAVLTreeMap<Container> highlowcontainer = new Short2ObjectAVLTreeMap<Container>(); // does
																							// not
					
																							// be
        public void add(int x) {
                set(x);
        }
																							// 	
	public void set(int x) {
		short hb = Util.highbits(x);
		Container z = highlowcontainer.get(hb);
		if(z != null) {
		        Container z2 = z.add(Util.lowbits(x));
		        if(z2 != z) highlowcontainer.put(hb,z2);
		} else {
			ArrayContainer newac = new ArrayContainer();
			highlowcontainer.put(hb, newac.add(Util.lowbits(x)));
		}
	}

	public static RoaringBitmap and(RoaringBitmap x1, RoaringBitmap x2) {
		RoaringBitmap answer = new RoaringBitmap();
		final Iterator<Entry<Short, Container>> p1 = x1.highlowcontainer
				.entrySet().iterator();
		final Iterator<Entry<Short, Container>> p2 = x2.highlowcontainer
				.entrySet().iterator();
		main: if (p1.hasNext() && p2.hasNext()) {
			Entry<Short, Container> s1 = p1.next();
			Entry<Short, Container> s2 = p2.next();

			do {
				if (s1.getKey().shortValue() < s2.getKey().shortValue()) {
					if (!p1.hasNext())
						break main;
					s1 = p1.next();
				} else if (s1.getKey().shortValue() > s2.getKey().shortValue()) {
					if (!p2.hasNext())
						break main;
					s2 = p2.next();
				} else { 
					Container C = Util.and(s1.getValue(), s2.getValue());
					if(C.getCardinality()>0)
					answer.highlowcontainer.put(s1.getKey(),C);
					if (!p1.hasNext())
						break main;
					if (!p2.hasNext())
						break main;
					s1 = p1.next();
					s2 = p2.next();
				}
			} while (true);
		}
		return answer;
	}

	public static RoaringBitmap or(RoaringBitmap x1, RoaringBitmap x2) {
		RoaringBitmap answer = new RoaringBitmap();
		final Iterator<Entry<Short, Container>> p1 = x1.highlowcontainer
				.entrySet().iterator();
		final Iterator<Entry<Short, Container>> p2 = x2.highlowcontainer
				.entrySet().iterator();
		main: if (p1.hasNext() && p2.hasNext()) {
			Entry<Short, Container> s1 = p1.next();
			Entry<Short, Container> s2 = p2.next();

			while (true) {
				if (s1.getKey().shortValue() < s2.getKey().shortValue()) {
					answer.highlowcontainer.put(s1.getKey(), s1.getValue());
					if (!p1.hasNext()) { 
						do {
							answer.highlowcontainer.put(s2.getKey(),
									s2.getValue());
							if (!p2.hasNext())
								break;
							s2 = p2.next();
						} while (true);
						break main;
					}
					s1 = p1.next();
				} else if (s1.getKey().shortValue() > s2.getKey().shortValue()) { 
					answer.highlowcontainer.put(s2.getKey(), s2.getValue());
					if (!p2.hasNext()) { 
						do {
							answer.highlowcontainer.put(s1.getKey(),
									s1.getValue());
							if (!p1.hasNext())
								break;
							s1 = p1.next();
						} while (true);
						break main;
					}
					s2 = p2.next();
				} else { 
					answer.highlowcontainer.put(s1.getKey(),
							Util.or(s1.getValue(), s2.getValue()));
					if (!p1.hasNext()) { 
						while (p2.hasNext()) {
							s2 = p2.next();
							answer.highlowcontainer.put(s2.getKey(),
									s2.getValue());
						}
						break main;
					}

					if (!p2.hasNext()) { 
						while (p1.hasNext()) {
							s1 = p1.next();
							answer.highlowcontainer.put(s1.getKey(),
									s1.getValue());
						}
						break main;
					}
					s1 = p1.next();
					s2 = p2.next();
				}
			}
		}
		return answer;
	}
	
	public void validate() {
		for (Container C: this.highlowcontainer.values())
			C.validate();
	}

	public static RoaringBitmap xor(RoaringBitmap x1, RoaringBitmap x2) {
		RoaringBitmap answer = new RoaringBitmap();
		final Iterator<Entry<Short, Container>> p1 = x1.highlowcontainer
				.entrySet().iterator();
		final Iterator<Entry<Short, Container>> p2 = x2.highlowcontainer
				.entrySet().iterator();
		main: if (p1.hasNext() && p2.hasNext()) {
			Entry<Short, Container> s1 = p1.next();
			Entry<Short, Container> s2 = p2.next();

			while (true) {
				if (s1.getKey().shortValue() < s2.getKey().shortValue()) {
					answer.highlowcontainer.put(s1.getKey(), s1.getValue());
					if (!p1.hasNext()) { 
						do {
							answer.highlowcontainer.put(s2.getKey(),
									s2.getValue());
							if (!p2.hasNext())
								break;
							s2 = p2.next();
						} while (true);
						break main;
					}
					s1 = p1.next();
				} else if (s1.getKey().shortValue() > s2.getKey().shortValue()) { 
					answer.highlowcontainer.put(s2.getKey(), s2.getValue());
					if (!p2.hasNext()) { 
						do {
							answer.highlowcontainer.put(s1.getKey(),
									s1.getValue());
							if (!p1.hasNext())
								break;
							s1 = p1.next();
						} while (true);
						break main;
					}
					s2 = p2.next();
				} else { 
					Container C = Util.xor(s1.getValue(), s2.getValue());
					if (C.getCardinality()>0)
						answer.highlowcontainer.put(s1.getKey(), C);
					if (!p1.hasNext()) {
						while (p2.hasNext()) {
							s2 = p2.next();
							answer.highlowcontainer.put(s2.getKey(),
									s2.getValue());
						}
						break main;
					}

					if (!p2.hasNext()) {
						while (p1.hasNext()) {
							s1 = p1.next();
							answer.highlowcontainer.put(s1.getKey(),
									s1.getValue());
						}
						break main;
					}
					s1 = p1.next();
					s2 = p2.next();
				}
			}
		}
		return answer;
	}

	public int[] getIntegers() {
		int[] array = new int[this.getCardinality()];
		int pos=0;
		final Iterator<Entry<Short, Container>> p1 = this.highlowcontainer
				.entrySet().iterator();
		Entry<Short, Container> s; 
		while(p1.hasNext())
		{
			s = p1.next();
			if(s.getValue() instanceof ArrayContainer)
				for(int i=0; i<s.getValue().getCardinality(); i++)
			    array[pos++] = (16 << s.getKey().shortValue()) |
				               ((ArrayContainer)s.getValue()).content[i];
			else if(s.getValue() instanceof BitmapContainer)
				for(int i=((BitmapContainer)s.getValue()).nextSetBit(0); i>=0; 
						i=((BitmapContainer)s.getValue()).nextSetBit(i+1))
			array[pos++] = (16 << s.getKey().shortValue()) | i;
		}	
		return array;
	}
	
	public void remove(int x) {
		short hb = Util.highbits(x);
		if (highlowcontainer.containsValue(hb)) {
			Container cc = highlowcontainer.get(hb).remove(Util.lowbits(x));
			if (cc.getCardinality() == 0)
				highlowcontainer.remove(hb);
			else
				highlowcontainer.put(hb, cc);
		}
	}

	public boolean contains(int x) {
		short hb = Util.highbits(x);
		if (highlowcontainer.containsKey(hb)) {
			Container C = highlowcontainer.get(hb); 
			if(C instanceof BitmapContainer)
			 return ((BitmapContainer) C).contains(BitmapContainer.toIntUnsigned(Util.lowbits(x)));
			else return ((ArrayContainer) C).contains(Util.lowbits(x));
		}
		return false;
	}

	@Override
	public Iterator<Integer> iterator() {
		final Iterator<Entry<Short, Container>> parent = highlowcontainer
				.entrySet().iterator();

		return new Iterator<Integer>() {
			int parentw;
			int actualval;

			Iterator<Short> child;

			@Override
			public boolean hasNext() {
				return child.hasNext();
			}

			public Iterator<Integer> init() {
				if (parent.hasNext()) {
					Entry<Short, Container> esc = parent.next();
					parentw = esc.getKey().shortValue() << 16;
					child = esc.getValue().iterator();
				} else {
					child = new Iterator<Short>() {
						@Override
						public boolean hasNext() {
							return false;
						}

						@Override
						public Short next() {
							return null;
						}

						@Override
						public void remove() {
						}

					};
				}
				return this;
			}

	@Override
	public Integer next() {
	int lowerbits = child.next().shortValue() & 0xFFFF;
	actualval = lowerbits | parentw;
	if (!child.hasNext()) {
	if (parent.hasNext()) {
		Entry<Short, Container> esc = parent.next();
		parentw = esc.getKey().shortValue() << 16;
		child = esc.getValue().iterator();
	}
	}
				return actualval;
	}

	@Override
	public void remove() {
	RoaringBitmap.this.remove(actualval);
	}
		}.init();
	}
	
	public int getSizeInBytes(){
		int size = 0;
		final Iterator<Entry<Short, Container>> p1 = this.highlowcontainer
				.entrySet().iterator();
		Entry<Short, Container> s;
		do{
			s=p1.next();
			// we add the 16 highbits of a node and the size of its leafs
			size+=16+this.highlowcontainer.get(s.getKey()).getSizeInBits();
		}while(p1.hasNext());
		
		return size/8;
	}
	
	public int getCardinality(){
		int size = 0;
		final Iterator<Entry<Short, Container>> p1 = this.highlowcontainer
				.entrySet().iterator();
		Entry<Short, Container> s;
		while(p1.hasNext()) {
			s=p1.next();
			// we add the 16 highbits of a node and the size of its leafs
			size+=this.highlowcontainer.get(s.getKey()).getCardinality();
		} 
		
		return size;
	}
	
	
	@SuppressWarnings("unchecked")
	@Override
	public RoaringBitmap clone() {
		try {
			RoaringBitmap x = (RoaringBitmap) super.clone();
			x.highlowcontainer = highlowcontainer
					.clone();
			return x;
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
			throw new RuntimeException("shouldn't happen with clone");
		}
	}
}
