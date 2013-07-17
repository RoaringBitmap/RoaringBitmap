package me.lemire.roaringbitmap;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

public class RoaringBitmap implements Iterable<Integer> {

	public SortedMap<Short, Container> highlowcontainer = new TreeMap<Short, Container>(); 

	public void add(int x) {
		short hb = Util.highbits(x);
		if (highlowcontainer.containsKey(hb)) {
			highlowcontainer.put(hb, highlowcontainer.get(hb).add(Util.lowbits(x)));
		} else {
			ArrayContainer newac = new ArrayContainer();
			highlowcontainer.put(hb, newac.add(Util.lowbits(x)));
		}
	}

	public static RoaringBitmap and(RoaringBitmap x1, RoaringBitmap x2) {
		RoaringBitmap answer = new RoaringBitmap();
		final Iterator<Entry<Short, Container>> p1 = x1.highlowcontainer.entrySet().iterator();
		final Iterator<Entry<Short, Container>> p2 = x2.highlowcontainer.entrySet().iterator();
		main: if (p1.hasNext() && p2.hasNext()) {
			Entry<Short, Container> s1 = p1.next();
			Entry<Short, Container> s2 = p2.next();
			int cas = 0;
			do {
				/**
				 * Daniel: I think you can rewrite this without the switch/case. 
				 * Please do so.
				 */
				switch (cas) {
				case 1:
					s1 = p1.next();
					break;
				case 2:
					s2 = p2.next();
					break;
				case 3:
					s1 = p1.next();
					s2 = p2.next();
					break;
				}

				if (s1.getKey().shortValue() < s2.getKey().shortValue()) {
					if (!p1.hasNext())
						break main;
					cas = 1;
				} else if (s1.getKey().shortValue() > s2.getKey().shortValue()) {
					if (!p2.hasNext())
						break main;
					cas = 2;
				} else { // égalité
					Container a = Util.and(s1.getValue(), s2.getValue());
					if(a.getCardinality() > 0)
						answer.highlowcontainer.put(s1.getKey(),a);
					if (!p1.hasNext())
						break main;
					if (!p2.hasNext())
						break main;
					cas = 3;
				}
			} while (true);
		}
		return answer;
	}

	public static RoaringBitmap or(RoaringBitmap x1, RoaringBitmap x2) {
		RoaringBitmap answer = new RoaringBitmap();
		final Iterator<Entry<Short, Container>> p1 = x1.highlowcontainer.entrySet().iterator();
		final Iterator<Entry<Short, Container>> p2 = x2.highlowcontainer.entrySet().iterator();
		main: if (p1.hasNext() && p2.hasNext()) {
			Entry<Short, Container> s1 = p1.next();
			Entry<Short, Container> s2 = p2.next();

			int cas = 0;

			while (true) {
				/**
				 * Daniel: Please write this without a switch case. It is a bit ugly.
				 */
				switch (cas) {
				case 1:
					s1 = p1.next();
					break;
				case 2:
					s2 = p2.next();
					break;
				case 3:
					s1 = p1.next();
					s2 = p2.next();
					break;
				}

				if (s1.getKey().shortValue() < s2.getKey().shortValue()) {
					// System.out.println(s1.getKey().shortValue()+" < "+s2.getKey().shortValue());
					answer.highlowcontainer.put(s1.getKey(), s1.getValue());
					// Si set p1 terminé, alors ajouter ce qui reste de set p2
					// dans answer
					if (!p1.hasNext()) { // System.out.println("while s1 < s2");
						do {
							answer.highlowcontainer.put(s2.getKey(), s2.getValue());
							if (!p2.hasNext())
								break;
							s2 = p2.next();
						} while (true);
						break main;
					}
					cas = 1;
				} else if (s1.getKey().shortValue() > s2.getKey().shortValue()) { // System.out.println(s1.getKey().shortValue()+" > "+s2.getKey().shortValue());
					answer.highlowcontainer.put(s2.getKey(), s2.getValue());
					// Si set p2 terminé, alors ajouter ce qui reste de set p1
					// dans answer
					if (!p2.hasNext()) { // System.out.println("while s1 > s2");
						do {
							answer.highlowcontainer.put(s1.getKey(), s1.getValue());
							if (!p1.hasNext())
								break;
							s1 = p1.next();
						} while (true);
						break main;
					}
					cas = 2;
				} else { // égalité
					// System.out.println(s1.getKey().shortValue()+" = "+s2.getKey().shortValue());
					answer.highlowcontainer.put(s1.getKey(),
							Util.or(s1.getValue(), s2.getValue()));
					if (!p1.hasNext()) { // System.out.println("while s1 = s2 !p1");
						while (p2.hasNext()) {
							s2 = p2.next();
							answer.highlowcontainer.put(s2.getKey(), s2.getValue());
						}
						break main;
					}

					if (!p2.hasNext()) { // System.out.println("while s1 = s2 !p2");
						while (p1.hasNext()) {
							s1 = p1.next();
							answer.highlowcontainer.put(s1.getKey(), s1.getValue());
						}
						break main;
					}
					cas = 3;
				}
			}
		}
		return answer;
	}

	public static RoaringBitmap xor(RoaringBitmap x1, RoaringBitmap x2) {
		RoaringBitmap answer = new RoaringBitmap();
		final Iterator<Entry<Short, Container>> p1 = x1.highlowcontainer.entrySet().iterator();
		final Iterator<Entry<Short, Container>> p2 = x2.highlowcontainer.entrySet().iterator();
		main: if (p1.hasNext() && p2.hasNext()) {
			Entry<Short, Container> s1 = p1.next();
			Entry<Short, Container> s2 = p2.next();

			int cas = 0;

			while (true) {
				/**
				 * Daniel: I think you can rewrite this without the switch. 
				 * Please do so.
				 */
				
				
				switch (cas) {
				case 1:
					s1 = p1.next();
					break;
				case 2:
					s2 = p2.next();
					break;
				case 3:
					s1 = p1.next();
					s2 = p2.next();
					break;
				}

				if (s1.getKey().shortValue() < s2.getKey().shortValue()) {
					// System.out.println(s1.getKey().shortValue()+" < "+s2.getKey().shortValue());
					answer.highlowcontainer.put(s1.getKey(), s1.getValue());
					// Si set p1 terminé, alors ajouter ce qui reste de set p2
					// dans answer
					if (!p1.hasNext()) { // System.out.println("while s1 < s2");
						do {
							answer.highlowcontainer.put(s2.getKey(), s2.getValue());
							if (!p2.hasNext())
								break;
							s2 = p2.next();
						} while (true);
						break main;
					}
					cas = 1;
				} else if (s1.getKey().shortValue() > s2.getKey().shortValue()) { // System.out.println(s1.getKey().shortValue()+" > "+s2.getKey().shortValue());
					answer.highlowcontainer.put(s2.getKey(), s2.getValue());
					// Si set p2 terminé, alors ajouter ce qui reste de set p1
					// dans answer
					if (!p2.hasNext()) { // System.out.println("while s1 > s2");
						do {
							answer.highlowcontainer.put(s1.getKey(), s1.getValue());
							if (!p1.hasNext())
								break;
							s1 = p1.next();
						} while (true);
						break main;
					}
					cas = 2;
				} else { // égalité
					Container C = Util.xor(s1.getValue(), s2.getValue());
					if (C.getCardinality() > 0)
						answer.highlowcontainer.put(s1.getKey(), C);
					if (!p1.hasNext()) { // System.out.println("while s1 = s2 !p1");
						while (p2.hasNext()) {
							s2 = p2.next();
							answer.highlowcontainer.put(s2.getKey(), s2.getValue());
						}
						break main;
					}

					if (!p2.hasNext()) { // System.out.println("while s1 = s2 !p2");
						while (p1.hasNext()) {
							s1 = p1.next();
							answer.highlowcontainer.put(s1.getKey(), s1.getValue());
						}
						break main;
					}
					cas = 3;
				}
			}
		}
		return answer;
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
		if (highlowcontainer.containsValue(hb))
			return highlowcontainer.get(hb).contains(Util.lowbits(x));
		return false;
	}

	@Override
	public Iterator<Integer> iterator() {
		final Iterator<Entry<Short, Container>> parent = highlowcontainer.entrySet()
				.iterator();

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
				return new Integer(actualval);
			}

			@Override
			public void remove() {
				RoaringBitmap.this.remove(actualval);
			}
		}.init();
	}
}
