package me.lemire.roaringbitmap;

import it.unimi.dsi.fastutil.shorts.Short2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.shorts.ShortBidirectionalIterator;
import it.unimi.dsi.fastutil.shorts.ShortSortedSet;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map.Entry;

public final class RoaringBitmap implements Iterable<Integer>, Cloneable, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3L;
	public Short2ObjectAVLTreeMap<Container> highlowcontainer = new Short2ObjectAVLTreeMap<Container>(); // does
	//public static int nbOR = 0, nbAND=0, nbXOR=0;																						// not

        /**
         * set the value to "true", whether it already appears on not.
         */     																									// be
        public void add(final int x) {
                set(x);
        }
	
        /**
         * set the value to "true", whether it already appears on not.
         */ 	
	public void set(final int x) {
		final short hb = Util.highbits(x);
		final Container z = highlowcontainer.get(hb);
		if(z != null) {
		        Container z2 = z.add(Util.lowbits(x));
                        if(z2 != z) {		          
		          highlowcontainer.put(hb,z2); //Replace the ArrayContainer by the new bitmapContainer
		        }
		} else {
			ArrayContainer newac = ContainerFactory.getArrayContainer();
			highlowcontainer.put(hb, newac.add(Util.lowbits(x)));
		}
	}

	public static RoaringBitmap and(final RoaringBitmap x1, final RoaringBitmap x2) {
	        final RoaringBitmap answer = new RoaringBitmap();
	        final ShortBidirectionalIterator p1 = x1.highlowcontainer.keySet().iterator();
	        final ShortBidirectionalIterator p2 = x2.highlowcontainer.keySet().iterator();
		main: if (p1.hasNext() && p2.hasNext()) {
		        short s1 = p1.next();
		        short s2 = p2.next();

			do {
				if (s1 < s2) {
					if (!p1.hasNext())
						break main;
					s1 = p1.next();
				} else if (s1 > s2) {
					if (!p2.hasNext())	break main;
					s2 = p2.next();
				} else { 
					//nbAND++;
					Container C = Util.and(x1.highlowcontainer.get(s1), x2.highlowcontainer.get(s2));
					if(C.getCardinality()>0)
						answer.highlowcontainer.put(s1,C);
					if (!p1.hasNext())	break main;
					if (!p2.hasNext())	break main;
					s1 = p1.next();
					s2 = p2.next();
				}
			} while (true);
		}
		return answer;
	}
	
	// DL: I have a theory that this might be suboptimal, see and().
        public static RoaringBitmap oldand(final RoaringBitmap x1, final RoaringBitmap x2) {
                System.out.println("and");
                final RoaringBitmap answer = new RoaringBitmap();
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
                                        if (!p2.hasNext())      break main;
                                        s2 = p2.next();
                                } else { 
                                        System.out.println("got match, going down to container");
                                        //nbAND++;
                                        Container C = Util.and(s1.getValue(), s2.getValue());
                                        if(C.getCardinality()>0)
                                                answer.highlowcontainer.put(s1.getKey(),C);
                                        if (!p1.hasNext())      break main;
                                        if (!p2.hasNext())      break main;
                                        s1 = p1.next();
                                        s2 = p2.next();
                                }
                        } while (true);
                }
                return answer;
        }

	
	public static RoaringBitmap or(final RoaringBitmap x1, final RoaringBitmap x2) {
	        final RoaringBitmap answer = new RoaringBitmap();
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
					//nbOR++;
					answer.highlowcontainer.put(s1.getKey(),
							Util.or(s1.getValue(), s2.getValue()));
					if (!p1.hasNext()) { 
						while (p2.hasNext()) {
							s2 = p2.next();
							answer.highlowcontainer.put(s2.getKey(),s2.getValue());
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

	public static RoaringBitmap xor(final RoaringBitmap x1, final RoaringBitmap x2) {
	        final RoaringBitmap answer = new RoaringBitmap();
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
					//nbXOR++;
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
	        final int[] array = new int[this.getCardinality()];
		int pos=0;
		final Iterator<Entry<Short, Container>> p1 = this.highlowcontainer
				.entrySet().iterator();
		while(p1.hasNext())
		{
		        final Entry<Short, Container> s = p1.next();
			final short hs = s.getKey().shortValue();
			final ShortIterator si = s.getValue().getShortIterator();
			while(si.hasNext()) {
			        array[pos++] 
			        		= (hs<<16) | 
			        			si.next();
			}
		}	
		return array;
	}
	
	public void remove(final int x) {
		final short hb = Util.highbits(x);
		if (highlowcontainer.containsKey(hb)) {
		        final Container corig = highlowcontainer.get(hb);
		        final Container cc = corig.remove(Util.lowbits(x));
		        if (cc.getCardinality() == 0)
				highlowcontainer.remove(hb);
			else if(cc != corig)
				highlowcontainer.put(hb, cc);
		}
	}

	public boolean contains(final int x) {
		final short hb = Util.highbits(x);
		if (highlowcontainer.containsKey(hb)) {
		        final Container C = highlowcontainer.get(hb);
			return C.contains(Util.lowbits(x));
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

                        ShortIterator child;

                        @Override
                        public boolean hasNext() {
                                return child.hasNext();
                        }

                        public Iterator<Integer> init() {
                                if (parent.hasNext()) {
                                        Entry<Short, Container> esc = parent
                                                .next();
                                        parentw = esc.getKey().shortValue() << 16;
                                        child = esc.getValue()
                                                .getShortIterator();
                                } else {
                                        child = new ShortIterator() {

                                                @Override
                                                public boolean hasNext() {
                                                        return false;
                                                }

                                                @Override
                                                public short next() {
                                                        return 0;
                                                }
                                        };

                                }
                                return this;
                        }

                        @Override
                        public Integer next() {
                                int lowerbits = child.next() & 0xFFFF;
                                actualval = lowerbits | parentw;
                                if (!child.hasNext()) {
                                        if (parent.hasNext()) {
                                                Entry<Short, Container> esc = parent
                                                        .next();
                                                parentw = esc.getKey()
                                                        .shortValue() << 16;
                                                child = esc.getValue()
                                                        .getShortIterator();
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

	@Override
	public String toString(){
		int nbNodes = this.highlowcontainer.size();
		int nbArrayC = 0, nbBitmapC=0, minAC=1024, maxAC=0, minBC=65535, maxBC=0, card, avAC=0, avBC=0;
		for (Container C : this.highlowcontainer.values())
			if(C instanceof ArrayContainer) {
				nbArrayC++;
				card = C.getCardinality(); 
				if(card<minAC) minAC=C.getCardinality();
				if(card>maxAC) maxAC=C.getCardinality();
				avAC+=card;
			}
			else {
				nbBitmapC++;
				card = C.getCardinality();
				if(C.getCardinality()<minBC) minBC=C.getCardinality();
				if(C.getCardinality()>maxBC) maxBC=C.getCardinality();
				avBC+=card;
			}
		try {avAC/=nbArrayC;}catch(ArithmeticException e)	{avAC=0;}
		try {avBC/=nbBitmapC;}catch(ArithmeticException e)	{avBC=0;} 
		String desc = "We have "+nbNodes+" nodes with "+nbArrayC+" ArrayContainers min : "+minAC+" max : "+maxAC
			+" average : "+avAC+" and "+nbBitmapC+" BitmapContainers min : "+minBC+" max : "+maxBC+" avBC : "+avBC;
		return desc;		
	}
	
	/**
	 * A RoaringBitmap is made of several container. The containers
	 * can be reused if the RoaringBitmap object is going to be discarded.
	 * Note that it is unsafe to keep accessing the RoaringBitmap object after
	 * this call.
	 */
        public void recycleContainers() {
                for (Container c : highlowcontainer.values()) {
                        if (c instanceof ArrayContainer) {
                                ContainerFactory
                                        .putBackInStore((ArrayContainer) c);

                        } else if (c instanceof BitmapContainer) {
                                ContainerFactory
                                        .putBackInStore((BitmapContainer) c);
                        }
                }
        }
}
