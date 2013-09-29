package me.lemire.roaringbitmap;

import java.io.Serializable;
import me.lemire.roaringbitmap.SpeedyArray.Element;

public final class SpeedyRoaringBitmap implements Cloneable, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3L;
	public SpeedyArray highlowcontainer = null;
       
	public SpeedyRoaringBitmap(int nbKeys) {
		highlowcontainer = new SpeedyArray(nbKeys);
	}
	
	/**
         * set the value to "true", whether it already appears on not.
         */     																									
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
	
	public void validate() {
		for (int i=0; i<this.highlowcontainer.getnbKeys(); i++)
				this.highlowcontainer.getArray()[i].value.validate();
				
	}

	public static SpeedyRoaringBitmap and(final SpeedyRoaringBitmap x1, final SpeedyRoaringBitmap x2) {
	    final SpeedyRoaringBitmap answer = new SpeedyRoaringBitmap(x1.getNbNodes()>=x2.getNbNodes()?x1.getNbNodes()
	        		:x2.getNbNodes());
	    int pos1 = 0, pos2 = 0;
	    int length1 = x1.highlowcontainer.getnbKeys(), 
	        	length2 = x2.highlowcontainer.getnbKeys();
	        
		main: if (pos1 < length1 && pos2 < length2) {
		        short s1 = x1.highlowcontainer.getArray()[pos1].key;
		        short s2 = x2.highlowcontainer.getArray()[pos2].key;
			do {
				if (s1 < s2) {
					pos1++;
					if (pos1==length1)
						break main;
					s1 = x1.highlowcontainer.getArray()[pos1].key;
				} else if (s1 > s2) {
					pos2++;
					if (pos2==length2)	
						break main;
					s2 = x2.highlowcontainer.getArray()[pos2].key;
				} else { 
					Container C = Util.and(x1.highlowcontainer.get(s1), x2.highlowcontainer.get(s2));
					if(C.getCardinality()>0)
						answer.highlowcontainer.putEnd(s1,C);
					pos1++; 
					pos2++;
					if (pos1==length1)	break main;
					if (pos2==length2)	break main;
					s1 = x1.highlowcontainer.getArray()[pos1].key;
					s2 = x2.highlowcontainer.getArray()[pos2].key;
				}
			} while (true);
		}
		return answer;
	}
	
	public void inPlaceAND(final SpeedyRoaringBitmap x2) {
		 
	    int pos1 = 0, pos2 = 0;
	    int length1 = this.getNbNodes(), 
	        length2 = x2.getNbNodes();
	        
		main: if (pos1 < length1 && pos2 < length2) {
		        short s1 = this.getArray()[pos1].key;
		        short s2 =   x2.getArray()[pos2].key;
			do {
				if (s1 < s2) {
					this.highlowcontainer.remove(s1);
					if (pos1==this.getNbNodes())
						break main;
					s1 = this.getArray()[pos1].key;
				} else if (s1 > s2) {
					pos2++;
					if (pos2==length2)	
						break main;
					s2 = x2.getArray()[pos2].key;
				} else { 
					Container C = Util.and(this.get(s1), x2.get(s2));
					if(C.getCardinality()>0)
						this.highlowcontainer.put(s1,C);
					pos1++; 
					pos2++;
					if (pos1==this.getNbNodes())	
						break main;
					if (pos2==length2)	
						break main;
					s1 = this.getArray()[pos1].key;
					s2 =   x2.getArray()[pos2].key;
				}
			} while (true);
		}
}
	
	public static SpeedyRoaringBitmap or(final SpeedyRoaringBitmap x1, final SpeedyRoaringBitmap x2) {
		int desiredcapacity = x1.getNbNodes()+x2.getNbNodes()>65536?x1.getNbNodes()+x2.getNbNodes():65536;
	    final SpeedyRoaringBitmap answer = new SpeedyRoaringBitmap(desiredcapacity);
	    int pos1 = 0, pos2 = 0;
        int length1 = x1.highlowcontainer.getnbKeys(), 
        	length2 = x2.highlowcontainer.getnbKeys();
        
		main: if (pos1 < length1 && pos2 < length2) {
	        short s1 = x1.highlowcontainer.getArray()[pos1].key;
	        short s2 = x2.highlowcontainer.getArray()[pos2].key;

			while (true) {
				if (s1 < s2) {
					answer.highlowcontainer.putEnd(s1, x1.highlowcontainer.get(s1));
					pos1++;
					if (pos1==length1) { 
						do {
							answer.highlowcontainer.putEnd(s2,	x2.highlowcontainer.get(s2));
							pos2++;
							if (pos2==length2)
								break;
							s2 = x2.highlowcontainer.getArray()[pos2].key;
						} while (true);
						break main;
					}
					s1 = x1.highlowcontainer.getArray()[pos1].key;
				} else if (s1 > s2) { 
					answer.highlowcontainer.putEnd(s2, x2.highlowcontainer.get(s2));
					pos2++;
					if (pos2==length2) { 
						do {
							answer.highlowcontainer.putEnd(s1,x1.highlowcontainer.get(s1));
							pos1++;
							if (pos1==length1)
								break;
							s1 = x1.highlowcontainer.getArray()[pos1].key;
						} while (true);
						break main;
					}
					s2 = x2.highlowcontainer.getArray()[pos2].key;
				} else {
					//nbOR++;
					answer.highlowcontainer.putEnd(s1,Util.or(x1.get(s1), x2.get(s2)));
					pos1++;
					pos2++;
					if (pos1==length1) { 
						while (pos2<length2) {
							s2 = x2.highlowcontainer.getArray()[pos2].key;
							answer.highlowcontainer.putEnd(s2, x2.highlowcontainer.get(s2));
							pos2++;
						}
						break main;
					}

					if (pos2==length2) { 
						while (pos1<length1) {
							s1 = x1.highlowcontainer.getArray()[pos1].key;
							answer.highlowcontainer.put(s1, x1.highlowcontainer.get(s1));
							pos1++;
						}
						break main;
					}
					s1 = x1.highlowcontainer.getArray()[pos1].key;
					s2 = x2.highlowcontainer.getArray()[pos2].key;
				}
			}
		}
		return answer;
	}
	
	public void inPlaceOR (final SpeedyRoaringBitmap x2) {
		int pos1 = 0, pos2 = 0;
        int length1 = this.getNbNodes(), 
        	length2 = x2.getNbNodes();
        
		main: if (pos1 < length1 && pos2 < length2) {
	        short s1 = this.getArray()[pos1].key;
	        short s2 = x2.getArray()[pos2].key;

			while (true) {
				if (s1 < s2) {
					pos1++;
					if (pos1==length1) { 
						do {
							this.highlowcontainer.put(s2,x2.highlowcontainer.get(s2));
							pos2++;
							if (pos2==length2)
								break;
							s2 = x2.getArray()[pos2].key;
						} while (true);
						break main;
					}
					s1 = this.getArray()[pos1].key;
				} else if (s1 > s2) { 
					this.highlowcontainer.put(s2, x2.highlowcontainer.get(s2));
					pos2++;
					if (pos2==length2) 						
						break main;
					
					s2 = x2.highlowcontainer.getArray()[pos2].key;
				} else {
					//nbOR++;
					this.highlowcontainer.put(s1,Util.or(this.get(s1), x2.get(s2)));
					pos1++;
					pos2++;
					if (pos1==length1) { 
						while (pos2<length2) {
							s2 = x2.highlowcontainer.getArray()[pos2].key;
							this.highlowcontainer.put(s2, x2.highlowcontainer.get(s2));
							pos2++;
						}
						break main;
					}

					if (pos2==length2)						
						break main;
					
					s1 = this.getArray()[pos1].key;
					s2 = x2.getArray()[pos2].key;
				}
			}
		}
	}
	
	public static SpeedyRoaringBitmap xor(final SpeedyRoaringBitmap x1, final SpeedyRoaringBitmap x2) {
		int desiredcapacity = x1.getNbNodes()+x2.getNbNodes()>65536?x1.getNbNodes()+x2.getNbNodes():65536;
	    final SpeedyRoaringBitmap answer = new SpeedyRoaringBitmap(desiredcapacity);
	    int pos1 = 0, pos2 = 0;
        int length1 = x1.highlowcontainer.getnbKeys(), 
        	length2 = x2.highlowcontainer.getnbKeys();
        
		main: if (pos1 < length1 && pos2 < length2) {
	        short s1 = x1.highlowcontainer.getArray()[pos1].key;
	        short s2 = x2.highlowcontainer.getArray()[pos2].key;

			while (true) {
				if (s1 < s2) {
					answer.highlowcontainer.putEnd(s1, x1.highlowcontainer.get(s1));
					pos1++;
					if (pos1==length1) { 
						do {
							answer.highlowcontainer.putEnd(s2,x2.highlowcontainer.get(s2));
							pos2++;
							if (pos2==length2)
								break;
							s2 = x2.highlowcontainer.getArray()[pos2].key;
						} while (true);
						break main;
					}
					s1 = x1.highlowcontainer.getArray()[pos1].key;
				} else if (s1 > s2) { 
					answer.highlowcontainer.putEnd(s2, x2.highlowcontainer.get(s2));
					pos2++;
					if (pos2==length2) { 
						do {
							answer.highlowcontainer.putEnd(s1,x1.highlowcontainer.get(s1));
							pos1++;
							if (pos1==length1)
								break;
							s1 = x1.highlowcontainer.getArray()[pos1].key;
						} while (true);
						break main;
					}
					s2 = x2.highlowcontainer.getArray()[pos2].key;
				} else {
					//nbXOR++;
					answer.highlowcontainer.putEnd(s1,
							Util.xor(x1.get(s1), x2.get(s2)));
					pos1++;
					pos2++;
					if (pos1==length1) { 
						while (pos2<length2) {
							s2 = x2.highlowcontainer.getArray()[pos2].key;
							answer.highlowcontainer.putEnd(s2, x2.highlowcontainer.get(s2));
							pos2++;
						}
						break main;
					}

					if (pos2==length2) { 
						while (pos1<length1) {
							s1 = x1.highlowcontainer.getArray()[pos1].key;
							answer.highlowcontainer.putEnd(s1, x1.highlowcontainer.get(s1));
							pos1++;
						}
						break main;
					}
					s1 = x1.highlowcontainer.getArray()[pos1].key;
					s2 = x2.highlowcontainer.getArray()[pos2].key;
				}
			}
		}
		return answer;
	}
	
	public  void inPlaceXOR(final SpeedyRoaringBitmap x2) {
		int pos1 = 0, pos2 = 0;
        int length1 = this.getNbNodes(), 
        	length2 = x2.getNbNodes();
        
		main: if (pos1 < length1 && pos2 < length2) {
	        short s1 = this.getArray()[pos1].key;
	        short s2 = x2.getArray()[pos2].key;

			while (true) {
				if (s1 < s2) {
					pos1++;
					if (pos1==length1) { 
						do {
							this.highlowcontainer.put(s2,x2.highlowcontainer.get(s2));
							pos2++;
							if (pos2==length2)
								break;
							s2 = x2.getArray()[pos2].key;
						} while (true);
						break main;
					}
					s1 = this.getArray()[pos1].key;
				} else if (s1 > s2) { 
					this.highlowcontainer.put(s2, x2.highlowcontainer.get(s2));
					pos2++;
					if (pos2==length2) 						
						break main;
					
					s2 = x2.highlowcontainer.getArray()[pos2].key;
				} else {
					//nbXOR++;
					this.highlowcontainer.put(s1,Util.xor(this.get(s1), x2.get(s2)));
					pos1++;
					pos2++;
					if (pos1==length1) { 
						while (pos2<length2) {
							s2 = x2.highlowcontainer.getArray()[pos2].key;
							this.highlowcontainer.put(s2, x2.highlowcontainer.get(s2));
							pos2++;
						}
						break main;
					}

					if (pos2==length2)						
						break main;
					
					s1 = this.getArray()[pos1].key;
					s2 = x2.getArray()[pos2].key;
				}
			}
		}
}

	public int[] getIntegers() {
	    final int[] array = new int[this.getCardinality()];
		int pos=0, pos2=0;
		while(pos<this.highlowcontainer.getnbKeys())
		{
			final short hs = this.highlowcontainer.getArray()[pos].key;
			if(hs<0) System.out.println("negatif hs : "+array[pos2-1]+" hs : "+hs);
			short s = hs;
			final ShortIterator si = this.highlowcontainer.getArray()[pos++].value.getShortIterator();
			while(si.hasNext()) {
				array[pos2] = Util.toIntUnsigned(hs);
				array[pos2] = (array[pos2]<<16);
				short s2 = si.next();
			     array[pos2++] 
			        		|=  
			        			Util.toIntUnsigned(s2);
			     if(array[pos2-1]<0) System.out.println("negatif : "+array[pos2-1]+" hs : "+hs+" s : "+s+" s2 : "+s2);
			}
		}	
		return array;
	}
	
	public void remove(final int x) {
		final short hb = Util.highbits(x);
		if (highlowcontainer.ContainsKey(hb)) {
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
		if (highlowcontainer.ContainsKey(hb)) {
		        final Container C = highlowcontainer.get(hb);
			return C.contains(Util.lowbits(x));
		}
		return false;
	}
        	
	public int getSizeInBytes(){
                int size = 0;
                for (int i=0; i<this.highlowcontainer.getnbKeys(); i++) 
                {
                	Container c = this.highlowcontainer.getArray()[i].value;
	                size+=2+c.getSizeInBytes();
                }
	        return size;
	}
	
	public Container get(short key) {
		return this.highlowcontainer.get(key);
	}
	
	public boolean containsKey(short key) {
		return this.highlowcontainer.ContainsKey(key);
	}	
	
	public Element[] getArray() {
		return this.highlowcontainer.getArray();
	}
	
	/**
	 * return an array that contains the number of short integers in each node. 
	 * The number of short integers of the i th node will be stocked in the array's position i.
	 * @return int[] number of short integers per node   
	 */
	public int[] getIntsPerNode () {
		int nb[] = new int[this.highlowcontainer.getnbKeys()], pos = 0;
		for (int i=0; i<this.highlowcontainer.getnbKeys(); i++)
			nb[pos++] = this.highlowcontainer.getArray()[i].value.getCardinality();
		return nb;
	}
	
	public int getNbNodes() {
		return this.highlowcontainer.getnbKeys();
	}
	
	public int getCardinality() {
		int size = 0;
		for (int i=0; i<this.getNbNodes(); i++) {
             size+=this.highlowcontainer.getArray()[i].value.getCardinality();
                }
		return size;
	}
		
	@Override
	public SpeedyRoaringBitmap clone() {
		try {
			SpeedyRoaringBitmap x = (SpeedyRoaringBitmap) super.clone();
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
		int nbNodes = this.highlowcontainer.getnbKeys();
		int nbArrayC = 0, nbBitmapC=0, minAC=1024, maxAC=0, minBC=65535, maxBC=0, card, avAC=0, avBC=0;
		for (int i=0; i<this.highlowcontainer.getnbKeys(); i++) {
			Container C = this.highlowcontainer.getArray()[i].value;
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
        	for (int i=0; i<this.highlowcontainer.getnbKeys(); i++) {
    			Container c = this.highlowcontainer.getArray()[i].value;
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
