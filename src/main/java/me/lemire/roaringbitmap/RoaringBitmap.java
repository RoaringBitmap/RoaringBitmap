package me.lemire.roaringbitmap;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

public class RoaringBitmap implements Iterable<Integer> {

	SortedMap<Short, Container> c = new TreeMap<Short, Container>(); // does not have to be a tree

	public static void afficher(RoaringBitmap x) {
		final Iterator<Entry<Short, Container>> p1 = x.c.entrySet().iterator();		
		Entry<Short, Container> s1;
		
		while(p1.hasNext()) { 
			s1=p1.next();
			System.out.println("\n"+s1.getKey().shortValue());
			Container c = s1.getValue();
			c.afficher();				
		}
	}
	
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
				if(s1.getKey().shortValue() < s2.getKey().shortValue()) {
					if(!p1.hasNext()) break main;
					s1 = p1.next();
				} else
				if(s1.getKey().shortValue()>s2.getKey().shortValue()) {
					if(!p2.hasNext()) break main;
					s2 = p2.next();
				} else { // égalité
					answer.c.put(s1.getKey(), Util.and(s1.getValue(),s2.getValue()));
					if(!p1.hasNext()) break main;					
					if(!p2.hasNext()) break main;
					s1 = p1.next(); s2 = p2.next();
				}				
			} while(true);
		}
		return answer;		
	}
	
	public static RoaringBitmap or(RoaringBitmap x1, RoaringBitmap x2) {
		RoaringBitmap answer = new RoaringBitmap();
		final Iterator<Entry<Short, Container>> p1 = x1.c.entrySet().iterator();
		final Iterator<Entry<Short, Container>> p2 = x2.c.entrySet().iterator();
		main:
		if(p1.hasNext() && p2.hasNext()) {
			Entry<Short, Container> s1 = p1.next();
			Entry<Short, Container> s2 = p2.next();
						
			while(true) {
				//System.out.println("Compare "+s1.getKey().shortValue()+" et "+s2.getKey().shortValue());
								
				if(s1.getKey().shortValue() < s2.getKey().shortValue()) 
				{
					//System.out.println(s1.getKey().shortValue()+" < "+s2.getKey().shortValue());
					answer.c.put(s1.getKey(),  s1.getValue());
					//Si set p1 terminé, alors ajouter ce qui reste de set p2 dans answer
					if(!p1.hasNext()) { //System.out.println("while s1 < s2");
										do { 
											 answer.c.put(s2.getKey(),  s2.getValue());
											 if(!p2.hasNext()) break;
											 s2 = p2.next();
											}   while(true);
										break main;
									  }
					s1 = p1.next();					
				} else if(s1.getKey().shortValue() > s2.getKey().shortValue()) 
				{	//System.out.println(s1.getKey().shortValue()+" > "+s2.getKey().shortValue());
					answer.c.put(s2.getKey(), s2.getValue());
					//Si set p2 terminé, alors ajouter ce qui reste de set p1 dans answer
					if(!p2.hasNext()) { //System.out.println("while s1 > s2");
										do {  
											 answer.c.put(s1.getKey(),  s1.getValue());
											 if (!p1.hasNext()) break;
											 s1 = p1.next();
											} while(true);  
										break main;
									  }
					s2 = p2.next();					
				} else { // égalité
					//System.out.println(s1.getKey().shortValue()+" = "+s2.getKey().shortValue());
					answer.c.put(s1.getKey(), Util.or(s1.getValue(),s2.getValue()));
					if(!p1.hasNext()) { //System.out.println("while s1 = s2 !p1");
										while(p2.hasNext()) { s2 = p2.next(); 
					  										  answer.c.put(s2.getKey(),  s2.getValue());
					  										}   
										break main;
									  }
					
					if(!p2.hasNext()) { //System.out.println("while s1 = s2 !p2");
										while(p1.hasNext()) { s1 = p1.next(); 
															  answer.c.put(s1.getKey(),  s1.getValue());
					  										}   
										break main;
									  }					
					s1 = p1.next(); s2 = p2.next();
				}				
			} 
		}
		return answer;		
	}
	
	public static RoaringBitmap xor(RoaringBitmap x1, RoaringBitmap x2) {
		RoaringBitmap answer = new RoaringBitmap();
		final Iterator<Entry<Short, Container>> p1 = x1.c.entrySet().iterator();
		final Iterator<Entry<Short, Container>> p2 = x2.c.entrySet().iterator();
		main:
		if(p1.hasNext() && p2.hasNext()) {
			Entry<Short, Container> s1 = p1.next();
			Entry<Short, Container> s2 = p2.next();
			
			int cas=0;
			
			while(true) {
				//System.out.println("Compare "+s1.getKey().shortValue()+" et "+s2.getKey().shortValue());
								
				if(s1.getKey().shortValue() < s2.getKey().shortValue()) 
				{
					//System.out.println(s1.getKey().shortValue()+" < "+s2.getKey().shortValue());
					answer.c.put(s1.getKey(),  s1.getValue());
					//Si set p1 terminé, alors ajouter ce qui reste de set p2 dans answer
					if(!p1.hasNext()) { //System.out.println("while s1 < s2");
										do { 
											 answer.c.put(s2.getKey(),  s2.getValue());
											 if(!p2.hasNext()) break;
											 s2 = p2.next();
											}   while(true);
										break main;
									  }
					s1 = p1.next();						
				} else if(s1.getKey().shortValue() > s2.getKey().shortValue()) 
				{	//System.out.println(s1.getKey().shortValue()+" > "+s2.getKey().shortValue());
					answer.c.put(s2.getKey(), s2.getValue());
					//Si set p2 terminé, alors ajouter ce qui reste de set p1 dans answer
					if(!p2.hasNext()) { //System.out.println("while s1 > s2");
										do {  
											 answer.c.put(s1.getKey(),  s1.getValue());
											 if (!p1.hasNext()) break;
											 s1 = p1.next();
											} while(true);  
										break main;
									  }
					s2 = p2.next();					
				} else { // égalité
					//System.out.println(s1.getKey().shortValue()+" = "+s2.getKey().shortValue());
					Container C = null; C = Util.xor(s1.getValue(),s2.getValue());
					if(C!=null) answer.c.put(s1.getKey(), C);
					if(!p1.hasNext()) { //System.out.println("while s1 = s2 !p1");
										while(p2.hasNext()) { s2 = p2.next(); 
					  										  answer.c.put(s2.getKey(),  s2.getValue());
					  										}   
										break main;
									  }
					
					if(!p2.hasNext()) { //System.out.println("while s1 = s2 !p2");
										while(p1.hasNext()) { s1 = p1.next(); 
															  answer.c.put(s1.getKey(),  s1.getValue());
					  										}   
										break main;
									  }					
					s1 = p1.next(); s2 = p2.next();
				}				
			} 
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
