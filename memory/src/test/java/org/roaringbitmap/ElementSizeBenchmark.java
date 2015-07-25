package org.roaringbitmap;

import net.sourceforge.sizeof.SizeOf;
import org.junit.Test;

import java.util.*;

public class ElementSizeBenchmark {
	
	
	private static void calculateElementAverageSize() {
		Random r = new Random();
		RoaringBitmap rb= new RoaringBitmap();
		int N= 65536;
		for(int i=0; i<N; i++)
			rb.add(r.nextInt() % N);
	    System.out.println("Number of containers :"+rb.highLowContainer.size());
	    for(int k = 0; k < rb.highLowContainer.size(); ++k ) {
		  System.out.println("shallow size ="+SizeOf.humanReadable(SizeOf.sizeOf(rb.highLowContainer.array[k])));

		  System.out.println("deep size ="+SizeOf.humanReadable(SizeOf.deepSizeOf(rb.highLowContainer.array[k])));
	    }
	    
	}   

    @Test
	public void benchmark() {
		Locale.setDefault(Locale.US);
        System.out.println("# " + System.getProperty("java.vendor")
                + " " + System.getProperty("java.version") + " "
                + System.getProperty("java.vm.name"));
        System.out.println("# " + System.getProperty("os.name") + " "
                + System.getProperty("os.arch") + " "
                + System.getProperty("os.version"));
        System.out.println("# processors: "
                + Runtime.getRuntime().availableProcessors());
        System.out.println("# max mem.: "
                + Runtime.getRuntime().maxMemory()/1024);
        System.out.println("########");
		try {
            SizeOf.setMinSizeToLog(0);
            SizeOf.skipStaticField(true);
            SizeOf.skipFinalField(false);
    }catch (IllegalStateException e) {
            System.out.println("# disabling sizeOf, run  -javaagent:lib/SizeOf.jar or equiv. to enable");
    }
		calculateElementAverageSize();
	}
}