
import java.io.*;
import java.net.*;
import java.text.*;
import java.util.*;
import java.util.zip.*;
import org.roaringbitmap.*;
import org.roaringbitmap.buffer.*;

public class simplebenchmark {

    public static final String CENSUS_INCOME = "census-income";
    public static final String CENSUS1881 = "census1881";
    public static final String DIMENSION_008 = "dimension_008";
    public static final String DIMENSION_003 = "dimension_003";
    public static final String DIMENSION_033 = "dimension_033";
    public static final String USCENSUS2000 = "uscensus2000";
    public static final String WEATHER_SEPT_85 = "weather_sept_85";
    public static final String WIKILEAKS_NOQUOTES = "wikileaks-noquotes";
    public static final String CENSUS_INCOME_SRT = "census-income_srt";
    public static final String CENSUS1881_SRT = "census1881_srt";
    public static final String WEATHER_SEPT_85_SRT = "weather_sept_85_srt";
    public static final String WIKILEAKS_NOQUOTES_SRT = "wikileaks-noquotes_srt";
    public static int rep = 100;
    static List<RoaringBitmap> toBitmaps(ZipRealDataRetriever zip)  throws IOException, URISyntaxException {
        ArrayList<RoaringBitmap> answer = new ArrayList<RoaringBitmap>();
        for(int[] data : zip.fetchBitPositions()) {
            RoaringBitmap r = RoaringBitmap.bitmapOf(data);
            r.runOptimize();
            answer.add(r);
        }
        return answer;
    }
    static List<ImmutableRoaringBitmap> toBufferBitmaps(ZipRealDataRetriever zip)  throws IOException, URISyntaxException {
        ArrayList<ImmutableRoaringBitmap> answer = new ArrayList<ImmutableRoaringBitmap>();
        for(int[] data : zip.fetchBitPositions()) {
            MutableRoaringBitmap r = MutableRoaringBitmap.bitmapOf(data);
            r.runOptimize();
            answer.add(r);
        }
        return answer;
    }

    static int universeSize(ZipRealDataRetriever zip)  throws IOException, URISyntaxException {
        int answer = 0;
        for(int[] data : zip.fetchBitPositions()) {
            if(data[data.length - 1] > answer) answer = data[data.length - 1];
        }
        return answer;
    }

    public static void benchmark(ZipRealDataRetriever zip)  throws IOException,URISyntaxException {
        int maxvalue = universeSize(zip);
        // bitspervalue nanotimefor2by2and nanotimefor2by2or nanotimeforwideor nanotimeforcontains (first normal then buffer)
        System.out.print(String.format("%1$-25s", zip.getName())+"\t\t");
        List<RoaringBitmap> bitmaps = toBitmaps(zip);
        display(bitmaps,maxvalue);
        //
        System.out.print("\t");
        List<ImmutableRoaringBitmap> bufferbitmaps = toBufferBitmaps(zip);
        bufferdisplay(bufferbitmaps,maxvalue);
        System.out.println();


    }

    static void display(List<RoaringBitmap> bitmaps, int maxvalue) {
        long totalcard = 0;
        long totalbytes = 0;
        DecimalFormat dfbitsperval = new DecimalFormat("0.00");

        for(RoaringBitmap r : bitmaps) {
            totalcard += r.getCardinality();
            totalbytes += r.getSizeInBytes();
        }

        List<Long> timings = timings(bitmaps,maxvalue);
        for(int repeat = 0; repeat < rep; ++ repeat) {
            List<Long> newtime = timings(bitmaps,maxvalue);
            for(int i = 0 ; i < newtime.size() ; ++i) {
                if(newtime.get(i) < timings.get(i)) timings.set(i,newtime.get(i));
            }
        }

        System.out.print(String.format("%1$-10s",dfbitsperval.format(totalbytes * 8.0 / totalcard)));
        System.out.flush();
        for(long time : timings) {
            System.out.print(String.format("%1$10s",time));
        }

    }
    static void bufferdisplay(List<ImmutableRoaringBitmap> bitmaps, int maxvalue) {
        long totalcard = 0;
        long totalbytes = 0;
        DecimalFormat dfbitsperval = new DecimalFormat("0.00");

        for(ImmutableRoaringBitmap r : bitmaps) {
            totalcard += r.getCardinality();
            totalbytes += r.getSizeInBytes();
        }

        List<Long> timings = buffertimings(bitmaps,maxvalue);
        for(int repeat = 0; repeat < rep; ++ repeat) {
            List<Long> newtime = buffertimings(bitmaps,maxvalue);
            for(int i = 0 ; i < newtime.size() ; ++i) {
                if(newtime.get(i) < timings.get(i)) timings.set(i,newtime.get(i));
            }
        }

        for(long time : timings) {
            System.out.print(String.format("%1$10s",time));
        }

    }

    static ArrayList<Long> timings(List<RoaringBitmap> bitmaps, int maxvalue) {
        long successive_and = 0;
        long successive_or = 0;
        long total_or = 0;
        long start, stop;
        ArrayList<Long> timings = new ArrayList<Long>();

        start = System.nanoTime();
        for (int i = 0; i < bitmaps.size() - 1; ++i) {
            successive_and += RoaringBitmap.and(bitmaps.get(i),bitmaps.get(i + 1)).getCardinality();
        }
        if(successive_and == 0xFFFFFFFF) System.out.println(); // to defeat clever compilers
        stop = System.nanoTime();
        timings.add(stop - start);

        start = System.nanoTime();
        for (int i = 0; i < bitmaps.size() - 1; ++i) {
            successive_or += RoaringBitmap.or(bitmaps.get(i),bitmaps.get(i + 1)).getCardinality();
        }
        if(successive_or == 0xFFFFFFFF) System.out.println(); // to defeat clever compilers
        stop = System.nanoTime();

        timings.add(stop - start);

        start = System.nanoTime();
        total_or = RoaringBitmap.or(bitmaps.iterator()).getCardinality();
        if(total_or == 0xFFFFFFFF) System.out.println(); // to defeat clever compilers
        stop = System.nanoTime();
        timings.add(stop - start);
        int quartcount = 0;
        start = System.nanoTime();
        for(RoaringBitmap rb : bitmaps) {
            if(rb.contains(maxvalue / 4 )) ++ quartcount;
            if(rb.contains(maxvalue / 2 )) ++ quartcount;
            if(rb.contains(3 * maxvalue / 4 )) ++ quartcount;
        }
        if(quartcount == 0) System.out.println(); // to defeat clever compilers
        stop = System.nanoTime();
        timings.add(stop - start);
        return timings;

    }

    static ArrayList<Long> buffertimings(List<ImmutableRoaringBitmap> bitmaps, int maxvalue) {
        long successive_and = 0;
        long successive_or = 0;
        long total_or = 0;
        long start, stop;
        ArrayList<Long> timings = new ArrayList<Long>();

        start = System.nanoTime();
        for (int i = 0; i < bitmaps.size() - 1; ++i) {
            successive_and += ImmutableRoaringBitmap.and(bitmaps.get(i),bitmaps.get(i + 1)).getCardinality();
        }
        if(successive_and == 0xFFFFFFFF) System.out.println(); // to defeat clever compilers
        stop = System.nanoTime();
        timings.add(stop - start);

        start = System.nanoTime();
        for (int i = 0; i < bitmaps.size() - 1; ++i) {
            successive_or += ImmutableRoaringBitmap.or(bitmaps.get(i),bitmaps.get(i + 1)).getCardinality();
        }
        if(successive_or == 0xFFFFFFFF) System.out.println(); // to defeat clever compilers
        stop = System.nanoTime();
        timings.add(stop - start);

        start = System.nanoTime();
        total_or = ImmutableRoaringBitmap.or(bitmaps.iterator()).getCardinality();
        if(total_or == 0xFFFFFFFF) System.out.println(); // to defeat clever compilers
        stop = System.nanoTime();
        timings.add(stop - start);
        int quartcount = 0;
        start = System.nanoTime();
        for(ImmutableRoaringBitmap rb : bitmaps) {
            if(rb.contains(maxvalue / 4 )) ++ quartcount;
            if(rb.contains(maxvalue / 2 )) ++ quartcount;
            if(rb.contains(3 * maxvalue / 4 )) ++ quartcount;
        }
        if(quartcount == 0) System.out.println(); // to defeat clever compilers
        stop = System.nanoTime();
        timings.add(stop - start);
        return timings;

    }




    public static void main(String[] args)  throws IOException, URISyntaxException {
        if(args.length != 1) {
            System.out.println("You need to provide exactly one parameter.");
            System.out.println("E.g., java simplebenchmark ../real-roaring-dataset/src/main/resources/real-roaring-dataset/census1881.zip");
            return;
        }
        try {
            benchmark(new ZipRealDataRetriever(args[0]));
        } catch(FileNotFoundException  fnf) {
            System.err.println("I can't find the file "+ args[0]);
        }
    }
}


class ZipRealDataRetriever {

    private final String dataset;

    public ZipRealDataRetriever(String dataset) throws IOException, URISyntaxException {
        this.dataset = dataset;
    }

    public List<int[]> fetchBitPositions() throws IOException {
        List<int[]> bitPositions = new ArrayList<>();

        try (final ZipInputStream zis = getResourceAsStream()) {
        BufferedReader buf = new BufferedReader(new InputStreamReader(zis));

        while (true) {
            ZipEntry nextEntry = zis.getNextEntry();
            if (nextEntry == null) {
            break;
            }

            try {
            String oneLine = buf.readLine(); // a single, perhaps very long, line
            String[] positions = oneLine.split(",");
            int[] ans = new int[positions.length];
            for (int i = 0; i < positions.length; i++) {
                ans[i] = Integer.parseInt(positions[i]);
            }
            bitPositions.add(ans);
            } catch (IOException e) {
            throw new RuntimeException(e);
            }
        }
        }
        return bitPositions;
    }

    public String getName() {
        return new File(dataset).getName();
    }

    private ZipInputStream getResourceAsStream() throws FileNotFoundException {
        return new ZipInputStream(new FileInputStream(dataset));
    }

}
