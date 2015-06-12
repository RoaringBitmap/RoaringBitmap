package org.roaringbitmap.bithacking;


import org.openjdk.jmh.annotations.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class UnsignedVSFlip {
    short[] key;
    int N;
    
    @Setup
    public void setup() {
        Random r = new Random();
        N = 65536;
        key = new short[N];
        for(int k = 0; k < N; ++k)
            while(key[k] == 0) 
            	key[k] = (short)r.nextInt();        
    }
    
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public int tointUnsignedTime() {
        int answer = 0;
        for(int k = 0; k < N; ++k)
            answer += key[k] & 0xFFFF;
        return answer;
    }
    
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public int flipTime() {
        int answer = 0;
        for(int k = 0; k < N; ++k)
            answer += (short) (key[k] ^ Short.MIN_VALUE);
        return answer;
    }
    

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public int flipTimeNoCast() {
        int answer = 0;
        for(int k = 0; k < N; ++k)
            answer += key[k] ^ Short.MIN_VALUE;
        return answer;
    }
}