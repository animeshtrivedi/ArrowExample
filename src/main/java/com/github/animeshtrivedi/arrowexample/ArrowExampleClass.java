package com.github.animeshtrivedi.arrowexample;

import java.util.Random;

/**
 * Created by atr on 15.12.17.
 */
public class ArrowExampleClass {
    public int anInt;
    public long aLong;
    public byte[] arr;
    public float aFloat;
    public Random random;

    public ArrowExampleClass(Random random){
        this.random = random;
        this.anInt = this.random.nextInt();
        this.aLong = this.random.nextLong();
        this.arr = new byte[this.random.nextInt(1024)];
        this.random.nextBytes(this.arr);
        this.aFloat = this.random.nextFloat();
    }

}
