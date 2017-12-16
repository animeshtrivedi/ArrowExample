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

    public ArrowExampleClass(Random random, int index){
        this.random = random;
        this.anInt = index; // this.random.nextInt();
        this.aLong = this.random.nextLong();
        this.arr = new byte[this.random.nextInt(1024)];
        this.random.nextBytes(this.arr);
        this.aFloat = this.random.nextFloat();
    }

    public String firstX(int items){
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < items; i++) {
            sb.append(String.format("0x%02x", this.arr[i])+ " ");
        }
        return sb.toString();
    }
    public String toString() {
        return  anInt + "\t | " +
                + aLong + "\t | " +
                " arr[" + this.arr.length + "] " + firstX(5) + "\t | " +
                + aFloat;
    }
}
