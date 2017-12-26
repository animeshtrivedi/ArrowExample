package com.github.animeshtrivedi.arrowexample;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * Created by atr on 21.12.17.
 */
public class ArrowOutputStream implements WritableByteChannel {
    private FileOutputStream outStream;
    private Boolean isOpen;
    private byte[] tempBuffer;
    private long bytesSoFar;

    public ArrowOutputStream(FileOutputStream outStream){
        this.outStream = outStream;
        this.isOpen = true;
        this.tempBuffer = new byte[1024*1024]; // 1MB buffering
        this.bytesSoFar = 0;
    }

    private int writeDirectBuffer(ByteBuffer src) throws IOException {
        int remaining = src.remaining();
        int soFar = 0;
        while(soFar < remaining){
            int toPush = Math.min(remaining - soFar, this.tempBuffer.length);
            // this will move the position index
            src.get(this.tempBuffer, 0, toPush);
            // i have no way of knowing how much can i push at HDFS
            this.outStream.write(this.tempBuffer, 0, toPush);
            soFar+=toPush;
        }
        this.bytesSoFar+=remaining;
        return remaining;
    }

    private int writeHeapBuffer(ByteBuffer src) throws IOException {
        int remaining = src.remaining();
        // get the heap buffer directly and pass the offset and length
        this.outStream.write(src.array(), src.position(), src.remaining());
        src.position(src.position() + remaining);
        this.bytesSoFar+=remaining;
        return remaining;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        if(src.isDirect()){
            return writeDirectBuffer(src);
        } else {
            return writeHeapBuffer(src);
        }
    }

    @Override
    public boolean isOpen() {
        return this.isOpen;
    }

    @Override
    public void close() throws IOException {
        // flushes the client buffer
        this.outStream.flush();
        this.outStream.close();
        this.isOpen = false;
        System.out.println("Output stream wrote " + this.bytesSoFar + " bytes");
    }
}