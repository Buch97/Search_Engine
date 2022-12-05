package it.unipi.bean;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class InvertedIndex {
    private final RandomAccessFile raf;
    private static MappedByteBuffer index;

    public InvertedIndex(String filename) throws IOException {
        this.raf = new RandomAccessFile(filename, "rw");
        this.index = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, Files.size(Paths.get(filename)));
        

    }

    public static MappedByteBuffer getIndex(){
        return index;
    }
}
