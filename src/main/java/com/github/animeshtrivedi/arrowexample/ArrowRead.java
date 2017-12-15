package com.github.animeshtrivedi.FileBench.example;

import com.google.common.collect.ImmutableList;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.file.ArrowFileReader;
import org.apache.arrow.vector.file.SeekableReadChannel;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import static org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE;

/**
 * Created by atr on 15.12.17.
 */
public class ArrowRead {
    private RootAllocator ra = null;

    public ArrowRead(){
        this.ra = new RootAllocator(Integer.MAX_VALUE);
    }

    private Schema makeSchema(){
        ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
        childrenBuilder.add(new Field("int", FieldType.nullable(new ArrowType.Int(32, true)), null));
        childrenBuilder.add(new Field("long", FieldType.nullable(new ArrowType.Int(64, true)), null));
        childrenBuilder.add(new Field("binary", FieldType.nullable(new ArrowType.Binary()), null));
        childrenBuilder.add(new Field("double", FieldType.nullable(new ArrowType.FloatingPoint(SINGLE)), null));
        return new Schema(childrenBuilder.build(), null);
    }

    private File validateFile(String fileName, boolean shouldExist) {
        if (fileName == null) {
            throw new IllegalArgumentException("missing file parameter");
        }
        File f = new File(fileName);
        if (shouldExist && (!f.exists() || f.isDirectory())) {
            throw new IllegalArgumentException(fileName + " file not found: " + f.getAbsolutePath());
        }
        if (!shouldExist && f.exists()) {
            throw new IllegalArgumentException(fileName + " file already exists: " + f.getAbsolutePath());
        }
        return f;
    }

    public void makeRead(String filename) throws Exception {
        File arrowFile = validateFile(filename, true);
        FileInputStream fileInputStream = new FileInputStream(arrowFile);
        DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();

        ArrowFileReader arrowFileReader = new ArrowFileReader(new SeekableReadChannel(fileInputStream.getChannel()),
                this.ra);
        VectorSchemaRoot root  = arrowFileReader.getVectorSchemaRoot();
        System.out.println(" schema is "  + root.getSchema().toString());
        System.out.println(" numRows: " + root.getRowCount());

    }
    public static void main(String[] args) {
        System.out.println("Hello World!"); // Display the string.
        ArrowRead ex = new ArrowRead();
        try {
            ex.makeRead("./example.arrow");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
