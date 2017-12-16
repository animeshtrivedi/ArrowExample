package com.github.animeshtrivedi.arrowexample;

import com.google.common.collect.ImmutableList;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.file.ArrowBlock;
import org.apache.arrow.vector.file.ArrowFileReader;
import org.apache.arrow.vector.file.SeekableReadChannel;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

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
        System.out.println("\nReading the arrow file : " + filename);
        VectorSchemaRoot root  = arrowFileReader.getVectorSchemaRoot();
        System.out.println("File size : " + arrowFile.length() +
                " schema is "  + root.getSchema().toString());

        List<ArrowBlock> arrowBlocks = arrowFileReader.getRecordBlocks();
        System.out.println("Number of arrow blocks are " + arrowBlocks.size());
        for (int i = 0; i < arrowBlocks.size(); i++) {
            ArrowBlock rbBlock = arrowBlocks.get(i);
            if (!arrowFileReader.loadRecordBatch(rbBlock)) {
                throw new IOException("Expected to read record batch");
            }
            System.out.println("\t["+i+"] ArrowBlock, offset: " + rbBlock.getOffset() +
                    ", metadataLength: " + rbBlock.getMetadataLength() +
                    ", bodyLength " + rbBlock.getBodyLength());
            /* we can now process this block, it is now loaded */
            System.out.println("\t["+i+"] row count for this block is " + root.getRowCount());
            List<FieldVector> fieldVector = root.getFieldVectors();
            System.out.println("\t["+i+"] number of fieldVectors (corresponding to columns) : " + fieldVector.size());

            FieldVector fieldVector1 = fieldVector.get(0);
            ValueVector.Accessor accessor = fieldVector1.getAccessor();
            System.out.println("\t["+i+"] accessor 0 " + getAccessorString(accessor));
            for(int j = 0; j < accessor.getValueCount(); j++){
                if(!accessor.isNull(j)){
                    System.out.println("\t\t intAccessor " + accessor.getObject(j));
                } else {
                    System.out.println("\t\t intAccessor NULL at " + j);
                }
            }
        }
        System.out.println("Done processing the file");
        arrowFileReader.close();
    }

    private String getAccessorString(ValueVector.Accessor accessor){
        return  "accessorType: " + accessor.getClass().getCanonicalName()
                + " valueCount " + accessor.getValueCount()
                + " nullCount " + accessor.getNullCount();
    }

//
//        int batchNumber = 0;
//        int runningIndex = 0;
//        while(hasMore){
//            System.out.println("\t processing batch ..." + batchNumber);
//            // do something here
//            FieldVector fv = root.getVector("int");
//            ValueVector.Accessor accessor = fv.getAccessor();
//            System.out.println("\tintAccessor | nullCount: " + accessor.getNullCount() + " getValueCount: " + accessor.getValueCount());
//            System.out.println("\t isNull at 0 " + accessor.isNull(0) + " value " + accessor.getObject(0));
//            // and then accounting stuff
//            batchNumber++;
//            hasMore = arrowFileReader.loadNextBatch();
//        }

    public void someDeadCode(ArrowFileReader arrowFileReader) throws IOException {
        for (ArrowBlock rbBlock : arrowFileReader.getRecordBlocks()) {
            if (!arrowFileReader.loadRecordBatch(rbBlock)) {
                throw new IOException("Expected to read record batch");
            }
            System.out.println(" \t\t " + rbBlock);
        }
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
