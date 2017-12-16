package com.github.animeshtrivedi.arrowexample;

import com.google.common.collect.ImmutableList;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.file.ArrowBlock;
import org.apache.arrow.vector.file.ArrowFileReader;
import org.apache.arrow.vector.file.SeekableReadChannel;
import org.apache.arrow.vector.types.Types;
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
    private long checkSum = 0;

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
            for(int j = 0; j < fieldVector.size(); j++){
                Types.MinorType mt = fieldVector.get(j).getMinorType();
                switch(mt){
                    case INT: showIntAccessor(fieldVector.get(j)); break;
                    case BIGINT: showBigIntAccessor(fieldVector.get(j)); break;
                    case VARBINARY: showVarBinaryAccessor(fieldVector.get(j)); break;
                    case FLOAT4: showFloat4Accessor(fieldVector.get(j));break;
                    default: throw new Exception(" MinorType " + mt);
                }
                //showAccessor(fieldVector.get(j).getAccessor());
                //System.out.println("\t["+i+"] accessor " + j + " | " + getAccessorString(accessor));
            }
        }
        System.out.println("Done processing the file");
        arrowFileReader.close();
        System.err.println("****** > " + this.checkSum);
    }

    private String getAccessorString(ValueVector.Accessor accessor){
        return  "accessorType: " + accessor.getClass().getCanonicalName()
                + " valueCount " + accessor.getValueCount()
                + " nullCount " + accessor.getNullCount();
    }

    private void showAccessor(ValueVector.Accessor accessor){
        for(int j = 0; j < accessor.getValueCount(); j++){
            if(!accessor.isNull(j)){
                System.out.println("\t\t accessorType:  " + accessor.getClass().getCanonicalName()
                        + " value[" + j +"] " + accessor.getObject(j));
            } else {
                System.out.println("\t\t accessorType:  " + accessor.getClass().getCanonicalName() + " NULL at " + j);
            }
        }
    }

    private void showIntAccessor(FieldVector fx){
        NullableIntVector.Accessor accessor = ((NullableIntVector) fx).getAccessor();
        for(int j = 0; j < accessor.getValueCount(); j++){
            if(!accessor.isNull(j)){
                int value = accessor.get(j);
                System.out.println("\t\t intAccessor[" + j +"] " + value);
                this.checkSum+=value;
            } else {
                System.out.println("\t\t intAccessor[" + j +"] : NULL ");
            }
        }
    }

    private void showBigIntAccessor(FieldVector fx){
        NullableBigIntVector.Accessor accessor = ((NullableBigIntVector)fx).getAccessor();
        for(int j = 0; j < accessor.getValueCount(); j++){
            if(!accessor.isNull(j)){
                long value = accessor.get(j);
                System.out.println("\t\t bigIntAccessor[" + j +"] " + value);
                this.checkSum+=value;
            } else {
                System.out.println("\t\t bigIntAccessor[" + j +"] : NULL ");
            }
        }
    }

    private void showVarBinaryAccessor(FieldVector fx){
        NullableVarBinaryVector.Accessor accessor =((NullableVarBinaryVector) fx).getAccessor();
        for(int j = 0; j < accessor.getValueCount(); j++){
            if(!accessor.isNull(j)){
                byte[] value = accessor.get(j);
                System.out.println("\t\t varBinaryAccessor[" + j +"] " + ArrowExampleClass.firstX(value, 5));
                this.checkSum+=ArrowExampleClass.hashArray(value);
            } else {
                System.out.println("\t\t varBinaryAccessor[" + j +"] : NULL ");
            }
        }
    }

    private void showFloat4Accessor(FieldVector fx){
        NullableFloat4Vector.Accessor accessor = ((NullableFloat4Vector)fx).getAccessor();
        for(int j = 0; j < accessor.getValueCount(); j++){
            if(!accessor.isNull(j)){
                float value = accessor.get(j);
                System.out.println("\t\t float4[" + j +"] " + value);
                this.checkSum+=value;
            } else {
                System.out.println("\t\t float4[" + j +"] : NULL ");
            }
        }
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
