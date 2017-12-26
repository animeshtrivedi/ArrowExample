package com.github.animeshtrivedi.arrowexample;

import java.io.File;
import java.io.FileOutputStream;
import java.util.List;
import java.util.Random;

import com.google.common.collect.ImmutableList;
import io.netty.buffer.ArrowBuf;
import org.apache.arrow.memory.*;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.pojo.*;

import static com.github.animeshtrivedi.arrowexample.Utils.validateFile;
import static org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE;

/**
 * Created by atr on 14.12.17.
 */
public class ArrowWrite {
    // lets say we want to make a schema of int, long, binary, double
    // we have 1 element

    private ArrowExampleClass data[];
    private RootAllocator ra = null;
    private Random random;
    private int entries;
    private int maxEntries;
    private long checkSum;
    private long nullEntries;
    private boolean useNullValues;
    private FileOutputStream fileOutputStream;
    private VectorSchemaRoot root;
    private ArrowFileWriter arrowFileWriter;
    private int batchSize;

    public ArrowWrite(){
        this.useNullValues = false;
        this.nullEntries = 0;
        this.maxEntries = 1024;
        this.checkSum = 0;
        this.batchSize = 100;
        random = new Random(System.nanoTime());
        this.entries = this.random.nextInt(this.maxEntries);
        this.data = new ArrowExampleClass[this.entries];
        for(int i =0; i < this.entries; i++){
            this.data[i] = new ArrowExampleClass(this.random, i);
            long csum = this.data[i].getSumHash();
            //System.out.println(this.data[i].toString() + " csum: " + csum);
            checkSum+=csum;
        }
        long s1 = showColumnSum();
        System.out.println();
        //essentially here is the problem - this sum should match
        //System.out.println("They match : " + (s1 == checkSum) + " colSum " + s1 + " rowSum " + this.checkSum + " difference is " + (this.checkSum - s1));
        this.ra = new RootAllocator(Integer.MAX_VALUE);
    }

    private long showColumnSum(){
        long intSum = 0;
        long longSum = 0;
        long arrSum = 0;
        long floatSum = 0;
        for(int i =0; i < this.entries; i++){
            intSum+=this.data[i].anInt;
            longSum+=this.data[i].aLong;
            arrSum+=ArrowExampleClass.hashArray(this.data[i].arr);
            floatSum+=this.data[i].aFloat;
        }
        System.out.println("intSum " + intSum + " longSum " + longSum + " arrSum " + arrSum + " floatSum " + floatSum);
        return intSum + longSum + arrSum + floatSum;
    }


    private Schema makeSchema(){
        ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
        childrenBuilder.add(new Field("int", FieldType.nullable(new ArrowType.Int(32, true)), null));
        childrenBuilder.add(new Field("long", FieldType.nullable(new ArrowType.Int(64, true)), null));
        childrenBuilder.add(new Field("binary", FieldType.nullable(new ArrowType.Binary()), null));
        childrenBuilder.add(new Field("double", FieldType.nullable(new ArrowType.FloatingPoint(SINGLE)), null));
        return new Schema(childrenBuilder.build(), null);
    }

    public void setupWrite(String filename, boolean useCustom) throws Exception {
        File arrowFile = validateFile(filename, false);
        this.fileOutputStream = new FileOutputStream(arrowFile);
        Schema schema = makeSchema();
        this.root = VectorSchemaRoot.create(schema, this.ra);
        DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
        if (!useCustom) {
            /* default java implementation of the channel */
            this.arrowFileWriter = new ArrowFileWriter(root,
                    provider,
                    this.fileOutputStream.getChannel());
        } else {
            /* custom channel implementation in ArrowOutputStream */
            this.arrowFileWriter = new ArrowFileWriter(root,
                    provider,
                    new ArrowOutputStream(this.fileOutputStream));
        }

        if (false) {
            // show some stuff about the schema and layout
            for (Field field : root.getSchema().getFields()) {
                FieldVector vector = root.getVector(field.getName());
                showFieldLayout(field, vector);
            }
        }
        System.out.println("Generated " + this.entries + " data entries , batch size " + batchSize + " usingCustomWriter: " + useCustom + " useNullValues " + this.useNullValues);
    }
    public void writeData() throws Exception{
        // writing logic starts here
        this.batchSize = 100;
        arrowFileWriter.start();
        for(int i = 0; i < this.entries;) {
            int toProcessItems = Math.min(this.batchSize, this.entries - i);
            // set the batch row count
            root.setRowCount(toProcessItems);
            for (Field field : root.getSchema().getFields()) {
                FieldVector vector = root.getVector(field.getName());
                switch (vector.getMinorType()) {
                    case INT:
                        writeFieldInt(vector, i, toProcessItems);
                        break;
                    case BIGINT:
                        writeFieldLong(vector, i, toProcessItems);
                        break;
                    case VARBINARY:
                        writeFieldVarBinary(vector, i, toProcessItems);
                        break;
                    case FLOAT4:
                        writeFieldFloat4(vector, i, toProcessItems);
                        break;
                    default:
                        throw new Exception(" Not supported yet type: " + vector.getMinorType());
                }
            }
            arrowFileWriter.writeBatch();
            i+=toProcessItems;
        }
        arrowFileWriter.end();
        arrowFileWriter.close();
        fileOutputStream.flush();
        fileOutputStream.close();
        System.err.println("****** : " + this.checkSum);
    }

    private int isSet(){
        if(useNullValues) {
            if (this.random.nextInt() % 10 == 0) {
                this.nullEntries++;
                return 0;
            }
        }
        return 1;
    }

    private void writeFieldInt(FieldVector fieldVector, int from, int items){
        IntVector intVector = (IntVector) fieldVector;
        intVector.setInitialCapacity(items);
        intVector.allocateNew();
        for(int i = 0; i < items; i++){
            intVector.setSafe(i, isSet(), this.data[from + i].anInt);
        }
        // how many are set
        fieldVector.setValueCount(items);
    }

    private void writeFieldLong(FieldVector fieldVector, int from, int items){
        BigIntVector bigIntVector = (BigIntVector) fieldVector;
        bigIntVector.setInitialCapacity(items);
        bigIntVector.allocateNew();
        for(int i = 0; i < items; i++){
            bigIntVector.setSafe(i, isSet(), this.data[from + i].aLong);
        }
        // how many are set
        bigIntVector.setValueCount(items);
    }

    private void writeFieldVarBinary(FieldVector fieldVector, int from, int items){
        VarBinaryVector varBinaryVector = (VarBinaryVector) fieldVector;
        varBinaryVector.setInitialCapacity(items);
        varBinaryVector.allocateNew();
        for(int i = 0; i < items; i++){
            if(isSet() == 0){
                varBinaryVector.setNull(i);
            } else {
                varBinaryVector.setIndexDefined(i);
                varBinaryVector.setValueLengthSafe(i, this.data[from + i].arr.length);
                varBinaryVector.setSafe(i, this.data[from + i].arr);
            }
        }
        // how many are set
        varBinaryVector.setValueCount(items);
    }

    private void writeFieldFloat4(FieldVector fieldVector, int from, int items){
        Float4Vector float4Vector  = (Float4Vector ) fieldVector;
        float4Vector.setInitialCapacity(items);
        float4Vector.allocateNew();
        for(int i = 0; i < items; i++){
            float4Vector.setSafe(i, isSet(), this.data[from + i].aFloat);
        }
        // how many are set
        float4Vector.setValueCount(items);
    }

    private void showFieldLayout(Field field, FieldVector fieldVector){
        // per field execution
        TypeLayout typeLayout = TypeLayout.getTypeLayout(field.getType());
        List<BufferLayout.BufferType> vectorTypes = typeLayout.getBufferTypes();
        ArrowBuf[] vectorBuffers = new ArrowBuf[vectorTypes.size()];

        if (vectorTypes.size() != vectorBuffers.length) {
            throw new IllegalArgumentException("vector types and vector buffers are not the same size: " + vectorTypes.size() + " != " + vectorBuffers.length);
        }
        System.out.println(" ----- [ " + field.toString() + " ] -------- ");
        System.out.println("FieldVector type: " + fieldVector.getClass().getCanonicalName());
        System.out.println("TypeLayout is " +  typeLayout.toString() + " vectorSize is " + vectorTypes.size());
        for(int i = 0; i < vectorTypes.size(); i++){
            /* fields in the vector type tells how to locate, for primitive types it has only 2 validity and data
            whereas for binary it has 3, validity, offset and data. I suppose if I remove the nullable part, then
            it will be only 1 and 2 types - can confirm? TODO:
            */
            System.out.println(" \t vector type entries [" + i + "] " + vectorTypes.get(i).toString());
        }
        System.out.println("*********************************************");
//
//        fieldVector.allocateNew();
//        fieldVector.getMutator().setValueCount(this.entries);
//        System.out.println(" Setting up the mutator count to be " + this.entries + " ** " + fieldVector.getMutator().getClass().getCanonicalName() + " >>>>> " + field.getFieldType().getType().getTypeID());
//        /* based upon the schema */
//        for (int v = 0; v < vectorTypes.size(); v++){
//            /* we get specific ArrowVectorType and associated BufferBacked */
//            ArrowVectorType vectorType = vectorTypes.get(v);
//            BufferBacked bufferBacked = fieldInnerVectors.get(v);
//            /* I don't like this explicit casting ? */
//            ValueVector valueVector = (ValueVector) bufferBacked;
//            System.out.println("\t valueVector minor type is : " + valueVector.getMinorType());
//            valueVector.setInitialCapacity(this.entries);
//            valueVector.allocateNew();
//            valueVector.clear();
//        }
    }

    public static void main(String[] args) {
        ArrowWrite ex = new ArrowWrite();
        try {
            System.out.println("Number of arguments " + args.length);
            if(args.length == 2){
                ex.useNullValues = true;
                ex.setupWrite("./example.arrow", true);
            } else if(args.length == 1){
                ex.setupWrite("./example.arrow", true);
            } else{
                ex.setupWrite("./example.arrow", false);
            }
            ex.writeData();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("null entries " + ex.nullEntries);
    }
}