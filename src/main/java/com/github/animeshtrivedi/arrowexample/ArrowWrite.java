package com.github.animeshtrivedi.FileBench.example;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import com.google.common.collect.ImmutableList;
import io.netty.buffer.ArrowBuf;
import org.apache.arrow.memory.*;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.schema.ArrowVectorType;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.file.ArrowFileWriter;
import org.apache.arrow.vector.schema.TypeLayout;
import org.apache.arrow.vector.types.pojo.*;

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
    private int maxEntries = 1024;

    public ArrowWrite(){
        random = new Random(System.nanoTime());
        this.entries = 10; //this.random.nextInt(this.maxEntries);
        this.data = new ArrowExampleClass[this.entries];
        for(int i =0; i < this.entries; i++){
            this.data[i] = new ArrowExampleClass(this.random);
        }
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

    public static void main(String[] args) {
        System.out.println("Hello World!"); // Display the string.
        ArrowWrite ex = new ArrowWrite();
        try {
            ex.makeWrite("./example.arrow");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void makeWrite(String filename) throws Exception {
        File arrowFile = validateFile(filename, false);
        FileOutputStream fileOutputStream = new FileOutputStream(arrowFile);
        Schema schema = makeSchema();
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, this.ra);
        DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
        ArrowFileWriter arrowWriter = new ArrowFileWriter(root, provider, fileOutputStream.getChannel());
        arrowWriter.start();
        System.out.println(" Generated " + this.entries + " data entries ");
        root.setRowCount(this.entries);
        for (Field field : root.getSchema().getFields()) {
            FieldVector vector = root.getVector(field.getName());
            System.out.println(" \t >> minor type on the column " + vector.getMinorType());
            switch (vector.getMinorType()) {
                case INT: writeFieldInt(field, vector); break;
                case BIGINT: writeFieldLong(field, vector); break;
                case VARBINARY: writeFieldVarBinary(field, vector); break;
                case FLOAT4: writeFieldFloat4(field, vector); break;
                default: throw new Exception(" Does not work " + vector.getMinorType());
            }
        }
        arrowWriter.end();
        arrowWriter.close();
    }

    private void writeFieldInt(Field field, FieldVector fieldVector){
        fieldVector.getMutator().setValueCount(this.entries * 2);
        ArrowBuf buf = fieldVector.getDataBuffer();
        for(int i = 0; i < this.entries; i++){
            buf.setInt(i, this.data[i].anInt);
        }
    }

    private void writeFieldLong(Field field, FieldVector fieldVector){
        fieldVector.getMutator().setValueCount(this.entries * 2);
        ArrowBuf buf = fieldVector.getDataBuffer();
        for(int i = 0; i < this.entries; i++){
            buf.setLong(i, this.data[i].aLong);
        }
    }

    private void writeFieldVarBinary(Field field, FieldVector fieldVector){

    }

    private void writeFieldFloat4(Field field, FieldVector fieldVector){
        fieldVector.getMutator().setValueCount(this.entries * 2);
        ArrowBuf buf = fieldVector.getDataBuffer();
        for(int i = 0; i < this.entries; i++){
            buf.setFloat(i, this.data[i].aFloat);
        }
    }

    private void writeFieldLayout(Field field, FieldVector fieldVector){
        // per field execution
        TypeLayout typeLayout = TypeLayout.getTypeLayout(field.getType());
        List<ArrowVectorType> vectorTypes = field.getTypeLayout().getVectorTypes();
        List<BufferBacked> fieldInnerVectors = fieldVector.getFieldInnerVectors();
        if (vectorTypes.size() != fieldInnerVectors.size()) {
            throw new IllegalArgumentException("vector types and inner vectors are not the same size: " + vectorTypes.size() + " != " + fieldInnerVectors.size());
        }
        System.out.println(" ----- " + field.toString() + " -------- ");
        System.out.println("FieldVector type: " + fieldVector.getClass().getCanonicalName());
        System.out.println(" typeLayout is " +  typeLayout.toString() + " vectorSize is " + vectorTypes.size() + " innverVector Size: " + fieldInnerVectors.size());
        for(int i = 0; i < vectorTypes.size(); i++){
            /* fields in the vector type tells how to locate, for primitive types it has only 2 validity and data
            whereas for binary it has 3, validity, offset and data. I suppose if I remove the nullable part, then
            it will be only 1 and 2 types - can confirm? TODO:
            */
            System.out.println(" \t vector type entries [" + i + "] " + vectorTypes.get(i).toString());
        }
        System.out.println(" ------------- ");

        fieldVector.allocateNew();
        fieldVector.getMutator().setValueCount(this.entries);
        System.out.println(" Setting up the mutator count to be " + this.entries + " ** " + fieldVector.getMutator().getClass().getCanonicalName() + " >>>>> " + field.getFieldType().getType().getTypeID());
        /* based upon the schema */
        for (int v = 0; v < vectorTypes.size(); v++){
            /* we get specific ArrowVectorType and associated BufferBacked */
            ArrowVectorType vectorType = vectorTypes.get(v);
            BufferBacked bufferBacked = fieldInnerVectors.get(v);
            /* I don't like this explicit casting ? */
            ValueVector valueVector = (ValueVector) bufferBacked;
            System.out.println("\t valueVector minor type is : " + valueVector.getMinorType());
            valueVector.setInitialCapacity(this.entries);
            valueVector.allocateNew();
            valueVector.clear();
        }
    }

    private void writeInts(IntVector intVector) throws IOException {
        intVector.setInitialCapacity(this.entries);
        intVector.clear();
        for(int i=0; i< this.entries; i++){
            intVector.getMutator().set(i, this.data[i].anInt);
        }
    }

    private VarBinaryVector writeBinary() throws IOException {
        VarBinaryVector binaryVector = new VarBinaryVector("varBinary", this.ra);
        binaryVector.setInitialCapacity(this.entries);
        binaryVector.clear();
        for(int i=0; i< this.entries; i++){
            binaryVector.getMutator().setSafe(i, this.data[i].arr, 0, this.data[i].arr.length);
        }
        return binaryVector;
    }
}