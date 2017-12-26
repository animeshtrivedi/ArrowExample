package com.github.animeshtrivedi.arrowexample;

import com.google.common.collect.ImmutableList;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

import static com.github.animeshtrivedi.arrowexample.Utils.validateFile;
import static org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE;

/**
 * Created by atr on 15.12.17.
 */
public class ArrowRead {
    private RootAllocator ra = null;
    private long checkSumx;
    private long intCsum;
    private long longCsum;
    private long arrCsum;
    private long floatCsum;
    private long nullEntries;

    public ArrowRead(){
        this.ra = new RootAllocator(Integer.MAX_VALUE);
        this.nullEntries = 0;
        this.checkSumx = 0;
        this.intCsum = 0;
        this.longCsum = 0;
        this.arrCsum = 0;
        this.floatCsum = 0;
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
                    case FLOAT8: showFloat8Accessor(fieldVector.get(j));break;
                    default: throw new Exception(" MinorType " + mt);
                }
                //showAccessor(fieldVector.get(j).getAccessor());
                //System.out.println("\t["+i+"] accessor " + j + " | " + getAccessorString(accessor));
            }
        }
        System.out.println("Done processing the file");
        arrowFileReader.close();
        long s1 = this.intCsum + this.longCsum + this.arrCsum + this.floatCsum;
        System.out.println("intSum " + intCsum + " longSum " + longCsum + " arrSum " + arrCsum + " floatSum " + floatCsum + " = " + s1);
        System.err.println("Colsum Checksum > " + this.checkSumx + " , difference " + (s1 - this.checkSumx));
    }

    private String getAccessorString(ValueVector accessor){
        return  "accessorType: " + accessor.getClass().getCanonicalName()
                + " valueCount " + accessor.getValueCount()
                + " nullCount " + accessor.getNullCount();
    }

    private void showAccessor(ValueVector accessor){
        for(int j = 0; j < accessor.getValueCount(); j++){
            if(!accessor.isNull(j)){
                System.out.println("\t\t accessorType:  " + accessor.getClass().getCanonicalName()
                        + " value[" + j +"] " + accessor.getObject(j));
            } else {
                this.nullEntries++;
                System.out.println("\t\t accessorType:  " + accessor.getClass().getCanonicalName() + " NULL at " + j);
            }
        }
    }

    private void showIntAccessor(FieldVector fx){
        IntVector intVector = ((IntVector) fx);
        for(int j = 0; j < intVector.getValueCount(); j++){
            if(!intVector.isNull(j)){
                int value = intVector.get(j);
                System.out.println("\t\t intAccessor[" + j +"] " + value);
                intCsum+=value;
                this.checkSumx+=value;
            } else {
                this.nullEntries++;
                System.out.println("\t\t intAccessor[" + j +"] : NULL ");
            }
        }
    }

    private void showBigIntAccessor(FieldVector fx){
        BigIntVector bigIntVector = ((BigIntVector)fx);
        for(int j = 0; j < bigIntVector.getValueCount(); j++){
            if(!bigIntVector.isNull(j)){
                long value = bigIntVector.get(j);
                System.out.println("\t\t bigIntAccessor[" + j +"] " + value);
                longCsum+=value;
                this.checkSumx+=value;
            } else {
                this.nullEntries++;
                System.out.println("\t\t bigIntAccessor[" + j +"] : NULL ");
            }
        }
    }

    private void showVarBinaryAccessor(FieldVector fx){
        VarBinaryVector varBinaryVector =((VarBinaryVector) fx);
        for(int j = 0; j < varBinaryVector.getValueCount(); j++){
            if(!varBinaryVector.isNull(j)){
                byte[] value = varBinaryVector.get(j);
                long valHash = ArrowExampleClass.hashArray(value);
                System.out.println("\t\t varBinaryAccessor[" + j +"] " + ArrowExampleClass.firstX(value, 5));
                arrCsum += valHash;
                this.checkSumx+=valHash;
            } else {
                this.nullEntries++;
                System.out.println("\t\t varBinaryAccessor[" + j +"] : NULL ");
            }
        }
    }

    private void showFloat4Accessor(FieldVector fx){
        Float4Vector float4Vector = ((Float4Vector)fx);
        for(int j = 0; j < float4Vector.getValueCount(); j++){
            if(!float4Vector.isNull(j)){
                float value = float4Vector.get(j);
                System.out.println("\t\t float4[" + j +"] " + value);
                floatCsum+=value;
                this.checkSumx+=value;
            } else {
                this.nullEntries++;
                System.out.println("\t\t float4[" + j +"] : NULL ");
            }
        }
    }

    private void showFloat8Accessor(FieldVector fx){
        Float8Vector float8Vector = ((Float8Vector)fx);
        for(int j = 0; j < float8Vector.getValueCount(); j++){
            if(!float8Vector.isNull(j)){
                double value = float8Vector.get(j);
                System.out.println("\t\t float8[" + j +"] " + value);
                floatCsum+=value;
                this.checkSumx+=value;
            } else {
                this.nullEntries++;
                System.out.println("\t\t float8[" + j +"] : NULL ");
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
            System.out.println(" args are " + args.length);
            if(args.length == 2) {
                ex.makeRead(args[1]);
            } else {
                ex.makeRead("./example.arrow");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(" >>> null entires " + ex.nullEntries);
    }
}
