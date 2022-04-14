package com.blueyonder.parquetdatamultiplier;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;

public class TransportEquipmentCapacityCreator {

    public static void main(String args[]) throws IOException {

        String masterdatafileName = "/Users/1022177/eclipse-workspace/parquetdatamultiplier/inputfiles/transportequipmentcapacity.parquet";
        ParquetReader pr = new ParquetReader();
        ParquetFileReader reader = pr.getParquetReader(masterdatafileName);

        MessageType schema = reader.getFooter()
            .getFileMetaData()
            .getSchema();

        String outputFileName = "/Users/1022177/eclipse-workspace/parquetdatamultiplier/outputfiles/transportequipmentcapacity.parquet";

        ParquetMultiplierWriter pw = new ParquetMultiplierWriter();
        ParquetWriter<Group> tecwriter = pw.getParquetWriter(schema, outputFileName);

        SimpleGroup newSimpleGroup = new SimpleGroup(schema);
        newSimpleGroup.add("TREQC_TREQ_ID", 3504215442L);
        newSimpleGroup.add("TREQC_UY_ID", 50001L);
        newSimpleGroup.add("TREQC_MIN_CAPACITY", 10.0);
        newSimpleGroup.add("TREQC_MAX_CAPACITY", 100.0);
        newSimpleGroup.add("TREQC_IS_HARD_CONSTRAINT", true);
        tecwriter.write(newSimpleGroup);
        
        SimpleGroup newSimpleGroup1 = new SimpleGroup(schema);
        newSimpleGroup1.add("TREQC_TREQ_ID", 3504215442L);
        newSimpleGroup1.add("TREQC_UY_ID", 50002L);
        newSimpleGroup1.add("TREQC_MIN_CAPACITY", 20.0);
        newSimpleGroup1.add("TREQC_MAX_CAPACITY", 200.0);
        newSimpleGroup1.add("TREQC_IS_HARD_CONSTRAINT", true);
        tecwriter.write(newSimpleGroup1);
        
        SimpleGroup newSimpleGroup2 = new SimpleGroup(schema);
        newSimpleGroup2.add("TREQC_TREQ_ID", 3504215442L);
        newSimpleGroup2.add("TREQC_UY_ID", 50003L);
        newSimpleGroup2.add("TREQC_MIN_CAPACITY", 25.0);
        newSimpleGroup2.add("TREQC_MAX_CAPACITY", 250.0);
        newSimpleGroup2.add("TREQC_IS_HARD_CONSTRAINT", false);
        tecwriter.write(newSimpleGroup2);
        tecwriter.close();
        reader.close();

    }

}
