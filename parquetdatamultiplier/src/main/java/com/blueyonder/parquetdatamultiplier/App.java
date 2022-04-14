package com.blueyonder.parquetdatamultiplier;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.schema.MessageType;

import java.io.File;
import java.io.IOException;

public class App 
{
    public static void main( String[] args ) throws IOException
    {
        String[] fileNames = {"masterdata","unitconversions"};
        String input_dir = "/Users/1022177/eclipse-workspace/parquetdatamultiplier/inputfiles/";
        String output_dir = "/Users/1022177/eclipse-workspace/parquetdatamultiplier/outputfiles/";
        //int multiplierConstant = 2;
        
        int [] multiplierconstantArray = {2,3,6,15,30,90};
        //int [] multiplierconstantArray = {15,30,90};
        //int [] multiplierconstantArray = {90,210,300};
        //String[] folderArray = {"74KSKUDATA","100KSKUDATA","200KSKUDATA","500KSKUDATA","1000KSKUDATA","3000KSKUDATA","7000KSKUDATA","10000KSKUDATA"};
        String[] folderArray = {"74KSKUDATA","111KSKUDATA","222KSKUDATA","555KSKUDATA","1110KSKUDATA","3330KSKUDATA"};
        
        //String[] folderArray = {"3000KSKUDATA","7000KSKUDATA","10000KSKUDATA"};
        for(int i = 0;i< multiplierconstantArray.length;i++) {
          int multiplierConstant = multiplierconstantArray[i];
          String folder = folderArray[i];
          for(String filename : fileNames) {
            
            
            ParquetReader  pr = new ParquetReader();
            ParquetFileReader reader = pr.getParquetReader(input_dir+filename+".parquet");
            ParquetMultiplierWriter pw = new ParquetMultiplierWriter();
            MessageType schema = getSchema(reader);
            String outputDirPath = output_dir+folder+"/";
            boolean dirCreated = new File(outputDirPath).mkdir();
            ParquetWriter<Group> writer = pw.getParquetWriter(schema,outputDirPath+filename+".parquet");
            UpdateLogic ul = UpdateLogicFactory.getUpdateLogicFactory(filename);
            if("unitconversions".equalsIgnoreCase(filename)) {
              UnitConversionsUpdateLogic ucUl = (UnitConversionsUpdateLogic)ul;
              ParquetFileReader masterDataReader = pr.getParquetReader(input_dir+"masterdata.parquet");
              ucUl.buildProductIdMap(masterDataReader);
              masterDataReader.close();
            }
            if("schedrcpts".equalsIgnoreCase(filename)) {
                SchedRcptsUpdateLogic srUl = (SchedRcptsUpdateLogic)ul;
                ParquetFileReader masterDataReader = pr.getParquetReader(input_dir+"masterdata.parquet");
                srUl.buildProductIdMap(masterDataReader);
                masterDataReader.close();
              }
            DataMultiplier dm = new DataMultiplier(reader,writer,multiplierConstant,ul);
            dm.multiplyData();
            reader.close();
            writer.close();
            
          }
          
          
        }
        /*for(String filename : fileNames) {
          
          
          ParquetReader  pr = new ParquetReader();
          ParquetFileReader reader = pr.getParquetReader(input_dir+filename+".parquet");
          ParquetMultiplierWriter pw = new ParquetMultiplierWriter();
          MessageType schema = getSchema(reader);
          ParquetWriter<Group> writer = pw.getParquetWriter(schema, output_dir+filename+".parquet");
          UpdateLogic ul = UpdateLogicFactory.getUpdateLogicFactory(filename);
          if("unitconversions".equalsIgnoreCase(filename)) {
            UnitConversionsUpdateLogic ucUl = (UnitConversionsUpdateLogic)ul;
            ParquetFileReader masterDataReader = pr.getParquetReader(input_dir+"masterdata.parquet");
            ucUl.buildProductIdMap(masterDataReader);
          }
          DataMultiplier dm = new DataMultiplier(reader,writer,multiplierConstant,ul);
          dm.multiplyData();
          reader.close();
          writer.close();
          
        }*/
        
    }
    
    private static MessageType getSchema(ParquetFileReader reader) {
      return reader.getFooter().getFileMetaData().getSchema();
    }
}
