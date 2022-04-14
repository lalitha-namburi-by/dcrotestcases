package com.blueyonder.parquetdatamultiplier;

import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.Type;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SchemaUpdater {

    // masterdata,unitconversions,purchasegroupvendorminimums,
    // transportequipmentcapacity,procurementcalendartimes,aggregated_order_projections,
    // autoapprovalexceptions,schedrcpts,shipquantityroundingrules,safetystock

    private static final Map<String, String> masterDataMap = new HashMap<String, String>() {
        {
            put("SOP_QUANTITY_MAX", "SOP_QUANTITY_MAX");
            put("PP_P_ID","PP_P_ID");
            put("P_EXTERNAL_CODE","P_EXTERNAL_CODE");
            put("P_UY_ID","P_UY_ID");
            put("SO_DATE","SO_DATE");
            put("SO_QUANTITY","SO_QUANTITY");
            put("PP_L_ID_SOURCE","PP_L_ID_SOURCE");
            put("L_EXTERNAL_CODE_SOURCE","L_EXTERNAL_CODE_SOURCE");
            put("PP_L_ID_TARGET","PP_L_ID_TARGET");
            put("L_EXTERNAL_CODE_TARGET","L_EXTERNAL_CODE_TARGET");
            put("PP_PPY_ID","PP_PPY_ID");
            put("PP_PC_ID","PP_PC_ID");
            put("PP_SQRR_ID","PP_SQRR_ID");
            put("PP_ACTIVE_FROM","PP_ACTIVE_FROM");
            put("PP_ACTIVE_UPTO","PP_ACTIVE_UPTO");
            put("PP_ORDER_QUANTITY_MIN","PP_ORDER_QUANTITY_MIN");
            put("PP_ORDER_MULTIPLE","PP_ORDER_MULTIPLE");
            put("PP_PUG_ID","PP_PUG_ID");
            put("PP_PURCHASE_SUB_GROUP","PP_PURCHASE_SUB_GROUP");
            put("PUG_TREQ_ID","PUG_TREQ_ID");
            put("PUG_PROJECTED_ORDER_DURATION","PUG_PROJECTED_ORDER_DURATION");
            put("PUG_COVERAGE_DURATION_TOLERANCE","PUG_COVERAGE_DURATION_TOLERANCE");
            put("PUG_AAR_ID","PUG_AAR_ID");
            put("PUG_PC_ID","PUG_PC_ID");
            put("PUG_LOAD_BUILD_DOWN_TOLERANCE","PUG_LOAD_BUILD_DOWN_TOLERANCE");
            put("PUG_LOAD_BUILD_UP_TOLERANCE","PUG_LOAD_BUILD_UP_TOLERANCE");
            put("PUG_LBRE_ID","PUG_LBRE_ID");
            put("PUG_LMRE_ID","PUG_LMRE_ID");
            put("PUG_LOAD_TOLERANCE","PUG_LOAD_TOLERANCE");
            put("PUG_UNIQUE_PRODUCT_PER_PALLET","PUG_UNIQUE_PRODUCT_PER_PALLET");
            put("PUG_TMRE_ID","PUG_TMRE_ID");
            put("PUG_VMRE_ID","PUG_VMRE_ID");
            put("VOP_DESIRED_COVERAGE_DURATION","VOP_DESIRED_COVERAGE_DURATION");
            put("VOP_MAX_COVERAGE_DURATION","VOP_MAX_COVERAGE_DURATION");
            put("AAR_ADDITIONAL_COVERAGE_DURATION","AAR_ADDITIONAL_COVERAGE_DURATION");
            put("AAR_MAX_VEHICLE_LOAD_COUNT","AAR_MAX_VEHICLE_LOAD_COUNT");
            put("AAR_UY_ID","AAR_UY_ID");
            put("AAR_HISTORICAL_ORDER_COUNT","AAR_HISTORICAL_ORDER_COUNT");
            put("AAR_LOWER_THRESHOLD","AAR_LOWER_THRESHOLD");
            put("AAR_UPPER_THRESHOLD","AAR_UPPER_THRESHOLD");
            put("AM_ACTIVE_UPTO","AM_ACTIVE_UPTO");
        }

    };

    private static final Map<String, String> unitConversionsMap = new HashMap<String, String>() {
        {
            put("P_P_ID_MASTER", "P_P_ID_MASTER");
            put("UY_NAME_SRC", "UY_NAME_SRC");
            put("UY_ID_SRC", "UY_ID_SRC");
            put("UY_NAME_TGT", "UY_NAME_TGT");
            put("UY_ID_TGT", "UY_ID_TGT");
            put("UC_FACTOR", "UC_FACTOR");
        }

    };

    private static final Map<String, String> purchaseGroupVendorMinimumsMap = new HashMap<String, String>() {
        {
            put("PUGVM_PUG_ID", "PUGVM_PUG_ID");
            put("PUGVM_PURCHASE_SUB_GROUP", "PUGVM_PURCHASE_SUB_GROUP");
            put("PUGVM_UY_ID", "PUGVM_UY_ID");
            put("PUGVM_MIN_VALUE", "PUGVM_MIN_VALUE");
            put("PUGVM_CU_ID", "PUGVM_CU_ID");
        }

    };

    private static final Map<String, String> transportEquipmentCapacityMap = new HashMap<String, String>() {
        {
            put("TREQC_TREQ_ID", "TREQC_TREQ_ID");
            put("TREQC_UY_ID", "TREQC_UY_ID");
            put("TREQC_MIN_CAPACITY", "TREQC_MIN_CAPACITY");
            put("TREQC_MAX_CAPACITY", "TREQC_MAX_CAPACITY");
            put("TREQC_IS_HARD_CONSTRAINT", "TREQC_IS_HARD_CONSTRAINT");
            put("TREQC_CU_ID", "TREQC_CU_ID");
        }

    };

    private static final Map<String, String> procurementCalendarTimesMap = new HashMap<String, String>() {
        {
            put("PCT_PC_ID", "PCT_PC_ID");
            put("PCT_FINAL_ORDER_TIME", "PCT_FINAL_ORDER_TIME");
            put("PCT_EXPECTED_ARRIVAL", "PCT_EXPECTED_ARRIVAL");
            put("PCT_EXPECTED_AVAILABILITY", "PCT_EXPECTED_AVAILABILITY");
        }

    };

    private static final Map<String, String> aggregated_order_projectionsMap = new HashMap<String, String>() {
        {
            put("P_EXTERNAL_CODE", "P_EXTERNAL_CODE");
            put("SUPPLIER_EXTERNAL_CODE", "SUPPLIER_EXTERNAL_CODE");
            put("AGGREGATED_ORDER_PROJECTION_PERIOD_FROM", "AGGREGATED_ORDER_PROJECTION_PERIOD_FROM");
            put("AGGREGATED_ORDER_PROJECTION_PERIOD_UPTO", "AGGREGATED_ORDER_PROJECTION_PERIOD_UPTO");
            put("AGGREGATED_ORDER_PROJECTION_MEAN", "AGGREGATED_ORDER_PROJECTION_MEAN");
            put("AGGREGATED_ORDER_PROJECTION_VARIANCE", "AGGREGATED_ORDER_PROJECTION_VARIANCE");
        }

    };

    private static final Map<String, String> autoapprovalexceptionsMap = new HashMap<String, String>() {
        {
            put("AAEX_AAR_ID", "AAEX_AAR_ID");
            put("AAEX_OOPEX_ID", "AAEX_OOPEX_ID");
        }

    };

    private static final Map<String, String> schedrcptsMap = new HashMap<String, String>() {
        {
            put("H_EDLC_P_ID", "H_EDLC_P_ID");
            put("H_EDLC_L_ID_TARGET", "H_EDLC_L_ID_TARGET");
            put("H_EDLC_EXPECTED_DELIVERY_DATE", "H_EDLC_EXPECTED_DELIVERY_DATE");
            put("H_EDLC_QUANTITY", "H_EDLC_QUANTITY");
        }

    };

    private static final Map<String, String> shipquantityroundingrulesMap = new HashMap<String, String>() {
        {
            put("SQRR_ID", "SQRR_ID");
            put("SQRR_DESIRED_SHIP_QUANTITY", "SQRR_DESIRED_SHIP_QUANTITY");
            put("SQRR_DESCRIPTION", "SQRR_DESCRIPTION");
            put("SQRR_ROUND_DOWN_FACTOR", "SQRR_ROUND_DOWN_FACTOR");
            put("SQRR_ROUND_UP_FACTOR", "SQRR_ROUND_UP_FACTOR");
        }

    };

    private static final Map<String, String> safetystockMap = new HashMap<String, String>() {
        {
            put("P_EXTERNAL_CODE", "P_EXTERNAL_CODE");
            put("EXTERNAL_SUPPLIER_EXTERNAL_CODE", "EXTERNAL_SUPPLIER_EXTERNAL_CODE");
            put("SUPPLIER_EXTERNAL_CODE", "SUPPLIER_EXTERNAL_CODE");
            put("EFFECTIVE_FROM", "EFFECTIVE_FROM");
            put("EFFECTIVE_UPTO", "EFFECTIVE_UPTO");
            put("SAFETY_STOCK_PER_DAY", "SAFETY_STOCK_PER_DAY");
            
        }

    };

    private static final Map<String, Map<String, String>> alternatColumnMaps = new HashMap<String, Map<String, String>>() {
        {
            put("masterdata", masterDataMap);
            put("unitconversions", unitConversionsMap);
            put("purchasegroupvendorminimums", purchaseGroupVendorMinimumsMap);
            put("transportequipmentcapacity", transportEquipmentCapacityMap);
            put("procurementcalendartimes", procurementCalendarTimesMap);
            put("aggregated_order_projections", aggregated_order_projectionsMap);
            put("autoapprovalexceptions", autoapprovalexceptionsMap);
            put("schedrcpts", schedrcptsMap);
            put("shipquantityroundingrules", shipquantityroundingrulesMap);
            put("safetystock", safetystockMap);
        }
    };

    /*public static void main(String[] args) throws IOException {
        String input_dir = "/Users/1022177/eclipse-workspace/parquetdatamultiplier/inputfiles/";
        ParquetReader pr = new ParquetReader();
        ParquetFileReader reader = pr.getParquetReader(input_dir+"unitconversions" + ".parquet");
        MessageType schema = getSchema(reader);
        System.out.print(schema.toString());
        String schemaString = schema.toString();
        String newSchemaString = schemaString.replace("P_P_ID_MASTER", "P_ID");
        System.out.println(newSchemaString);
        MessageType newSchema = MessageTypeParser.parseMessageType(newSchemaString);
        System.out.println(newSchema.toString());
    }*/

    public static void main(String args[]) throws IOException {
        String[] fileNames = { "masterdata", "unitconversions","purchasegroupvendorminimums","transportequipmentcapacity","procurementcalendartimes","aggregated_order_projections","autoapprovalexceptions","schedrcpts","shipquantityroundingrules","safetystock" };
        String input_dir = "/Users/1022177/eclipse-workspace/parquetdatamultiplier/inputfiles/";
        String output_dir = "/Users/1022177/eclipse-workspace/parquetdatamultiplier/outputfiles/";

        String[] folderArray = {"DCRO-100.01","DCRO-100.02","DCRO-100.03","DCRO-100.04","DCRO-100.05","DCRO-100.06","DCRO-100.07","DCRO-100.08","DCRO-100.09","DCRO-100.10","DCRO-100.11","DCRO-100.12","DCRO-100.13", "DCRO-100.14","DCRO-100.15","DCRO-100.16","DCRO-100.17","DCRO-100.18","DCRO-100.19","DCRO-100.20","DCRO-100.21","DCRO-100.22","DCRO-100.23","DCRO-100.24","DCRO-100.25","DCRO-100.26",
                "DCRO-100.27","DCRO-100.28","DCRO-100.29","DCRO-100.30","DCRO-100.31","DCRO-100.32","DCRO-100.33","DCRO-100.34","DCRO-100.35","DCRO-100.36","DCRO-100.37","OOPT-GAA319.03","OOPT-GAA319.07","OOPT-GAA319.09","OOPT-GAA319.11","OOPT-GAA319.13","OOPT-GAA319.14","OOPT-GAA319.15","OOPT-GAA319.17","OOPT-GEX334.01","OOPT-GEX334.02","OOPT-GEX334.03","OOPT-GEX334.04","OOPT-GEX334.05","OOPT-GEX334.09","OOPT-GEX334.10","OOPT-GSD335.01","OOPT-GSD335.02","OOPT-GSD335.04","OOPT-GSD335.06","OOPT-GSD335.07","OOPT-GSD335.08","OOPT-GSD335.10","OOPT-GSD335.15","OOPT-GSD335.16","OOPT-GSD335.17","OOPT-GSD335.18","OOPT-MBG327.01","OOPT-MBG327.02","OOPT-MBG327.03","OOPT-MBG327.04","OOPT-MBG327.05","OOPT-MBG327.06","OOPT-MBG327.07","OOPT-MBG327.08","OOPT-MBG327.09","OOPT-MBG327.10","OOPT-MBG327.11","OOPT-MBG327.12","OOPT-MBG327.13","OOPT-MBG327.14","OOPT-MBG327.15","OOPT-MBG327.16","OOPT-MBG327.17","OOPT-MBG327.18","OOPT-MBG327.19","OOPT-MBG327.20","OOPT-MBG327.21"};

        for (int i = 0; i < folderArray.length; i++) {
            String folder = folderArray[i];
            String inputDirPath = input_dir + folder + "/";
            String outputDirPath = output_dir + folder + "/";
            for (String filename : fileNames) {

                ParquetReader pr = new ParquetReader();
                ParquetFileReader reader = pr.getParquetReader(inputDirPath + filename + ".parquet");
                ParquetMultiplierWriter pw = new ParquetMultiplierWriter();
                MessageType schema = getSchema(reader);
                boolean dirCreated = new File(outputDirPath).mkdir();
                Map<String, String> newColumnNameMap = alternatColumnMaps.get(filename);
                MessageType newSchema = buildNewSchema(schema, newColumnNameMap);
                //newSchema.toString();
                ParquetWriter<Group> writer = pw.getParquetWriter(newSchema, outputDirPath + filename + ".parquet");

                PageReadStore pages;
                while ((pages = reader.readNextRowGroup()) != null) {
                    long rows = pages.getRowCount();
                    MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                    RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

                    for (int j = 0; j < rows; j++) {
                        SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
                        SimpleGroup newSimmpleGroup = copyData(simpleGroup, schema, newSchema, newColumnNameMap);
                        writer.write(newSimmpleGroup);

                    }
                }
                reader.close();
                writer.close();

            }
        }
    }

    private static MessageType buildNewSchema(MessageType oldSchema, Map<String, String> newColumnNameMap) {
        String newSchemaString = oldSchema.toString();
        for (Type field : oldSchema.getFields()) {
            String fieldName = field.getName();
            String newFieldName = newColumnNameMap.get(fieldName.toUpperCase());
            newSchemaString = newSchemaString.replace(fieldName, newFieldName);

        }
        MessageType newSchema = MessageTypeParser.parseMessageType(newSchemaString);
        return newSchema;
    }

    private static SimpleGroup copyData(SimpleGroup simpleGroup, MessageType oldSchema, MessageType newSchema, Map<String, String> newColumnNameMap) {
        SimpleGroup newSimpleGroup = new SimpleGroup(newSchema);
        for (Type field : oldSchema.getFields()) {
            String fieldName = field.getName();
            String newFieldName = newColumnNameMap.get(fieldName.toUpperCase());
            if (field.isPrimitive()) {
                String fieldType = field.asPrimitiveType()
                    .getPrimitiveTypeName()
                    .name();
                copyValue(simpleGroup, newSimpleGroup, fieldName, newFieldName, fieldType);
            } else {

            }
        }
        return newSimpleGroup;
    }

    private static void copyValue(SimpleGroup originalGroup, SimpleGroup newSimpleGroup, String oldFieldName, String newFieldName, String fieldType) {
        try {
            if ("BINARY".equals(fieldType)) {
                String value = originalGroup.getString(oldFieldName, 0);
                newSimpleGroup.add(newFieldName, value);
            } else if ("DOUBLE".equals(fieldType)) {
                double value = (Double) originalGroup.getDouble(oldFieldName, 0);
                newSimpleGroup.add(newFieldName, value);
            } else if ("INT64".equals(fieldType)) {
                long value = (Long) originalGroup.getLong(oldFieldName, 0);
                newSimpleGroup.add(newFieldName, value);
            } else if ("BOOLEAN".equals(fieldType)) {
                boolean value = (Boolean) originalGroup.getBoolean(oldFieldName, 0);
                newSimpleGroup.add(newFieldName, value);
            } else {
                System.out.println("No matching type found");
            }
        } catch (Exception ex) {
            System.out.println("Exception caught");
        }

    }

    private static MessageType getSchema(ParquetFileReader reader) {
        return reader.getFooter()
            .getFileMetaData()
            .getSchema();
    }

}
