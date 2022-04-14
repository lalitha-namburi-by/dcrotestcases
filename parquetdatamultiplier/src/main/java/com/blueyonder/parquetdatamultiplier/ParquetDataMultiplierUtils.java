package com.blueyonder.parquetdatamultiplier;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.parquet.example.data.simple.SimpleGroup;

public class ParquetDataMultiplierUtils {

	public static int updateValuesFailures = 0;
	
  public static long generateLongCode(String value) {
    if (value == null || value.trim().length() == 0) {
      return 0;
    }
    long hashcode = value.hashCode();

    if (hashcode < 0) {

      long intMaxValue = Integer.MAX_VALUE;
      hashcode = (2 * intMaxValue) + hashcode;
    }
    return hashcode;

  }


  public static Object getValue(SimpleGroup originalGroup, String fieldName, String fieldType) {
    Object value = null;
    if ("BINARY".equals(fieldType)) {
      System.out.println(fieldName);
      value = originalGroup.getString(fieldName, 0);

      // newSimpleGroup.add(fieldName, value);
    } else if ("DOUBLE".equals(fieldType)) {
      value = (Double) originalGroup.getDouble(fieldName, 0);
      // newSimpleGroup.add(fieldName, value);
    } else if ("INT64".equals(fieldType)) {
      value = (Long) originalGroup.getLong(fieldName, 0);
      // newSimpleGroup.add(fieldName, value);
    } else if ("BOOLEAN".equals(fieldType)) {
      value = (Boolean) originalGroup.getBoolean(fieldName, 0);
      // newSimpleGroup.add(fieldName, value);
    } else {
      System.out.println("No matching type found");

    }
    return value;
  }

	public static void appendToExistingStringValueColumn(SimpleGroup originalGroup, SimpleGroup newSimpleGroup,
			String fieldName, String fieldType, String prefix) {
		try {
			if ("BINARY".equals(fieldType)) {
				String value = originalGroup.getString(fieldName, 0);
				value = prefix + "_" + value;
				newSimpleGroup.add(fieldName, value);
			} else {
				System.out.println("No matching type found");
			}
		} catch (Exception ex) {
			System.out.println("Exception caught in appending");
		}
	}

  public static void addValue(
      SimpleGroup originalGroup,
      SimpleGroup newSimpleGroup,
      String fieldName,
      String fieldType) {
    try {
      if ("BINARY".equals(fieldType)) {
        String value = originalGroup.getString(fieldName, 0);
        newSimpleGroup.add(fieldName, value);
      } else if ("DOUBLE".equals(fieldType)) {
        double value = (Double) originalGroup.getDouble(fieldName, 0);
        newSimpleGroup.add(fieldName, value);
      } else if ("INT64".equals(fieldType)) {
        long value = (Long) originalGroup.getLong(fieldName, 0);
        newSimpleGroup.add(fieldName, value);
      } else if ("BOOLEAN".equals(fieldType)) {
        boolean value = (Boolean) originalGroup.getBoolean(fieldName, 0);
        newSimpleGroup.add(fieldName, value);
      } else {
        System.out.println("No matching type found");
      }
    } catch (Exception ex) {
        System.out.println("Exception caught in adding");
    }


  }
    
  public static void updateValueToNewNomenclature(SimpleGroup originalGroup, SimpleGroup newSimpleGroup,
			String fieldName, String fieldType, String testCaseName, String prefix) {
		try {
			if ("INT64".equals(fieldType)) {
				long value = (Long) originalGroup.getLong(fieldName, 0);

				String strValue = String.valueOf(value);

				String serialNumber = strValue.substring(strValue.length() - 3);

				String finalValueStr = prefix + serialNumber;

				long finalValue = Long.parseLong(finalValueStr);

				newSimpleGroup.add(fieldName, finalValue);
			} else {
				System.out.println("No matching type found");
			}
		} catch (Exception ex) {
			updateValuesFailures++;
			System.out.println("Exception caught in changing value");
		}
	}

  public static String getTypeFromTestCase(String testcase) {
	  String typeToNumber = "";
	  
	  if(testcase.charAt(0) == '5') {
		  typeToNumber = "9001";
	  } else if(testcase.charAt(0) == 'A') {
		  typeToNumber = "9002";
	  } else if(testcase.charAt(0) == 'D') {
		  if(testcase.charAt(1) == 'C') {
			  typeToNumber = "9003";
		  } else if(testcase.charAt(1) == 'S') {
			  typeToNumber = "9004";
		  }
	  } else if(testcase.charAt(0) == 'M') {
		  typeToNumber = "9005";
	  } else if(testcase.charAt(0) == 'O') {
		  String subType = testcase.substring(5, 8);
		  
		  if(subType.equalsIgnoreCase("AAA")) {
			  typeToNumber = "9006";
		  } else if(subType.equalsIgnoreCase("NDB")) {
			  typeToNumber = "9007";
		  } else if(subType.equalsIgnoreCase("ARQ")) {
			  typeToNumber = "9008";
		  } else if(subType.equalsIgnoreCase("BAO")) {
			  typeToNumber = "9009";
		  } else if(subType.equalsIgnoreCase("BCS")) {
			  typeToNumber = "9010";
		  } else if(subType.equalsIgnoreCase("BOG")) {
			  typeToNumber = "9011";
		  } else if(subType.equalsIgnoreCase("BPM")) {
			  typeToNumber = "9012";
		  } else if(subType.equalsIgnoreCase("BPP")) {
			  typeToNumber = "9013";
		  } else if(subType.equalsIgnoreCase("Cas")) {
			  typeToNumber = "9014";
		  } else if(subType.equalsIgnoreCase("COA")) {
			  typeToNumber = "9015";
		  } else if(subType.equalsIgnoreCase("CSS")) {
			  typeToNumber = "9016";
		  } else if(subType.equalsIgnoreCase("CUR")) {
			  typeToNumber = "9017";
		  } else if(subType.equalsIgnoreCase("CVP")) {
			  typeToNumber = "9018";
		  } else if(subType.equalsIgnoreCase("DBS")) {
			  typeToNumber = "9019";
		  } else if(subType.equalsIgnoreCase("DEL")) {
			  typeToNumber = "9020";
		  } else if(subType.equalsIgnoreCase("DSD")) {
			  typeToNumber = "9021";
		  } else if(subType.equalsIgnoreCase("DTS")) {
			  typeToNumber = "9022";
		  } else if(subType.equalsIgnoreCase("FBK")) {
			  typeToNumber = "9023";
		  } else if(subType.equalsIgnoreCase("FPE")) {
			  typeToNumber = "9024";
		  } else if(subType.equalsIgnoreCase("FPI")) {
			  typeToNumber = "9025";
		  } else if(subType.equalsIgnoreCase("GAA")) {
			  typeToNumber = "9026";
		  } else if(subType.equalsIgnoreCase("GEX")) {
			  typeToNumber = "9027";
		  } else if(subType.equalsIgnoreCase("GFO")) {
			  typeToNumber = "9028";
		  } else if(subType.equalsIgnoreCase("GLS")) {
			  typeToNumber = "9029";
		  } else if(subType.equalsIgnoreCase("GMO")) {
			  typeToNumber = "9030";
		  } else if(subType.equalsIgnoreCase("GOA")) {
			  typeToNumber = "9031";
		  } else if(subType.equalsIgnoreCase("GOD")) {
			  typeToNumber = "9032";
		  } else if(subType.equalsIgnoreCase("GOO")) {
			  typeToNumber = "9033";
		  } else if(subType.equalsIgnoreCase("GOR")) {
			  typeToNumber = "9034";
		  } else if(subType.equalsIgnoreCase("GPO")) {
			  typeToNumber = "9035";
		  } else if(subType.equalsIgnoreCase("GSD")) {
			  typeToNumber = "9036";
		  } else if(subType.equalsIgnoreCase("GSI")) {
			  typeToNumber = "9037";
		  } else if(subType.equalsIgnoreCase("GSO")) {
			  typeToNumber = "9038";
		  } else if(subType.equalsIgnoreCase("GSS")) {
			  typeToNumber = "9039";
		  } else if(subType.equalsIgnoreCase("IGN")) {
			  typeToNumber = "9040";
		  } else if(subType.equalsIgnoreCase("LBS")) {
			  typeToNumber = "9041";
		  } else if(subType.equalsIgnoreCase("LUO")) {
			  typeToNumber = "9042";
		  } else if(subType.equalsIgnoreCase("MBG")) {
			  typeToNumber = "9043";
		  } else if(subType.equalsIgnoreCase("MBR")) {
			  typeToNumber = "9044";
		  } else if(subType.equalsIgnoreCase("MOA")) {
			  typeToNumber = "9045";
		  } else if(subType.equalsIgnoreCase("MSO")) {
			  typeToNumber = "9046";
		  } else if(subType.equalsIgnoreCase("PRO")) {
			  typeToNumber = "9047";
		  } else if(subType.equalsIgnoreCase("REC")) {
			  typeToNumber = "9048";
		  }
		  
	  } else if(testcase.charAt(0) == 'S') {
		  if(testcase.charAt(1) == 'C') {
			  typeToNumber = "9049";
		  } else if(testcase.charAt(1) == 'R') {
			  typeToNumber = "9050";
		  }
	  } else if(testcase.charAt(0) == 'V') {
		  typeToNumber = "9051";
	  }
	  
	  return typeToNumber.trim().isEmpty() ? "000" : typeToNumber;
  }

  public static String getNumberFromTestCase(String testcase) {
		String numberToNumber = "";

		if (testcase.charAt(0) == '5') {
			numberToNumber = testcase.substring(2);
		} else if (testcase.charAt(0) == 'A') {
			numberToNumber = testcase.split("-")[1].substring(2);
		} else if (testcase.charAt(0) == 'D') {
			numberToNumber = testcase.split("-")[1].substring(0, 3);
		} else if (testcase.charAt(0) == 'M') {
			numberToNumber = testcase.split("-")[1].substring(2);
		} else if (testcase.charAt(0) == 'O') {
			String subType = testcase.substring(5, 8);

			if (subType.equalsIgnoreCase("AAA")) {
				numberToNumber = "0" + testcase.split("_")[1].substring(3, 5);
			} else if (subType.equalsIgnoreCase("NDB")) {
				numberToNumber = "0" + testcase.split("_")[1].substring(3, 5);
			} else if (subType.equalsIgnoreCase("ARQ")) {
				numberToNumber = testcase.split("-")[1].substring(3, 6);
			} else if (subType.equalsIgnoreCase("BAO")) {
				numberToNumber = testcase.split("-")[1].substring(3, 6);
			} else if (subType.equalsIgnoreCase("BCS")) {
				numberToNumber = testcase.split("-")[1].substring(3, 6);
			} else if (subType.equalsIgnoreCase("BOG")) {
				numberToNumber = testcase.split("-")[1].substring(3, 6);
			} else if (subType.equalsIgnoreCase("BPM")) {
				numberToNumber = testcase.split("-")[1].substring(3, 6);
			} else if (subType.equalsIgnoreCase("BPP")) {
				numberToNumber = testcase.split("-")[1].substring(3, 6);
			} else if (subType.equalsIgnoreCase("Cas")) {
				numberToNumber = testcase.split("-")[1].substring(3, 6);
			} else if (subType.equalsIgnoreCase("COA")) {
				numberToNumber = testcase.split("-")[1].substring(3, 6);
			} else if (subType.equalsIgnoreCase("CSS")) {
				numberToNumber = testcase.split("-")[1].substring(3, 6);
			} else if (subType.equalsIgnoreCase("CUR")) {
				numberToNumber = testcase.split("-")[1].substring(3, 6);
			} else if (subType.equalsIgnoreCase("CVP")) {
				numberToNumber = testcase.split("-")[1].substring(3, 6);
			} else if (subType.equalsIgnoreCase("DBS")) {
				numberToNumber = testcase.split("-")[1].substring(3, 6);
			} else if (subType.equalsIgnoreCase("DEL")) {
				numberToNumber = testcase.split("-")[1].substring(3, 6);
			} else if (subType.equalsIgnoreCase("DSD")) {
				numberToNumber = testcase.split("-")[1].substring(3, 6);
			} else if (subType.equalsIgnoreCase("DTS")) {
				numberToNumber = testcase.split("-")[1].substring(3, 6);
			} else if (subType.equalsIgnoreCase("FBK")) {
				numberToNumber = testcase.split("-")[1].substring(3, 6);
			} else if (subType.equalsIgnoreCase("FPE")) {
				numberToNumber = testcase.split("-")[1].substring(3, 6);
			} else if (subType.equalsIgnoreCase("FPI")) {
				numberToNumber = testcase.split("-")[1].substring(3, 6);
			} else if (subType.equalsIgnoreCase("GAA")) {
				numberToNumber = testcase.split("-")[1].substring(3, 6);
			} else if (subType.equalsIgnoreCase("GEX")) {
				numberToNumber = testcase.split("-")[1].substring(3, 6);
			} else if (subType.equalsIgnoreCase("GFO")) {
				numberToNumber = testcase.split("-")[1].substring(3, 6);
			} else if (subType.equalsIgnoreCase("GLS")) {
				numberToNumber = testcase.split("-")[1].substring(3, 6);
			} else if (subType.equalsIgnoreCase("GMO")) {
				numberToNumber = testcase.split("-")[1].substring(3, 6);
			} else if (subType.equalsIgnoreCase("GOA")) {
				numberToNumber = testcase.split("-")[1].substring(3, 6);
			} else if (subType.equalsIgnoreCase("GOD")) {
				numberToNumber = testcase.split("-")[1].substring(3, 6);
			} else if (subType.equalsIgnoreCase("GOO")) {
				numberToNumber = testcase.split("-")[1].substring(3, 6);
			} else if (subType.equalsIgnoreCase("GOR")) {
				numberToNumber = testcase.split("-")[1].substring(3, 6);
			} else if (subType.equalsIgnoreCase("GPO")) {
				numberToNumber = testcase.split("-")[1].substring(3, 6);
			} else if (subType.equalsIgnoreCase("GSD")) {
				numberToNumber = testcase.split("-")[1].substring(3, 6);
			} else if (subType.equalsIgnoreCase("GSI")) {
				numberToNumber = testcase.split("-")[1].substring(3, 6);
			} else if (subType.equalsIgnoreCase("GSO")) {
				numberToNumber = testcase.split("-")[1].substring(3, 6);
			} else if (subType.equalsIgnoreCase("GSS")) {
				numberToNumber = testcase.split("-")[1].substring(3, 6);
			} else if (subType.equalsIgnoreCase("IGN")) {
				numberToNumber = "0" + testcase.split("-")[1].substring(3, 5);
			} else if (subType.equalsIgnoreCase("LBS")) {
				numberToNumber = testcase.split("-")[1].substring(3, 6);
			} else if (subType.equalsIgnoreCase("LUO")) {
				numberToNumber = testcase.split("-")[1].substring(3, 6);
			} else if (subType.equalsIgnoreCase("MBG")) {
				numberToNumber = testcase.split("-")[1].substring(3, 6);
			} else if (subType.equalsIgnoreCase("MBR")) {
				numberToNumber = testcase.split("-")[1].substring(3, 6);
			} else if (subType.equalsIgnoreCase("MOA")) {
				numberToNumber = testcase.split("-")[1].substring(3, 6);
			} else if (subType.equalsIgnoreCase("MSO")) {
				numberToNumber = testcase.split("-")[1].substring(3, 6);
			} else if (subType.equalsIgnoreCase("PRO")) {
				numberToNumber = testcase.split("-")[1].substring(3, 6);
			} else if (subType.equalsIgnoreCase("REC")) {
				if (testcase.charAt(8) == '1') {
					numberToNumber = "0" + testcase.split("-")[1].substring(3, 5);
				} else {
					numberToNumber = "0" + testcase.split("_")[1].substring(0, 2);
				}
			}

		} else if (testcase.charAt(0) == 'S') {
			if (testcase.charAt(1) == 'C') {
				numberToNumber = testcase.split("-")[1].substring(0, 3);
			} else if (testcase.charAt(1) == 'R') {
				numberToNumber = testcase.split("_")[1].substring(2, 5);
			}
		} else if (testcase.charAt(0) == 'V') {
			numberToNumber = testcase.split("-")[1].substring(2, 5);
		}

		return numberToNumber.trim().isEmpty() ? "000" : numberToNumber;
	}
  
  public static String getSubNumberFromTestCase(String testcase) {
	  String subNumberToNumber = "";

		if (testcase.charAt(0) == '5') {
			subNumberToNumber = "001";
		} else if (testcase.charAt(0) == 'A') {
			subNumberToNumber = "001";
		} else if (testcase.charAt(0) == 'D') {
			subNumberToNumber = "0" + testcase.split("[.]")[1].substring(0);
		} else if (testcase.charAt(0) == 'M') {
			subNumberToNumber = "001";
		} else if (testcase.charAt(0) == 'O') {
			subNumberToNumber = "0" + testcase.split("[.]")[1].substring(0);
		} else if (testcase.charAt(0) == 'S') {
			if (testcase.charAt(1) == 'C') {
				subNumberToNumber = "0" + testcase.split("[.]")[1].substring(0);
			} else if (testcase.charAt(1) == 'R') {
				subNumberToNumber = "001";
			}
		} else if (testcase.charAt(0) == 'V') {
			subNumberToNumber = "001";
		}

		return subNumberToNumber.trim().isEmpty() ? "000" : subNumberToNumber;
  }
  
  public static String getColumnNumber(String columnName) {
		String columnNumber = "";
		if (columnName.equalsIgnoreCase("PP_P_ID") || columnName.equalsIgnoreCase("H_EDLC_P_ID") || columnName.equalsIgnoreCase("P_P_ID_MASTER")) {
			//Product ID
			columnNumber = "001";
		} else if (columnName.equalsIgnoreCase("PP_L_ID_SOURCE") || columnName.equalsIgnoreCase("H_EDLC_L_ID_SOURCE") ) {
			//Source ID
			columnNumber = "002";
		} else if (columnName.equalsIgnoreCase("PP_L_ID_TARGET") || columnName.equalsIgnoreCase("H_EDLC_L_ID_TARGET")) {
			//Dest ID
			columnNumber = "003";
		} else if (columnName.equalsIgnoreCase("PP_PC_ID") || columnName.equalsIgnoreCase("PC_ID") || columnName.equalsIgnoreCase("PUG_PC_ID") || columnName.equalsIgnoreCase("PCT_PC_ID")) {
			//Calendar ID
			columnNumber = "004";
		} else if (columnName.equalsIgnoreCase("PP_PUG_ID") || columnName.equalsIgnoreCase("PUGVM_PUG_ID")) {
			//Purchase Group ID
			columnNumber = "005";
		} else if (columnName.equalsIgnoreCase("P_UY_ID") || columnName.equalsIgnoreCase("PUGVM_UY_ID")) {
			//Unit Of Measure ID
			columnNumber = "006";
		} else if (columnName.equalsIgnoreCase("PUG_TREQ_ID") || columnName.equalsIgnoreCase("TREQC_TREQ_ID") || columnName.equalsIgnoreCase("TREQ_ID")) {
			//Vehicle ID
			columnNumber = "007";
		} else if(columnName.equalsIgnoreCase("PP_PPY_ID")) {
			columnNumber = "007";
		} else if(columnName.equalsIgnoreCase("PP_SQRR_ID") || columnName.equalsIgnoreCase("SQRR_ID")) {
			//SQRR ID
			columnNumber = "010";
		} else if(columnName.equalsIgnoreCase("PUG_AAR_ID") || columnName.equalsIgnoreCase("AAEX_AAR_ID")) {
			//AutoApproval ID
			columnNumber = "011";
		} else if(columnName.equalsIgnoreCase("VOP_CU_ID") || columnName.equalsIgnoreCase("ER_CU_ID")) {
			//Currency conversion ID 1
			columnNumber = "012";
		} else if(columnName.equalsIgnoreCase("PUGVM_CU_ID") || columnName.equalsIgnoreCase("TREQC_CU_ID") || columnName.equalsIgnoreCase("ER_CU_ID_REFERENCE")) {
			//Currency conversion ID 2
			columnNumber = "013";
		}

		return columnNumber.trim().isEmpty() ? "000" : columnNumber;
	}
  

  
	public static boolean needsUpdate(String fieldName) {
		List<String> updateList = List.of("PP_P_ID", "H_EDLC_P_ID", "P_P_ID_MASTER", "PP_L_ID_SOURCE",
				"H_EDLC_L_ID_SOURCE", "PP_L_ID_TARGET", "H_EDLC_L_ID_TARGET", "PP_PC_ID", "PC_ID", "PUG_PC_ID",
				"PCT_PC_ID", "PP_PUG_ID", "PUGVM_PUG_ID", "PUG_TREQ_ID", "TREQC_TREQ_ID", "TREQ_ID", "PP_PPY_ID",
				"PP_SQRR_ID", "SQRR_ID", "PUG_AAR_ID", "AAEX_AAR_ID", "VOP_CU_ID", "ER_CU_ID", "PUGVM_CU_ID",
				"TREQC_CU_ID", "ER_CU_ID_REFERENCE").stream().map(String::toUpperCase).collect(Collectors.toList());
		return updateList.contains(fieldName);
	}
	
	public static boolean isTestCase(String testcase) {
//		if(testcase.charAt(0) == 'D') {
//			return true;
//		}
		return true;
	}

}
