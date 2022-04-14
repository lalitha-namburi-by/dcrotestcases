package com.blueyonder.parquetdatamultiplier;

public class UpdateLogicFactory {


  public static UpdateLogic getUpdateLogicFactory(String fileName) {

    if ("masterdata".equalsIgnoreCase(fileName)) {
      return new MasterDataUpdateLogic();
    } else if ("aggregated_order_projections".equalsIgnoreCase(fileName)) {
      return new AOPUpdateLogic();
    } else if("unitconversions".equalsIgnoreCase(fileName)){
      return new UnitConversionsUpdateLogic();
    } else if("safetystock".equalsIgnoreCase(fileName)){
      return new SSUpdateLogic();  
    } else if("schedrcpts".equalsIgnoreCase(fileName)){
       return new SchedRcptsUpdateLogic(); 
    }else {
      return null;
    }

  }

}
