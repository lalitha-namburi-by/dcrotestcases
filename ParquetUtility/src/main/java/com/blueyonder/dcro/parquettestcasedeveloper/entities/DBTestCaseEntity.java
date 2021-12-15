package com.blueyonder.dcro.parquettestcasedeveloper.entities;

public class DBTestCaseEntity {

    private String testcasename;

    private String dburl;

    private String dbusername;

    private String dbpassword;

    private String schemaname;

    private String masterdataquery;

    private String unitconversionsquery;
    
    private String currencyconversionsquery;

    public String getCurrencyconversionsquery() {
      return currencyconversionsquery;
    }

    public void setCurrencyconversionsquery(String currencyconversionsquery) {
      this.currencyconversionsquery = currencyconversionsquery;
    }

    private String purchasegroupvendorminimumsquery;

    private String transportequipmentcapacityquery;

    private String shipquantityroundingrulesquery;
    
    private String procurementcalendartimesquery;
    
    public String getProcurementcalendartimesquery() {
      return procurementcalendartimesquery;
    }

    public void setProcurementcalendartimesquery(String procurementcalendartimesquery) {
      this.procurementcalendartimesquery = procurementcalendartimesquery;
    }

    private String safetystockquery;

    public String getSafetystockquery() {
		return safetystockquery;
	}

	public void setSafetystockquery(String safetystockquery) {
		this.safetystockquery = safetystockquery;
	}

	private String autoapprovalexceptionsquery;

    private String schedrcptsquery;

    private String aggregated_order_projectionsquery;

    //private String h_edlc_expected_delivery_date;

    //private String aggregated_order_projection_period_from;

    //private String aggregated_order_projection_period_upto;

    //private String so_date;

    //private String so_quantity;

    public String getTestcasename() {
        return testcasename;
    }

    public void setTestcasename(String testcasename) {
        this.testcasename = testcasename;
    }

    public String getDburl() {
        return dburl;
    }

    public void setDburl(String dburl) {
        this.dburl = dburl;
    }

    public String getDbusername() {
        return dbusername;
    }

    public void setDbusername(String dbusername) {
        this.dbusername = dbusername;
    }

    public String getDbpassword() {
        return dbpassword;
    }

    public void setDbpassword(String dbpassword) {
        this.dbpassword = dbpassword;
    }

    public String getSchemaname() {
        return schemaname;
    }

    public void setSchemaname(String schemaname) {
        this.schemaname = schemaname;
    }

    public String getMasterdataquery() {
        return masterdataquery;
    }

    public void setMasterdataquery(String masterdataquery) {
        this.masterdataquery = masterdataquery;
    }

    public String getUnitconversionsquery() {
        return unitconversionsquery;
    }

    public void setUnitconversionsquery(String unitconversionsquery) {
        this.unitconversionsquery = unitconversionsquery;
    }

    public String getPurchasegroupvendorminimumsquery() {
        return purchasegroupvendorminimumsquery;
    }

    public void setPurchasegroupvendorminimumsquery(String purchasegroupvendorminimumsquery) {
        this.purchasegroupvendorminimumsquery = purchasegroupvendorminimumsquery;
    }

    public String getTransportequipmentcapacityquery() {
        return transportequipmentcapacityquery;
    }

    public void setTransportequipmentcapacityquery(String transportequipmentcapacityquery) {
        this.transportequipmentcapacityquery = transportequipmentcapacityquery;
    }

    public String getShipquantityroundingrulesquery() {
        return shipquantityroundingrulesquery;
    }

    public void setShipquantityroundingrulesquery(String shipquantityroundingrulesquery) {
        this.shipquantityroundingrulesquery = shipquantityroundingrulesquery;
    }

    public String getAutoapprovalexceptionsquery() {
        return autoapprovalexceptionsquery;
    }

    public void setAutoapprovalexceptionsquery(String autoapprovalexceptionsquery) {
        this.autoapprovalexceptionsquery = autoapprovalexceptionsquery;
    }

    public String getSchedrcptsquery() {
        return schedrcptsquery;
    }

    public void setSchedrcptsquery(String schedrcptsquery) {
        this.schedrcptsquery = schedrcptsquery;
    }

    public String getAggregated_order_projectionsquery() {
        return aggregated_order_projectionsquery;
    }

    public void setAggregated_order_projectionsquery(String aggregated_order_projectionsquery) {
        this.aggregated_order_projectionsquery = aggregated_order_projectionsquery;
    }

    /*public String getAggregated_order_projection_period_from() {
        return aggregated_order_projection_period_from;
    }

    public String getH_edlc_expected_delivery_date() {
        return h_edlc_expected_delivery_date;
    }

    public void setH_edlc_expected_delivery_date(String h_edlc_expected_delivery_date) {
        this.h_edlc_expected_delivery_date = h_edlc_expected_delivery_date;
    }*/

    /*public String getSo_date() {
        return so_date;
    }

    public void setSo_date(String so_date) {
        this.so_date = so_date;
    }

    public String getSo_quantity() {
        return so_quantity;
    }

    public void setSo_quantity(String so_quantity) {
        this.so_quantity = so_quantity;
    }*/

    /*public void setAggregated_order_projection_period_from(String aggregated_order_projection_period_from) {
        this.aggregated_order_projection_period_from = aggregated_order_projection_period_from;
    }

    public String getAggregated_order_projection_period_upto() {
        return aggregated_order_projection_period_upto;
    }

    public void setAggregated_order_projection_period_upto(String aggregated_order_projection_period_upto) {
        this.aggregated_order_projection_period_upto = aggregated_order_projection_period_upto;
    }*/

}
