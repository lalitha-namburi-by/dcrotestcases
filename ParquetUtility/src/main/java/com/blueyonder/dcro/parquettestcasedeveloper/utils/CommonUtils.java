package com.blueyonder.dcro.parquettestcasedeveloper.utils;

import java.io.File;
import java.util.List;

import org.springframework.stereotype.Component;

import com.blueyonder.dcro.parquettestcasedeveloper.entities.DBTestCaseEntity;
import com.blueyonder.dcro.parquettestcasedeveloper.entities.GridColumn;
import com.blueyonder.dcro.parquettestcasedeveloper.entities.ParquetGrid;

@Component
public class CommonUtils {


  public GridColumn[] buildColumnDetails(List<String> mappings) {

    GridColumn[] columnDetails = new GridColumn[mappings.size()];
    int index = 0;
    for (String mapping : mappings) {

      String[] s = mapping.split("-");

      GridColumn gridColumn = new GridColumn();
      gridColumn.setColumnName(s[0]);
      boolean isNullable = s[1].charAt(0) == 'R' ? false : true;
      gridColumn.setNullable(isNullable);
      gridColumn.setColumnDataType(s[2].toLowerCase());
      if (s.length == 4) {
        gridColumn.setMappedscpocolumn(s[3]);
      }
      columnDetails[index++] = gridColumn;
    }
    return columnDetails;
  }

  public String buildGridNamesList(List<ParquetGrid> gridList) {
    String gridnameslist = "";
    int i = 0;
    for (ParquetGrid grid : gridList) {
      gridnameslist += grid.getGridName();
      if (i < (gridList.size() - 1)) {
        gridnameslist += "-";
      }
      i++;

    }
    return gridnameslist;
  }

  public String fetchQuery(String entityName, DBTestCaseEntity entity) {

    if ("masterdata".equalsIgnoreCase(entityName)) {
      return entity.getMasterdataquery();
    } else if ("unitconversions".equalsIgnoreCase(entityName)) {
      return entity.getUnitconversionsquery();
    } else if ("currencyconversions".equalsIgnoreCase(entityName)) {
      return entity.getCurrencyconversionsquery();
    } else if ("purchasegroupvendorminimums".equalsIgnoreCase(entityName)) {
      return entity.getPurchasegroupvendorminimumsquery();
    } else if ("transportequipmentcapacity".equalsIgnoreCase(entityName)) {
      return entity.getTransportequipmentcapacityquery();
    } else if ("procurementcalendartimes".equalsIgnoreCase(entityName)) {
      return entity.getProcurementcalendartimesquery();
    } else if ("shipquantityroundingrules".equalsIgnoreCase(entityName)) {
      return entity.getShipquantityroundingrulesquery();
    } else if ("safetystock".equalsIgnoreCase(entityName)) {
      return entity.getSafetystockquery();
    } else if ("autoapprovalexceptions".equalsIgnoreCase(entityName)) {
      return entity.getAutoapprovalexceptionsquery();
    } else if ("schedrcpts".equalsIgnoreCase(entityName)) {
      return entity.getSchedrcptsquery();
    } else if ("aggregated_order_projections".equalsIgnoreCase(entityName)) {
      return entity.getAggregated_order_projectionsquery();
    } else {
      return "";
    }

  }

  public boolean createTestCaseDirectoryIFNotPresent(String testCaseFolder, String testCaseName) {
    File newDir = new File(testCaseFolder + File.separator + testCaseName);
    return newDir.mkdir();
  }

  public void updateEntityQueries(DBTestCaseEntity entity, String var) {

    String masterdataQuery =
        "SELECT SDP.ITEM, DEFAULTUOM, SOURCE, DEST,SOURCING, EFF,  DISC, MAJORSHIPQTY, MINORSHIPQTY, ORDERGROUP,ORDERGROUPMEMBER,CONVENIENTROUNDINGPROFILE, TRANSMODE, PROJORDERDUR, COVDURADJTOLERANCE, ORDERREVIEWCAL,LOADBUILDADJDOWNTOLERANCE,LOADBUILDADJUPTOLERANCE,LOADBUILDRULE,LOADMINIMUMRULE,LOADTOLERANCE,SKUPERPALLETSW,TRANSMODEMINRULE,VENDORMINRULE,OHPOST,OH, VOPDEST.DRPCOVDUR,VOPDEST.MAXCOVDUR, VOPDEST.MAXOHRULE, VOPDEST.MAXOH, VOPDEST.EXPDATE,SDP.INDDMDUNITCOST, AAR.AUTOAPPROVALPROFILE,AAR.MAXOPTMDUR,AAR.MAXVEHICLELOADCOUNT,AAR.UOM, AAR.HISTORDERCOUNT, AAR.LOWERTOLERANCE, AAR.UPPERTOLERANCE FROM ( SELECT P.ITEM,P.DEFAULTUOM,PP.SOURCE,PP.DEST,PP.SOURCING,PP.EFF,PP.DISC,PP.MAJORSHIPQTY,PP.MINORSHIPQTY,PP.ORDERGROUP,PP.ORDERGROUPMEMBER,PP.CONVENIENTROUNDINGPROFILE, PG.TRANSMODE,PG.PROJORDERDUR, OGP.COVDURADJTOLERANCE, OGP.AUTOAPPROVALPROFILE,OGP.ORDERREVIEWCAL,OGP.LOADBUILDADJDOWNTOLERANCE,OGP.LOADBUILDADJUPTOLERANCE,OGP.LOADBUILDRULE,OGP.LOADMINIMUMRULE,OGP.LOADTOLERANCE,OGP.SKUPERPALLETSW, OGP.TRANSMODEMINRULE, OGP.VENDORMINRULE,ADEST.OHPOST,ADEST.OH FROM SOURCING PP, ITEM P, LOC LSRC,LOC LDEST, SKU ASRC,SKU ADEST, ORDERGROUP PG,ORDERGROUPPARAM OGP, TRANSMODE TE WHERE PP.ORDERGROUP=PG.ORDERGROUP AND PP.ORDERGROUP LIKE '"
            + var
            + "' AND PP.ORDERGROUPMEMBER IS NOT NULL AND PG.TRANSMODE=TE.TRANSMODE AND PG.ORDERGROUPPARAM=OGP.ORDERGROUPPARAM AND PP.ITEM=P.ITEM AND PP.SOURCE=LSRC.LOC AND PP.DEST=LDEST.LOC AND PP.ITEM = ASRC.ITEM AND PP.SOURCE= ASRC.LOC AND PP.ITEM=ADEST.ITEM AND PP.DEST=ADEST.LOC) A LEFT OUTER JOIN SKUPLANNINGPARAM VOPDEST ON (A.ITEM = VOPDEST.ITEM AND A.DEST = VOPDEST.LOC) LEFT OUTER JOIN SKUDEMANDPARAM SDP ON (A.ITEM = SDP.ITEM AND A.DEST = SDP.LOC) LEFT OUTER JOIN AUTOAPPROVALPROFILE AAR ON (A.AUTOAPPROVALPROFILE = AAR.AUTOAPPROVALPROFILE)";
    entity.setMasterdataquery(masterdataQuery);

    String unitConversionsQuery =
        "select * from uomcategoryconvfactor where sourceuom in (select defaultuom from item where item like '"
            + var
            + "')";
    entity.setUnitconversionsquery(unitConversionsQuery);

    String purchasegroupvendorminimumsQuery =
        "select distinct o.ordergroup,o.ordergroupmember,o.uom,o.mincap from ordergroupcap o ,sourcing s where o.ordergroup like '"
            + var
            + "' and s.ordergroup=o.ordergroup";
    entity.setPurchasegroupvendorminimumsquery(purchasegroupvendorminimumsQuery);

    String transportequipmentcapacityQuery =
        "select t.transmode,t.uom,t.mincap,t.maxcap,t.checkcapsw from transmodecap t where t.transmode like '"
            + var
            + "'";
    entity.setTransportequipmentcapacityquery(transportequipmentcapacityQuery);

    String procurementcalendartimesQuery =
        "select og.orderreviewcal,orderplacedate,arrivdate,needcovdur,mincovdur from orderheader,(select orderreviewcal,ordergroup from ordergroupparam,ordergroup where ordergroup like '"
            + var
            + "' and ordergroup.ordergroupparam = ordergroupparam.ordergroupparam) OG where OG.ordergroup=orderheader.ordergroup and rownum &lt 2";
    entity.setProcurementcalendartimesquery(procurementcalendartimesQuery);

    String shipquantityroundingrulesQuery =
        "select convenientroundingprofile,convenientshipqty,roundupfactor,rounddownfactor,descr from convenientroundingdata where convenientroundingprofile in ( select convenientroundingprofile from sourcing where ordergroup like '"
            + var
            + "') order by convenientroundingprofile, convenientshipqty ,roundupfactor asc";
    entity.setShipquantityroundingrulesquery(shipquantityroundingrulesQuery);

    String safetystockQuery =
        "select SS1.Item, SS1.loc, SS1.eff,SS1.qty from statss SS1,SOURCING S1 WHERE S1.ORDERGROUP like '"
            + var
            + "' AND SS1.ITEM=S1.ITEM AND SS1.LOC=S1.DEST UNION select SS1.Item, SS1.loc, SS1.eff,SS1.qty from SS SS1,SOURCING S1 WHERE S1.ORDERGROUP like 'OOPT_OGT%305.01%' AND SS1.ITEM=S1.ITEM AND SS1.LOC=S1.DEST order by item, loc, eff asc";
    entity.setSafetystockquery(safetystockQuery);

    String autoapprovalexceptionsQuery =
        "select distinct approvalexception,exception from approvalexception order by approvalexception";
    entity.setAutoapprovalexceptionsquery(autoapprovalexceptionsQuery);

    String schedrcptsQuery =
        "select schedrcpts.item,schedrcpts.loc,schedrcpts.scheddate,schedrcpts.qty from schedrcpts,item where schedrcpts.item=item.item and item.item like '"
            + var
            + "'";
    entity.setSchedrcptsquery(schedrcptsQuery);

    String aggregated_order_projectionsQuery =
        "select ITEM,LOC,STARTDATE,DUR,QTY from skuexternalfcst where item like '" + var + "'";
    entity.setAggregated_order_projectionsquery(aggregated_order_projectionsQuery);
    
    String currencyconversionsquery = "select * from temp_cc where item like '" + var + "'";
    entity.setCurrencyconversionsquery(currencyconversionsquery);

  }


}
