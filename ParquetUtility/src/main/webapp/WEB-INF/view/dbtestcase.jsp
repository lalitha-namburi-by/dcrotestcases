<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>Create Test Case From DB</title>
</head>
<body>

<a href="/"><input type="button" value ="GO TO HOME"/></a>

<form action="/createTestCaseFromDB" method="post">

Enter New TestCase Name : <input type="text" name="testcasename"/>

<br/>
<br/>
<b>DB Details :</b>
<br/>
<table>
	
	<tr>
		<td>URL :</td> 
		<td> <input type="text" name="dburl" size="100"/></td>
	</tr>
	
	<tr>
		<td>USERNAME :</td>
		<td> <input type="text" name="dbusername"/></td>
	</tr>

	<tr>
		<td>PASSWORD :</td>
		<td> <input type="password" name="dbpassword"/></td>
	</tr>

	<tr>
		<td>SCHEMA NAME :</td>
		<td> <input type="text" name="schemaname"/></td>
	</tr>

</table>

<br/>
<input type="submit" value="Create TestCase">
<br/>
<br/>
<b>Queries :</b>
<br/>
<table>
	<tr>
		<td>MasterData Query :</td>
		<td> <textarea name ="masterdataquery" rows="20" cols="150">SELECT SDP.ITEM, DEFAULTUOM, SOURCE, DEST,SOURCING, EFF,  DISC, MAJORSHIPQTY, MINORSHIPQTY, ORDERGROUP,ORDERGROUPMEMBER,CONVENIENTROUNDINGPROFILE, TRANSMODE, PROJORDERDUR, COVDURADJTOLERANCE, ORDERREVIEWCAL,LOADBUILDADJDOWNTOLERANCE,LOADBUILDADJUPTOLERANCE,LOADBUILDRULE,LOADMINIMUMRULE,LOADTOLERANCE,SKUPERPALLETSW,TRANSMODEMINRULE,VENDORMINRULE,OHPOST,OH, VOPDEST.DRPCOVDUR,VOPDEST.MAXCOVDUR, VOPDEST.MAXOHRULE, VOPDEST.MAXOH, VOPDEST.EXPDATE,SDP.INDDMDUNITCOST, AAR.AUTOAPPROVALPROFILE,AAR.MAXOPTMDUR,AAR.MAXVEHICLELOADCOUNT,AAR.UOM, AAR.HISTORDERCOUNT, AAR.LOWERTOLERANCE, AAR.UPPERTOLERANCE FROM ( SELECT P.ITEM,P.DEFAULTUOM,PP.SOURCE,PP.DEST,PP.SOURCING,PP.EFF,PP.DISC,PP.MAJORSHIPQTY,PP.MINORSHIPQTY,PP.ORDERGROUP,PP.ORDERGROUPMEMBER,PP.CONVENIENTROUNDINGPROFILE, PG.TRANSMODE,PG.PROJORDERDUR, OGP.COVDURADJTOLERANCE, OGP.AUTOAPPROVALPROFILE,OGP.ORDERREVIEWCAL,OGP.LOADBUILDADJDOWNTOLERANCE,OGP.LOADBUILDADJUPTOLERANCE,OGP.LOADBUILDRULE,OGP.LOADMINIMUMRULE,OGP.LOADTOLERANCE,OGP.SKUPERPALLETSW, OGP.TRANSMODEMINRULE, OGP.VENDORMINRULE,ADEST.OHPOST,ADEST.OH FROM SOURCING PP, ITEM P, LOC LSRC,LOC LDEST, SKU ASRC,SKU ADEST, ORDERGROUP PG,ORDERGROUPPARAM OGP, TRANSMODE TE WHERE PP.ORDERGROUP=PG.ORDERGROUP AND PP.ORDERGROUP LIKE 'OOPT_OGT%305.01%' AND PP.ORDERGROUPMEMBER IS NOT NULL AND PG.TRANSMODE=TE.TRANSMODE AND PG.ORDERGROUPPARAM=OGP.ORDERGROUPPARAM AND PP.ITEM=P.ITEM AND PP.SOURCE=LSRC.LOC AND PP.DEST=LDEST.LOC AND PP.ITEM = ASRC.ITEM AND PP.SOURCE= ASRC.LOC AND PP.ITEM=ADEST.ITEM AND PP.DEST=ADEST.LOC) A LEFT OUTER JOIN SKUPLANNINGPARAM VOPDEST ON (A.ITEM = VOPDEST.ITEM AND A.DEST = VOPDEST.LOC) LEFT OUTER JOIN SKUDEMANDPARAM SDP ON (A.ITEM = SDP.ITEM AND A.DEST = SDP.LOC) LEFT OUTER JOIN AUTOAPPROVALPROFILE AAR ON (A.AUTOAPPROVALPROFILE = AAR.AUTOAPPROVALPROFILE)
            </textarea>
        </td>
		<!--<td>
			<table>
				
				<tr>
					<td>masterdata.so_date : </td>
					<td><input type="text" name="so_date"/></td>
				</tr>
				
				<tr>
					<td>masterdata.so_quantity : </td>
					<td><input type="text" name="so_quantity"/></td>
				</tr>
				
				  <tr>
					<td>masterdata.pp_sqrr_id : </td>
					<td><input type="text" name="pp_sqrr_id"/></td>
				</tr> 
			</table>
		</td>-->
	</tr>
	
	<tr>
		<td>UnitConversions query :</td> 
		<td>
			<textarea name ="unitconversionsquery" rows="2" cols="150">select * from uomcategoryconvfactor where sourceuom in (select defaultuom from item where item like 'OOPT_OGT%305.01%')
			</textarea>
		</td>
	</tr>
	
	<tr>
        <td>CurrencyConversions query :</td> 
        <td>
            <textarea name ="currencyconversionsquery" rows="2" cols="150">select * from temp_cc where item like 'OOPT_OGT%305.01%'
            </textarea>
        </td>
    </tr>
	
	<tr>
		<td>PurchaseGroupVendorMinimums Query :</td> 
		<td>
			<textarea name ="purchasegroupvendorminimumsquery" rows="2" cols="150">select distinct o.ordergroup,o.ordergroupmember,o.uom,o.mincap from ordergroupcap o ,sourcing s where o.ordergroup like 'OOPT_OGT%305.01%' and s.ordergroup=o.ordergroup
			</textarea>
		</td>
	</tr>
	
	<tr>
		<td>TransportEquipmentCapacity Query : </td>
		<td>
			<textarea name ="transportequipmentcapacityquery" rows="4" cols="150">select t.transmode,t.uom,t.mincap,t.maxcap,t.checkcapsw from transmodecap t where t.transmode like 'OOPT_OGT%305.01%'
			</textarea>
		</td>
	</tr>
	
	<tr>
        <td>ProcurementCalendarTimes Query : </td>
        <td>
            <textarea name ="procurementcalendartimesquery" rows="4" cols="150">select og.orderreviewcal,orderplacedate,arrivdate,needcovdur,mincovdur from orderheader,(select orderreviewcal,ordergroup from ordergroupparam,ordergroup where ordergroup like 'OOPT_OGT%305.01%' and ordergroup.ordergroupparam = ordergroupparam.ordergroupparam) OG where OG.ordergroup=orderheader.ordergroup and rownum &lt 2
            </textarea>
        </td>
    </tr>
	
	<tr>
		<td>ShipQuantityRoundingRules Query : </td>
		<td>
			<textarea name ="shipquantityroundingrulesquery" rows="2" cols="150">select convenientroundingprofile,convenientshipqty,roundupfactor,rounddownfactor,descr from convenientroundingdata where convenientroundingprofile in ( select convenientroundingprofile from sourcing where ordergroup like 'OOPT_OGT%305.01%') order by convenientroundingprofile, convenientshipqty ,roundupfactor asc
			</textarea>
		</td>
	</tr>
	
	 <tr>
		<td>Safety Stock Query :</td>
		<td>
			<textarea name ="safetystockquery" rows="4" cols="150">select SS1.Item, SS1.loc, SS1.eff,SS1.qty from statss SS1,SOURCING S1 WHERE S1.ORDERGROUP like 'OOPT_OGT%305.01%' AND SS1.ITEM=S1.ITEM AND SS1.LOC=S1.DEST UNION select SS1.Item, SS1.loc, SS1.eff,SS1.qty from SS SS1,SOURCING S1 WHERE S1.ORDERGROUP like 'OOPT_OGT%305.01%' AND SS1.ITEM=S1.ITEM AND SS1.LOC=S1.DEST order by item, loc, eff asc
			</textarea>
		</td>
	</tr>
	
	<tr>
		<td>AutoApprovalExceptions Query :</td> 
		<td>
			<textarea name ="autoapprovalexceptionsquery" rows="2" cols="150">select distinct approvalexception,exception from approvalexception order by approvalexception
			</textarea></td>
	</tr>
	
	<tr>
		<td>Schedrcpts Query :</td> 
		<td>
			<textarea name ="schedrcptsquery" rows="4" cols="150">select schedrcpts.item,schedrcpts.loc,schedrcpts.scheddate,schedrcpts.qty from schedrcpts,item where schedrcpts.item=item.item and item.item like 'OOPT_OGT%'
			</textarea>
		</td>
		<td>
		<!-- <table>
			<tr>
				<td>schedrcpts.h_edlc_expected_delivery_date : </td>
				<td><input type="text" name="h_edlc_expected_delivery_date"/></td>
			</tr>
		</table> -->
		</td>
	</tr>
	
	<tr>
		<td>Aggregated_Order_Projections Query : </td>
		<td>
			<textarea name ="aggregated_order_projectionsquery" rows="2" cols="150">select ITEM,LOC,STARTDATE,DUR,QTY from skuexternalfcst where item like 'OOPT_OGT%305.01%'
			</textarea>
		</td>
		<!--  <td>
			<table>
				<tr>
					<td>aggregated_order_projections.aggregated_order_projection_period_from :</td> 
					<td><input type="text" name="aggregated_order_projection_period_from"/></td>
				</tr>
				
				<tr>
					<td>aggregated_order_projections.aggregated_order_projection_period_upto :</td> 
					<td><input type="text" name="aggregated_order_projection_period_upto"/></td>
				</tr>
			</table>
		</td> -->
	</tr>
</table>

<br/>
<br/>


</form>
</body>
</html>