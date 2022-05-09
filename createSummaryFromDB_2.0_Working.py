#!/usr/bin/env python3
import pyarrow.parquet as pq
import pandas as pd
import json
import time
import datetime
import sys
import os
import pytz
import psycopg2
from io import BytesIO
from azure.storage.blob import BlockBlobService


def tz_aware(dt):
    return dt.tzinfo is not None and dt.tzinfo.utcoffset(dt) is not None

def convert_timestamp(dt):

    if tz_aware(dt):
        #print("tz aware")
        timestamp = dt.tz_convert(tz='US/Eastern')
    else:
        #print("tz localize")
        timestamp = dt.tz_localize(tz='US/Eastern')

    return timestamp


def write_parquet_to_textfile(filepath,data):
    text_file = open(filepath, 'w')
    text_file.write(data.to_string())
    text_file.close()

class SOQ:

    def __init__(self,opd,soq):
        self.opd = opd
        self.soq= soq

def getDataFrameFromABS(block_blob_service,container_name,parquet_file_path):
   byte_stream = BytesIO()
   try:
      block_blob_service.get_blob_to_stream(container_name=container_name, blob_name=parquet_file_path, stream=byte_stream)
      df = pq.read_table(source=byte_stream).to_pandas()
   finally:
      byte_stream.close()
   return df

current_dir = os.getcwd()

testcase = sys.argv[1]
print(testcase)
unformattedorderplacedate = sys.argv[2]
args = len(sys.argv)
print("args length = ", args)
orderplacedate = unformattedorderplacedate.replace("-", "/")

element = datetime.datetime.strptime(orderplacedate,"%Y/%m/%d")

opdtimestamp = datetime.datetime.timestamp(element)

global connection
orderheader_dict = {}
sku_soq_dict = {}

print('DB Connection trying')

def get_data_from_db(sql_query):
    df = pd.read_sql(sql_query,connection)
    return df

db_user_name = 'stratosphere@o3lm1hpcc3g7g7jqoxmu6ffz'
db_password = '1DWvt4Cue5AI2Ub3GYGb'
db_hostname = 'localhost'
db_port = 5111
db_name = 'postgres'

try:
    connection = psycopg2.connect(user = db_user_name,
                                  password = db_password,
                                  host = db_hostname,
                                  port = db_port,
                                  database = db_name)
    print('Connection obtained')
    orderheader_sql_query = "select orderid,arrivdate from orderheader where orderheader.createdate ='2022-05-04 11:40:00' union select orderid,arrivdate from lrr_proj_orderheader where lrr_proj_orderheader.createdate ='2022-05-04 11:40:00'"
    orderheaderdata = get_data_from_db(orderheader_sql_query)
    print(orderheaderdata)
    for index,row in orderheaderdata.iterrows():
        orderid = row['orderid']
        orderplacedate_1 = row['arrivdate']
        orderheader_dict[orderid] = orderplacedate_1


    ordersku_sql_query = "select ordersku.item,ordersku.dest,ordersku.source,ordersku.soq,ordersku.orderid,ordersku.arrivdate, orderheader.createdate from ordersku,orderheader where orderheader.orderid=ordersku.orderid and orderheader.createdate ='2022-05-04 11:40:00' union select lrr_proj_ordersku.item,lrr_proj_ordersku.dest,lrr_proj_ordersku.source,lrr_proj_ordersku.soq,lrr_proj_ordersku.orderid,lrr_proj_ordersku.arrivdate, lrr_proj_orderheader.createdate from lrr_proj_ordersku,lrr_proj_orderheader where lrr_proj_orderheader.orderid=lrr_proj_ordersku.orderid and lrr_proj_orderheader.createdate ='2022-05-04 11:40:00'"

    orderskudata = get_data_from_db(ordersku_sql_query)
    print(orderskudata)
    for index, row in orderskudata.iterrows():
        item = row['item']
        dest = row['dest']
        source = row['source']
        soq = row['soq']
        orderid = row['orderid']
        orderplacedate_2 = orderheader_dict[orderid]

        key = str(item)+'@'+str(dest)

        try:
            vendor_dict = sku_soq_dict[key]
        except KeyError:
            vendor_dict = {}
            sku_soq_dict[key] = vendor_dict

        try:
            soqlist = vendor_dict[source] 
        except KeyError:
            soqlist =[]
            vendor_dict[source] = soqlist
        soqObject = SOQ(orderplacedate_2,soq)
        soqlist.append(soqObject)
    
except (Exception, psycopg2.Error) as error :
    print ("Error while connecting to PostgreSQL", error)
finally:
    if connection:
        connection.close()
        print("PostgreSQL connection is closed")



print('ABS Connection trying')

proxy = 'http://bywww.blue-yonder.org:8888'

os.environ['http_proxy'] = proxy 
os.environ['HTTP_PROXY'] = proxy
os.environ['https_proxy'] = proxy
os.environ['HTTPS_PROXY'] = proxy


account_name = 'cpgsz5l7eccba'
sas_token = 'se=2030-01-10T00%3A09%3A00Z&sp=rwdl&sv=2019-02-02&sr=c&sig=F345V/StCG1tzHCGAku44GYCgHAknBNI61vPNmc%2Br5A%3D'
container_name = 'loblaws-prod2-aggregated-order-projections'

order_place_date_folder = '2022-05-04'
timestamp_folder='1651662019'

block_blob_service = BlockBlobService(account_name=account_name, sas_token=sas_token)

buy_guide_parquet = 'dcro-input/'+order_place_date_folder+'/'+timestamp_folder+'/batch0/dcro-supplier-splits/buy_guide_data.parquet'
buyGuideParquet = getDataFrameFromABS(block_blob_service,container_name,buy_guide_parquet)

buyguide_dict = {}
for index,row in buyGuideParquet.iterrows():
    item = str(row['PP_P_ID'])
    dest = str(row['PP_L_ID_TARGET'])
    key = item+'@'+dest

    #print(key)
    
    try:
        sku_dict = buyguide_dict[key]
    except KeyError:
        sku_dict = {}
        buyguide_dict[key] = sku_dict
    

    vendor = str(row['PP_L_ID_SOURCE'])
    
    try:
        datalist = sku_dict[vendor]
    except KeyError:
        datalist = []
        sku_dict[vendor] = datalist
    datalist.append(row)
    
    
filepath = '/Users/1022773/Desktop'+'/summary.txt'
text_file = open(filepath, 'w')


schedrcptsupplierdata = pd.DataFrame()
manualordersdata = pd.DataFrame()
additional_vendor_orders_data = pd.DataFrame()

schedrcpt_supplier_parquet = 'dcro-input/'+order_place_date_folder+'/'+timestamp_folder+'/batch0/dcro-supplier-splits/schedrcpts_supplier_data.parquet'
schedrcptsupplierdata = getDataFrameFromABS(block_blob_service,container_name,schedrcpt_supplier_parquet)

manual_orders_parquet = 'dcro-input/'+order_place_date_folder+'/'+timestamp_folder+'/batch0/dcro-unapproved-orders/manual_orders_data.parquet'
manualordersdata = getDataFrameFromABS(block_blob_service,container_name,manual_orders_parquet)


additional_vendor_orders_parquet = 'dcro-input/'+order_place_date_folder+'/'+timestamp_folder+'/batch0/snd-data/additional_vendor_orders.parquet'
additional_vendor_orders_data=getDataFrameFromABS(block_blob_service,container_name,additional_vendor_orders_parquet)

print("buyguide dict", buyguide_dict)

print('Doing summary calcualtion')

for key,value in buyguide_dict.items():
    print(key)
    keydata = key.split('@')
    item = int(keydata[0])
    dest = int(keydata[1])

    master_parquet = 'dcro-input/'+order_place_date_folder+'/'+timestamp_folder+'/batch0/snd-data/masterdata.parquet'
    masterdata = getDataFrameFromABS(block_blob_service,container_name,master_parquet)
    filtered_masterdata = masterdata[(masterdata['PP_P_ID'] ==item)&(masterdata['PP_L_ID_TARGET'] == dest)]
    item_code = filtered_masterdata['P_EXTERNAL_CODE'].iloc[0]
    dest_code = filtered_masterdata['L_EXTERNAL_CODE_TARGET'].iloc[0]
    dsd_code = filtered_masterdata['L_LY_ID_TARGET'].iloc[0]
    
    startdatelist = []
    enddatelist = []
    soqtotallist =[]
    srstotallist =[]
    msototallist =[]
    splitpercentagelist =[]
    vendorlist = []
    ranklist = []
    volumeexactlist =[]
    volumeuptolist =[]
    for key1, value1 in value.items():
        filtered_schedrcpt_supplier = pd.DataFrame()
        filtered_manual_orders = pd.DataFrame()
        
        source = int(key1)
        if not schedrcptsupplierdata.empty:
            filtered_schedrcpt_supplier = schedrcptsupplierdata[(schedrcptsupplierdata['H_EDLC_L_ID_TARGET']==dest)&(schedrcptsupplierdata['H_EDLC_P_ID']==item)&(schedrcptsupplierdata['H_EDLC_L_ID_SOURCE']==source)]
            
        if not manualordersdata.empty:
            filtered_manual_orders = manualordersdata[(manualordersdata['PP_L_ID_TARGET']==dest)&(manualordersdata['PP_P_ID']==item)&(manualordersdata['PP_L_ID_SOURCE']==source)]
        
        for row in value1 :
            startdate = row['START_DATE']
            enddate =  row['END_DATE']
            
            starttimestamp = convert_timestamp(startdate)
            endtimestamp = convert_timestamp(enddate)
            srstotal =0
            if dsd_code != 1000:
                if not filtered_schedrcpt_supplier.empty:
                    for index1,row1 in filtered_schedrcpt_supplier.iterrows():
                        delivery_date = row1['H_EDLC_EXPECTED_DELIVERY_DATE']
                        delivery_timestamp = convert_timestamp(delivery_date)
                        if starttimestamp <= delivery_timestamp < endtimestamp:
                            srstotal = srstotal + row1['H_EDLC_QUANTITY']
                        
            msototal =0
            if not filtered_manual_orders.empty:
                for index2,row2 in filtered_manual_orders.iterrows():
                    delivery_date = row2['DELIVERY_DATE']
                    delivery_timestamp = convert_timestamp(delivery_date)
                    if starttimestamp <= delivery_timestamp < endtimestamp:
                        msototal = msototal + row2['QUANTITY']
                        
            try:
                vendordata = sku_soq_dict[key]
            except KeyError:
                continue

            try:
                soqlist = vendordata[key1]
            except KeyError:
                continue

            split_percentage = row['SPLIT_PERCENTAGE']
            soqtotal =0
            for soqObject in soqlist:
                orderplacedate_3 = soqObject.opd
                timestamp = convert_timestamp(pd.Timestamp(orderplacedate_3))
                if starttimestamp <= timestamp < endtimestamp:
                    soqtotal = soqtotal + soqObject.soq
            rank = row['PRIORITY']
            volumeexact = row['VOLUME_EXACT']
            volumeupto= row['VOLUME_UPTO']
            ranklist.append(rank)
            volumeexactlist.append(volumeexact)
            volumeuptolist.append(volumeupto)
            startdatelist.append(startdate)
            enddatelist.append(enddate)
            soqtotallist.append(soqtotal)
            srstotallist.append(srstotal)
            msototallist.append(msototal)
            splitpercentagelist.append(split_percentage)
            vendorlist.append(key1)
    data = {'startdate':startdatelist,'enddate':enddatelist,'vendor':vendorlist,'rank':ranklist,'volume_exact':volumeexactlist,'volume_upto':volumeuptolist,'soq':soqtotallist,'srs':srstotallist,'mso':msototallist,'buyguide percentage':splitpercentagelist}
    df = pd.DataFrame(data)
    if df.empty:
        continue
    totaldict = {}
    for index, row in df.iterrows():
        startdate = row['startdate']
        try:
            totalvalue = totaldict[startdate]
            totalvalue = totalvalue + row['soq'] + row['srs'] + row['mso']
            totaldict[startdate] = totalvalue
        except KeyError:
            totaldict[startdate]= row['soq'] + row['srs']+ row['mso']

    actual_percentage = []
    order_total = []
    for index, row in df.iterrows():
        startdate = row['startdate']
        soq = row['soq'] + row['srs']+ row['mso']
        total = totaldict[startdate]
        order_total.append(total)
        percentage = 0
        if (total > 0):
            percentage = (soq/total)*100
        actual_percentage.append(percentage)

    df['actual_percentage'] = actual_percentage
    df['order_total'] = order_total

    

    demand_parquet = 'dcro-input/'+order_place_date_folder+'/'+timestamp_folder+'/batch0/lsf-output-data/aggregated_order_projections.parquet'
    demanddata = getDataFrameFromABS(block_blob_service,container_name,demand_parquet)
    filtered_demand = demanddata[(demanddata['SUPPLIER_EXTERNAL_CODE']==dest_code)&(demanddata['P_EXTERNAL_CODE']==item_code)]
    filtered_demand = filtered_demand.drop_duplicates(subset=['AGGREGATED_ORDER_PROJECTION_PERIOD_FROM','AGGREGATED_ORDER_PROJECTION_PERIOD_UPTO'])

    filtered_ss = pd.DataFrame()
    if dsd_code != 1000:
        ss_parquet = 'dcro-input/'+order_place_date_folder+'/'+timestamp_folder+'/batch0/safety_stock_output/safetystock.parquet'
        ssdata = getDataFrameFromABS(block_blob_service,container_name,ss_parquet)
        filtered_ss = ssdata[(ssdata['SUPPLIER_EXTERNAL_CODE']==dest_code)&(ssdata['P_EXTERNAL_CODE']==item_code)]
        filtered_ss = filtered_ss.drop_duplicates(subset=['EFFECTIVE_FROM','EFFECTIVE_UPTO'])
    
    filtered_schedrcpt = pd.DataFrame()
    if dsd_code != 1000:
        schedrcpt_parquet = 'dcro-input/'+order_place_date_folder+'/'+timestamp_folder+'/batch0/snd-data/schedrcpts.parquet'
        schedrcptdata = getDataFrameFromABS(block_blob_service,container_name,schedrcpt_parquet)
        filtered_schedrcpt = schedrcptdata[(schedrcptdata['H_EDLC_L_ID_TARGET']==dest)&(schedrcptdata['H_EDLC_P_ID']==item)]
        filtered_schedrcpt = filtered_schedrcpt.drop_duplicates(subset=['H_EDLC_EXPECTED_DELIVERY_DATE'])


    filtered_manual_orders = pd.DataFrame()
    if not manualordersdata.empty:
        filtered_manual_orders = manualordersdata[(manualordersdata['PP_L_ID_TARGET']==dest)&(manualordersdata['PP_P_ID']==item)]
        filtered_manual_orders = filtered_manual_orders.drop_duplicates(subset=['PP_L_ID_SOURCE','DELIVERY_DATE'])

    filtered_additional_vendor_orders = pd.DataFrame()
    if not additional_vendor_orders_data.empty:
        filtered_additional_vendor_orders = additional_vendor_orders_data[(additional_vendor_orders_data['PP_P_ID']==item)&(additional_vendor_orders_data['PP_L_ID_TARGET']==dest)]


    timedomain =[]
    for i in range(0,21):
        date = element + datetime.timedelta(days=i)
        date_1 = pd.Timestamp(date, unit='s')
        item_1 = convert_timestamp(date_1)
        timedomain.append(item_1)
    demandlist = []
    sslist =[]
    schedrcptslist =[]
    msorderlist =[]
    soq1_list = []
    avolist = []

    vendor_soq_dict = {}

    filtered_orderskudata = orderskudata[(orderskudata['item']==str(item))&(orderskudata['dest']==str(dest))]
    for item in timedomain:
        item_1 = item
        demand = 0;
        for index,row in filtered_demand.iterrows():
            period_from = row['AGGREGATED_ORDER_PROJECTION_PERIOD_FROM']
            period_upto = row['AGGREGATED_ORDER_PROJECTION_PERIOD_UPTO']
            period_from_stamp = convert_timestamp(period_from)
            period_upto_stamp = convert_timestamp(period_upto)
            if((item_1 < period_upto_stamp) &  (item_1 >=period_from_stamp)):
                demand= demand+ row['AGGREGATED_ORDER_PROJECTION_MEAN']
        demandlist.append(demand)

        ss = 0;
        for index,row in filtered_ss.iterrows():
            period_from = row['EFFECTIVE_FROM']
            period_upto = row['EFFECTIVE_UPTO']
            period_from_stamp = convert_timestamp(period_from)
            period_upto_stamp = convert_timestamp(period_upto)
            
            if((item_1 < period_upto_stamp) &  (item_1 >=period_from_stamp)):
                ss= ss+ row['SAFETY_STOCK_PER_DAY']
        sslist.append(ss)

        schedrcpt = 0;
        for index,row in filtered_schedrcpt.iterrows():
            delivery_date = row['H_EDLC_EXPECTED_DELIVERY_DATE']
            deliyery_date_string = str(delivery_date)
            delivery_date_list = deliyery_date_string.split(" ")
            date_string = str(item_1)
            date_string_list = date_string.split(" ")
            if(date_string_list[0] == delivery_date_list[0]):
                schedrcpt= schedrcpt+   row['H_EDLC_QUANTITY']
        schedrcptslist.append(schedrcpt)
        
        msorder = 0;
        for index,row in filtered_manual_orders.iterrows():
            delivery_date = row['DELIVERY_DATE']
            deliyery_date_string = str(delivery_date)
            delivery_date_list = deliyery_date_string.split(" ")
            date_string = str(item_1)
            date_string_list = date_string.split(" ")
            if(date_string_list[0] == delivery_date_list[0]):
                msorder= msorder+ row['QUANTITY']
        msorderlist.append(msorder)


        avo = 0;
        for index,row in filtered_additional_vendor_orders.iterrows():  
            arrival_date = row['ARRIVAL_DATE']
            arrival_date_stamp = convert_timestamp(arrival_date)
            arrival_date_string = str(arrival_date_stamp)
            arrival_date_list = arrival_date_string.split(" ")
            date_string = str(item_1)
            date_string_list = date_string.split(" ")
            if(date_string_list[0]  == arrival_date_list[0]):
                avo= avo+ row['REQUESTED_QUANTITY']
        avolist.append(avo)
        
        soq_1 = 0;
        for index,row in filtered_orderskudata.iterrows():
            arrivdatestring = str(row['arrivdate'])
            list_1 = arrivdatestring.split(" ")
            arrivdate = list_1[0]
            item_1_string = str(item_1)
            list_2 = item_1_string.split(" ")
            source_1 = row['source']
            if(arrivdate == list_2[0]):
                soq_1 = soq_1 + row['soq']
                arrivedate_dict = {}
                try:
                    arrivedate_dict = vendor_soq_dict[source_1]
                except KeyError:
                    arrivedate_dict = {}
                    vendor_soq_dict[source_1] = arrivedate_dict
                arrivedate_dict[arrivdate] = row['soq']


        soq1_list.append(soq_1)

    data_1 = {'date':timedomain,'demand':demandlist,'ss':sslist,'avo':avolist,'schedrcpt':schedrcptslist,'mso':msorderlist,'soq':soq1_list}
    for key_1,value in vendor_soq_dict.items():
        vendor_soq_list=[]
        for item in timedomain:
            item_3_string = str(item)
            list_3 = item_3_string.split(" ")
            item_string = list_3[0]
            try:
                soq_5 = value[item_string]
                vendor_soq_list.append(soq_5)
            except KeyError:
                vendor_soq_list.append(0)
        data_1[key_1] = vendor_soq_list

    df_1 = pd.DataFrame(data_1)
    curr_projoh =0
    projoh_list =[]
    projavail_list =[]
    ignored_demand_list =[]
    incoming_supply_list = []
    for index, row in df_1.iterrows():
        curr_projoh=curr_projoh+row['schedrcpt']+row['mso']+row['soq']-row['demand']-row['avo']
        ignored_demand = 0
        if(curr_projoh < 0):
            ignored_demand = curr_projoh * -1
            curr_projoh = 0
        projoh_list.append(curr_projoh)
        projavail_list.append(curr_projoh-row['ss'])
        ignored_demand_list.append(ignored_demand)
        incoming_supply = row['mso']+row['soq']
        incoming_supply_list.append(incoming_supply)

    df_1['projoh'] = projoh_list
    df_1['projavail'] = projavail_list
    df_1['ignored_demand'] = ignored_demand_list
    df_1['incoming_supply'] = incoming_supply_list

    df.sort_values(by=['startdate','enddate','vendor'], inplace=True)
    text_file.write(key)
    text_file.write('\n')
    text_file.write(df.to_string())
    text_file.write('\n')
    text_file.write(df_1.to_string())
    
    #print("df data =", df)
    curr_startdate = None
    curr_enddate = None
    total_volume_exact = 0
    total_volume_upto = 0
    total_soq = 0
    total_incoming = 0
    startdatelist_1 = []
    enddatelist_1 = []
    total_volume_exact_list = []
    total_volume_upto_list = []
    total_soq_list = []
    total_incoming_list = []
    for index, row in df.iterrows():
        if(curr_startdate == row['startdate']):
            total_volume_exact = total_volume_exact +row['volume_exact']
            total_volume_upto = total_volume_upto + row['volume_upto']
            total_soq = total_soq + row['soq']
            total_incoming = total_incoming + row['srs'] + row['mso']
        else:
            if(curr_startdate != None):
                startdatelist_1.append(curr_startdate)
                enddatelist_1.append(curr_enddate)
                total_volume_exact_list.append(total_volume_exact)
                total_volume_upto_list.append(total_volume_upto)
                total_soq_list.append(total_soq)
                total_incoming_list.append(total_incoming)
            total_volume_exact = row['volume_exact']
            total_volume_upto = row['volume_upto']
            total_soq = row['soq']
            total_incoming = row['srs'] + row['mso']
        curr_startdate = row['startdate']
        curr_enddate = row['enddate']
    startdatelist_1.append(curr_startdate)
    enddatelist_1.append(curr_enddate)
    total_volume_exact_list.append(total_volume_exact)
    total_volume_upto_list.append(total_volume_upto)
    total_soq_list.append(total_soq)
    total_incoming_list.append(total_incoming)

    data_2 = {'startdate':startdatelist_1,'enddate':enddatelist_1,'volume_exact':total_volume_exact_list,'volume_upto':total_volume_upto_list,'ordered_total':total_soq_list,'srs_and_mso':total_incoming_list}
   
    df_2 = pd.DataFrame(data_2)

    aop_and_ss_list =[]
    net_ss_list =[]
    net_ignored_demand_list = []
    prev_net_ss = 0
    for index, row in df_2.iterrows():
        startdate = row['startdate']
        enddate = row['enddate']
        order = row['ordered_total']
        srs_and_mso=row['srs_and_mso']
        total_demand = 0
        end_ss = 0
        for i,r in df_1.iterrows():
            date =  r['date']
            demand = r['demand']
            avo=r['avo']
            starttimestamp = convert_timestamp(startdate)
            endtimestamp = convert_timestamp(enddate)
            
            if(starttimestamp <=date <endtimestamp):
                total_demand = total_demand + demand+avo
            if(date<endtimestamp):
                end_ss=r['ss']
        net_ss = end_ss - prev_net_ss
        prev_net_ss = end_ss
        aop_and_ss = total_demand + net_ss
        aop_and_ss_list.append(aop_and_ss)
        net_ss_list.append(net_ss)
        net_ignored_demand = order+srs_and_mso-aop_and_ss
        net_ignored_demand_list.append(net_ignored_demand)



    df_2['aop_and_ss_and_avo'] = aop_and_ss_list
    df_2['net_ss'] = net_ss_list
    df_2['ignored_demand'] = net_ignored_demand_list

    text_file.write('\n')
    text_file.write(df_2.to_string())
    text_file.write('\n')

text_file.close()
print('Summary calcualtion done')
