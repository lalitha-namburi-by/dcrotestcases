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
        print("tz aware")
        timestamp = dt.tz_convert(tz='US/Eastern')
    else:
        print("tz localize")
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
      # Do work on df ...
      #print(df)
   finally:
      # Add finally block to ensure closure of the stream
      byte_stream.close()
   return df

#testcase = sys.argv[1]
current_dir = os.getcwd()

testcase = sys.argv[1]
print(testcase)
unformattedorderplacedate = sys.argv[2]
orderplacedate = unformattedorderplacedate.replace("-", "/")

element = datetime.datetime.strptime(orderplacedate,"%Y/%m/%d")

#print(element)
  
opdtimestamp = datetime.datetime.timestamp(element)



global connection
orderheader_dict = {}
sku_soq_dict = {}

def get_data_from_db(sql_query):
    df = pd.read_sql(sql_query,connection)
    return df

db_user_name = 'lrr_solar_dev@postgraas-instance8-westeurope-a'
db_password = 'lrr_solar_dev_Passworda288a425ae50#'
db_hostname = 'postgraas-instance8-westeurope-a.postgres.database.azure.com'
db_port = 5432
db_name = 'lrr_solar_dev'

#create_date = 2021-07-05 12:43:00
try:
    connection = psycopg2.connect(user = db_user_name,
                                  password = db_password,
                                  host = db_hostname,
                                  port = db_port,
                                  database = db_name)

    #cursor = connection.cursor()
    # Print PostgreSQL Connection properties
    #print(connection.get_dsn_parameters(), "\n")
    #cursor.execute("select orderid,arrivdate from orderheader union select orderid,arrivdate from lrr_proj_orderheader")
    #records = cursor.fetchall()
    #print(records)

    #data = pd.DataFrame(cursor.fetchall())
    #data.columns = cursor.column_names
    #print(data)
    orderheader_sql_query = "select orderid,arrivdate from orderheader where orderheader.createdate ='2021-07-12 09:37:00' union select orderid,arrivdate from lrr_proj_orderheader where lrr_proj_orderheader.createdate ='2021-07-12 09:37:00'"
    orderheaderdata = get_data_from_db(orderheader_sql_query)
    print(orderheaderdata)
    for index,row in orderheaderdata.iterrows():
        orderid = row['orderid']
        #orderplacedate_1 = row['orderplacedate']
        orderplacedate_1 = row['arrivdate']
        orderheader_dict[orderid] = orderplacedate_1


    #ordersku_sql_query = "select item,dest,source,soq,orderid,arrivdate from ordersku union select item,dest,source,soq,orderid,arrivdate from lrr_proj_ordersku"
    ordersku_sql_query = "select ordersku.item,ordersku.dest,ordersku.source,ordersku.soq,ordersku.orderid,ordersku.arrivdate, orderheader.createdate from ordersku,orderheader where orderheader.orderid=ordersku.orderid and orderheader.createdate ='2021-07-12 09:37:00' union select lrr_proj_ordersku.item,lrr_proj_ordersku.dest,lrr_proj_ordersku.source,lrr_proj_ordersku.soq,lrr_proj_ordersku.orderid,lrr_proj_ordersku.arrivdate, lrr_proj_orderheader.createdate from lrr_proj_ordersku,lrr_proj_orderheader where lrr_proj_orderheader.orderid=lrr_proj_ordersku.orderid and lrr_proj_orderheader.createdate ='2021-07-12 09:37:00'"

    orderskudata = get_data_from_db(ordersku_sql_query)
    print(orderskudata)
    for index, row in orderskudata.iterrows():
        item = row['item']
        #print(item)
        dest = row['dest']
        #print(dest)
        source = row['source']
        #print(source)
        soq = row['soq']
        #print(soq)
        orderid = row['orderid']
        orderplacedate_2 = orderheader_dict[orderid]
        #print(orderplacedate)

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
    #closing database connection.
    if connection:
        #cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")





proxy = 'http://bywww.blue-yonder.org:8888'

os.environ['http_proxy'] = proxy 
os.environ['HTTP_PROXY'] = proxy
os.environ['https_proxy'] = proxy
os.environ['HTTPS_PROXY'] = proxy

#To print proxies
#print(urllib.request.getproxies())


account_name = 'bvzdtlbxhptci'
sas_token = 'se=2021-11-30T00%3A00%3A00Z&sp=rwdl&sv=2019-02-02&sr=c&sig=bP1tnDaQprH4Yu1hXXOxDy5kTEkeVKrLm/vvPTxNH/M%3D'
container_name = 'lrr-solar-dev-aggregated-order-projections'

order_place_date_folder = '2021-02-01'
timestamp_folder='1625750601'

block_blob_service = BlockBlobService(account_name=account_name, sas_token=sas_token)

#print(opdtimestamp)

buy_guide_parquet = 'dcro-input/'+order_place_date_folder+'/'+timestamp_folder+'/batch0/dcro-supplier-splits/buy_guide_data.parquet'
buyGuideParquet = getDataFrameFromABS(block_blob_service,container_name,buy_guide_parquet)

buyguide_dict = {}
for index,row in buyGuideParquet.iterrows():
    key = str(row['PP_P_ID'])+'@'+str(row['PP_L_ID_TARGET'])
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
    
print(buyguide_dict)

#engine=create_engine('postgresql+psycopg2://1022177:password@localhost/postgres')

#output_dir = current_dir+'/dcroengineoutput/'+testcase+'/'
#orderheaderfilelist = [output_dir+'OrderHeader.parquet',output_dir+'lrr_proj_orderheader.parquet',output_dir+'LongTermProjections/lrr_proj_orderheader.parquet']
#existingorderheaderfilelist = []
#for orderheaderfile in orderheaderfilelist:
#   if(os.path.exists(orderheaderfile)):
#       existingorderheaderfilelist.append(orderheaderfile)

#orderheader_df_list = [pd.read_parquet(file) for file in existingorderheaderfilelist]

#orderheaderdata = pd.concat(orderheader_df_list)
#print(orderheaderdata)
#orderheader_parquet = '/Users/1022177/dcrotestcases/dcroengineoutput/50101/OrderHeader.parquet'
#orderheaderdata = pd.read_sql_query("select orderid,arrivdate from orderheader union select orderid,arrivdate from lrr_proj_orderheader",engine)

#orderheaderdata = pd.read_parquet(orderheader_parquet)

#orderheader_dict = {}
#for index,row in orderheaderdata.iterrows():
#   orderid = row['orderid']
#   #orderplacedate_1 = row['orderplacedate']
#   orderplacedate_1 = row['arrivdate']
#   orderheader_dict[orderid] = orderplacedate_1 

#ordersku_parquet = '/Users/1022177/dcrotestcases/dcroengineoutput/50101/OrderSku.parquet'
#orderskufilelist = [output_dir+'OrderSku.parquet',output_dir+'lrr_proj_ordersku.parquet',output_dir+'LongTermProjections/lrr_proj_ordersku.parquet']

#existingorderskufilelist = []
#for orderskufile in orderskufilelist:
#   if(os.path.exists(orderskufile)):
#       existingorderskufilelist.append(orderskufile)
#ordersku_df_list = [pd.read_parquet(file) for file in existingorderskufilelist]
#orderskudata = pd.concat(ordersku_df_list)
#orderskudata = pd.read_sql_query("select item,dest,source,soq,orderid,arrivdate from ordersku union select item,dest,source,soq,orderid,arrivdate from lrr_proj_ordersku",engine)
#orderskudata = pd.read_parquet(ordersku_parquet)

#sku_soq_dict = {}

#for index, row in orderskudata.iterrows():
#   item = row['item']
#   #print(item)
#   dest = row['dest']
#   #print(dest)
#   source = row['source']
#   #print(source)
#   soq = row['soq']
#   #print(soq)
#   orderid = row['orderid']
#   orderplacedate_2 = orderheader_dict[orderid]
#   #print(orderplacedate)

#   key = str(item)+'@'+str(dest)

#   try:
#       vendor_dict = sku_soq_dict[key]
#   except KeyError:
#       vendor_dict = {}
#       sku_soq_dict[key] = vendor_dict

#   try:
#       soqlist = vendor_dict[source] 
#   except KeyError:
#       soqlist =[]
#       vendor_dict[source] = soqlist
#   soqObject = SOQ(orderplacedate_2,soq)
#   soqlist.append(soqObject)

#print(sku_soq_dict)


#filepath = output_dir+'summary.txt'
filepath = '/Users/1022177/Desktop'+'/summary.txt'
text_file = open(filepath, 'w')
for key,value in buyguide_dict.items():
    print(key)
    startdatelist = []
    enddatelist = []
    soqtotallist =[]
    splitpercentagelist =[]
    vendorlist = []
    ranklist = []
    volumelist =[]
    for key1, value1 in value.items():
        #print(key1)
        
        for row in value1 :
            #print(row)
            startdate = row['START_DATE']
            enddate =  row['END_DATE']
            #print(startdate)
            #print(type(startdate))

            #print(enddate)
            #print(type(enddate))
            vendordata = sku_soq_dict[key]
            soqlist = vendordata[key1]
            split_percentage = row['SPLIT_PERCENTAGE']
            #print(split_percentage)
            soqtotal =0
            for soqObject in soqlist:
                orderplacedate_3 = soqObject.opd
                #element = datetime.datetime.strptime(orderplacedate, "%Y-%m-%d %H:%M:%S")
                #print(type(element))
                #timestamp = datetime.datetime.timestamp(element)
                #timestamp = time.mktime(datetime.datetime.strptime(orderplacedate, "%Y-%m-%d %H:%M:%S").timetuple())
                timestamp = convert_timestamp(pd.Timestamp(orderplacedate_3))
                #print(type(timestamp))
                #print(timestamp)
                starttimestamp = convert_timestamp(startdate)
                endtimestamp = convert_timestamp(enddate)
            
                if starttimestamp <= timestamp < endtimestamp:
                   print("in here")
                   soqtotal = soqtotal + soqObject.soq
                #if startdate <= timestamp < enddate:
                #    print("in here")
                #    soqtotal = soqtotal + soqObject.soq
            #print(soqtotal)
            rank = row['PRIORITY']
            volume = row['VOLUME_EXACT']
            ranklist.append(rank)
            volumelist.append(volume)
            startdatelist.append(startdate)
            enddatelist.append(enddate)
            soqtotallist.append(soqtotal)
            splitpercentagelist.append(split_percentage)
            vendorlist.append(key1)
    print(soqtotallist)
    data = {'startdate':startdatelist,'enddate':enddatelist,'vendor':vendorlist,'rank':ranklist,'volume':volumelist,'soq':soqtotallist,'buyguide percentage':splitpercentagelist}
    df = pd.DataFrame(data)
    print(df)
    totaldict = {}
    for index, row in df.iterrows():
        startdate = row['startdate']
        try:
            totalvalue = totaldict[startdate]
            totalvalue = totalvalue + row['soq']
            totaldict[startdate] = totalvalue
        except KeyError:
            totaldict[startdate]= row['soq']

    actual_percentage = []
    for index, row in df.iterrows():
        startdate = row['startdate']
        soq = row['soq']
        total = totaldict[startdate]
        percentage = 0
        if (total > 0):
            percentage = (soq/total)*100
        actual_percentage.append(percentage)

    df['actual_percentage'] = actual_percentage
    keydata = key.split('@')
    item = int(keydata[0])
    dest = int(keydata[1])
    

    master_parquet = 'dcro-input/'+order_place_date_folder+'/'+timestamp_folder+'/batch0/snd-data/masterdata.parquet'
    masterdata = getDataFrameFromABS(block_blob_service,container_name,master_parquet)
    filtered_masterdata = masterdata[(masterdata['PP_P_ID'] ==item)&(masterdata['PP_L_ID_TARGET'] == dest)]
    item_code = filtered_masterdata['P_EXTERNAL_CODE'].iloc[0]
    #print(item_code)
    dest_code = filtered_masterdata['L_EXTERNAL_CODE_TARGET'].iloc[0]
    #print(dest_code)

    demand_parquet = 'dcro-input/'+order_place_date_folder+'/'+timestamp_folder+'/batch0/lsf-output-data/aggregated_order_projections.parquet'

    demanddata = getDataFrameFromABS(block_blob_service,container_name,demand_parquet)
    filtered_demand = demanddata[(demanddata['SUPPLIER_EXTERNAL_CODE']==dest_code)&(demanddata['P_EXTERNAL_CODE']==item_code)]
    #print(filtered_demand)

    ss_parquet = 'dcro-input/'+order_place_date_folder+'/'+timestamp_folder+'/batch0/safety_stock_output/safetystock.parquet'
    ssdata = getDataFrameFromABS(block_blob_service,container_name,ss_parquet)
    filtered_ss = ssdata[(ssdata['SUPPLIER_EXTERNAL_CODE']==dest_code)&(ssdata['P_EXTERNAL_CODE']==item_code)]
    #print(filtered_ss)

    schedrcpt_parquet = 'dcro-input/'+order_place_date_folder+'/'+timestamp_folder+'/batch0/snd-data/schedrcpts.parquet'
    schedrcptdata = getDataFrameFromABS(block_blob_service,container_name,schedrcpt_parquet)
    filtered_schedrcpt = schedrcptdata[(schedrcptdata['H_EDLC_L_ID_TARGET']==dest)&(schedrcptdata['H_EDLC_P_ID']==item)]
    #print(filtered_schedrcpt)


    timedomain =[]
    for i in range(0,21):
        #date = opdtimestamp + datetime.timedelta(days=i).total_seconds()
        date = element + datetime.timedelta(days=i)
        #print(date)
        date_1 = pd.Timestamp(date, unit='s')
        item_1 = convert_timestamp(date_1)
        #print(item_1)
        timedomain.append(item_1)
    #print(timedomain)
    demandlist = []
    sslist =[]
    schedrcptslist =[]
    soq1_list = []

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
            if((item_1 <= period_upto_stamp) &  (item_1 >=period_from_stamp)):
               demand= demand+ row['AGGREGATED_ORDER_PROJECTION_MEAN']
            #if((item_1 <= period_upto) &  (item_1 >=period_from)):
            #   demand= demand+ row['AGGREGATED_ORDER_PROJECTION_MEAN']
        demandlist.append(demand)

        ss = 0;
        for index,row in filtered_ss.iterrows():
            period_from = row['EFFECTIVE_FROM']
            period_upto = row['EFFECTIVE_UPTO']
            period_from_stamp = convert_timestamp(period_from)
            period_upto_stamp = convert_timestamp(period_upto)
            
            if((item_1 <= period_upto_stamp) &  (item_1 >=period_from_stamp)):
               ss= ss+ row['SAFETY_STOCK_PER_DAY']
            #if((item_1 <= period_upto) &  (item_1 >=period_from)):
            #   ss= ss+ row['SAFETY_STOCK_PER_DAY']
        sslist.append(ss)

        schedrcpt = 0;
        #print("date"+str(item_1))
        for index,row in filtered_schedrcpt.iterrows():
            delivery_date = row['H_EDLC_EXPECTED_DELIVERY_DATE']
            deliyery_date_string = str(delivery_date)
            delivery_date_list = deliyery_date_string.split(" ")
            date_string = str(item_1)
            date_string_list = date_string.split(" ")
            if(date_string_list[0] == delivery_date_list[0]):
                schedrcpt= schedrcpt+   row['H_EDLC_QUANTITY']
        schedrcptslist.append(schedrcpt)

        soq_1 = 0;
        for index,row in filtered_orderskudata.iterrows():
            arrivdatestring = str(row['arrivdate'])
            #print(arrivdatestring)
            list_1 = arrivdatestring.split(" ")
            arrivdate = list_1[0]
            #arrivdate = row['arrivdate']
            #print(arrivdate)
            item_1_string = str(item_1)
            list_2 = item_1_string.split(" ")
            #print(list_2[0])
            #print(arrivdate)
            source_1 = row['source']
            #print(row['soq'])
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

    data_1 = {'date':timedomain,'demand':demandlist,'ss':sslist,'schedrcpt':schedrcptslist,'soq':soq1_list}
    #print(vendor_soq_dict)
    for key_1,value in vendor_soq_dict.items():
        #print("source : "+key_1)
        vendor_soq_list=[]
        for item in timedomain:
            #print(type(item))
            item_3_string = str(item)
            list_3 = item_3_string.split(" ")
            item_string = list_3[0]
            #print(item_string)
            try:
                soq_5 = value[item_string]
                vendor_soq_list.append(soq_5)
            except KeyError:
                vendor_soq_list.append(0)
        data_1[key_1] = vendor_soq_list



    #print(data_1)


    
    df_1 = pd.DataFrame(data_1)
    curr_projoh =0
    projoh_list =[]
    projavail_list =[]
    ignored_demand_list =[]
    for index, row in df_1.iterrows():
        curr_projoh=curr_projoh+row['schedrcpt']+row['soq']-row['demand']
        ignored_demand = 0
        if(curr_projoh < 0):
            ignored_demand = curr_projoh * -1
            curr_projoh = 0
        projoh_list.append(curr_projoh)
        projavail_list.append(curr_projoh-row['ss'])
        ignored_demand_list.append(ignored_demand)

    #print(projoh_list)
    df_1['projoh'] = projoh_list
    df_1['projavail'] = projavail_list
    df_1['ignored_demand'] = ignored_demand_list

    #print(df_1)
    #print(demandlist)
    #print(sslist)
    #print(schedrcptslist)
    #print(soq1_list)

    df.sort_values(by=['startdate','enddate','vendor'], inplace=True)
    text_file.write(key)
    text_file.write('\n')
    text_file.write(df.to_string())
    text_file.write('\n')
    text_file.write(df_1.to_string())

    curr_startdate = None
    curr_enddate = None
    total_volume = 0
    total_soq = 0
    startdatelist_1 = []
    enddatelist_1 = []
    total_volume_list = []
    total_soq_list = []
    for index, row in df.iterrows():
        #print()
        if(curr_startdate == row['startdate']):
            total_volume = total_volume +row['volume']
            total_soq = total_soq + row['soq']
        else:
            if(curr_startdate != None):
                startdatelist_1.append(curr_startdate)
                enddatelist_1.append(curr_enddate)
                total_volume_list.append(total_volume)
                total_soq_list.append(total_soq)
            total_volume = row['volume']
            total_soq = row['soq']
        curr_startdate = row['startdate']
        curr_enddate = row['enddate']
    startdatelist_1.append(curr_startdate)
    enddatelist_1.append(curr_enddate)
    total_volume_list.append(total_volume)
    total_soq_list.append(total_soq)

    data_2 = {'startdate':startdatelist_1,'enddate':enddatelist_1,'volume':total_volume_list,'ordered_total':total_soq_list}
    df_2 = pd.DataFrame(data_2)

    aop_and_ss_list =[]
    net_ss_list =[]
    prev_net_ss = 0
    for index, row in df_2.iterrows():
        startdate = row['startdate']
        enddate = row['enddate']
        total_demand = 0
        end_ss = 0
        for i,r in df_1.iterrows():
            date =  r['date']
            demand = r['demand']
            #print(type(date))
            #print(type(startdate))
            #timestamp = pd.Timestamp(date).tz_localize(tz='US/Eastern')
            starttimestamp = convert_timestamp(startdate)
            endtimestamp = convert_timestamp(enddate)
            
            if(starttimestamp <=date <=endtimestamp):
                total_demand = total_demand + demand
            #if(startdate <=date <=enddate):
            #    total_demand = total_demand + demand
            #print("date "+str(date))
            #print("enddate "+str(enddate))
            string_date = str(date)
            string_enddate = str(enddate)
            string_date_list = string_date.split(" ")
            string_end_date_list = string_enddate.split(" ")
            if(date<endtimestamp):
                print(date)
                print(enddate)
                end_ss=r['ss']
        net_ss = end_ss - prev_net_ss
        prev_net_ss = end_ss
        aop_and_ss = total_demand + net_ss
        aop_and_ss_list.append(aop_and_ss)
        net_ss_list.append(net_ss)


    df_2['aop_and_ss'] = aop_and_ss_list
    df_2['net_ss'] = net_ss_list

    text_file.write('\n')
    text_file.write(df_2.to_string())

    #print(df)
    #print(df_2)
text_file.close()
