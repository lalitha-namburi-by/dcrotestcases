#!/usr/bin/env python3
import pyarrow.parquet as pq
import pandas as pd
import json
import time
import datetime
import sys
import os
import pytz

def write_parquet_to_textfile(filepath,data):
	text_file = open(filepath, 'w')
	text_file.write(data.to_string())
	text_file.close()

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

class SOQ:

	def __init__(self,opd,soq):
		self.opd = opd
		self.soq= soq

#testcase = sys.argv[1]
current_dir = os.getcwd()

testcase = sys.argv[1]
print(testcase)
unformattedorderplacedate = sys.argv[2]
orderplacedate = unformattedorderplacedate.replace("-", "/")

element = datetime.datetime.strptime(orderplacedate,"%Y/%m/%d")

#print(element)
  
opdtimestamp = datetime.datetime.timestamp(element)

#print(opdtimestamp)

buy_guide_parquet = current_dir+'/dcroengineinput/'+testcase+'/buy_guide_data.parquet'
buyGuideParquet = pd.read_parquet(buy_guide_parquet)
#print(buyGuideParquet)

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
	
#print(buyguide_dict)
output_dir = current_dir+'/dcroengineoutput/'+testcase+'/'
orderheaderfilelist = [output_dir+'OrderHeader.parquet',output_dir+'lrr_proj_orderheader.parquet',output_dir+'LongTermProjections/lrr_proj_orderheader.parquet']
existingorderheaderfilelist = []
for orderheaderfile in orderheaderfilelist:
	if(os.path.exists(orderheaderfile)):
		existingorderheaderfilelist.append(orderheaderfile)

orderheader_df_list = [pd.read_parquet(file) for file in existingorderheaderfilelist]

orderheaderdata = pd.concat(orderheader_df_list)
#print(orderheaderdata)
#orderheader_parquet = '/Users/1022177/dcrotestcases/dcroengineoutput/50101/OrderHeader.parquet'

#orderheaderdata = pd.read_parquet(orderheader_parquet)

orderheader_dict = {}
for index,row in orderheaderdata.iterrows():
	orderid = row['orderid']
	#orderplacedate_1 = row['orderplacedate']
	orderplacedate_1 = row['arrivdate']
	orderheader_dict[orderid] = orderplacedate_1 

#ordersku_parquet = '/Users/1022177/dcrotestcases/dcroengineoutput/50101/OrderSku.parquet'
orderskufilelist = [output_dir+'OrderSku.parquet',output_dir+'lrr_proj_ordersku.parquet',output_dir+'LongTermProjections/lrr_proj_ordersku.parquet']

existingorderskufilelist = []
for orderskufile in orderskufilelist:
	if(os.path.exists(orderskufile)):
		existingorderskufilelist.append(orderskufile)
ordersku_df_list = [pd.read_parquet(file) for file in existingorderskufilelist]
orderskudata = pd.concat(ordersku_df_list)

#orderskudata = pd.read_parquet(ordersku_parquet)

sku_soq_dict = {}

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

#print(sku_soq_dict)


filepath = output_dir+'summary.txt'
text_file = open(filepath, 'w')

schedrcptsupplierdata = pd.DataFrame()
manualordersdata = pd.DataFrame()

schedrcpt_supplier_parquet = current_dir+'/dcroengineinput/'+testcase+'/schedrcpts_supplier_data.parquet'
if(os.path.exists(schedrcpt_supplier_parquet)):
	schedrcptsupplierdata = pd.read_parquet(schedrcpt_supplier_parquet)

manual_orders_parquet = current_dir+'/dcroengineinput/'+testcase+'/manual_orders_data.parquet'
if(os.path.exists(manual_orders_parquet)):
	manualordersdata = pd.read_parquet(manual_orders_parquet)
	#print(manualordersdata)

for key,value in buyguide_dict.items():
	print(key)
	keydata = key.split('@')
	item = int(keydata[0])
	dest = int(keydata[1])
	
	startdatelist = []
	enddatelist = []
	soqtotallist =[]
	srstotallist =[]
	msototallist =[]
	splitpercentagelist =[]
	vendorlist = []
	ranklist = []
	volumelist =[]
	for key1, value1 in value.items():
		#print(key1)
		filtered_schedrcpt_supplier = pd.DataFrame()
		filtered_manual_orders = pd.DataFrame()
		
		source = int(key1)
		if not schedrcptsupplierdata.empty:
			#print(schedrcptsupplierdata)
			filtered_schedrcpt_supplier = schedrcptsupplierdata[(schedrcptsupplierdata['H_EDLC_L_ID_TARGET']==dest)&(schedrcptsupplierdata['H_EDLC_P_ID']==item)&(schedrcptsupplierdata['H_EDLC_L_ID_SOURCE']==source)]
            
		if not manualordersdata.empty:
			filtered_manual_orders = manualordersdata[(manualordersdata['PP_L_ID_TARGET']==dest)&(manualordersdata['PP_P_ID']==item)&(manualordersdata['PP_L_ID_SOURCE']==source)]
			#print(filtered_manual_orders)
		
		for row in value1 :
			#print(row)
			startdate = row['START_DATE']
			enddate =  row['END_DATE']
			
			starttimestamp = convert_timestamp(startdate)
			endtimestamp = convert_timestamp(enddate)
			#print(startdate)
			#print(type(startdate))
			srstotal =0
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
				#timestamp = pd.Timestamp(orderplacedate_3).tz_localize(tz='US/Eastern')
				timestamp = convert_timestamp(pd.Timestamp(orderplacedate_3))
				#print(type(timestamp))
				#print(timestamp)
				#starttimestamp = startdate.tz_localize(tz='US/Eastern')
				#endtimestamp = enddate.tz_localize(tz='US/Eastern')
			
				if starttimestamp <= timestamp < endtimestamp:
					#print("in here")
					soqtotal = soqtotal + soqObject.soq
			#print(soqtotal)
			rank = row['PRIORITY']
			volume = row['VOLUME_EXACT']
			ranklist.append(rank)
			volumelist.append(volume)
			startdatelist.append(startdate)
			enddatelist.append(enddate)
			soqtotallist.append(soqtotal)
			srstotallist.append(srstotal)
			msototallist.append(msototal)
			splitpercentagelist.append(split_percentage)
			vendorlist.append(key1)
	#print(soqtotallist)
	data = {'startdate':startdatelist,'enddate':enddatelist,'vendor':vendorlist,'rank':ranklist,'volume':volumelist,'soq':soqtotallist,'srs':srstotallist,'mso':msototallist,'buyguide percentage':splitpercentagelist}
	df = pd.DataFrame(data)
	#print(df)
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
	for index, row in df.iterrows():
		startdate = row['startdate']
		soq = row['soq'] + row['srs']+ row['mso']
		total = totaldict[startdate]
		percentage = 0
		if (total > 0):
			percentage = (soq/total)*100
		actual_percentage.append(percentage)

	df['actual_percentage'] = actual_percentage
	master_parquet = current_dir+'/dcroengineinput/'+testcase+'/masterdata.parquet'
	masterdata = pd.read_parquet(master_parquet)
	filtered_masterdata = masterdata[(masterdata['PP_P_ID'] ==item)&(masterdata['PP_L_ID_TARGET'] == dest)]
	item_code = filtered_masterdata['P_EXTERNAL_CODE'].iloc[0]
	#print(item_code)
	dest_code = filtered_masterdata['L_EXTERNAL_CODE_TARGET'].iloc[0]
	#print(dest_code)

	demand_parquet = current_dir+'/dcroengineinput/'+testcase+'/aggregated_order_projections.parquet'
	demanddata = pd.read_parquet(demand_parquet)
	filtered_demand = demanddata[(demanddata['SUPPLIER_EXTERNAL_CODE']==dest_code)&(demanddata['P_EXTERNAL_CODE']==item_code)]
	#print(filtered_demand)
	filtered_demand=filtered_demand.drop_duplicates(subset=['AGGREGATED_ORDER_PROJECTION_PERIOD_FROM','AGGREGATED_ORDER_PROJECTION_PERIOD_UPTO'])

	ss_parquet = current_dir+'/dcroengineinput/'+testcase+'/safetystock.parquet'
	ssdata = pd.read_parquet(ss_parquet)
	filtered_ss = ssdata[(ssdata['SUPPLIER_EXTERNAL_CODE']==dest_code)&(ssdata['P_EXTERNAL_CODE']==item_code)]
	filtered_ss=filtered_ss.drop_duplicates(subset=['EFFECTIVE_FROM','EFFECTIVE_UPTO'])
	#print(filtered_ss)

	schedrcpt_parquet = current_dir+'/dcroengineinput/'+testcase+'/schedrcpts.parquet'
	schedrcptdata = pd.read_parquet(schedrcpt_parquet)
	print(schedrcptdata)
	print("masterdata location : "+dest_code)
	print(dest)
	filtered_schedrcpt = schedrcptdata[(schedrcptdata['H_EDLC_L_ID_TARGET']==dest)&(schedrcptdata['H_EDLC_P_ID']==item)]
	filtered_schedrcpt=filtered_schedrcpt.drop_duplicates(subset=['H_EDLC_EXPECTED_DELIVERY_DATE'])

	filtered_manual_orders = pd.DataFrame()
	if not manualordersdata.empty:
		filtered_manual_orders = manualordersdata[(manualordersdata['PP_L_ID_TARGET']==dest)&(manualordersdata['PP_P_ID']==item)]
	#print(filtered_schedrcpt)
	filtered_manual_orders=filtered_manual_orders.drop_duplicates(subset=['DELIVERY_DATE'])


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
	msorderlist =[]
	soq1_list = []

	vendor_soq_dict = {}

	filtered_orderskudata = orderskudata[(orderskudata['item']==str(item))&(orderskudata['dest']==str(dest))]
	for item in timedomain:
		item_1 = item
		demand = 0;
		for index,row in filtered_demand.iterrows():
			period_from = row['AGGREGATED_ORDER_PROJECTION_PERIOD_FROM']
			period_upto = row['AGGREGATED_ORDER_PROJECTION_PERIOD_UPTO']
			#period_from_stamp = period_from.tz_localize('US/Eastern')
			#period_upto_stamp = period_upto.tz_localize('US/Eastern')
			period_from_stamp = convert_timestamp(period_from)
			period_upto_stamp = convert_timestamp(period_upto)
			if((item_1 <= period_upto_stamp) &  (item_1 >=period_from_stamp)):
				demand= demand+ row['AGGREGATED_ORDER_PROJECTION_MEAN']
		demandlist.append(demand)

		ss = 0;
		for index,row in filtered_ss.iterrows():
			period_from = row['EFFECTIVE_FROM']
			period_upto = row['EFFECTIVE_UPTO']
			#period_from_stamp = period_from.tz_localize('US/Eastern')
			#period_upto_stamp = period_upto.tz_localize('US/Eastern')
			period_from_stamp = convert_timestamp(period_from)
			period_upto_stamp = convert_timestamp(period_upto)
			
			if((item_1 <= period_upto_stamp) &  (item_1 >=period_from_stamp)):
				ss= ss+ row['SAFETY_STOCK_PER_DAY']
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
		
		soq_1 = 0;
		for index,row in filtered_orderskudata.iterrows():
			arrivdatestring = row['arrivdate']
			#print(arrivdatestring)
			list_1 = arrivdatestring.split(" ")
			arrivdate = list_1[0]
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

	data_1 = {'date':timedomain,'demand':demandlist,'ss':sslist,'schedrcpt':schedrcptslist,'mso':msorderlist,'soq':soq1_list}
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
		curr_projoh=curr_projoh+row['schedrcpt']+row['mso']+row['soq']-row['demand']
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
	total_incoming = 0
	startdatelist_1 = []
	enddatelist_1 = []
	total_volume_list = []
	total_soq_list = []
	total_incoming_list = []
	for index, row in df.iterrows():
		#print()
		if(curr_startdate == row['startdate']):
			total_volume = total_volume +row['volume']
			total_soq = total_soq + row['soq']
			total_incoming = total_incoming + row['srs'] + row['mso']
		else:
			if(curr_startdate != None):
				startdatelist_1.append(curr_startdate)
				enddatelist_1.append(curr_enddate)
				total_volume_list.append(total_volume)
				total_soq_list.append(total_soq)
				total_incoming_list.append(total_incoming)
			total_volume = row['volume']
			total_soq = row['soq']
			total_incoming = row['srs'] + row['mso']
		curr_startdate = row['startdate']
		curr_enddate = row['enddate']
	startdatelist_1.append(curr_startdate)
	enddatelist_1.append(curr_enddate)
	total_volume_list.append(total_volume)
	total_soq_list.append(total_soq)
	total_incoming_list.append(total_incoming)

	data_2 = {'startdate':startdatelist_1,'enddate':enddatelist_1,'volume':total_volume_list,'ordered_total':total_soq_list,'srs_and_mso':total_incoming_list}
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
			#starttimestamp = startdate.tz_localize(tz='US/Eastern')
			#endtimestamp = enddate.tz_localize(tz='US/Eastern')
			starttimestamp = convert_timestamp(startdate)
			endtimestamp = convert_timestamp(enddate)
			
			if(starttimestamp <=date <endtimestamp):
				total_demand = total_demand + demand
			#print("date "+str(date))
			#print("enddate "+str(enddate))
			#string_date = str(date)
			#string_enddate = str(enddate)
			#string_date_list = string_date.split(" ")
			#string_end_date_list = string_enddate.split(" ")
			#if(string_date_list[0] == string_end_date_list[0]):
			#	print(string_date_list[0])
			#	print(string_end_date_list[0])
			#	end_ss = r['ss']
			if(date<enddate):
				#print(date)
				#print(enddate)
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
	text_file.write('\n')

	#print(df)
	#print(df_2)
text_file.close()
