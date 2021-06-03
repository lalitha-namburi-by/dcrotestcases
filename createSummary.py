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

class SOQ:

	def __init__(self,opd,soq):
		self.opd = opd
		self.soq= soq

#testcase = sys.argv[1]
current_dir = os.getcwd()

testcase = sys.argv[1]
unformattedorderplacedate = sys.argv[2]
orderplacedate = unformattedorderplacedate.replace("-", "/")

element = datetime.datetime.strptime(orderplacedate,"%Y/%m/%d")

print(element)
  
opdtimestamp = datetime.datetime.timestamp(element)

print(opdtimestamp)

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
	
#print(json.dumps(buyguide_dict, indent=4, sort_keys=True))
output_dir = current_dir+'/dcroengineoutput/'+testcase+'/'
orderheaderfilelist = [output_dir+'OrderHeader.parquet',output_dir+'lrr_proj_orderheader.parquet',output_dir+'LongTermProjections/lrr_proj_orderheader.parquet']
orderheader_df_list = [pd.read_parquet(file) for file in orderheaderfilelist]

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

ordersku_df_list = [pd.read_parquet(file) for file in orderskufilelist]
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

startdatelist = []
enddatelist = []

soqtotallist =[]
splitpercentagelist =[]
vendorlist = []
ranklist = []
volumelist =[]
filepath = output_dir+'summary.txt'
text_file = open(filepath, 'w')
for key,value in buyguide_dict.items():
	#print(key)
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
				timestamp = pd.Timestamp(orderplacedate_3).tz_localize(tz='US/Eastern')
				#print(type(timestamp))
				#print(timestamp)
			
				if startdate <= timestamp < enddate:
				   soqtotal = soqtotal + soqObject.soq
			#print(soqtotal)
			rank = row['PRIORITY']
			volume = row['VOLUME']
			ranklist.append(rank)
			volumelist.append(volume)
			startdatelist.append(startdate)
			enddatelist.append(enddate)
			soqtotallist.append(soqtotal)
			splitpercentagelist.append(split_percentage)
			vendorlist.append(key1)

	data = {'startdate':startdatelist,'enddate':enddatelist,'vendor':vendorlist,'rank':ranklist,'volume':volumelist,'soq':soqtotallist,'buyguide percentage':splitpercentagelist}
	df = pd.DataFrame(data)
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
		percentage = (soq/total)*100
		actual_percentage.append(percentage)

	df['actual_percentage'] = actual_percentage
	keydata = key.split('@')
	item = int(keydata[0])
	dest = int(keydata[1])
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

	ss_parquet = current_dir+'/dcroengineinput/'+testcase+'/safetystock.parquet'
	ssdata = pd.read_parquet(ss_parquet)
	filtered_ss = ssdata[(ssdata['SUPPLIER_EXTERNAL_CODE']==dest_code)&(ssdata['P_EXTERNAL_CODE']==item_code)]
	#print(filtered_ss)

	schedrcpt_parquet = current_dir+'/dcroengineinput/'+testcase+'/schedrcpts.parquet'
	schedrcptdata = pd.read_parquet(schedrcpt_parquet)
	filtered_schedrcpt = schedrcptdata[(schedrcptdata['H_EDLC_L_ID_TARGET']==dest_code)&(schedrcptdata['H_EDLC_P_ID']==item_code)]
	#print(filtered_schedrcpt)


	timedomain =[]
	for i in range(0,21):
		#date = opdtimestamp + datetime.timedelta(days=i).total_seconds()
		date = element + datetime.timedelta(days=i)
		#print(date)
		date_1 = pd.Timestamp(date, unit='s')
		item_1 = date_1.tz_localize('US/Eastern')
		print(item_1)
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
			if((item_1 <= period_upto) &  (item_1 >=period_from)):
				demand= demand+	row['AGGREGATED_ORDER_PROJECTION_MEAN']
		demandlist.append(demand)

		ss = 0;
		for index,row in filtered_ss.iterrows():
			period_from = row['EFFECTIVE_FROM']
			period_upto = row['EFFECTIVE_UPTO']
			if((item_1 <= period_upto) &  (item_1 >=period_from)):
				ss= ss+	row['SAFETY_STOCK_PER_DAY']
		sslist.append(ss)

		schedrcpt = 0;
		for index,row in filtered_schedrcpt.iterrows():
			delivery_date = row['H_EDLC_EXPECTED_DELIVERY_DATE']
			if(item_1 == delivery_date):
				schedrcpt= schedrcpt+	row['H_EDLC_QUANTITY']
		schedrcptslist.append(schedrcpt)

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
			if(startdate <=date <=enddate):
				total_demand = total_demand + demand
			if(date == enddate):
				end_ss = r['ss']
		aop_and_ss = total_demand + end_ss
		aop_and_ss_list.append(aop_and_ss)
		net_ss_list.append(end_ss)


	df_2['aop_and_ss'] = aop_and_ss_list
	df_2['net_ss'] = net_ss_list

	text_file.write('\n')
	text_file.write(df_2.to_string())

	#print(df)
	#print(df_2)
text_file.close()
