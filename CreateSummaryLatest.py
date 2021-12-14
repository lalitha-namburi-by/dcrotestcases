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



class Configuration :
	def __init__(self,testcase,opd):
		self.testcase = testcase
		self.opd = opd

		self.range = 21

		# file_system can be of type LFS ,ABS, DB
		self.input_data_file_system_type = "LFS"
		self.output_data_file_system_type = "LFS"

		#DB details
		self.db_user_name = 'stratosphere@o3kztgfjajz5xvuct4g8sqib'
		self.db_password = 'IK0rkkafFiLgsInixkQ1'
		self.db_hostname = 'localhost'
		self.db_port = 5111
		self.db_name = 'o4vv6gxfci6fc02c4rz5vclp'
		self.create_date = ''


		#ABS details
		self.account_name = 'bvzdtlbxhptci'
		self.sas_token = 'se=2021-11-30T00%3A00%3A00Z&sp=rwdl&sv=2019-02-02&sr=c&sig=JWxcQoJBRaMpxNpBH5/GhF9lXfa5MpDkNKlXPddTHFA%3D'
		self.container_name = 'lrr-dev-aggregated-order-projections'
		self.timestamp_folder='1633972401'

		#LFS details
		self.current_dir = os.getcwd()

		#Summary File Path
		self.summaryFilePath = self.current_dir+'/dcroengineoutput/'+self.testcase+'/'+'summary.txt'

		self.summaryBuilderType = "TEXT"



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

class LocalFileSystem:

	def __init__(self,local_directory):
		self.local_directory = local_directory

	def getDataFrame(self,file_name,filter,columns):
		file_path = self.getFilePath(file_name)
		#df = pd.read_parquet(file_path)
		df = pd.DataFrame()
		try:
			df = pq.read_table(source=file_path,filters=filter).to_pandas()
		except FileNotFoundError:
			print("FileNotFoundError : "+file_path)
		return df

	def getFilePath(self,file_name):
		return self.local_directory+"/"+file_name+".parquet"

	def close(self):
		print("Nothing in closing block")


class ABSFileSystem:
	def __init__(self,block_blob_service,container_name,order_place_date_folder,timestamp_folder):
		self.block_blob_service = block_blob_service
		self.container_name=container_name
		self.abs_directory = "dcro-input/"+order_place_date_folder+"/"+timestamp_folder+"/"

		proxy = 'http://bywww.blue-yonder.org:8888'

		os.environ['http_proxy'] = proxy
		os.environ['HTTP_PROXY'] = proxy
		os.environ['https_proxy'] = proxy
		os.environ['HTTPS_PROXY'] = proxy

	def getDataFrame(self,file_name,filter,columns):
		file_path = self.getFilePath(file_name)
		byte_stream = BytesIO()
		try:
			self.block_blob_service.get_blob_to_stream(container_name=self.container_name, blob_name=file_path, stream=byte_stream)
			df = pq.read_table(source=byte_stream,filters=filter).to_pandas()
		finally:
			byte_stream.close()
		return df

	def getFilePath(self,file_name):
		return self.abs_directory+self.getParentFolder(file_name)+"/"+file_name+".parquet"

	def getParentFolder(self,file_name):
		if file_name == "aggregated_order_projections":
			return "lsf-output-data"
		elif file_name == "safetystock":
			return "safety_stock_output"
		elif file_name == "unapproved_orders_data" | file_name =="manual_orders_data":
			return "dcro-unapproved-orders"
		elif file_name == "historical_orders_data":
			return "dcro-historical-orders"
		elif file_name == "latest_short_term_order_projections":
			return "order-projections-data"
		elif file_name == "buy_guide_data" | file_name == "schedrcpts_supplier_data":
			return "dcro-supplier-splits"
		else :
			return "snd-data"


	def close(self):
		print("Nothing in closing block")



class DBFileSystem:
	def __init__(self,db_user_name,db_password,db_hostname,db_port,db_name,create_date):
		self.create_date = create_date
		try:
			self.connection = psycopg2.connect(user = db_user_name,
								  password = db_password,
								  host = db_hostname,
								  port = db_port,
								  database = db_name)
		except (Exception, psycopg2.Error) as error :
			print ("Error while connecting to PostgreSQL", error)

	def getDataFrame(self,file_name,filter,columns):
		sql_query = self.buildQuery(file_name,filter)
		if(sql_query == ""):
			return pd.DataFrame()
		df = pd.read_sql(sql_query,self.connection)
		return df

	def buildQuery(self,file_name,filter):
		sql_query = self.getQuery(file_name)
		sql_query = self.appendFilterInQuery(sql_query,filter)
		return sql_query

	def appendFilterInQuery(self,sql_query,filter):
		return sql_query

	def getQuery(self,file_name):
		if file_name == 'OrderHeader' | file_name == 'lrr_proj_orderheader':
			return "select orderid,arrivdate from "+file_name+" where "+file_name+".createdate = "+ self.create_date
		elif file_name == 'OrderSku' | file_name == 'lrr_proj_ordersku':
			header_table = ""
			if file_name == 'OrderSku':
				header_table = 'OrderHeader'
			else:
				header_table = 'lrr_proj_orderheader'
			return "select os.item,os.dest,os.source,os.soq,os.orderid,os.arrivdate, oh.createdate from "+file_name+" as os,"+header_table+" as oh where oh.orderid=os.orderid and oh.createdate = "+self.create_date
		else:
			return ""


	def close(self):
		if self.connection:
			self.connection.close()
			print("PostgreSQL connection is closed")


class FileSystemFactory:
	def __init__(self,config):
		self.config = config

	def getFileSystem(self,file_system):
		file_system_type = self.getFileSystemType(file_system)

		if file_system_type == "LFS":
			return LocalFileSystem(self.getDirectory(file_system))
		elif file_system_type == "ABS":
			block_blob_service = BlockBlobService(account_name=account_name, sas_token=sas_token)
			return ABSFileSystem(block_blob_service,self.config.container_name,self.config.order_place_date_folder,self.config.timestamp_folder)
		else :
			return DBFileSystem(self.config.db_user_name,self.config.db_password,self.config.db_hostname,self.config.db_port,self.config.db_name,self.config.create_date)

	def getDirectory(self,file_system):
		if file_system == "input":
			return self.config.current_dir+'/dcroengineinput/'+self.config.testcase
		else :
			return self.config.current_dir+'/dcroengineoutput/'+self.config.testcase

	def getFileSystemType(self,file_system):
		if file_system == "input":
			return self.config.input_data_file_system_type
		else:
			return self.config.output_data_file_system_type



def buildFilter(filter_column_names,filter_column_values):
	predicate_list = []
	predicate_index = 0
	for filter_column_name in filter_column_names:
		if filter_column_values[predicate_index] is None:
			return None
		predicate =(filter_column_names[predicate_index],'=',filter_column_values[predicate_index])
		predicate_list.append(predicate)
		predicate_index +=1

	return predicate_list

class SummaryBuilderFactory:
	def __init__(self,config):
		self.config = config

	def getSummaryBuilder(self):
		if self.config.summaryBuilderType == "String":
			return StringSummaryBuilder()
		else:
			return TextFileSummaryBuilder(self.config.summaryFilePath)

class StringSummaryBuilder:
	def __init__(self):
		self.summary = ""

	def appendToSummary(self,str):
		self.summary += str
		self.summary += '\n'
		self.summary += '\n'

	def getSummary(self):
		return self.summary

class TextFileSummaryBuilder:
	def __init__(self,summaryFilePath):
		self.summaryFilePath = summaryFilePath
		self.summary_text_file = open(self.summaryFilePath, 'w')

	def appendToSummary(self,str):
		self.summary_text_file.write(str)
		self.summary_text_file.write('\n')

	def getSummary(self):
		self.summary_text_file.close()
		return self.summaryFilePath




current_dir = os.getcwd()

testcase = sys.argv[1]
print(testcase)
unformattedorderplacedate = sys.argv[2]

filtered_item = None
filtered_dest = None

if(len(sys.argv)>3):
	filtered_item = int(sys.argv[3])
	filtered_dest = int(sys.argv[4])


orderplacedate = unformattedorderplacedate.replace("-", "/")

opd = datetime.datetime.strptime(orderplacedate,"%Y/%m/%d")

config = Configuration(testcase,opd)

summaryBuilderFactory = SummaryBuilderFactory(config)
summaryBuilder = summaryBuilderFactory.getSummaryBuilder()

summaryBuilder.appendToSummary(config.testcase)

fs_factory = FileSystemFactory(config)



if config.input_data_file_system_type == "LFS":
	details_file_path = current_dir+'/dcroengineinput/'+config.testcase+'/details.txt'
	if(os.path.exists(details_file_path)):
		with open(details_file_path, "r") as input:
			for line in input:
				summaryBuilder.appendToSummary(line)




#######################################
#Read input Data

input_data_file_system = fs_factory.getFileSystem("input")

masterdata_file_name = "masterdata"

buyguidedata_file_name = "buy_guide_data"

schedrcpts_supplier_data_file_name = "schedrcpts_supplier_data"

manual_orders_data_file_name = "manual_orders_data"

additional_vendor_orders_file_name = "additional_vendor_orders"

aggregated_order_projections_file_name = "aggregated_order_projections"

safetystock_file_name = "safetystock"

schedrcpts_file_name = "schedrcpts"

#TODO: seggregate input reading on the basis of internal or external codes

#read master Data
masterdata_df = input_data_file_system.getDataFrame(masterdata_file_name,buildFilter(['PP_P_ID','PP_L_ID_TARGET'],[filtered_item,filtered_dest]),None)
#read buy guide data
buy_guide_data_df = input_data_file_system.getDataFrame(buyguidedata_file_name,buildFilter(['PP_P_ID','PP_L_ID_TARGET'],[filtered_item,filtered_dest]),None)
#read schedrcpts_supplier_data file
schedrcpts_supplier_data_df = input_data_file_system.getDataFrame(schedrcpts_supplier_data_file_name,buildFilter(['H_EDLC_P_ID','H_EDLC_L_ID_TARGET'],[filtered_item,filtered_dest]),None)

manual_orders_data_df = input_data_file_system.getDataFrame(manual_orders_data_file_name, buildFilter(['PP_P_ID','PP_L_ID_TARGET'],[filtered_item,filtered_dest]),None)

additional_vendor_orders_df = input_data_file_system.getDataFrame(additional_vendor_orders_file_name, buildFilter(['PP_P_ID','PP_L_ID_TARGET'],[filtered_item,filtered_dest]),None)

schedrcpts_df = input_data_file_system.getDataFrame(schedrcpts_file_name,buildFilter(['H_EDLC_P_ID','H_EDLC_L_ID_TARGET'],[filtered_item,filtered_dest]),None)

#TODO: build filter fith external codes and pass it in below ones
filtered_external_item_code = None
filtered_external_dest_code = None
if filtered_item != None :
	print(masterdata_df)
	filtered_masterdata = masterdata_df[(masterdata_df['PP_P_ID'] ==filtered_item)&(masterdata_df['PP_L_ID_TARGET'] == filtered_dest)]
	print(filtered_masterdata)
	filtered_external_item_code = filtered_masterdata['P_EXTERNAL_CODE'].iloc[0]
	filtered_external_dest_code = filtered_masterdata['L_EXTERNAL_CODE_TARGET'].iloc[0]



aggregated_order_projections_df = input_data_file_system.getDataFrame(aggregated_order_projections_file_name,buildFilter(['P_EXTERNAL_CODE','SUPPLIER_EXTERNAL_CODE'],[filtered_external_item_code,filtered_external_dest_code]),None)

safetystock_df = input_data_file_system.getDataFrame(safetystock_file_name,buildFilter(['P_EXTERNAL_CODE','SUPPLIER_EXTERNAL_CODE'],[filtered_external_item_code,filtered_external_dest_code]),None)

input_data_file_system.close()
########################################
#Read output Data

output_data_file_system = fs_factory.getFileSystem("output")

#Read Order Header Data
order_header_output_fliter = None
order_header_file_names_list = ['OrderHeader','lrr_proj_orderheader','LongTermProjections/lrr_proj_orderheader']
orderheader_df_list = [output_data_file_system.getDataFrame(file,order_header_output_fliter,None) for file in order_header_file_names_list]
order_header_df = pd.concat(orderheader_df_list)

#Read Order SKU data
order_sku_output_fliter = None
order_sku_file_names_list = ['OrderSku','lrr_proj_ordersku','LongTermProjections/lrr_proj_ordersku']
ordersku_df_list = [output_data_file_system.getDataFrame(file,order_sku_output_fliter,None) for file in order_sku_file_names_list]
order_sku_df = pd.concat(ordersku_df_list)



output_data_file_system.close()


#########################################
#PreProcessing Step

#Building buyguide dictionary
buyguide_dict = {}
for index,row in buy_guide_data_df.iterrows():
	key = str(row['PP_P_ID'])+'@'+str(row['PP_L_ID_TARGET'])

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

#create byguides for destSKUS that are not present in buyGuide
masterdata_buyguide_dict = {}
for index,row in masterdata_df.iterrows():
	item = str(row['PP_P_ID'])
	dest = str(row['PP_L_ID_TARGET'])

	key = item+'@'+dest

	#if buyguide_dict[key] is None:
	if key not in buyguide_dict.keys():
		try:
			source_list = masterdata_buyguide_dict[key]
		except KeyError:
			source_list = []
			masterdata_buyguide_dict[key] = source_list

		vendor = str(row['PP_L_ID_SOURCE'])
		source_list.append(vendor)


for key,value in masterdata_buyguide_dict.items():
	count = 0;
	#count Number of sources to calculate percentage
	for vendor in value:
		count = count+1;

	percentage = 100/count;

	#now create buyguides for these and add them in buyguide dict

	for vendor in value:

		datalist=[]
		for i in range(0,2):
			date = config.opd + datetime.timedelta(days=i)
			start_date = pd.Timestamp(date, unit='s')
			date1 = config.opd +  datetime.timedelta(days=i+1)
			end_date = pd.Timestamp(date1, unit='s')

			row = {'START_DATE':start_date,'END_DATE':end_date,'SPLIT_PERCENTAGE':percentage,'PRIORITY':-1,'VOLUME_EXACT':-1,'VOLUME_UPTO':-1}
			datalist.append(row)

		try:
			sku_dict = buyguide_dict[key]
		except KeyError:
			sku_dict = {}
			buyguide_dict[key] = sku_dict


		sku_dict[vendor] = datalist


orderheader_dict = {}
for index,row in order_header_df.iterrows():
	orderid = row['orderid']
	orderplacedate_1 = row['arrivdate']
	orderheader_dict[orderid] = orderplacedate_1

sku_soq_dict = {}
for index, row in order_sku_df.iterrows():
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


#########################################

#Main processing Logic
for key,value in buyguide_dict.items():
	print(key)
	keydata = key.split('@')
	item = int(keydata[0])
	dest = int(keydata[1])

	filtered_masterdata = masterdata_df[(masterdata_df['PP_P_ID'] ==item)&(masterdata_df['PP_L_ID_TARGET'] == dest)]
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
		if not schedrcpts_supplier_data_df.empty:
			filtered_schedrcpt_supplier = schedrcpts_supplier_data_df[(schedrcpts_supplier_data_df['H_EDLC_L_ID_TARGET']==dest)&(schedrcpts_supplier_data_df['H_EDLC_P_ID']==item)&(schedrcpts_supplier_data_df['H_EDLC_L_ID_SOURCE']==source)]

		if not manual_orders_data_df.empty:
			filtered_manual_orders = manual_orders_data_df[(manual_orders_data_df['PP_L_ID_TARGET']==dest)&(manual_orders_data_df['PP_P_ID']==item)&(manual_orders_data_df['PP_L_ID_SOURCE']==source)]

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
			soqlist = vendordata[key1]
			split_percentage = row['SPLIT_PERCENTAGE']
			soqtotal =0
			for soqObject in soqlist:
				orderplacedate_3 = soqObject.opd
				timestamp = convert_timestamp(pd.Timestamp(orderplacedate_3))
				if starttimestamp <= timestamp < endtimestamp:
					soqtotal = soqtotal + soqObject.soq
			rank = row['PRIORITY']
			volumeexact = row['VOLUME_EXACT']
			volumeupto = row['VOLUME_UPTO']
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

	filtered_demand = aggregated_order_projections_df[(aggregated_order_projections_df['SUPPLIER_EXTERNAL_CODE']==dest_code)&(aggregated_order_projections_df['P_EXTERNAL_CODE']==item_code)]
	filtered_demand = filtered_demand.drop_duplicates(subset=['AGGREGATED_ORDER_PROJECTION_PERIOD_FROM','AGGREGATED_ORDER_PROJECTION_PERIOD_UPTO'])

	filtered_ss = pd.DataFrame()
	if dsd_code != 1000:
		filtered_ss = safetystock_df[(safetystock_df['SUPPLIER_EXTERNAL_CODE']==dest_code)&(safetystock_df['P_EXTERNAL_CODE']==item_code)]
		filtered_ss = filtered_ss.drop_duplicates(subset=['EFFECTIVE_FROM','EFFECTIVE_UPTO'])

	filtered_schedrcpt = pd.DataFrame()
	if dsd_code != 1000:
		filtered_schedrcpt = schedrcpts_df[(schedrcpts_df['H_EDLC_L_ID_TARGET']==dest)&(schedrcpts_df['H_EDLC_P_ID']==item)]
		filtered_schedrcpt = filtered_schedrcpt.drop_duplicates(subset=['H_EDLC_EXPECTED_DELIVERY_DATE'])

	filtered_manual_orders = pd.DataFrame()
	if not manual_orders_data_df.empty:
		filtered_manual_orders = manual_orders_data_df[(manual_orders_data_df['PP_L_ID_TARGET']==dest)&(manual_orders_data_df['PP_P_ID']==item)]
		filtered_manual_orders = filtered_manual_orders.drop_duplicates(subset=['PP_L_ID_SOURCE','DELIVERY_DATE'])

	filtered_additional_vendor_orders = pd.DataFrame()
	if not additional_vendor_orders_df.empty:
		filtered_additional_vendor_orders = additional_vendor_orders_df[(additional_vendor_orders_df['PP_P_ID']==item)&(additional_vendor_orders_df['PP_L_ID_TARGET']==dest)]

	timedomain =[]
	for i in range(0,config.range):
		date = config.opd + datetime.timedelta(days=i)
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

	filtered_orderskudata = order_sku_df[(order_sku_df['item']==str(item))&(order_sku_df['dest']==str(dest))]

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
		demandlist.append(demand)

		ss = 0;
		for index,row in filtered_ss.iterrows():
			period_from = row['EFFECTIVE_FROM']
			period_upto = row['EFFECTIVE_UPTO']
			period_from_stamp = convert_timestamp(period_from)
			period_upto_stamp = convert_timestamp(period_upto)

			if((item_1 <= period_upto_stamp) &  (item_1 >=period_from_stamp)):
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

	summaryBuilder.appendToSummary(key)
	summaryBuilder.appendToSummary(df.to_string())
	summaryBuilder.appendToSummary(df_1.to_string())

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

	summaryBuilder.appendToSummary(df_2.to_string())


#############################################
#Post processing


print(summaryBuilder.getSummary())
