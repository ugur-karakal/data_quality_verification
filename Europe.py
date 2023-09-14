# -*- coding: utf-8 -*-
"""
Created on Sat Sep 9 23:09:07 2023

This is a script file for European data analysis.

@author     : Ugur Karakal
e-mail      : ugur.karakal@gmail.com
"""

import os
import pandas as pd
import numpy as np
import datetime

data_dir = "input"
input_dir = os.path.join(os.getcwd(), data_dir)
result_dir = "output"
output_dir = os.path.join(os.getcwd(), result_dir)

file_Carrier_Germany_UK_data = input_dir + "\\" + "Carrier_Germany_data.xlsx"
file_Vendor_volume_data = input_dir + "\\" + "Vendor_Volume_data.csv"
file_Vendor_Berlin_data = input_dir + "\\" + "Vendor_Berlin_data.csv"
file_Vendor_Munich_data = input_dir + "\\" + "Vendor_Munich_data.csv"

new_Germany_name = "Germany"
new_date_name = "Date"
new_volume_name = "Traffic_Volume"
new_Pankow_name = "Pankow_Request"
new_Mitte_name = "Mitte_Request"
new_Altstadt_name = "Altstadt_Request"
new_Garching_name = "Garching_Request"

suffix_Vendor = "_Vendor"
suffix_Carrier = "_Carrier"
suffix_ratio = "_Ratio"
counter_type_name = "Counter_Type"

missing_Vendor = "Vendor Data Missing"
missing_Carrier = "Carrier Data Missing"
ne_inactive = "NE Inactive"
Vendor_Germany_data_issue = "Vendor Germany/UK Data issue"
data_not_exist = "Data Not Exist in Files"

threshold_upper = 103
threshold_lower = 97
threshold_orange = 90

###########     COLOR & FORMATTING FUNCTION     ############
def color_value(value):
    if isinstance(value, str):
        color = "white"
        return "background-color: %s" % color
    elif isinstance(value, float):
        if value > 999999:
            color = "#07F"
            return "background-color: %s" % color
        elif value > threshold_upper and value <= 999999:
            color = "#0AF"
            return "background-color: %s" % color
        elif value >= threshold_lower and value <= threshold_upper:
            color = "lightgreen"
            return "background-color: %s" % color
        elif value >= threshold_orange and value < threshold_lower:
            color = "orange"
            return "background-color: %s" % color
        elif value < threshold_orange:
            color= "red"
            return "background-color: %s" % color
############################################################

#######     FORMATTING DATE COLUMN   ########
def formatting_df_date(df, col):
    df[col] = df[col].map(str)
    return df
######################################################

###############################################################################
###########     CARRIER VOLUME DATA     ###############################
###############################################################################
Germany_UK_Carrier = pd.read_excel(file_Carrier_Germany_UK_data, sheet_name="Germany_UK")

Germany_list_drop = ["suc", "comp", "mod", "%", "downlink", "uplink"]
for i in range(len(Germany_list_drop)):
    Germany_UK_Carrier.drop(Germany_UK_Carrier.columns[Germany_UK_Carrier.columns.str.contains(Germany_list_drop[i],
                                                                             case=False)], axis=1, inplace=True)
temp_Germany_col_name = Germany_UK_Carrier.columns.tolist()
Germany_col_strip = []
for i in temp_Germany_col_name:
    Germany_col_strip.append(i.strip())
Germany_UK_Carrier.columns = Germany_col_strip

Germany_UK_Carrier.rename(columns={"TIME": new_date_name}, inplace=True)
Germany_UK_Carrier.rename(columns={"Total_Volume_Gbyte": new_volume_name}, inplace=True)
Germany_UK_Carrier.rename(columns={"SUM(Pankow Requests)": new_Pankow_name}, inplace=True)
Germany_UK_Carrier.rename(columns={"SUM(Mitte Requests)": new_Mitte_name}, inplace=True)
Germany_UK_Carrier.rename(columns={"SUM(Altstadt Requests)": new_Altstadt_name}, inplace=True)
Germany_UK_Carrier.rename(columns={"SUM(Garching Requests)": new_Garching_name}, inplace=True)

Germany_UK_Carrier[new_Germany_name] = Germany_UK_Carrier[new_Germany_name].str.strip()

Germany_column_header = Germany_UK_Carrier.columns.tolist()

formatting_df_date(Germany_UK_Carrier, new_date_name)

Germany_column_request = Germany_UK_Carrier.columns[Germany_UK_Carrier.columns.str.contains("Req", case=False)].tolist()

###########     FILL 0 (ZERO) INSTEAD OF NA VALUES     ###########
Germany_column_stat = [new_volume_name]
for e in range(len(Germany_column_request)):
    Germany_column_stat.append(Germany_column_request[e])
def recode_empty_cells(dataframe, list_of_columns):
    for column in list_of_columns:
        dataframe[column] = dataframe[column].replace(r'\s+', np.nan, regex=True)
        dataframe[column] = dataframe[column].fillna(0)
    return dataframe

recode_empty_cells(Germany_UK_Carrier, Germany_column_stat)
############################################################

#######     ASSIGN NEXT VALUES to NEGATIVE VALUES   ########
Germany_UK_Carrier.sort_values([new_Germany_name, new_date_name], ascending=[True, True], inplace=True)
Germany_UK_Carrier = Germany_UK_Carrier.reset_index(drop=True)

for row_index in range(Germany_UK_Carrier.index.max(skipna=False) - Germany_UK_Carrier.index.min(skipna=False)):
    for col in Germany_column_stat:
        if Germany_UK_Carrier.at[row_index, col] < 0:
            if abs(row_index - Germany_UK_Carrier.index.min(skipna=False)) <= abs(row_index - Germany_UK_Carrier.index.max(skipna=False)):
                temp_index = row_index + 1
                skip = 0
                while skip == 0:
                    if Germany_UK_Carrier.at[temp_index, col] >= 0:
                        Germany_UK_Carrier.at[row_index, col] = Germany_UK_Carrier.at[temp_index, col]
                        skip = 1
                    else:
                        temp_index = temp_index + 1
            else:
                temp_index = row_index - 1
                skip = 0
                while skip == 0:
                    if Germany_UK_Carrier.at[temp_index, col] >= 0:
                        Germany_UK_Carrier.at[row_index, col] = Germany_UK_Carrier.at[temp_index, col]
                        skip = 1
                    else:
                        temp_index = temp_index - 1
############################################################

Germany_UK_Carrier_ne_name = Germany_UK_Carrier[new_Germany_name].unique().tolist()

# Germany_UK_Carrier[new_date_name] = Germany_UK_Carrier[new_date_name].dt.strftime('%Y%m%d')
# Germany_UK_Carrier[new_date_name] = pd.to_datetime(Germany_UK_Carrier[new_date_name], errors='coerce')
# Germany_UK_Carrier[new_date_name] = Germany_UK_Carrier[new_date_name].dt.strftime('%Y%m%d')
Germany_UK_Carrier[new_date_name] = Germany_UK_Carrier[new_date_name].str[:8]
Germany_UK_Carrier_date_array = Germany_UK_Carrier[new_date_name].unique().tolist()
Germany_UK_Carrier_date_array = sorted(Germany_UK_Carrier_date_array)

if len(Germany_UK_Carrier_date_array) == 8:
    if Germany_UK_Carrier_date_array[0] < "20210101":
        Germany_UK_Carrier = Germany_UK_Carrier[~(Germany_UK_Carrier[new_date_name].str.contains(Germany_UK_Carrier_date_array[0]))]
        Germany_UK_Carrier_date_array = np.delete(Germany_UK_Carrier_date_array, 0)
    else:
        Germany_UK_Carrier = Germany_UK_Carrier[~(Germany_UK_Carrier[new_date_name].str.contains(Germany_UK_Carrier_date_array[-1]))]
        Germany_UK_Carrier_date_array = np.delete(Germany_UK_Carrier_date_array, -1)

Germany_UK_date_Carrier = []
for i in range(len(Germany_UK_Carrier_date_array)):
    Germany_UK_date_Carrier.append(Germany_UK_Carrier_date_array[i])

Germany_UK_Carrier_date_array = Germany_UK_Carrier[new_date_name].unique().tolist()
Germany_UK_Carrier_date_array = sorted(Germany_UK_Carrier_date_array)

Germany_UK_Carrier = Germany_UK_Carrier.groupby([new_Germany_name, new_date_name]).agg({new_volume_name:"sum",
                                                                         new_Pankow_name:"sum",
                                                                         new_Mitte_name:"sum",
                                                                         new_Altstadt_name:"sum",
                                                                         new_Garching_name:"sum"}).reset_index()

###############################################################################
#####################     VENDOR VOLUME DATA     ########################
###############################################################################
volume_Vendor = pd.read_csv(file_Vendor_volume_data)
volume_Vendor.rename(columns={"NAME": new_Germany_name,
                          "Time": new_date_name,
                          "Total Traffic(TB)": new_volume_name}, inplace=True)

list_volume_Vendor = volume_Vendor[new_Germany_name].unique().tolist()

formatting_df_date(volume_Vendor, new_date_name)

date_Vendor_volume = volume_Vendor[new_date_name].unique().tolist()
date_Vendor_volume = sorted(date_Vendor_volume)
for i, v in enumerate(date_Vendor_volume):
    date_Vendor_volume[i] = str(v)
###############################################################################

###############################################################################
#######################     VENDOR BERLIN DATA     ############################
###############################################################################
Berlin_Vendor = pd.read_csv(file_Vendor_Berlin_data)
Berlin_Vendor.rename(columns={"Germany": new_Germany_name,
                      "Time": new_date_name,
                      "Pankow Requests(times)": new_Pankow_name,
                      "Mitte Requests(times)": new_Mitte_name},
             inplace=True)

Berlin_Vendor[new_Germany_name] = Berlin_Vendor[new_Germany_name].astype(str)
Berlin_Vendor[new_Germany_name] = Berlin_Vendor[new_Germany_name].str.strip()

formatting_df_date(Berlin_Vendor, new_date_name)
###############################################################################

#######     VENDOR BERLIN GERMANY NAME PARSING      ########
list_Berlin_Vendor_Germany = Berlin_Vendor[new_Germany_name].unique().tolist()
list_Berlin_Vendor_Germany = sorted(list_Berlin_Vendor_Germany)

for g in list_Berlin_Vendor_Germany:
    Berlin_Vendor[new_Germany_name] = Berlin_Vendor[new_Germany_name].replace([g], g.partition('_')[0])

##########     VENDOR BERLIN DATE LIST      ###########
date_Vendor_Berlin = Berlin_Vendor[new_date_name].unique().tolist()
date_Vendor_Berlin = sorted(date_Vendor_Berlin)
############################################################

###############################################################################
#######################     VENDOR MUNICH DATA     ############################
###############################################################################
Munich_Vendor = pd.read_csv(file_Vendor_Munich_data)
Munich_Vendor.rename(columns={"England": new_Germany_name,
                       "Time": new_date_name,
                       "Altstadt Requests (times)": new_Altstadt_name,
                       "Garching Requests (times)": new_Garching_name},
              inplace=True)

Munich_Vendor[new_Germany_name] = Munich_Vendor[new_Germany_name].astype(str)
Munich_Vendor[new_Germany_name] = Munich_Vendor[new_Germany_name].str.strip()

#######     VENDOR MUNICH GERMANY NAME PARSING      ########
list_Munich_Vendor_England = Munich_Vendor[new_Germany_name].unique().tolist()
list_Munich_Vendor_England = sorted(list_Munich_Vendor_England)

for g in list_Munich_Vendor_England:
    Munich_Vendor[new_Germany_name] = Munich_Vendor[new_Germany_name].replace([g], g.partition("_")[0])

formatting_df_date(Munich_Vendor, new_date_name)
############################################################

#######     VENDOR MUNICH DATE LIST      ########
date_Vendor_Munich = Munich_Vendor[new_date_name].unique().tolist()
date_Vendor_Munich = sorted(date_Vendor_Munich)
############################################################

###############################################################################
##################     CARRIER & VENDOR DATE PARSING & LIST     ###############
###############################################################################
Germany_col_sorted = []

def conv_to_string(lists):
    for i, v in enumerate(lists):
        lists[i] = str(v)
    return lists

def date_column_parse(df, datelist):
    for c in range(len(datelist)):
        if '/' not in datelist[c] and '-' not in datelist[c]:
            df[new_date_name] = df[new_date_name].replace([datelist[c]], datetime.datetime.strptime(datelist[c], "%Y%m%d").strftime("%Y-%m-%d"))
            datelist[c] = datetime.datetime.strptime(datelist[c], "%Y%m%d").strftime("%Y-%m-%d")
        elif "/" in datelist[c]:
            df[new_date_name] = df[new_date_name].replace([datelist[c]], datetime.datetime.strptime(datelist[c], "%m/%d/%Y").strftime("%Y-%m-%d"))
            datelist[c] = datetime.datetime.strptime(datelist[c], "%m/%d/%Y").strftime("%Y-%m-%d")

date_column_parse(Germany_UK_Carrier, conv_to_string(Germany_UK_date_Carrier))
date_column_parse(volume_Vendor, conv_to_string(date_Vendor_volume))
date_column_parse(Berlin_Vendor, conv_to_string(date_Vendor_Berlin))
date_column_parse(Munich_Vendor, conv_to_string(date_Vendor_Munich))

for c in range(len(date_Vendor_volume)):
    Germany_col_sorted.append(date_Vendor_volume[c])

Germany_col_sorted.append(new_Germany_name)
###############################################################################

###############################################################################
##################     COMBINE VENDOR DATA                      ###############
###############################################################################
temp_Vendor_df = pd.merge(volume_Vendor, Berlin_Vendor, how="outer", on=[new_Germany_name, new_date_name])
Germany_UK_Vendor_summed = pd.merge(temp_Vendor_df, Munich_Vendor, how="outer", on=[new_Germany_name, new_date_name])
###############################################################################

##################     GBYTE CONVERSION OF VENDOR DATA          ###############
Germany_UK_Vendor_summed[new_volume_name] = Germany_UK_Vendor_summed[new_volume_name].multiply(1024)

##################     COMBINE ALL VENDOR & CARRIER DATA          #############
df_outer_Germany_UK = pd.merge(Germany_UK_Vendor_summed, Germany_UK_Carrier, how="outer", on=[new_Germany_name, new_date_name], suffixes=(suffix_Vendor, suffix_Carrier))

##################     RATIO CREATION            #############
df_outer_Germany_UK[new_volume_name + suffix_ratio] = (df_outer_Germany_UK[new_volume_name + 
                                                                       suffix_Vendor]/df_outer_Germany_UK[new_volume_name + suffix_Carrier]).multiply(100)
df_outer_Germany_UK[new_Pankow_name + suffix_ratio] = (df_outer_Germany_UK[new_Pankow_name + 
                                                                           suffix_Vendor]/df_outer_Germany_UK[new_Pankow_name + suffix_Carrier]).multiply(100)
df_outer_Germany_UK[new_Mitte_name + suffix_ratio] = (df_outer_Germany_UK[new_Mitte_name + 
                                                                           suffix_Vendor]/df_outer_Germany_UK[new_Mitte_name + suffix_Carrier]).multiply(100)
df_outer_Germany_UK[new_Altstadt_name + suffix_ratio] = (df_outer_Germany_UK[new_Altstadt_name + 
                                                                              suffix_Vendor]/df_outer_Germany_UK[new_Altstadt_name + suffix_Carrier]).multiply(100)
df_outer_Germany_UK[new_Garching_name + suffix_ratio] = (df_outer_Germany_UK[new_Garching_name + 
                                                                                suffix_Vendor]/df_outer_Germany_UK[new_Garching_name + suffix_Carrier]).multiply(100)

###############     VOLUME MISSING DATA            #############
df_outer_Germany_UK.loc[ (df_outer_Germany_UK[new_volume_name + suffix_Vendor] > 0) & 
                      ( ( df_outer_Germany_UK[new_volume_name + suffix_Carrier] == 0) | 
                       ( ~(df_outer_Germany_UK[new_volume_name + suffix_Carrier] >= 0) & 
                        ~(df_outer_Germany_UK[new_volume_name + suffix_Carrier] < 0) ) ),
                      new_volume_name + suffix_ratio] = missing_Carrier

df_outer_Germany_UK.loc[ (df_outer_Germany_UK[new_volume_name + suffix_Carrier] > 0) & 
                      ( ( df_outer_Germany_UK[new_volume_name + suffix_Vendor] == 0) | 
                       ( ~(df_outer_Germany_UK[new_volume_name + suffix_Vendor] >= 0) & 
                        ~(df_outer_Germany_UK[new_volume_name + suffix_Vendor] < 0) ) ),
                      new_volume_name + suffix_ratio] = missing_Vendor

df_outer_Germany_UK.loc[ ( ( df_outer_Germany_UK[new_volume_name + suffix_Vendor] == 0) | 
                        ( ~(df_outer_Germany_UK[new_volume_name + suffix_Vendor] >= 0) & 
                         ~(df_outer_Germany_UK[new_volume_name + suffix_Vendor] < 0) ) ) & 
                      ( ( df_outer_Germany_UK[new_volume_name + suffix_Carrier] == 0 ) | 
                       ( ~(df_outer_Germany_UK[new_volume_name + suffix_Carrier] >= 0) & 
                        ~(df_outer_Germany_UK[new_volume_name + suffix_Carrier] < 0) ) ), 
                      new_volume_name + suffix_ratio] = ne_inactive

df_outer_Germany_UK.loc[ (df_outer_Germany_UK[new_Germany_name] == "OTHER"), new_volume_name + suffix_ratio] = Vendor_Germany_data_issue
###############################################################################

###############     BERLIN PANKOW MISSING DATA          #############
df_outer_Germany_UK.loc[ (df_outer_Germany_UK[new_Pankow_name + suffix_Vendor] > 0) & 
                      ( ( df_outer_Germany_UK[new_Pankow_name + suffix_Carrier] == 0) | 
                       ( ~(df_outer_Germany_UK[new_Pankow_name + suffix_Carrier] >= 0) & 
                        ~(df_outer_Germany_UK[new_Pankow_name + suffix_Carrier] < 0) ) ), 
                      new_Pankow_name + suffix_ratio] = missing_Carrier

df_outer_Germany_UK.loc[ (df_outer_Germany_UK[new_Pankow_name + suffix_Carrier] > 0) & 
                      ( (df_outer_Germany_UK[new_Pankow_name + suffix_Vendor] == 0) | 
                       ( ~(df_outer_Germany_UK[new_Pankow_name + suffix_Vendor] >= 0) & 
                        ~(df_outer_Germany_UK[new_Pankow_name + suffix_Vendor] < 0) ) ), 
                      new_Pankow_name + suffix_ratio] = missing_Vendor

df_outer_Germany_UK.loc[ ( (df_outer_Germany_UK[new_Pankow_name + suffix_Vendor] == 0) | 
                        ( ~(df_outer_Germany_UK[new_Pankow_name + suffix_Vendor] >= 0) & 
                         ~(df_outer_Germany_UK[new_Pankow_name + suffix_Vendor] < 0) ) ) & 
                      ( ( df_outer_Germany_UK[new_Pankow_name + suffix_Carrier] == 0 ) |
                       ( ~(df_outer_Germany_UK[new_Pankow_name + suffix_Carrier] >= 0) & 
                        ~(df_outer_Germany_UK[new_Pankow_name + suffix_Carrier] < 0) ) ), 
                      new_Pankow_name + suffix_ratio] = ne_inactive

df_outer_Germany_UK.loc[ (df_outer_Germany_UK[new_Germany_name] == "OTHER"), new_Pankow_name + suffix_ratio] = Vendor_Germany_data_issue
#####################################################################

###############     BERLIN MITTE MISSING DATA           #############
df_outer_Germany_UK.loc[ (df_outer_Germany_UK[new_Mitte_name + suffix_Vendor] > 0) & 
                      ( (df_outer_Germany_UK[new_Mitte_name + suffix_Carrier] == 0) |
                       ( ~(df_outer_Germany_UK[new_Mitte_name + suffix_Carrier] >= 0) & 
                        ~(df_outer_Germany_UK[new_Mitte_name + suffix_Carrier] < 0) ) ), 
                      new_Mitte_name + suffix_ratio] = missing_Carrier

df_outer_Germany_UK.loc[ (df_outer_Germany_UK[new_Mitte_name + suffix_Carrier] > 0) & 
                      ( (df_outer_Germany_UK[new_Mitte_name + suffix_Vendor] == 0) |
                       ( ~(df_outer_Germany_UK[new_Mitte_name + suffix_Vendor] >= 0) & 
                        ~(df_outer_Germany_UK[new_Mitte_name + suffix_Vendor] < 0) ) ),
                      new_Mitte_name + suffix_ratio] = missing_Vendor

df_outer_Germany_UK.loc[ ( (df_outer_Germany_UK[new_Mitte_name + suffix_Vendor] == 0) | 
                        ( ~(df_outer_Germany_UK[new_Mitte_name + suffix_Vendor] >= 0) & 
                         ~(df_outer_Germany_UK[new_Mitte_name + suffix_Vendor] < 0) ) ) & 
                      ( ( df_outer_Germany_UK[new_Mitte_name + suffix_Carrier] == 0 ) |
                       ( ~(df_outer_Germany_UK[new_Mitte_name + suffix_Carrier] >= 0) & 
                        ~(df_outer_Germany_UK[new_Mitte_name + suffix_Carrier] < 0) ) ),
                      new_Mitte_name + suffix_ratio] = ne_inactive

df_outer_Germany_UK.loc[ (df_outer_Germany_UK[new_Germany_name] == "OTHER"), new_Mitte_name + suffix_ratio] = Vendor_Germany_data_issue
#####################################################################

###############     MUNICH ALTSTADT MISSING DATA           #############
df_outer_Germany_UK.loc[ (df_outer_Germany_UK[new_Altstadt_name + suffix_Vendor] > 0) & 
                      ( ( df_outer_Germany_UK[new_Altstadt_name + suffix_Carrier] == 0 ) | 
                       ( ~(df_outer_Germany_UK[new_Altstadt_name + suffix_Carrier] >= 0) & 
                        ~(df_outer_Germany_UK[new_Altstadt_name + suffix_Carrier] < 0) ) ), 
                      new_Altstadt_name + suffix_ratio] = missing_Carrier

df_outer_Germany_UK.loc[ (df_outer_Germany_UK[new_Altstadt_name + suffix_Carrier] > 0) & 
                      ( ( df_outer_Germany_UK[new_Altstadt_name + suffix_Vendor] == 0 ) | 
                       ( ~(df_outer_Germany_UK[new_Altstadt_name + suffix_Vendor] >= 0) & 
                        ~(df_outer_Germany_UK[new_Altstadt_name + suffix_Vendor] < 0) ) ), 
                      new_Altstadt_name + suffix_ratio] = missing_Vendor

df_outer_Germany_UK.loc[ ( (df_outer_Germany_UK[new_Altstadt_name + suffix_Vendor] == 0 ) | 
                        ( ~(df_outer_Germany_UK[new_Altstadt_name + suffix_Vendor] >= 0) & 
                         ~(df_outer_Germany_UK[new_Altstadt_name + suffix_Vendor] < 0) ) ) & 
                      ( ( df_outer_Germany_UK[new_Altstadt_name + suffix_Carrier] == 0 ) | 
                       ( ~(df_outer_Germany_UK[new_Altstadt_name + suffix_Carrier] >= 0) & 
                        ~(df_outer_Germany_UK[new_Altstadt_name + suffix_Carrier] < 0) ) ), 
                      new_Altstadt_name + suffix_ratio] = ne_inactive

df_outer_Germany_UK.loc[ (df_outer_Germany_UK[new_Germany_name] == "OTHER"), 
                      new_Altstadt_name + suffix_ratio] = Vendor_Germany_data_issue
#####################################################################

###############     MUNICH GARCHING MISSING DATA           #############
df_outer_Germany_UK.loc[ (df_outer_Germany_UK[new_Garching_name + suffix_Vendor] > 0) & 
                      ( ( df_outer_Germany_UK[new_Garching_name + suffix_Carrier] == 0) | 
                       ( ~(df_outer_Germany_UK[new_Garching_name + suffix_Carrier] >= 0) & 
                        ~(df_outer_Germany_UK[new_Garching_name + suffix_Carrier] < 0) ) ), 
                      new_Garching_name + suffix_ratio] = missing_Carrier

df_outer_Germany_UK.loc[ (df_outer_Germany_UK[new_Garching_name + suffix_Carrier] > 0) & 
                      ( ( df_outer_Germany_UK[new_Garching_name + suffix_Vendor] == 0) | 
                       ( ~(df_outer_Germany_UK[new_Garching_name + suffix_Vendor] >= 0) & 
                        ~(df_outer_Germany_UK[new_Garching_name + suffix_Vendor] < 0) ) ), 
                      new_Garching_name + suffix_ratio] = missing_Vendor

df_outer_Germany_UK.loc[ ( (df_outer_Germany_UK[new_Garching_name + suffix_Vendor] == 0 ) | 
                        ( ~(df_outer_Germany_UK[new_Garching_name + suffix_Vendor] >= 0) & 
                         ~(df_outer_Germany_UK[new_Garching_name + suffix_Vendor] < 0) ) ) &
                      ( ( df_outer_Germany_UK[new_Garching_name + suffix_Carrier] == 0 ) | 
                       ( ~(df_outer_Germany_UK[new_Garching_name + suffix_Carrier] >= 0) & 
                        ~(df_outer_Germany_UK[new_Garching_name + suffix_Carrier] < 0) ) ), 
                      new_Garching_name + suffix_ratio] = ne_inactive

df_outer_Germany_UK.loc[ (df_outer_Germany_UK[new_Germany_name] == "OTHER"), new_Garching_name + suffix_ratio] = Vendor_Germany_data_issue
#####################################################################

###############     MODIFY OUTPUT           #############
list_Germany_col_ratio = df_outer_Germany_UK.columns[df_outer_Germany_UK.columns.str.contains(suffix_ratio, case=False)].tolist()

list_df_Germany = []
dict_df_Germany = {}
for d in range(len(list_Germany_col_ratio)):
    list_df_Germany.append(list_Germany_col_ratio[d].partition(suffix_ratio)[0])
    dict_df_Germany[list_df_Germany[d]] = pd.DataFrame()
    dict_df_Germany[list_df_Germany[d]] = pd.pivot_table( df_outer_Germany_UK, 
                                                   values=list_Germany_col_ratio[d], 
                                                   index=new_Germany_name, 
                                                   columns=new_date_name, 
                                                   aggfunc=np.sum, dropna=False, 
                                                   fill_value=data_not_exist )
    dict_df_Germany[list_df_Germany[d]] = dict_df_Germany[list_df_Germany[d]].sort_values( Germany_col_sorted, 
                                                                              ascending=True )
    dict_df_Germany[list_df_Germany[d]].reset_index( drop=False, inplace=True )

    idx_Germany = 1
    list_temp_Germany = []
    for e in range(len(dict_df_Germany[list_df_Germany[d]])):
        list_temp_Germany.append(list_Germany_col_ratio[d])
    
    dict_df_Germany[list_df_Germany[d]].insert(loc=idx_Germany, column=counter_type_name, 
                                         value=list_temp_Germany)

Germany_concat_frames = pd.concat(dict_df_Germany)
Germany_concat_frames = Germany_concat_frames.sort_values(Germany_col_sorted, ascending=True)
#########################################################

#########################################################################
###############     APPLY COLOUR & DECIMAL FORMAT           #############
#########################################################################
d = dict.fromkeys(Germany_concat_frames.select_dtypes("float").columns, "{:.2f}")
with pd.option_context("display.precision", 1):
    Germany_comparison = Germany_concat_frames.style.applymap(color_value).format(d)
#########################################################################
