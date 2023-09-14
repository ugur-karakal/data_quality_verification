# -*- coding: utf-8 -*-
"""
Created on Sat Sep 9 23:09:07 2023

This is main file to perform whole data analysis solution.

@author: Ugur Karakal
"""

import os
import pandas as pd
import Europe

prefix = "Data_Verify_"
extension = ".xlsx"
date_first = ""
adding = "--"
date_last = ""

result_dir = "output"
output_directory = os.path.join(os.getcwd(), result_dir)

if Europe.date_Vendor_volume == Europe.date_Vendor_Berlin == Europe.date_Vendor_Munich:
    date_first = Europe.date_Vendor_volume[0]
    date_last = Europe.date_Vendor_volume[-1]
elif Europe.date_Vendor_volume == Europe.date_Vendor_Berlin:
    date_first = Europe.date_Vendor_volume[0]
    date_last = Europe.date_Vendor_volume[-1]
elif Europe.date_Vendor_volume == Europe.date_Vendor_Munich:
    date_first = Europe.date_Vendor_volume[0]
    date_last = Europe.date_Vendor_volume[-1]
elif Europe.date_Vendor_Berlin == Europe.date_Vendor_Munich:
    date_first = Europe.date_Vendor_Berlin[0]
    date_last = Europe.date_Vendor_Berlin[-1]

filename = prefix + date_first + adding + date_last + extension

with pd.ExcelWriter(output_directory + "\\" + filename) as writer:
    Europe.Germany_comparison.to_excel(writer, engine="openpyxl", sheet_name = "Germany_UK", index=False)
