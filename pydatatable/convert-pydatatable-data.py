print("pydatatable: converting 50GB join data")
import os
import datatable as dt

if os.path.isfile('data/J1_1e9_NA_0_0.csv'):
    dt.fread('data/J1_1e9_NA_0_0.csv').to_jay('data/J1_1e9_NA_0_0.jay')
if os.path.isfile('data/J1_1e9_1e9_0_0.csv'):
    dt.fread('data/J1_1e9_1e9_0_0.csv').to_jay('data/J1_1e9_1e9_0_0.jay')
if os.path.isfile('data/J1_1e9_1e6_0_0.csv'):
    dt.fread('data/J1_1e9_1e6_0_0.csv').to_jay('data/J1_1e9_1e6_0_0.jay')
if os.path.isfile('data/J1_1e9_1e3_0_0.csv'):
    dt.fread('data/J1_1e9_1e3_0_0.csv').to_jay('data/J1_1e9_1e3_0_0.jay')
if os.path.isfile('data/J1_1e9_NA_0_1.csv'):
    dt.fread('data/J1_1e9_NA_0_1.csv').to_jay('data/J1_1e9_NA_0_1.jay')
if os.path.isfile('data/J1_1e9_1e9_0_1.csv'):
    dt.fread('data/J1_1e9_1e9_0_1.csv').to_jay('data/J1_1e9_1e9_0_1.jay')
if os.path.isfile('data/J1_1e9_1e6_0_1.csv'):
    dt.fread('data/J1_1e9_1e6_0_1.csv').to_jay('data/J1_1e9_1e6_0_1.jay')
if os.path.isfile('data/J1_1e9_1e3_0_1.csv'):
    dt.fread('data/J1_1e9_1e3_0_1.csv').to_jay('data/J1_1e9_1e3_0_1.jay')
if os.path.isfile('data/J1_1e9_NA_5_0.csv'):
    dt.fread('data/J1_1e9_NA_5_0.csv').to_jay('data/J1_1e9_NA_5_0.jay')
if os.path.isfile('data/J1_1e9_1e9_5_0.csv'):
    dt.fread('data/J1_1e9_1e9_5_0.csv').to_jay('data/J1_1e9_1e9_5_0.jay')
if os.path.isfile('data/J1_1e9_1e6_5_0.csv'):
    dt.fread('data/J1_1e9_1e6_5_0.csv').to_jay('data/J1_1e9_1e6_5_0.jay')
if os.path.isfile('data/J1_1e9_1e3_5_0.csv'):
    dt.fread('data/J1_1e9_1e3_5_0.csv').to_jay('data/J1_1e9_1e3_5_0.jay')

print("pydatatable: done converting 50GB join data")