#! /usr/bin/python

import pandas as pd

parameter_filename = "export_parameters_20200501_190002.csv"
elements_filename = "export_elements_20200501_190001.csv"

# derive date of file from filename
file_date = parameter_filename[18:len(parameter_filename)-11]

# create export filename
export_csv_file = "EcoData_{}.csv".format(file_date)


parameter_dataframe = pd.read_csv(parameter_filename)
elements_dataframe = pd.read_csv(elements_filename)

# drop rows from paramater dataframe where NaN appears in the DSL related columns
parameter_dataframe.dropna(subset = ["downstreamcurrrate"], inplace=True)


# Calculate the average of the different DSL related parameters and store in the parameters dataframe
parameter_dataframe['avgCurrDN'] = parameter_dataframe.groupby('serialnumber')['downstreamcurrrate'].transform('mean')
parameter_dataframe['avgMaxDn'] = parameter_dataframe.groupby('serialnumber')['downstreammaxrate'].transform('mean')
parameter_dataframe['avgCurrUp'] = parameter_dataframe.groupby('serialnumber')['upstreamcurrrate'].transform('mean')
parameter_dataframe['avgMaxUp'] = parameter_dataframe.groupby('serialnumber')['upstreammaxrate'].transform('mean')
print("averaging end: {}".format(datetime.now()))


# Remove duplicates from parameter dataframe to reduce size
parameter_dataframe.drop_duplicates(subset ="serialnumber", keep = 'first', inplace = True) 

# map the downstream averages from the parameter file over into the elements file
elements_dataframe['avgCurrDN']=elements_dataframe['serialnumber'].map(parameter_dataframe.set_index('serialnumber')['avgCurrDN'])
elements_dataframe['avgMaxDn']=elements_dataframe['serialnumber'].map(parameter_dataframe.set_index('serialnumber')['avgMaxDn'])
elements_dataframe['avgCurUp']=elements_dataframe['serialnumber'].map(parameter_dataframe.set_index('serialnumber')['avgCurrUp'])
elements_dataframe['avgMaxUp']=elements_dataframe['serialnumber'].map(parameter_dataframe.set_index('serialnumber')['avgMaxUp'])

# export elements dataframe to a .csv file
elements_dataframe.to_csv(export_csv_file, index = False)



