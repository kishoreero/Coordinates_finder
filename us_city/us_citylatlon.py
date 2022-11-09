import numpy as np
from geopy import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import pandas as pd
import os,glob
import time
import configparser


print('process Started')

path_fig = configparser.ConfigParser()
path_fig.read('test_us.ini')
excel_path =  path_fig.get('test','excel_path')
input_path =  path_fig.get('test','input_path')
output_path =  path_fig.get('test','output_path')

#split dataset into chuncks 
df = pd.read_excel(excel_path)
ss = np.array_split(df, 175)

for test in range(len(ss)):
	ss[test].to_csv(f'{input_path}uscitysplit{test}.csv')
# print('wait 1.30 min')
time.sleep(10)

# function for finding location coordinates
def process_operation(df,i):
    i = i
    df['CSM'] = df[df.columns[0:2]].apply(lambda x: ','.join(x.dropna().astype(str)),axis=1)
    geolocator = Nominatim(user_agent="my_request")
    
    #applying the rate limiter wrapper
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

    #Applying the method to pandas DataFrame
    df['location'] = df['CSM'].apply(geocode)
    df['Lat'] = df['location'].apply(lambda x: x.latitude if x else None)
    df['Lon'] = df['location'].apply(lambda x: x.longitude if x else None)
    df.to_csv(f'{output_path}uscity_corr_{i}.csv')
    print(f'saved as csv uscity_corr_{i}.csv successfully')
    print('process completed',df.shape)



# read all splited csv and apply with function 
all_files = glob.glob(os.path.join(input_path, "*.csv"))

all_files.sort()

for i in range(len(all_files)):
    print(f'processing with {all_files[i]}')
    df = pd.read_csv(f'{all_files[i]}')
    df.drop(['Unnamed: 0'], axis=1,inplace=True)
    # print(df)
    try:
        process_operation(df,i)
    except:
        time.sleep(500)
        process_operation(df,i)


#   Merge all files generated coordinates csv 
merge_all_files = glob.glob(os.path.join(output_path, "*.csv"))
df_final = pd.concat((pd.read_csv(f) for f in merge_all_files), ignore_index=True)
df_final.drop(['Unnamed: 0', 'location','CSM',], axis=1,inplace=True)
df_final.to_csv('US-City_Coordinates_testrun.csv')

print('process Completed')
