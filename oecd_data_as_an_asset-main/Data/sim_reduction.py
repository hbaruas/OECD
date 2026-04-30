#%%
#Import libraries
import pandas as pd
import os
import glob
import numpy as np
import sys
from subprocess import Popen, PIPE
pd.set_option('display.max_columns', 40)

#%% Define key functions
def reduce_file(country):
    year = 2021

    df = pd.DataFrame()
    '''This programm has the objective to reduce the files coming from the NLP process to a manageable size
    and drop all text data that is not needed at this stage. '''
    if country == 'UK':
        path = Popen(f'hadoop fs -ls -C hdfs://sdsh/em_sources/_SDD/DIT_VALUEOFDATA/input_data/UK/{year}', shell=True, stdout=PIPE, stderr=PIPE)
        #path = 'V:\\DIT_VALUEOFDATA\\sources\\key_inputs\\UK\\full_input.csv'
    # if country == 'CAN':
    #     path = 'V:\\DIT_VALUEOFDATA\\sources\\key_inputs\\CAN\\full_input.csv'
    # if country == 'USA':
    #     path = '\\\\oecdmain\\em_sources\\MULTI_NATIONAL_ENTERPRISES\\FY2021\\US Split\\output\\key_data\\*.csv'


    listHadoopFiles, std_err = path.communicate()
    finalFileList = listHadoopFiles.decode("utf-8").splitlines()
    print(finalFileList)


    for i in finalFileList:
        readFile = pd.read_parquet(i)
        df = pd.concat([df,readFile])
    print("has finished appending") 
    print(df.head())
    df['noun_chunk'] = df['noun_chunk'].astype(str).str.strip(' ') 
    df['doc_BGTOcc'] = df['doc_BGTOcc'].astype(str).str.replace('[.-]','', regex = True) 
    df['noun_chunk'] = df['noun_chunk'].str.lower()
    df['counter'] = 1 
    df.loc[df['sim_data'] < 0.45, 'noun_chunk'] = np.NaN 
    df = df.groupby(by=['noun_chunk', 'doc_JobID', 'doc_BGTOcc'], dropna=False).agg(
            {'sim_data': 'mean',
                'counter' : 'sum'}).reset_index()
    print(df.head())
    print(len(df))



    if country == 'UK': 
        df.to_parquet(f"hdfs://sdsh/em_sources/_SDD/DIT_VALUEOFDATA/reduced_data/UK/{year}/reducedFile.parquet", index=False)
        df.to_csv(f"hdfs://sdsh/em_sources/_SDD/DIT_VALUEOFDATA/reduced_data/UK/{year}/reducedFile.csv", index=False)
        #df.to_parquet(f"hdfs://sdsh/em_sources/_SDD/DIT_VALUEOFDATA/AI/reduced_data/UK/Testing10Tokens/testOriginal.parquet", index=False)

        
        # if country == 'CAN':
        #     df.to_csv(f'V:\\DIT_VALUEOFDATA\\sources\\key_inputs\\CAN\\full_input_reduced_final.csv', index=False)
        # if country == 'USA':
        #     df.to_csv(f'V:\\DIT_VALUEOFDATA\\sources\\key_inputs\\USA\\reduced_input\\{f}_r.csv', index=False)

#%% Execute main function
if __name__ == '__main__':
    reduce_file('UK')
    # reduce_file('CAN')
    #reduce_file('USA')