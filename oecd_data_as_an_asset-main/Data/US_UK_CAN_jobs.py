#Import libraries
import pandas as pd
import os, sys
import numpy as np
pd.set_option('display.max_columns', 40)
np.random.seed(42)
import glob
import sys
from pyarrow import fs

#Input paths and key parameters
import inputs 

#Import key functions
import key_functions
fs = fs.HadoopFileSystem("default")

year = 2022

def job_analysis(country, mode, sector, data_entry, database, data_science, rel_share):
    '''Function takes the country, the mode (test or final), and the specific data entry, database and data science
    occupation IDs and analyses the text data. It moves in three iterations, first taking the processed text data (frame),
    then aggregating up to job level (first output), occupation level (second output), and sector level (third output).'''
    ## Load the key data inputs (processed text data from BGT)
 


    if country == 'UK': 

        frame = pd.read_csv(inputs.paths.UK_input_frame)
        frame['doc_BGTOcc'] = frame['doc_BGTOcc'].astype('str').str[:-2]  #ensure same formating as other BGTOccs ##use without .str[:-2] for parquet, but use #[:-2] for csv
 
    # if country == 'CAN':
    #     frame = pd.read_csv(inputs.paths.CAN_input_frame)

    # if country == 'USA':
    #     csv_files = glob.glob(os.path.join(inputs.paths.US_path, "*.csv"))
    #     li = []
    #     # loop over the list of csv files
    #     for f in csv_files:
    #         print('Location:', f)
    #         print('File Name:', f.split("\\")[-1])
    #         df = pd.read_csv(f, index_col=None)
    #         li.append(df)
    #     frame = pd.concat(li, axis=0, ignore_index=True)
    #     print(frame.head())
 
        # #Group and sum to aggregate the counts
        # df = df.groupby(by=['noun_chunk', 'doc_JobID', 'doc_BGTOcc'], dropna = False).agg(
        #         {'sim_data': 'mean',
        #          'counter' : 'sum'}).reset_index()
               
    #Drop noun_chunks that occur less than 100 times in the dataset 
    print("finished reading the df")
    frame['Count_byNoun'] = frame.groupby(by = ['noun_chunk'], dropna=False)['counter'].transform('sum') 
    # frame = frame.drop(frame[frame.Count_byNoun < 100].index)
    
    #Frequency measures
    frame['Count_Total'] = frame.counter.sum()
    frame['Count_byJob'] = frame.groupby(by = ['doc_JobID'], dropna = False)['counter'].transform('sum')
    frame['Count_byOcc'] = frame.groupby(by = ['doc_BGTOcc'], dropna = False)['counter'].transform('sum')
    frame['Count_byNounOcc'] = frame.groupby(by = ['noun_chunk','doc_BGTOcc'], dropna=False)['counter'].transform('sum')
    
    # Dispersion measure
    frame['Share_byNounOcc'] = frame.Count_byNounOcc/frame.Count_byOcc
    frame['Share_byNoun'] = frame.Count_byNoun/frame.Count_Total
    frame['relative_frequency'] = frame.Share_byNounOcc/frame.Share_byNoun
    print("finished dispersion")

    #Sensitivity analysis over different parameters
    li = []


    words = pd.concat([
            frame.query("doc_BGTOcc == @data_entry and sim_data > @inputs.key_parameters.sim_data and relative_frequency > @rel_share").drop(
                ['doc_BGTOcc','doc_JobID','Count_byJob'],axis=1).drop_duplicates(subset = 'noun_chunk').reset_index(drop = True).assign(Type='data_entry'),
            frame.query("doc_BGTOcc == @data_science  and sim_data > @inputs.key_parameters.sim_data  and relative_frequency > @rel_share").drop(
                ['doc_BGTOcc','doc_JobID','Count_byJob'],axis=1).drop_duplicates(subset = 'noun_chunk').reset_index(drop = True).assign(Type='data_analytics'),
            frame.query("doc_BGTOcc == @database and sim_data > @inputs.key_parameters.sim_data and relative_frequency > @rel_share").drop(
                ['doc_BGTOcc','doc_JobID','Count_byJob'],axis=1).drop_duplicates(subset = 'noun_chunk').reset_index(drop = True).assign(Type = 'database')      
    ], axis=0, ignore_index=True)

    print(words.head())
    #Ensure categories in list are independent of each other (no list contains the same key word)
    print("writing to first file")
    max_list = words.groupby('noun_chunk')['Share_byNounOcc'].agg('max')
    word_fin = pd.merge(max_list, words, how = 'left', on = ['Share_byNounOcc', 'noun_chunk'])
    #word_fin.to_csv(os.path.join(inputs.paths.output, f'{country}_joined_list_{inputs.key_parameters.FNAME[0]}_{0.5}.csv'))
    word_fin.to_csv(f"hdfs://sdsh/em_sources/_SDD/DIT_VALUEOFDATA/step4Files/{year}/{country}_joined_list_{inputs.key_parameters.FNAME[0]}_{0.5}.csv")
    #word_fin.to_parquet(os.path.join(inputs.paths.output, f'{country}_joined_list_{inputs.key_parameters.FNAME[0]}_{0.55}.parquet'))

    word_fin = word_fin[['noun_chunk','Type']]


    #check
    #Merge data together and match the relevant noun chunks
    frame = pd.merge(frame,word_fin, how = 'left', on = ['noun_chunk'])
    key_functions.chunk_tagger(frame)

    #1. Aggregation: noun chunks on job level
    df_job = frame.groupby(by=['doc_JobID']).agg(
        {'sim_data':'mean',
            'data_entry':'sum',
            'database':'sum',
            'data_analytics':'sum',
            'noun_chunk' : 'count'}).reset_index()
    df_job.rename(columns = {'doc_JobID':'JobID'}, inplace = True)
    print(len(df_job))
    print(df_job.head(5))
    
    #FILTERING OUT: keep only jobs as data intense that have more than 3 references to data in the job advert
    print("writing to second file")
    col_names = ['data_entry', 'database', 'data_analytics']
    df_job['Count_DataTerms'] = df_job[col_names].sum(axis=1) 
    df_job['data_entry'] = df_job.apply(lambda x: 0 if x.Count_DataTerms < inputs.key_parameters.data_threshold else x.data_entry/x.Count_DataTerms, axis =1)
    df_job['database'] = df_job.apply(lambda x: 0 if x.Count_DataTerms < inputs.key_parameters.data_threshold  else x.database/x.Count_DataTerms, axis =1)
    df_job['data_analytics'] = df_job.apply(lambda x: 0 if x.Count_DataTerms < inputs.key_parameters.data_threshold else x.data_analytics/x.Count_DataTerms, axis =1)
    
    #Load structured BGT dataset
    if country == 'UK':
        bgt_reduc = pd.read_csv(inputs.paths.UK_bgt)
        bgt_reduc['BGTOcc'] = bgt_reduc['BGTOcc'].str.replace('[.-]','', regex=True)
        


    # if country == 'CAN':
    #     bgt_reduc = pd.read_csv(inputs.paths.CAN_bgt)
    #     bgt_reduc['BGTOcc'] = bgt_reduc['BGTOcc'].str.replace('[.-]','', regex=True)

    # if country == 'USA':
    #     bgt_reduc = pd.read_csv(inputs.paths.US_bgt)
    #     bgt_reduc = bgt_reduc.drop('JobId', axis=1)
    #     bgt_reduc['BGTOcc'] = bgt_reduc['BGTOcc'].str.replace('[.-]','', regex=True)

    #Merge on the BGTOcc structured dataset
    df_job['JobID']=df_job['JobID'].astype('int64')  ###comment out if using csv
    #print(df_job.head(4))
    merged = pd.merge(bgt_reduc, df_job, how = 'left', on = 'JobID')
    merged = merged[['BGTOcc', 'BGTOccName', 'JobID', sector, 'data_entry', 'database', 'data_analytics', 'Count_DataTerms']]

    print(merged.head(5))
    #merged.to_csv(os.path.join(inputs.paths.output,f'{country}_merged_{inputs.key_parameters.FNAME[0]}_{mode}.csv'))
    merged.to_csv(f"hdfs://sdsh/em_sources/_SDD/DIT_VALUEOFDATA/step4Files/{year}/{country}_merged_{inputs.key_parameters.FNAME[0]}_{mode}.csv")
    #merged.to_parquet(os.path.join(inputs.paths.output,f'{country}_merged_{inputs.key_parameters.FNAME[0]}_{mode}.parquet'))

    print("writing to third file")
    #2. Aggregation: jobs to occupation level
    df_occ = merged.groupby(by=['BGTOcc', 'BGTOccName']).agg(
        {'data_entry': 'mean',
            'database': 'mean',
            'data_analytics': 'mean',
            'JobID' : 'count'}).reset_index()
    
    col_names = ['data_entry', 'database', 'data_analytics']
    df_occ['Data_Intensity'] = df_occ[col_names].sum(axis=1) 

    df_occ['contrib_de'] = (df_occ.JobID * df_occ.data_entry)/sum(df_occ.JobID)
    dentry_occ_weighted = sum(df_occ.contrib_de)
    print(dentry_occ_weighted)

    df_occ['contrib_db'] = (df_occ.JobID * df_occ.database)/sum(df_occ.JobID)
    db_occ_weighted = sum(df_occ.contrib_db)
    print(db_occ_weighted)

    df_occ['contrib_da'] = (df_occ.JobID * df_occ.data_analytics)/sum(df_occ.JobID)
    danal_occ_weighted = sum(df_occ.contrib_da)
    print(danal_occ_weighted)


    #1. FINAL OUTPUT:  Ranking of occupations based on data-intense jobs
    #df_occ.to_csv(os.path.join(inputs.paths.output,f'{country}_occs_{inputs.key_parameters.FNAME[0]}_{mode}.csv'))
    df_occ.to_csv(f"hdfs://sdsh/em_sources/_SDD/DIT_VALUEOFDATA/step4Files/{year}/{country}_occs_{inputs.key_parameters.FNAME[0]}_{mode}.csv")
    #df_occ.to_parquet(os.path.join(inputs.paths.output,f'{country}_occs_{inputs.key_parameters.FNAME[0]}_{mode}.parquet'))
                
    #2. Aggregation: job to sector level 
    if country == 'UK':
        df_sec = merged.groupby(by=['SICSection']).agg(
            {'data_entry':'mean',
                'database':'mean',
                'data_analytics':'mean',
                'JobID' : 'count'}).reset_index()

    # if country == 'CAN':
    #     #Load concordance table for NAICS
    #     concord = pd.read_csv(inputs.paths.concordance)
    #     merged = key_functions.concord_CAN(merged, concord)
    #     df_sec = merged.groupby(by=['ISIC4']).agg(
    #         {'data_entry':'mean',
    #             'database':'mean',
    #             'data_analytics':'mean',
    #             'JobID' : 'count'}).reset_index()

    # if country == 'USA':
    #     #Load concordance table for NAICS
    #     concord = pd.read_csv(inputs.paths.concordance)
    #     merged = key_functions.concord_US(merged, concord)
    #     df_sec = merged.groupby(by=['ISIC4']).agg(
    #         {'data_entry':'mean',
    #             'database':'mean',
    #             'data_analytics':'mean',
    #             'JobID' : 'count'}).reset_index()

    col_names = ['data_entry', 'database', 'data_analytics']
    df_sec['Data_Intensity'] = df_sec[col_names].sum(axis=1) 

    #2. FINAL OUTPUT:  Ranking of occupations based on data-intense jobs
    #df_sec.to_csv(os.path.join(inputs.paths.output,f'{country}_sector_{inputs.key_parameters.FNAME[0]}_{mode}.csv'))
    df_sec.to_csv(f"hdfs://sdsh/em_sources/_SDD/DIT_VALUEOFDATA/step4Files/{year}/{country}_sector_{inputs.key_parameters.FNAME[0]}_{mode}.csv")
    #df_sec.to_parquet(os.path.join(inputs.paths.output,f'{country}_sector_{inputs.key_parameters.FNAME[0]}_{mode}.parquet'))

    #Merge on SUT to calculate outcome on all-economy level
    if country == 'UK':
        #Prepare df_sic for merge on UK SUT
        df_sec['SICSection'] = df_sec['SICSection'].str.replace(',','', regex=True)
        df_sec['SICSection'] = df_sec['SICSection'].str.replace(';','', regex=True)
        df_sec['SICSection'] = df_sec['SICSection'].str.replace('ACTIVITIES OF HOUSEHOLDS AS EMPLOYERS UNDIFFERENTIATED GOODS-AND SERVICES-PRODUCING ACTIVITIES OF HOUSEHOLDS FOR OWN USE',
                        'ACT OF HOUSEHOLDS', regex=True)
        #Load SUT data
        SUT = pd.read_csv(inputs.paths.UK_SUT)
        SUT, SUT_Full_Select_Frame = key_functions.processing_SUT(country, SUT, SUT,  year)
        #Merge sectors to the SUT
        result = pd.merge(df_sec, SUT, how = 'left', on = 'SICSection')
                    
    # if country == 'CAN':
    #     #Load SUT Table
    #     SUT = pd.read_csv(inputs.paths.CAN_SUT)
    #     SUT = key_functions.processing_SUT(country, SUT)
    #     #Merge sectors to the SUT
    #     result = pd.merge(df_sec, SUT, how = 'left', on = 'ISIC4')
    
    # if country == 'USA':
    #     #Load SUT Table
    #     SUT = pd.read_csv(inputs.paths.US_SUT)
    #     SUT = key_functions.processing_SUT(country, SUT)
    #     #Merge sectors to the SUT
    #     result = pd.merge(df_sec, SUT, how = 'left', on = 'ISIC4')

    #Calculating investment in data
    if country == 'UK':
        result['data_entry_invest'] = result['data_entry'] * result['COMP_EMP'] * inputs.key_parameters.UK_alpha
        result['database_invest'] = result['database'] * result['COMP_EMP'] * inputs.key_parameters.UK_alpha
        result['data_science_invest'] = result['data_analytics'] * result['COMP_EMP'] * inputs.key_parameters.UK_alpha
    
    # if country == 'CAN':
    #     result['data_entry_invest'] = result['data_entry'] * result['COMP_EMP'] * inputs.key_parameters.CAN_alpha
    #     result['database_invest'] = result['database'] * result['COMP_EMP'] * inputs.key_parameters.CAN_alpha
    #     result['data_science_invest'] = result['data_analytics'] * result['COMP_EMP'] * inputs.key_parameters.CAN_alpha

    # if country == 'USA':
    #     result['data_entry_invest'] = result['data_entry'] * result['COMP_EMP'] * inputs.key_parameters.USA_alpha
    #     result['database_invest'] = result['database'] * result['COMP_EMP'] * inputs.key_parameters.USA_alpha
    #     result['data_science_invest'] = result['data_analytics'] * result['COMP_EMP'] * inputs.key_parameters.USA_alpha

    
    col_names = ['data_entry_invest', 'database_invest', 'data_science_invest']
    result['total_data_invest']= result[col_names].sum(axis=1)
    result['data_GVA'] = result['total_data_invest']/result['GVA']
    result['data_entry_GVA'] = result['data_entry_invest']/result['GVA']
    result['database_GVA'] = result['database_invest']/result['GVA']
    result['data_science_GVA'] = result['data_science_invest']/result['GVA']

    #OUTPUT 3: Final absolut investment in data
    #result.to_csv(os.path.join(inputs.paths.output,f'{country}_result_{inputs.key_parameters.FNAME[0]}_{mode}.csv'))
    result.to_csv(f"hdfs://sdsh/em_sources/_SDD/DIT_VALUEOFDATA/step4Files/{year}/{country}_result_{inputs.key_parameters.FNAME[0]}_{mode}.csv")
    
    #Calculate the overall economy result
    total_data = result['total_data_invest'].sum()
    if country == 'UK':
        getData =  SUT_Full_Select_Frame[['Transaction', 'ACTIVITY', 'Measure', 'Year', 'Value']].query(f"Transaction == 'Gross value added' and ACTIVITY == 'VTOT' and Measure == 'Current prices'")
        #print(getData.head())
        total_gva = getData['Value'].values
        print(total_gva)
    # if country == 'CAN':
    #     total_gva = 2161924 # in CAD million (2019)
    # if country == 'USA':
    #     total_gva = 20343314 # in USD million (2020)
    data_GVA = total_data/total_gva
        
    #Create pandas dataframe for sensitivity analysis
    d = {'Data Threshold': inputs.key_parameters.data_threshold, 
            'sim data': inputs.key_parameters.sim_data, 
            'Rel share': rel_share,
            'total_data_invest' : total_data,
            'total_GVA': total_gva,
            'share_data_GVA': data_GVA}
    df = pd.DataFrame(d, index=[0])
    li.append(df)
    print(df.head())
    return df.to_csv(f"hdfs://sdsh/em_sources/_SDD/DIT_VALUEOFDATA/step4Files/{year}/{country}_final_result_{inputs.key_parameters.FNAME[0]}_{mode}.csv") 
    #df.to_csv(os.path.join(inputs.paths.output,f'{country}_final_result_{inputs.key_parameters.FNAME[0]}_{mode}.csv'))
#, df.to_parquet(os.path.join(inputs.paths.output,f'{country}_final_result_{inputs.key_parameters.FNAME}_{file_number}_{mode}.parquet'))

if __name__ == '__main__':
    job_analysis('UK', 'alpha_lower', inputs.key_parameters.UK_sector, inputs.key_parameters.UK_data_entry, inputs.key_parameters.UK_database, inputs.key_parameters.UK_data_science, inputs.key_parameters.UK_rel_share)
    #job_analysis('UK', 'alpha_upper', inputs.key_parameters.UK_sector, inputs.key_parameters.UK_data_entry, inputs.key_parameters.UK_database, inputs.key_parameters.UK_data_science, inputs.key_parameters.UK_rel_share)
    # job_analysis('CAN', 'alpha_upper', inputs.key_parameters.CAN_sector, inputs.key_parameters.CAN_data_entry, inputs.key_parameters.CAN_database, inputs.key_parameters.CAN_data_science, inputs.key_parameters.CAN_rel_share)
    # puts.key_parameters.US_sector, inputs.key_parameters.US_data_entry, inputs.key_parameters.US_database, inputs.key_parameters.US_data_science, inputs.key_parameters.US_rel_share)