#Import libraries
import pandas as pd
import os, sys
import numpy as np
pd.set_option('display.max_columns', 40)
np.random.seed(42)
import glob


def chunk_tagger(df):
    '''Takes df and creates a dummy variable if the noun chunk
    is categorized as data entry, database or data analysis. It returns the df.'''
    list_t = ['data_entry', 'database', 'data_analytics']
    for i in list_t:
        df[i] = 0 
        df.loc[df['Type'] == i, i] = 1   
    return df

def processing_SUT(country, df, newDf, year):
    '''Function takes the country and the SUT table as input dataframe, processes it
    so it can be merged to BGT data and outputs the processes SUT table with relevant
    rows and columns.'''
    if country == 'UK':
        df = df[df.Year.eq(year)] 
    if country == 'CAN':
        df = df[df.Year.eq(2019)]
    if country == 'USA':
        df = df[df.Year.eq(2020)]
    
    newDf = df[['ACTIVITY', 'Transaction', 'Year', 'Value', 'Measure']].copy()
    df = df[df['ACTIVITY'].isin(['VA0', 'VB', 'VC', 'VD', 'VE', 
                                      'VF', 'VG', 'VH', 'VI', 'VJ', 
                                      'VK', 'VL', 'VM', 'VN', 'VO', 
                                      'VP', 'VQ', 'VR', 'VS', 'VT', 
                                      'VU', 'VW', 'VX', 'VY', 'VZ'])]
    df = df[['Activity', 'ACTIVITY', 'Transaction', 'Year', 'Value']]
    
    df = df.pivot_table(values='Value', 
                                index=['Activity', 'ACTIVITY', 'Year'],
                                columns='Transaction').reset_index()
    df = df.rename(columns = {'Gross value added': 'GVA', 
                              'Compensation of employees': 'COMP_EMP'})
    #Format the ACTIVITY column to merge on capital letters (A, B, C etc.)
    df['ACTIVITY'] = df['ACTIVITY'].apply(lambda x: str(x)[1:])
    df['ACTIVITY'] = df['ACTIVITY'].str.replace('0','', regex=True)
    #newDf['ACTIVITY'] = df['ACTIVITY'].copy()
    df.rename(columns={'ACTIVITY': 'ISIC4'}, inplace=True)
    
    if country == 'UK':
        #Format the Activity column to merge on ISIC names
        df['Activity'] = df['Activity'].str.upper()
        df['Activity'] = df['Activity'].str.replace(',','', regex=True)
        df['Activity'] = df['Activity'].str.replace(';','', regex=True)
        df['Activity'] = df['Activity'].str.replace('ACT. OF HH AS EMPLOYERS UNDIF. G&S-PRODUCING ACTIVITIES OF HH FOR OWN USE',
                        'ACT OF HOUSEHOLDS', regex=True)
        
        df.rename(columns={'Activity': 'SICSection'}, inplace=True)

        
    return df, newDf

def concord_CAN(df, concord):
    '''Function takes as input a dataframe (merged) and a concordance table (concord)
    and outputs a dataframe at job level with the ISIC4 classification'''
    #Prepare the merge to SUT tables based on ISIC4
    #Generate dict that contains the proportions of the concordance table
    d = {}
    for i in concord['NAICS2017_2d'].unique():
        d[i] = list(concord.loc[concord['NAICS2017_2d'] == i, 'ISIC4_1d'])

    def tag(x):
        '''Functions take the value contained in the cell and matches it to the value
        in the dictionary d'''
        return np.random.choice(d[x])  #random choice from the list belonging to key x
    
    ##PROBLEM MISSING VALUES
    df = df.dropna(subset=['NAICS'])
    #Take only 2d NAICS
    df['NAICS_2d'] = df['NAICS'].apply(lambda x: int(str(x)[0:2]))
    #Create the sector based tagging to transform to ISIC4
    df['ISIC4'] = df['NAICS_2d'].apply(tag)  #actually merging on ISIC4
    return df

def concord_US(df, concord):
    '''Function takes as input a dataframe (merged) and a concordance table (concord)
    and outputs a dataframe at job level with the ISIC4 classification'''
    #Prepare the merge to SUT tables based on ISIC4
    #Generate dict that contains the proportions of the concordance table
    d = {}
    for i in concord['NAICS2017_2d'].unique():
        d[i] = list(concord.loc[concord['NAICS2017_2d'] == i, 'ISIC4_1d'])

    def tag(x):
        '''Functions take the value contained in the cell and matches it to the value
        in the dictionary d'''
        return np.random.choice(d[x])  #random choice from the list belonging to key x
    
    ##PROBLEM MISSING VALUES
    df = df.dropna(subset=['Sector'])
    df = df.drop(df[df.Sector == 'na'].index)
    #Take only 2d NAICS
    df['NAICS_2d'] = df['Sector'].apply(lambda x: int(str(x)[0:2]))
    #Create the sector based tagging to transform to ISIC4
    df['ISIC4'] = df['NAICS_2d'].apply(tag)  #actually merging on ISIC4
    return df