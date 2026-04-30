from subprocess import Popen, PIPE
import pandas as pd 
import US_UK_CAN_jobs

class paths:
    #Define source paths
    year = US_UK_CAN_jobs.year

    #UK_input_frame = 'hdfs://sdsh/em_sources/_SDD/DIT_VALUEOFDATA/reduced_data/UK/2020/fullInputReducedFinal.csv'
    UK_input_frame = f'hdfs://sdsh/em_sources/_SDD/DIT_VALUEOFDATA/reduced_data/UK/{year}/reducedFile.csv'

    UK_bgt = f'hdfs://sdsh/em_sources/_SDD/DIT_VALUEOFDATA/source_data/reduced_source_data/UK/SubsetData{year}.csv'
    #UK_bgt = 'V:\\DIT_VALUEOFDATA\\sources\\key_inputs\\UK\\tblUK_reduc.csv' #structural LC dataset (reduced format = less columns)
    UK_SUT = 'V:\\DIT_VALUEOFDATA\\sources\\sources\\SUT_UK_OECD_NEW.csv' 

    CAN_input_frame = 'V:\\DIT_VALUEOFDATA\\sources\\key_inputs\\CAN\\full_input_reduced_final.csv' 
    CAN_SUT = 'V:\\DIT_VALUEOFDATA\\sources\\sources\\SUT_CAN_OECD_230425.csv'
    CAN_bgt = 'V:\\DIT_VALUEOFDATA\\sources\\key_inputs\\CAN\\tblCAN_reduc.csv'

    US_path = 'V:\\DIT_VALUEOFDATA\\sources\\key_inputs\\USA\\reduced_input\\'
    US_bgt = 'V:\\DIT_VALUEOFDATA\\sources\\key_inputs\\USA\\tblUS_reduc.csv'
    US_SUT = 'V:\\DIT_VALUEOFDATA\\sources\\sources\\SUT_USA_OECD_221206.csv'

    concordance = 'V:\\DIT_VALUEOFDATA\\sources\\key_inputs\\CAN\\NAICS_ISIC4_concordance.csv'
    output = f'hdfs://sdsh/em_sources/_SDD/DIT_VALUEOFDATA/step4Files/{year}'



class key_parameters:
    #Define key parameters
    UK_sector = 'SICSection'
    CAN_sector = 'NAICS'
    US_sector = 'Sector'

    UK_data_entry = '43902100' #BGTOcc IDs (occupation IDs) in this case for data entry clerk
    UK_database = '15114100'
    UK_data_science = '15111191'

    CAN_data_entry = 43902100.0
    CAN_database = 15114100.0
    CAN_data_science = 15111191.0

    US_data_entry = 43902100.0
    US_database = 15114100.0
    US_data_science = 15111191.0
    

    
    sim_data = 0.5
    data_threshold = 3
    #UK_rel_share = [10] * paths.lenOfDf #originally 10 
    UK_rel_share = 10.0
    CAN_rel_share = [12.01]
    US_rel_share = [12.37]


    alpha = 1.53 #mark-up (constant as in STATCAN) #TODO: run through upper bound alphas defined below
    UK_alpha = 1.53 #3.33 #1.53 #lower bound #3.33 #(upper bound)
    # UK_alpha = 2.9 (average of three sectors)
    CAN_alpha = 1.53 #3.33 (upper bound)
    # CAN_alpha = 3.08 (average of three sectors)
    USA_alpha = 3.05
    # USA_alpha = 3.23 (average of three sectors)

    FNAME = f'dt{data_threshold}_sim{sim_data}'