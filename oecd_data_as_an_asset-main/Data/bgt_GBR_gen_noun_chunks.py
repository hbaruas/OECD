#Import libraries for data wrangling, manipulation and data processing
import pandas as pd
import sys
import spacy
from spacy.tokens import Doc
from pyarrow import fs
import pyarrow.parquet as pq
import numpy as np
import glob
from subprocess import Popen, PIPE

import os
#spacy.prefer_gpu() #cuda

# Initialize the NLP spacy pipeline
SPACY_MODEL = None
def get_spacy_model():
    global SPACY_MODEL
    if not SPACY_MODEL:
       #nlp2 = spacy.load('en_core_web_lg', exclude=["lemmatizer", "ner"])
       nlp2 = spacy.load('en_core_web_md', exclude=["lemmatizer", "ner"]) ###lg change - virtual enviroment  first c then v(requirement file)
       SPACY_MODEL = nlp2
    return SPACY_MODEL

BGTjobs = []
docs = []
j=0

 
# Input file directory
year = 2019
path = Popen(f'hadoop fs -ls -C hdfs://sdsh/em_sources/_SDD/DIT_VALUEOFDATA/raw_data/UK/{year}', shell=True, stdout=PIPE, stderr=PIPE)
listHadoopFiles, std_err = path.communicate()


##contains all the list of files in the input directory
# ###the output of listHadoopFiles was <class 'bytes'>
finalFileList = listHadoopFiles.decode("utf-8").splitlines()
#print(finalFileList)

##gets the random number assigned to the file with the bat file
file_number = int(sys.argv[1])


##this is accessing the number given to the file in the list of parquet file read from the directory
file_name = finalFileList[file_number]
print('The file to be processed is: ', file_name)
 
df = pd.read_parquet(file_name)


print('The number assigned to it is: ', file_number)
print("has finished reading")
df.dropna(subset = ['JobText'], inplace=True) ###drops rows that has the none type

##removes all the numbers in the strings
def removeNumbersInString(stringText):
   newText = ''.join(filter(lambda textCheck: not textCheck.isdigit(), stringText))
   return newText

df['JobText'] = df['JobText'].apply(lambda x: removeNumbersInString(x))


##reads the columns needed
newDF = df['JobText']  
bgtVal = df["BGTOcc"].tolist()
JobID = df["JobID"].tolist()


nlp = get_spacy_model()

###preprocess some nlp processing cleaning/steps on the data text
docs = nlp.pipe(newDF, as_tuples=False, n_process=1)
print("finished nlp pipeline")


#Extracts the noun_chunks and calculates the cosine similarity of each job 
def chunky(doc):
    global j
    
    # Define the target token
    target_token_0 = nlp('data') 
    #deep learning

    chunks = doc.noun_chunks
    temp_output = []
    #l = [i for i in chunks if i.has.vector]
    for i in chunks:
        if i.has_vector:
            temp_output.append({'doc_BGTOcc': bgtVal[j],
                                'doc_JobID': JobID[j],
                                'noun_chunk': str(i),
                                'sim_data': i.similarity(target_token_0)})
    print("chunky reading okay\n\n")
    j+=1
    print(j)
    return temp_output


chunkies = [chunky(i) for i in docs]
flat_chunkies = []
for sublist in chunkies:
    for item in sublist:
        flat_chunkies.append(item)

# Creates a dataframe containing the relevant variables for analysis of the file
flat_chunkies = pd.DataFrame(flat_chunkies)
print(flat_chunkies.head(5))
#flat_chunkies.to_parquet(f"hdfs://sdsh/em_sources/_SDD/DIT_VALUEOFDATA/input_data/UK/2020GPU/data{file_number}.parquet", index=False)
flat_chunkies.to_parquet(f"hdfs://sdsh/em_sources/_SDD/DIT_VALUEOFDATA/input_data/UK/{year}/dataForWeek{file_number}.parquet", index=False)
