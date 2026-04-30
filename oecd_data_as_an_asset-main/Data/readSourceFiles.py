####jobID, BGTOcc, BGTOCCName, SICSection (could be names differently), please check, but basically the sectoral ID
import pandas as pd
import re

columnsName =["0", '"JobID"', '"JobDate"', '"CleanJobTitle"', '"Language"', '"CanonCountry"', '"Nation"', '"Region"', '"TTWA"', '"CanonCounty"', '"CanonCity"', '"Latitude"', '"Longitude"', '"CanonEmployer"', '"InternshipFlag"', '"MaxDegreeLevel"', '"CanonMinimumDegree"', '"MinDegreeLevel"', '"CanonJobHours"', '"CanonJobType"', '"MaxExperience"', '"MinExperience"', '"MaxAnnualSalary"', '"MinAnnualSalary"', '"MaxHourlySalary"', '"MinHourlySalary"', '"WorkFromHome"', '"UKSOCCode"', '"UKSOCUnitGroup"', '"UKSOCMinorGroup"', '"UKSOCSubMajorGroup"', '"UKSOCMajorGroup"', '"BGTOcc"', '"BGTOccName"', '"BGTOccGroupName"', '"BGTCareerAreaName"', '"SICCode"', '"SICClass"', '"SICGroup"', '"SICDivision"', '"SICSection"', '"StockTicker"', '"LocalAuthorityDistrict"', '"LocalEnterprisePartnership"', '"PreferredNQFLevels"', '"RequiredNQFLevels"']

##2013 has 7 characters for the job id
year = 2020
data = []
lines = []

with open(f"\\\\oecdmain\\em_sources\\NOVA\BGT\\UK\\UK_Main_{year}.txt") as f:
    for x in f:
        removedQuoteString = x.strip()
        lines.append(removedQuoteString)
    data=[re.split(r'\t+', i) for i in lines]

#df = pd.DataFrame()
df = pd.DataFrame(data)
df2 = df.applymap(lambda x: x.replace('"', '') if (isinstance(x, str)) else x)
df2.columns = df2.iloc[0]
#print(df2.columns)
df2.drop(index=0, inplace = True)##drops the first row im the df as it contrains the column names
print(df2.head())
df2.to_csv(f"hdfs://sdsh/em_sources/_SDD/DIT_VALUEOFDATA/source_data/full_source_data/UK/UkMain{year}.csv", index = False) 

##reduced version of only required headings
subsetDF = df2[['JobID', 'CanonEmployer', 'SICSection', 'BGTOcc', 'BGTOccName']]
subsetDF.to_csv(f"hdfs://sdsh/em_sources/_SDD/DIT_VALUEOFDATA/source_data/reduced_source_data/UK/SubsetData{year}.csv", index = False)
f.close()