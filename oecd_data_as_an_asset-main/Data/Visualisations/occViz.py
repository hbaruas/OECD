import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns


##building dataframe
df = pd.DataFrame()


listYear = [2012]
OccNameList = []
yearForDFCol = []
dataIntensityCollection = []

##import all the files needed
for i in listYear:
    readDF = pd.read_csv(f'hdfs://sdsh/em_sources/_SDD/DIT_VALUEOFDATA/step4Files/{i}/UK_occs_d_alpha_upper.csv')
    OccNameList.append(np.asarray(np.array(readDF["BGTOccName"])))
    #OccNameList.append(readDF["BGTOccName"])
    dataIntensityCollection.append(readDF["Data_Intensity"])
    for j in range(0, len(readDF)):
        yearForDFCol.append(i)
        dataIntensityCollection.append(readDF["Data_Intensity"][j])
        OccNameList.append(readDF["BGTOccName"][j])
        



df["OccupationName"]  = OccNameList
df["DataIntensity"]  = dataIntensityCollection
df["Year"]  = yearForDFCol
print(df.head())


# df_sorted = df.sort_values(by = 'DataIntensity', ascending=True)
# print(df_sorted.head())

# frame2 = df.groupby(['DataIntensity', 'Year'], dropna = False)['DataIntensity'].nlargest(20) 
# print(frame2.head())

# g = sns.FacetGrid(melted, col="variable",hue="segment",palette="Set3")
# g.map(sns.barplot,'interval','value')