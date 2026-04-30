import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns


##building dataframe
df = pd.DataFrame()


listYear = [2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020]
dataIntensityCollection = []
sicSectionDataCollection = []
yearForDFCol = []



##import all the files needed
for i in listYear:
    readDF = pd.read_csv(f'hdfs://sdsh/em_sources/_SDD/DIT_VALUEOFDATA/step4Files/{i}/UK_sector_d_alpha_upper.csv')

    for j in range(0, len(readDF)):
        dataIntensityCollection.append(readDF["Data_Intensity"][j])
        sicSectionDataCollection.append(readDF["SICSection"][j])
        yearForDFCol.append(i)


##adding to the df to make one big dataset
df["DataIntensity"]  = dataIntensityCollection
df["SicSection"]  = sicSectionDataCollection
df["Year"]  = yearForDFCol

df2 = pd.DataFrame(df.groupby(['Year', "SicSection"], as_index=False)['DataIntensity'].nlargest(5))

g = sns.FacetGrid(df2,col='Year')
g = g.map_dataframe(sns.barplot,"SicSection","DataIntensity",hue='SicSection',ci=None,palette = sns.color_palette("bright"))
for ax in g.axes.ravel():
    ax.legend()
plt.show()


