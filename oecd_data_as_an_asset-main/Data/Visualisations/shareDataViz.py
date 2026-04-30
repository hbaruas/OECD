'''
To show the increase in data from the final results visualisation
'''

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


df = pd.DataFrame()
##import all the files needed
listYear = [2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020]
dataGVA = []

##get the required info which is the dataGVA from the path
for i in listYear:
    readDFFinalResult = pd.read_csv(f'hdfs://sdsh/em_sources/_SDD/DIT_VALUEOFDATA/step4Files/{i}/UK_final_result_d_alpha_lower.csv')
    dataGVA.append(readDFFinalResult["share_data_GVA"])

df["Year"] = listYear
df["increaseInDataShare"] = np.asarray(np.array(dataGVA))
print(df.head())



plt.plot(df['Year'].astype(str), df['increaseInDataShare'], color='red')
plt.title('Data Share Increase Lower Bound (2012-2020)')
plt.xlabel('Year')
plt.ylabel('Data Share')
plt.show()


