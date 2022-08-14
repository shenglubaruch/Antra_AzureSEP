# -*- coding: utf-8 -*-
"""
Created on Thu Aug 11 21:36:11 2022

@author: ShengLu
"""
import pandas as pd

df1 = pd.read_table('pythonAssginment/people/people_1.txt', sep = '\t')
df2 = pd.read_table('pythonAssginment/people/people_2.txt', sep = '\t')
list1 = [df1, df2]
data = pd.concat(list1).head(3000)

for i, e in enumerate(zip(data['FirstName'], data['LastName'], data['Email'], data['Phone'], data['Address'])):
        data['FirstName'][i] = e[0].strip().capitalize()
        data['LastName'][i] = e[1].strip().capitalize()
        data['Email'][i] = e[2].strip().capitalize()
        data['Phone'][i] = e[3].strip().replace('-','')
        data['Address'][i] = e[4].strip().replace('No.','').replace('#','')

data = data.drop_duplicates()
print(len(data))
print(data.head(1000).sort_values(by = ['FirstName', 'LastName']))

while i < 1000000:
    print(i)
    i += 1