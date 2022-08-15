# -*- coding: utf-8 -*-
"""
Created on Thu Aug 11 21:36:11 2022

@author: ShengLu
"""
import pandas as pd

df1 = pd.read_table('pythonAssginment/people/people_1.txt', sep = '\t')
df2 = pd.read_table('pythonAssginment/people/people_2.txt', sep = '\t')
list1 = [df1, df2]
data = pd.concat(list1)
print('The length of the data before cleaning: ', len(data))

data['FirstName'] = data['FirstName'].apply(lambda x:x.strip().capitalize())
data['LastName'] = data['LastName'].apply(lambda x:x.strip().capitalize())
data['Email'] = data['Email'].apply(lambda x:x.strip().capitalize())
data['Phone'] = data['Phone'].apply(lambda x:x.strip().replace('-',''))
data['Address'] = data['Address'].apply(lambda x:x.strip().replace('No.','').replace('#',''))

data = data.drop_duplicates()
print('The length of the data after cleaning: ', len(data))
print(data.sort_values(by = ['FirstName', 'LastName']))


