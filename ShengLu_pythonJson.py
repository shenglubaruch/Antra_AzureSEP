# -*- coding: utf-8 -*-
"""
Created on Sat Aug 13 22:49:17 2022

@author: luxsh
"""

import pandas as pd

df = pd.read_json('pythonAssginment/movie.json')
s = int(len(df) / 8)
p = 0

for i in range(7):
    temp = df[p:p + s]
    temp.to_json('pythonAssginment/moive'+str(i+1)+'.json')
    p += s
df[p:].to_json('pythonAssginment/moive8.json')
